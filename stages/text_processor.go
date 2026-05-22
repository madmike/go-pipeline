package stages

import (
	"context"
	"regexp"
	"strings"
	"unicode"

	"github.com/madmike/go-infra/telemetry"
	"github.com/madmike/go-pipeline/core"
)

// TextProcessorStageConfig holds text processor configuration
type TextProcessorStageConfig struct {
	// StripCodeBlocks removes code blocks from text
	StripCodeBlocks bool
	// StripMarkdown removes markdown formatting
	StripMarkdown bool
	// ExpandAbbreviations expands common abbreviations
	ExpandAbbreviations bool
	// ExpandSymbols expands symbols like & to "and"
	ExpandSymbols bool
	Logger        telemetry.Logger
}

// TextProcessorStage sanitizes and buffers text for TTS consumption
// It sits between LLM and TTS, handling:
// - Markdown stripping (**, ##, ```)
// - Code block removal
// - Symbol/abbreviation expansion
// - Sentence boundary detection
// - Buffering into semantic chunks
type TextProcessorStage struct {
	config TextProcessorStageConfig
}

// NewTextProcessorStage creates a new text processor stage
func NewTextProcessorStage(config TextProcessorStageConfig) *TextProcessorStage {
	return &TextProcessorStage{
		config: config,
	}
}

// Name returns the stage name
func (s *TextProcessorStage) Name() string {
	return "text_processor"
}

// InputTypes returns the event types this stage accepts
func (s *TextProcessorStage) InputTypes() []core.EventType {
	return []core.EventType{core.EventTypeLLM}
}

// OutputTypes returns the event types this stage produces
func (s *TextProcessorStage) OutputTypes() []core.EventType {
	return []core.EventType{core.EventTypeLLM, core.EventTypeStatus, core.EventTypeDone}
}

// Process implements the Stage interface
func (s *TextProcessorStage) Process(ctx context.Context, input <-chan core.Event, output chan<- core.Event) error {
	logger := s.config.Logger
	if logger == nil {
		logger = &telemetry.NoOpLogger{}
	}
	logger = logger.WithModule(s.Name())

	var buffer strings.Builder

	// Regex patterns for text processing
	codeBlockRegex := regexp.MustCompile("(?s)```[^`]*```\n?")
	inlineCodeRegex := regexp.MustCompile("`[^`]+`")
	markdownBoldRegex := regexp.MustCompile(`\*\*([^*]+)\*\*`)
	markdownItalicRegex := regexp.MustCompile(`\*([^*]+)\*`)
	markdownHeaderRegex := regexp.MustCompile(`^#+\s+`)
	markdownLinkRegex := regexp.MustCompile(`\[([^\]]+)\]\([^\)]+\)`)
	htmlTagRegex := regexp.MustCompile(`<[^>]+>`)

	// Sentence boundary detection
	sentenceBoundaryRegex := regexp.MustCompile(`[.!?\n]`)

	for event := range input {
		// Forward DoneEvent immediately
		if doneEvent, ok := event.(core.DoneEvent); ok {
			logger.Info("text processor received DoneEvent, forwarding to TTS")

			// Flush any remaining buffer first
			if buffer.Len() > 0 {
				normalizedText := s.normalizeSentence(buffer.String())
				finalText := strings.TrimSpace(normalizedText)
				if finalText != "" {
					logger.Debug("emitting flushed text before DoneEvent", telemetry.String("text", finalText))
					output <- core.LLMEvent{Delta: finalText}
				}
			}

			// Forward DoneEvent to TTS
			logger.Debug("text processor forwarding DoneEvent to TTS now")
			output <- doneEvent
			return nil
		}

		// Pass through non-LLM events
		if statusEvent, ok := event.(core.StatusEvent); ok {
			output <- statusEvent
			continue
		}

		if llmEvent, ok := event.(core.LLMEvent); ok {
			delta := llmEvent.Delta

			// Clean the token immediately
			cleanedToken := s.cleanText(delta, codeBlockRegex, inlineCodeRegex, markdownBoldRegex, markdownItalicRegex, markdownHeaderRegex, markdownLinkRegex, htmlTagRegex)

			// Skip only if the cleaned token is completely empty (not just whitespace)
			if cleanedToken == "" {
				continue
			}

			// Accumulate into buffer
			buffer.WriteString(cleanedToken)

			// Check if buffer contains a sentence boundary
			currentText := buffer.String()

			// Look for sentence boundaries in the current buffer
			if s.isSentenceComplete(currentText, sentenceBoundaryRegex) {
				// Normalize and send the complete sentence
				normalizedText := s.normalizeSentence(currentText)
				finalSentence := strings.TrimRight(normalizedText, " \t\n\r")

				if strings.TrimSpace(finalSentence) != "" {
					logger.Debug("Emitting processed sentence", telemetry.String("text", finalSentence))
					select {
					case <-ctx.Done():
						return ctx.Err()
					case output <- core.LLMEvent{
						Delta: finalSentence,
					}:
					}
				}

				buffer.Reset()
			}
		}
	}

	// If we get here, input closed without DoneEvent - flush buffer
	if buffer.Len() > 0 {
		normalizedText := s.normalizeSentence(buffer.String())
		finalText := strings.TrimSpace(normalizedText)
		if finalText != "" {
			logger.Debug("emitting flushed text on input close", telemetry.String("text", finalText))
			output <- core.LLMEvent{Delta: finalText}
		}
	}

	return nil
}

// cleanText removes markdown, code blocks, and HTML from text
func (s *TextProcessorStage) cleanText(
	text string,
	codeBlockRegex, inlineCodeRegex, markdownBoldRegex, markdownItalicRegex,
	markdownHeaderRegex, markdownLinkRegex, htmlTagRegex *regexp.Regexp,
) string {
	result := text

	// Remove code blocks if configured
	if s.config.StripCodeBlocks {
		result = codeBlockRegex.ReplaceAllString(result, "")
		result = inlineCodeRegex.ReplaceAllString(result, "")
	}

	// Remove markdown if configured
	if s.config.StripMarkdown {
		// Extract link text, discard URL
		result = markdownLinkRegex.ReplaceAllString(result, "$1")
		// Remove bold/italic markers - try to match patterns first
		result = markdownBoldRegex.ReplaceAllString(result, "$1")
		result = markdownItalicRegex.ReplaceAllString(result, "$1")
		// Remove headers
		result = markdownHeaderRegex.ReplaceAllString(result, "")
		// Remove any remaining asterisks (from incomplete markdown or edge cases)
		// This catches cases where markdown wasn't fully matched due to streaming
		result = strings.ReplaceAll(result, "*", "")
	}

	// Remove HTML tags
	result = htmlTagRegex.ReplaceAllString(result, "")

	return result
}

// normalizeSentence expands abbreviations and symbols
func (s *TextProcessorStage) normalizeSentence(text string) string {
	result := text

	if s.config.ExpandSymbols {
		result = strings.ReplaceAll(result, "&", "and")
		result = strings.ReplaceAll(result, "@", "at")
		result = strings.ReplaceAll(result, "#", "number")
	}

	if s.config.ExpandAbbreviations {
		result = s.expandAbbreviations(result)
	}

	return result
}

// expandAbbreviations expands common abbreviations for TTS
func (s *TextProcessorStage) expandAbbreviations(text string) string {
	// Common abbreviations that should be expanded for TTS
	// Order matters - longer abbreviations should be replaced first to avoid partial matches
	abbreviations := []struct {
		abbr      string
		expansion string
	}{
		{"U.S.", "United States"},
		{"U.K.", "United Kingdom"},
		{"e.g.", "for example"},
		{"i.e.", "that is"},
		{"Dr.", "Doctor"},
		{"Mr.", "Mister"},
		{"Mrs.", "Misses"},
		{"Ms.", "Miss"},
		{"Prof.", "Professor"},
		{"St.", "Street"},
		{"Ave.", "Avenue"},
		{"Blvd.", "Boulevard"},
		{"etc.", "et cetera"},
		{"vs.", "versus"},
		{"No.", "Number"},
		{"Inc.", "Incorporated"},
		{"Ltd.", "Limited"},
		{"Co.", "Company"},
		{"Corp.", "Corporation"},
		{"Jan.", "January"},
		{"Feb.", "February"},
		{"Mar.", "March"},
		{"Apr.", "April"},
		{"Aug.", "August"},
		{"Sept.", "September"},
		{"Oct.", "October"},
		{"Nov.", "November"},
		{"Dec.", "December"},
		{"Mon.", "Monday"},
		{"Tue.", "Tuesday"},
		{"Wed.", "Wednesday"},
		{"Thu.", "Thursday"},
		{"Fri.", "Friday"},
		{"Sat.", "Saturday"},
		{"Sun.", "Sunday"},
	}

	result := text
	for _, pair := range abbreviations {
		// Simple string replacement - abbreviations are typically followed by space or end of string
		result = strings.ReplaceAll(result, pair.abbr, pair.expansion)
	}

	return result
}

// isSentenceComplete checks if text ends with a sentence boundary
func (s *TextProcessorStage) isSentenceComplete(text string, boundaryRegex *regexp.Regexp) bool {
	if len(text) == 0 {
		return false
	}

	// Trim trailing whitespace to check the actual last character
	trimmed := strings.TrimRightFunc(text, unicode.IsSpace)
	if len(trimmed) == 0 {
		return false
	}

	lastChar := rune(trimmed[len(trimmed)-1])

	// Check for sentence-ending punctuation
	if lastChar != '.' && lastChar != '?' && lastChar != '!' && lastChar != '\n' {
		return false
	}

	// Avoid false positives on abbreviations - only check the LAST period
	if lastChar == '.' && s.endsWithAbbreviation(trimmed) {
		return false
	}

	return true
}

// endsWithAbbreviation checks if text ends with a known abbreviation (not just contains one)
func (s *TextProcessorStage) endsWithAbbreviation(text string) bool {
	commonAbbreviations := []string{
		"Dr.", "Mr.", "Mrs.", "Ms.", "Prof.", "St.", "Ave.", "Blvd.",
		"etc.", "e.g.", "i.e.", "vs.", "No.", "Inc.", "Ltd.", "Co.", "Corp.",
		"U.S.", "U.K.", "Jan.", "Feb.", "Mar.", "Apr.", "Aug.", "Sept.", "Oct.", "Nov.", "Dec.",
		"Mon.", "Tue.", "Wed.", "Thu.", "Fri.", "Sat.", "Sun.",
	}

	for _, abbr := range commonAbbreviations {
		if strings.HasSuffix(text, abbr) {
			return true
		}
	}

	return false
}
