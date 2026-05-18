package stages

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	providers "github.com/madmike/go-ai-providers/core"
	"github.com/madmike/go-infra/telemetry"
	"github.com/madmike/go-pipeline/core"
)

// LLMStageConfig holds LLM stage configuration
type LLMStageConfig struct {
	Provider            providers.LLMProvider
	Model               string
	Temperature         *float64
	MaxTokens           *int
	SystemPrompt        string
	Context             string // RAG context
	ConversationHistory []providers.Message
	Logger              telemetry.Logger
}

// LLMStage represents an LLM processing stage
type LLMStage struct {
	config LLMStageConfig
}

// NewLLMStage creates a new LLM stage
func NewLLMStage(config LLMStageConfig) *LLMStage {
	if config.Logger == nil {
		config.Logger = &telemetry.NoOpLogger{}
	}
	return &LLMStage{
		config: config,
	}
}

// Name returns the stage name
func (s *LLMStage) Name() string {
	return "llm"
}

// InputTypes returns the event types this stage accepts
func (s *LLMStage) InputTypes() []core.EventType {
	return []core.EventType{core.EventTypeLLM, core.EventTypeSTT}
}

// OutputTypes returns the event types this stage produces
func (s *LLMStage) OutputTypes() []core.EventType {
	return []core.EventType{core.EventTypeLLM, core.EventTypeStatus, core.EventTypeDone}
}

// Process implements the Stage interface
// It reads text from the input channel and streams LLM responses to the output channel
func (s *LLMStage) Process(ctx context.Context, input <-chan core.Event, output chan<- core.Event) error {
	logger := s.config.Logger.WithModule(s.Name())

	logger.Info("LLMStage started processing")

	// Collect all input text
	var fullText string
	eventCount := 0
	for event := range input {
		eventCount++
		switch e := event.(type) {
		case core.LLMEvent:
			// User input may come in Content (full message) or Delta (streamed chunk).
			text := e.Content
			if text == "" {
				text = e.Delta
			}
			fullText += text
			logger.Debug("Received text input message", telemetry.String("text", text))
		case core.STTEvent:
			fullText += e.Text
			logger.Debug("Received STT input message", telemetry.String("text", e.Text))
		case core.ErrorEvent:
			// Log error from upstream but don't propagate - continue processing with what we have
			logger.Warn("Received error from upstream", telemetry.Err(e.Error))
		case core.DoneEvent:
			logger.Info("Received DoneEvent from upstream, finishing input collection")
			// Break the loop to start processing immediately
			goto EndCollection
		}
	}

EndCollection:

	logger.Info("Finished collecting input", telemetry.Int("event_count", eventCount), telemetry.String("full_text", fullText))

	// Check if input is empty or whitespace-only
	trimmedText := strings.TrimSpace(fullText)
	if trimmedText == "" {
		logger.Info("Received empty or whitespace-only input")
		// Emit DoneEvent to signal completion
		output <- core.DoneEvent{}
		return nil
	}

	// Emit thinking status only after receiving the complete input
	// Use a buffered send to ensure it's queued before we start streaming
	select {
	case <-ctx.Done():
		return ctx.Err()
	case output <- core.StatusEvent{
		Status:  core.StatusThinking,
		Target:  core.StatusTargetBot,
		Message: "Thinking...",
	}:
	}

	// Build messages for the LLM
	// Start with system prompt if provided
	messages := []providers.Message{}

	// Add system prompt first (always at index 0)
	if s.config.SystemPrompt != "" {
		messages = append(messages, providers.Message{
			Role:    "system",
			Content: s.config.SystemPrompt,
		})
	}

	// Add conversation history if provided
	if len(s.config.ConversationHistory) > 0 {
		messages = append(messages, s.config.ConversationHistory...)
	}

	// Add context if provided (RAG context)
	if s.config.Context != "" {
		messages = append(messages, providers.Message{
			Role:    "system",
			Content: fmt.Sprintf("Context:\n%s", s.config.Context),
		})
	}

	// Add current user message
	messages = append(messages, providers.Message{
		Role:    "user",
		Content: trimmedText,
	})

	// Create chat request
	req := providers.ChatRequest{
		Model:       s.config.Model,
		Messages:    messages,
		Temperature: s.config.Temperature,
		MaxTokens:   s.config.MaxTokens,
	}
	if reqJSON, err := json.Marshal(req); err == nil {
		logger.Trace("LLM full request",
			telemetry.String("provider", s.config.Provider.Name()),
			telemetry.String("request", string(reqJSON)))
	} else {
		logger.Warn("Failed to marshal LLM request for trace logging", telemetry.Err(err))
	}

	// Stream chat completion
	stream, err := s.config.Provider.StreamChatCompletion(ctx, req)
	if err != nil {
		logger.Error("Failed to start LLM stream", telemetry.Err(err))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case output <- core.ErrorEvent{
			Error:     fmt.Errorf("failed to start LLM stream: %w", err),
			Retryable: true,
		}:
		}
		// Send done event and return without error to allow pipeline to continue
		output <- core.DoneEvent{
			FullText:   "",
			TokensUsed: 0,
		}
		return nil
	}
	defer stream.Close()

	// Process stream and emit events
	var fullResponse string
	var tokensUsed int
	chunkCount := 0

	for {
		chunk, err := stream.Receive(ctx)
		if err != nil {
			logger.Error("Error receiving LLM chunk", telemetry.Err(err), telemetry.Int("chunks_received", chunkCount))
			select {
			case <-ctx.Done():
				return ctx.Err()
			case output <- core.ErrorEvent{
				Error:     fmt.Errorf("error receiving LLM chunk: %w", err),
				Retryable: false,
			}:
			}
			// Send done event with partial response and return without error to allow pipeline to continue
			output <- core.DoneEvent{
				FullText:   fullResponse,
				TokensUsed: tokensUsed,
			}
			return nil
		}

		if chunk == nil || chunk.Done {
			logger.Info("LLM stream finished", telemetry.Int("chunks_received", chunkCount), telemetry.String("full_response", fullResponse))
			logger.Trace("LLM full response",
				telemetry.String("provider", s.config.Provider.Name()),
				telemetry.String("response", fullResponse))
			break
		}

		// Skip only completely empty chunks, preserve spaces
		if chunk.Content == "" {
			continue
		}

		chunkCount++
		fullResponse += chunk.Content
		logger.Trace("Received LLM chunk", telemetry.String("content", chunk.Content), telemetry.Int("chunk_number", chunkCount))

		// Emit LLM event for each non-empty chunk from the provider
		select {
		case <-ctx.Done():
			return ctx.Err()
		case output <- core.LLMEvent{
			Delta:   chunk.Content,
			Content: fullResponse,
		}:
		}
	}

	// Emit done event with final response
	logger.Info("Emitting done event", telemetry.String("full_response", fullResponse), telemetry.Int("tokens_used", tokensUsed))
	output <- core.DoneEvent{
		FullText:   fullResponse,
		TokensUsed: tokensUsed,
	}

	return nil
}
