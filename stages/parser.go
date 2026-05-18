package stages

import (
	"context"
	"strings"
	"sync"

	"github.com/madmike/go-infra/telemetry"
	"github.com/madmike/go-pipeline/core"
)

type Parser interface {
	Parse(ctx context.Context, body []byte, url string) (string, string, map[string]any, error)
	// Returns: content, title, metadata, error
}

type ParserStageConfig struct {
	Parser      Parser
	Logger      telemetry.Logger
	WorkerCount int // Number of concurrent LLM processing workers
}

type ParserStage struct {
	config ParserStageConfig
}

func NewParserStage(config ParserStageConfig) *ParserStage {
	return &ParserStage{
		config: config,
	}
}

func (s *ParserStage) Name() string {
	return "ParserStage"
}

func (s *ParserStage) InputTypes() []core.EventType {
	return []core.EventType{core.EventTypeDocument}
}

func (s *ParserStage) OutputTypes() []core.EventType {
	return []core.EventType{core.EventTypeDocument}
}

func (s *ParserStage) Process(ctx context.Context, input <-chan core.Event, output chan<- core.Event) error {
	logger := s.config.Logger.WithModule("ParserStage")

	// Worker pool for parallel LLM processing
	workerCount := s.config.WorkerCount
	if workerCount <= 0 {
		workerCount = 5 // Default if not configured
	}
	var wg sync.WaitGroup

	// Create a semaphore to limit concurrent workers
	sem := make(chan struct{}, workerCount)

	logger.Debug("ParserStage starting", telemetry.Int("worker_count", workerCount))

	for event := range input {
		docEvent, ok := event.(core.DocumentEvent)
		if !ok {
			// Pass through non-document events immediately
			output <- event
			continue
		}

		logger.Debug("ParserStage received event", telemetry.String("url", docEvent.URL))

		// Launch worker goroutine
		wg.Add(1)
		sem <- struct{}{} // Acquire semaphore slot

		go func(doc core.DocumentEvent) {
			defer wg.Done()
			defer func() { <-sem }() // Release semaphore slot

			logger.Debug("Processing document", telemetry.String("url", doc.URL))

			// If already has error or skipped, pass through
			if doc.Error != nil || doc.Metadata["skipped"] == true {
				output <- doc
				return
			}

			content, title, metadata, err := s.config.Parser.Parse(ctx, []byte(doc.Content), doc.URL)
			if err != nil {
				if strings.Contains(err.Error(), "skip") {
					logger.Debug("Skipping document", telemetry.String("url", doc.URL))
					doc.Metadata["skipped"] = true
					output <- doc
					return
				}
				logger.Error("Failed to parse document", telemetry.Err(err))
				doc.Error = err
				output <- doc
				return
			}

			// Update document event
			doc.Content = content
			doc.Title = title
			if doc.Metadata == nil {
				doc.Metadata = make(map[string]any)
			}
			for k, v := range metadata {
				doc.Metadata[k] = v
			}

			logger.Debug("ParserStage completed successfully", telemetry.String("url", doc.URL))
			output <- doc
		}(docEvent)
	}

	// Wait for all workers to complete
	wg.Wait()
	return nil
}
