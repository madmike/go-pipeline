package stages

import (
	"context"

	"github.com/madmike/go-infra/telemetry"
	"github.com/madmike/go-pipeline/core"
)

type TextChunker interface {
	Chunk(text string) ([]core.Chunk, error)
}

type ChunkerStageConfig struct {
	Chunker TextChunker
	Logger  telemetry.Logger
}

type ChunkerStage struct {
	config ChunkerStageConfig
}

func NewChunkerStage(config ChunkerStageConfig) *ChunkerStage {
	return &ChunkerStage{
		config: config,
	}
}

func (s *ChunkerStage) Name() string {
	return "ChunkerStage"
}

func (s *ChunkerStage) InputTypes() []core.EventType {
	return []core.EventType{core.EventTypeDocument}
}

func (s *ChunkerStage) OutputTypes() []core.EventType {
	return []core.EventType{core.EventTypeDocument}
}

func (s *ChunkerStage) Process(ctx context.Context, input <-chan core.Event, output chan<- core.Event) error {
	logger := s.config.Logger.WithModule("ChunkerStage")

	for event := range input {
		docEvent, ok := event.(core.DocumentEvent)
		if !ok {
			output <- event
			continue
		}

		logger.Debug("ChunkerStage received event", telemetry.String("url", docEvent.URL))

		// Pass through errors and skipped documents
		if docEvent.Error != nil || docEvent.Metadata["skipped"] == true {
			logger.Debug("ChunkerStage passing through skipped/errored event", telemetry.String("url", docEvent.URL))
			output <- docEvent
			continue
		}

		logger.Debug("Chunking document", telemetry.String("url", docEvent.URL))

		chunks, err := s.config.Chunker.Chunk(docEvent.Content)
		if err != nil {
			logger.Error("Failed to chunk document", telemetry.Err(err))
			docEvent.Error = err
			output <- docEvent
			continue
		}

		logger.Debug("Chunking complete", telemetry.String("url", docEvent.URL), telemetry.Int("chunk_count", len(chunks)))
		docEvent.Chunks = chunks
		output <- docEvent
	}

	return nil
}
