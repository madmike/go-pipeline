package stages

import (
	"context"

	"github.com/madmike/go-infra/telemetry"
	"github.com/madmike/go-pipeline/core"
)

type Embedder interface {
	EmbedBatch(ctx context.Context, texts []string) ([][]float32, int, error)
}

type EmbedderStageConfig struct {
	Embedder  Embedder
	BatchSize int
	Logger    telemetry.Logger
}

type EmbedderStage struct {
	config EmbedderStageConfig
}

func NewEmbedderStage(config EmbedderStageConfig) *EmbedderStage {
	if config.BatchSize <= 0 {
		config.BatchSize = 10
	}
	return &EmbedderStage{
		config: config,
	}
}

func (s *EmbedderStage) Name() string {
	return "EmbedderStage"
}

func (s *EmbedderStage) InputTypes() []core.EventType {
	return []core.EventType{core.EventTypeDocument}
}

func (s *EmbedderStage) OutputTypes() []core.EventType {
	return []core.EventType{core.EventTypeDocument}
}

func (s *EmbedderStage) Process(ctx context.Context, input <-chan core.Event, output chan<- core.Event) error {
	logger := s.config.Logger.WithModule("EmbedderStage")

	for event := range input {
		docEvent, ok := event.(core.DocumentEvent)
		if !ok {
			output <- event
			continue
		}

		if docEvent.Error != nil {
			output <- docEvent
			continue
		}

		if len(docEvent.Chunks) == 0 {
			// No chunks to embed, pass through
			output <- docEvent
			continue
		}

		logger.Debug("Embedding document chunks",
			telemetry.String("url", docEvent.URL),
			telemetry.Int("chunks", len(docEvent.Chunks)))

		// Collect texts
		texts := make([]string, len(docEvent.Chunks))
		for i, chunk := range docEvent.Chunks {
			texts[i] = chunk.Content
		}

		// Embed in batches (if needed, though Embedder interface might handle it, we'll do simple pass for now)
		// The interface implies EmbedBatch handles multiple, but we might want to respect stage BatchSize if the list is huge.
		// For now, assuming Embedder handles the batch or we send all at once.
		// The ingestion worker sent all chunks at once.

		vectors, _, err := s.config.Embedder.EmbedBatch(ctx, texts)
		if err != nil {
			logger.Error("Failed to embed chunks", telemetry.Err(err))
			docEvent.Error = err
			output <- docEvent
			continue
		}

		docEvent.Embeddings = vectors
		output <- docEvent
	}

	return nil
}
