package stages

import (
	"context"
	"fmt"

	providers "github.com/madmike/go-ai-providers/core"
	"github.com/madmike/go-infra/telemetry"
	"github.com/madmike/go-pipeline/core"
	"github.com/madmike/go-storage/vectorstore"
)

// RAGStageConfig holds RAG stage configuration.
type RAGStageConfig struct {
	// VectorStore is the vector store to search.
	VectorStore vectorstore.VectorStore

	// EmbeddingProvider generates embeddings for queries.
	EmbeddingProvider providers.EmbeddingProvider

	// EmbeddingModel is the model to use for embeddings.
	EmbeddingModel string

	// SourceID filters results to a specific source.
	// Deprecated: Use SourceIDs for filtering by multiple sources.
	SourceID string

	// SourceIDs filters results to multiple sources.
	// Takes precedence over SourceID if set.
	SourceIDs []string

	// Threshold is the minimum similarity score (0.0-1.0).
	Threshold float32

	// MaxChunks is the maximum number of chunks to retrieve.
	MaxChunks int

	Logger telemetry.Logger
}

// RAGStage retrieves relevant context from a vector store.
type RAGStage struct {
	config RAGStageConfig
}

// NewRAGStage creates a new RAG stage.
func NewRAGStage(config RAGStageConfig) *RAGStage {
	if config.MaxChunks <= 0 {
		config.MaxChunks = 5
	}
	if config.Threshold <= 0 {
		config.Threshold = 0.7
	}
	return &RAGStage{config: config}
}

// Name returns the stage name.
func (s *RAGStage) Name() string {
	return "rag"
}

// InputTypes returns the event types this stage accepts
func (s *RAGStage) InputTypes() []core.EventType {
	return []core.EventType{core.EventTypeLLM}
}

// OutputTypes returns the event types this stage produces
func (s *RAGStage) OutputTypes() []core.EventType {
	return []core.EventType{core.EventTypeRAG, core.EventTypeStatus}
}

// Process implements the Stage interface.
// It reads the query from input, retrieves context, and passes raw RAG results to output.
func (s *RAGStage) Process(ctx context.Context, input <-chan core.Event, output chan<- core.Event) error {
	logger := s.config.Logger.WithModule(s.Name())
	logger.Info("RAGStage started processing")

	// Collect query text from input
	var queryText string
	for event := range input {
		if llmEvent, ok := event.(core.LLMEvent); ok {
			queryText += llmEvent.Delta
			logger.Debug("Received LLM event", telemetry.String("delta", llmEvent.Delta))
		} else if _, ok := event.(core.DoneEvent); ok {
			// Stop collecting on DoneEvent
			logger.Info("Received DoneEvent, finishing collection")
			break
		}
	}

	if queryText == "" {
		logger.Info("No query text received, finishing stage silently")
		// Emit DoneEvent to signal completion
		output <- core.DoneEvent{}
		return nil
	}

	// Emit searching status only when we actually have a query to search for
	output <- core.StatusEvent{
		Status:  core.StatusSearching,
		Target:  core.StatusTargetBot,
		Message: "Searching knowledge base...",
	}

	logger.Info("Collected query text", telemetry.String("query", queryText))

	// Perform search
	results, err := s.search(ctx, queryText)
	if err != nil {
		logger.Error("RAG search failed", telemetry.Err(err))
		// Emit empty RAG event on error to allow pipeline to continue (e.g. LLM without context)
		output <- core.RAGEvent{
			Query:   queryText,
			Results: []core.RAGResult{},
		}
	} else {
		logger.Info("RAG search completed", telemetry.Int("result_count", len(results)))
		output <- core.RAGEvent{
			Query:   queryText,
			Results: results,
		}
	}

	// Emit DoneEvent to signal completion to downstream stages
	logger.Info("Emitting DoneEvent")
	output <- core.DoneEvent{}

	return nil
}

// search generates embedding and searches vector store.
func (s *RAGStage) search(ctx context.Context, query string) ([]core.RAGResult, error) {
	// Skip if no vector store or embedding provider
	if s.config.VectorStore == nil || s.config.EmbeddingProvider == nil {
		return nil, fmt.Errorf("vector store or embedding provider not configured")
	}

	// Generate embedding for query
	embResp, err := s.config.EmbeddingProvider.GenerateEmbedding(ctx, providers.EmbeddingRequest{
		Model: s.config.EmbeddingModel,
		Text:  query,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to generate embedding: %w", err)
	}

	s.config.Logger.Debug("Generated query embedding",
		telemetry.Int("dimensions", len(embResp.Vector)),
		telemetry.String("model", s.config.EmbeddingModel))

	// Build search filter
	filter := vectorstore.SearchFilter{
		MinScore: s.config.Threshold,
	}

	// Use SourceIDs if provided, otherwise fall back to SourceID for backward compatibility
	if len(s.config.SourceIDs) > 0 {
		filter.SourceIDs = s.config.SourceIDs
	} else if s.config.SourceID != "" {
		filter.SourceID = s.config.SourceID
	}

	logger := s.config.Logger.WithModule(s.Name())
	logger.Info("Executing vector search",
		telemetry.Float64("threshold", float64(filter.MinScore)),
		telemetry.String("source_ids", fmt.Sprintf("%v", filter.SourceIDs)),
		telemetry.String("query", query))

	results, err := s.config.VectorStore.Search(ctx, embResp.Vector, filter, s.config.MaxChunks)
	if err != nil {
		return nil, fmt.Errorf("vector search failed: %w", err)
	}

	// Map results to core.RAGResult
	ragResults := make([]core.RAGResult, len(results))
	for i, res := range results {
		ragResults[i] = core.RAGResult{
			Content:    res.Content,
			Score:      res.Score,
			DocumentID: res.DocumentID,
			Metadata:   res.Metadata,
		}
	}

	return ragResults, nil
}
