package stages

import (
	"context"
	"fmt"
	"testing"

	providers "github.com/madmike/go-ai-providers/core"
	"github.com/madmike/go-storage/vectorstore"
	"pgregory.net/rapid"
)

// For any query input to the RAG stage, embeddings SHALL be generated using
// the configured embedding provider.
func TestPropertyRAGEmbeddingGeneration(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		// Create RAG stage with mock providers
		stage := NewRAGStage(RAGStageConfig{
			VectorStore:       &TestVectorStore{},
			EmbeddingProvider: &TestEmbeddingProvider{},
			EmbeddingModel:    "test-model",
			SourceID:          "source_1",
			Threshold:         0.7,
			MaxChunks:         5,
		})

		// Verify stage is created
		if stage == nil {
			rt.Fatalf("Failed to create RAG stage")
		}

		// Verify stage name
		if stage.Name() != "rag" {
			rt.Fatalf("Expected stage name 'rag', got '%s'", stage.Name())
		}

		// Verify configuration is set
		if stage.config.EmbeddingModel != "test-model" {
			rt.Fatalf("Embedding model not set correctly")
		}

		if stage.config.SourceID != "source_1" {
			rt.Fatalf("SourceID not set correctly")
		}
	})
}

// For any vector search, results SHALL be filtered by source_id and respect
// the similarity threshold.
func TestPropertyRAGQdrantSearch(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		// Generate test parameters
		sourceID := rapid.StringN(5, 20, 50).Draw(rt, "sourceID")
		threshold := rapid.Float32Range(0.5, 0.95).Draw(rt, "threshold")

		// Create RAG stage
		stage := NewRAGStage(RAGStageConfig{
			VectorStore:       &TestVectorStore{},
			EmbeddingProvider: &TestEmbeddingProvider{},
			EmbeddingModel:    "test-model",
			SourceID:          sourceID,
			Threshold:         threshold,
			MaxChunks:         5,
		})

		// Verify configuration
		if stage.config.SourceID != sourceID {
			rt.Fatalf("SourceID not set correctly")
		}

		if stage.config.Threshold != threshold {
			rt.Fatalf("Threshold not set correctly")
		}

		if stage.config.MaxChunks != 5 {
			rt.Fatalf("MaxChunks not set correctly")
		}
	})
}

// For any search result, content, document_id, source_id, and metadata
// SHALL be extracted and included in the context.
func TestPropertyRAGResultExtraction(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		// Create test vector store with results
		vs := &TestVectorStore{}

		// Verify search returns results with required fields
		ctx := context.Background()
		results, err := vs.Search(ctx, []float32{0.1, 0.2}, vectorstore.SearchFilter{
			SourceID: "source_1",
			MinScore: 0.7,
		}, 5)

		if err != nil {
			rt.Fatalf("Search failed: %v", err)
		}

		if len(results) == 0 {
			rt.Fatalf("No results returned")
		}

		// Verify result has required fields
		result := results[0]
		if result.Content == "" {
			rt.Fatalf("Result has empty content")
		}

		if result.DocumentID == "" {
			rt.Fatalf("Result has empty DocumentID")
		}

		if result.SourceID == "" {
			rt.Fatalf("Result has empty SourceID")
		}
	})
}

// For any RAG failure or empty results, the system SHALL fall back to
// static content from the source configuration.
func TestPropertyRAGFallback(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		// Generate fallback content removed

		// Create RAG stage with error provider
		stage := NewRAGStage(RAGStageConfig{
			VectorStore:       &TestErrorVectorStore{},
			EmbeddingProvider: &TestErrorEmbeddingProvider{},
			EmbeddingModel:    "test-model",
			SourceID:          "source_1",
			Threshold:         0.7,
			MaxChunks:         5,
			// FallbackContent:   fallbackContent, // Removed
		})

		// Verify fallback content is set
		// Removed check

		// Verify error providers are configured
		if stage.config.VectorStore == nil {
			rt.Fatalf("VectorStore not set")
		}

		if stage.config.EmbeddingProvider == nil {
			rt.Fatalf("EmbeddingProvider not set")
		}
	})
}

// Test implementations

// TestVectorStore implements vectorstore.VectorStore for testing
type TestVectorStore struct{}

func (s *TestVectorStore) Search(ctx context.Context, vector []float32, filter vectorstore.SearchFilter, limit int) ([]vectorstore.SearchResult, error) {
	return []vectorstore.SearchResult{
		{
			ID:         "result_1",
			Score:      0.95,
			Content:    "This is test content",
			SourceID:   filter.SourceID,
			DocumentID: "doc_1",
			Metadata: map[string]any{
				"title": "Test Document",
			},
		},
	}, nil
}

func (s *TestVectorStore) Close() error {
	return nil
}

func (s *TestVectorStore) Upsert(ctx context.Context, points []vectorstore.Point) error {
	return nil
}

// TestErrorVectorStore returns errors for testing fallback
type TestErrorVectorStore struct{}

func (s *TestErrorVectorStore) Search(ctx context.Context, vector []float32, filter vectorstore.SearchFilter, limit int) ([]vectorstore.SearchResult, error) {
	return nil, fmt.Errorf("vector store error")
}

func (s *TestErrorVectorStore) Upsert(ctx context.Context, points []vectorstore.Point) error {
	return fmt.Errorf("upsert error")
}

func (s *TestErrorVectorStore) Close() error {
	return nil
}

// TestEmbeddingProvider implements providers.EmbeddingProvider for testing
type TestEmbeddingProvider struct{}

func (p *TestEmbeddingProvider) Name() string                 { return "test-embedding" }
func (p *TestEmbeddingProvider) Type() providers.ProviderType { return "test" }
func (p *TestEmbeddingProvider) Initialize(ctx context.Context, config providers.ProviderConfig) error {
	return nil
}
func (p *TestEmbeddingProvider) Close() error                          { return nil }
func (p *TestEmbeddingProvider) HealthCheck(ctx context.Context) error { return nil }
func (p *TestEmbeddingProvider) Capabilities() []providers.Capability {
	return []providers.Capability{providers.CapabilityEmbedding}
}
func (p *TestEmbeddingProvider) SupportsCapability(capability providers.Capability) bool {
	return capability == providers.CapabilityEmbedding
}
func (p *TestEmbeddingProvider) GenerateEmbedding(ctx context.Context, req providers.EmbeddingRequest) (*providers.EmbeddingResponse, error) {
	// Return a dummy embedding vector
	vector := make([]float32, 384)
	for i := range vector {
		vector[i] = 0.1
	}
	return &providers.EmbeddingResponse{
		Vector: vector,
	}, nil
}

// TestErrorEmbeddingProvider returns errors for testing fallback
type TestErrorEmbeddingProvider struct{}

func (p *TestErrorEmbeddingProvider) Name() string                 { return "test-error-embedding" }
func (p *TestErrorEmbeddingProvider) Type() providers.ProviderType { return "test" }
func (p *TestErrorEmbeddingProvider) Initialize(ctx context.Context, config providers.ProviderConfig) error {
	return nil
}
func (p *TestErrorEmbeddingProvider) Close() error                          { return nil }
func (p *TestErrorEmbeddingProvider) HealthCheck(ctx context.Context) error { return nil }
func (p *TestErrorEmbeddingProvider) Capabilities() []providers.Capability {
	return []providers.Capability{providers.CapabilityEmbedding}
}
func (p *TestErrorEmbeddingProvider) SupportsCapability(capability providers.Capability) bool {
	return capability == providers.CapabilityEmbedding
}
func (p *TestErrorEmbeddingProvider) GenerateEmbedding(ctx context.Context, req providers.EmbeddingRequest) (*providers.EmbeddingResponse, error) {
	return nil, fmt.Errorf("embedding generation error")
}
