package stages

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/madmike/go-infra/telemetry"
	"github.com/madmike/go-pipeline/core"
	"github.com/madmike/go-storage/vectorstore"
)

type VectorStoreStageConfig struct {
	VectorStore vectorstore.VectorStore
	Logger      telemetry.Logger
}

type VectorStoreStage struct {
	config VectorStoreStageConfig
}

func NewVectorStoreStage(config VectorStoreStageConfig) *VectorStoreStage {
	return &VectorStoreStage{
		config: config,
	}
}

func (s *VectorStoreStage) Name() string {
	return "VectorStoreStage"
}

func (s *VectorStoreStage) InputTypes() []core.EventType {
	return []core.EventType{core.EventTypeDocument}
}

func (s *VectorStoreStage) OutputTypes() []core.EventType {
	return []core.EventType{core.EventTypeDocument}
}

func (s *VectorStoreStage) Process(ctx context.Context, input <-chan core.Event, output chan<- core.Event) error {
	logger := s.config.Logger.WithModule("VectorStoreStage")

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

		if len(docEvent.Embeddings) == 0 {
			// No embeddings to index, pass through
			output <- docEvent
			continue
		}

		logger.Debug("Indexing document chunks",
			telemetry.String("url", docEvent.URL),
			telemetry.Int("embeddings", len(docEvent.Embeddings)))

		points := make([]vectorstore.Point, len(docEvent.Embeddings))
		for i, vector := range docEvent.Embeddings {
			if i >= len(docEvent.Chunks) {
				logger.Warn("More embeddings than chunks",
					telemetry.Int("embeddings", len(docEvent.Embeddings)),
					telemetry.Int("chunks", len(docEvent.Chunks)))
				break
			}
			chunk := docEvent.Chunks[i]

			// Construct minimal payload
			payload := map[string]any{
				"source_id":   docEvent.SourceID,
				"document_id": docEvent.DocumentID,
				"chunk_index": chunk.Index,
			}

			logger.Debug("Upserting point with payload",
				telemetry.String("doc_id", docEvent.DocumentID),
				telemetry.Int("chunk", chunk.Index),
				telemetry.Any("payload", payload))

			// Generate a deterministic ID for proper upserts/deduplication if possible
			// Usually: uuid_v5(namespace, document_id + chunk_index)
			// But here we might just need a unique ID.
			// Let's assume consumer logic or a helper generates it.
			// For now, we will use a simple string combination or require it in Chunk metadata?
			// The ingestion service used `qdrant.GenerateVectorID(docID, chunk.Index)`.
			// Since we want this generic, we might need a helper function or rely on a standard way.
			// Let's generate it here using a simple string format or just random if not provided.
			// Ideally we want stability.
			// We'll use "docID_index" as ID if valid UUID not enforced by VectorStore,
			// or we should import the `uuid` package and generate v5.
			// To keep it simple and dependency-free for now, we'll try to use a unique string ID.
			// But Qdrant prefers UUIDs or Integers.
			// Let's use a standard format and rely on VectorStore implementation to handle it or error.
			// We'll trust that DocumentID + index is sufficient for now but using UUID generation logic inside this stage might be better.
			// But I don't want to add `google/uuid` dependency if I can avoid it in `pipeline`?
			// `pipeline` has `google/uuid` in `go.mod` of ingestion-service, but pipeline library itself?
			// Let's check `libraries/pipeline/go.mod`.

			pointIDRaw := fmt.Sprintf("%s_%d", docEvent.DocumentID, chunk.Index)
			// Ensure valid UUID for Qdrant by hashing the deterministic ID
			pointID := uuid.NewMD5(uuid.Nil, []byte(pointIDRaw)).String()

			points[i] = vectorstore.Point{
				ID:      pointID,
				Vector:  vector,
				Payload: payload,
			}
		}

		if err := s.config.VectorStore.Upsert(ctx, points); err != nil {
			logger.Error("Failed to upsert embeddings", telemetry.Err(err))
			docEvent.Error = err
			output <- docEvent
			continue
		}

		// Mark as indexed
		docEvent.IsFinal = true
		output <- docEvent
	}

	return nil
}
