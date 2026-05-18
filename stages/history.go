package stages

import (
	"context"

	"github.com/madmike/go-infra/telemetry"
	"github.com/madmike/go-pipeline/core"
)

// HistorySaver is a function that saves the assistant's response
type HistorySaver func(ctx context.Context, content string) error

// HistoryStageConfig holds configuration for HistoryStage
type HistoryStageConfig struct {
	Saver  HistorySaver
	Logger telemetry.Logger
}

// HistoryStage intercepts the DoneEvent to save the conversation history
type HistoryStage struct {
	config HistoryStageConfig
}

// NewHistoryStage creates a new HistoryStage
func NewHistoryStage(config HistoryStageConfig) *HistoryStage {
	return &HistoryStage{
		config: config,
	}
}

// Name returns the stage name
func (s *HistoryStage) Name() string {
	return "history"
}

// InputTypes returns the event types this stage accepts
func (s *HistoryStage) InputTypes() []core.EventType {
	// accepts all events effectively, but technically we only care about passthrough + done
	return []core.EventType{core.EventTypeLLM, core.EventTypeStatus, core.EventTypeDone, core.EventTypeAudio, core.EventTypeError}
}

// OutputTypes returns the event types this stage produces
func (s *HistoryStage) OutputTypes() []core.EventType {
	return []core.EventType{core.EventTypeLLM, core.EventTypeStatus, core.EventTypeDone, core.EventTypeAudio, core.EventTypeError}
}

// Process implements the Stage interface
func (s *HistoryStage) Process(ctx context.Context, input <-chan core.Event, output chan<- core.Event) error {
	logger := s.config.Logger.WithModule(s.Name())

	logger.Debug("HistoryStage started")

	for event := range input {
		// Pass through all events
		select {
		case <-ctx.Done():
			return ctx.Err()
		case output <- event:
		}

		// Intercept DoneEvent for saving
		if doneEvent, ok := event.(core.DoneEvent); ok {
			if doneEvent.FullText == "" {
				continue
			}

			logger.Debug("Saving history", telemetry.Int("content_length", len(doneEvent.FullText)))

			// Execute saver
			if err := s.config.Saver(ctx, doneEvent.FullText); err != nil {
				logger.Error("Failed to save history", telemetry.Err(err))
				// We don't stop the pipeline on save error, just log it
			} else {
				logger.Debug("History saved successfully")
			}
		}
	}

	return nil
}
