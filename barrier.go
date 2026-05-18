package pipeline

import (
	"context"
	"fmt"

	"github.com/madmike/go-pipeline/core"
)

// BarrierStage synchronizes multiple upstream branches and waits for all to complete
// before emitting a single DoneEvent downstream
type BarrierStage struct {
	name   string
	config *core.BarrierConfig
}

// NewBarrierStage creates a new barrier stage
func NewBarrierStage(name string, config *core.BarrierConfig) *BarrierStage {
	return &BarrierStage{
		name:   name,
		config: config,
	}
}

// Name returns the stage name
func (bs *BarrierStage) Name() string {
	return bs.name
}

// Process implements the Stage interface
// It waits for all upstream branches to complete and emits a single DoneEvent
func (bs *BarrierStage) Process(ctx context.Context, input <-chan core.Event, output chan<- core.Event) error {
	defer close(output)

	// Collect events from all upstream branches
	doneCount := 0
	var firstError error
	errorOccurred := false

	for event := range input {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Check if this is an error event
		if errorEvent, ok := event.(core.ErrorEvent); ok {
			// Fail-fast: propagate error immediately
			if !errorOccurred {
				firstError = errorEvent.Error
				errorOccurred = true
			}
			// Send error event downstream
			select {
			case <-ctx.Done():
				return ctx.Err()
			case output <- event:
			}
			continue
		}

		// Check if this is a DoneEvent
		if _, ok := event.(core.DoneEvent); ok {
			doneCount++
			// Don't collect DoneEvents yet - we'll emit a single one at the end
			continue
		}

		// Forward non-terminal events downstream
		select {
		case <-ctx.Done():
			return ctx.Err()
		case output <- event:
		}
	}

	// If an error occurred, return it
	if errorOccurred {
		return firstError
	}

	// Check if context was cancelled
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Verify we received DoneEvents from all upstream branches
	if doneCount != bs.config.UpstreamCount {
		return fmt.Errorf("barrier expected %d DoneEvents, got %d", bs.config.UpstreamCount, doneCount)
	}

	// Emit a single consolidated DoneEvent
	consolidatedDone := core.DoneEvent{
		FullText:      "",
		TokensUsed:    0,
		AudioDuration: 0,
		ActionsCount:  0,
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case output <- consolidatedDone:
	}

	return nil
}

// InputTypes returns the input event types this stage accepts
func (bs *BarrierStage) InputTypes() []core.EventType {
	// Barrier accepts all event types from upstream branches
	return []core.EventType{}
}

// OutputTypes returns the output event types this stage produces
func (bs *BarrierStage) OutputTypes() []core.EventType {
	// Barrier produces all event types it receives plus DoneEvent
	return []core.EventType{
		core.EventTypeStatus,
		core.EventTypeSTT,
		core.EventTypeLLM,
		core.EventTypeAudio,
		core.EventTypeAction,
		core.EventTypeError,
		core.EventTypeDone,
	}
}
