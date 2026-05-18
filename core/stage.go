package core

import "context"

// EventTypeWildcard represents a stage that accepts all event types
const EventTypeWildcard EventType = "*"

// Stage represents a processing stage in a pipeline
type Stage interface {
	Name() string
	Process(ctx context.Context, input <-chan Event, output chan<- Event) error

	// InputTypes returns the event types this stage accepts.
	// Returns empty slice to accept all event types.
	InputTypes() []EventType

	// OutputTypes returns the event types this stage produces.
	OutputTypes() []EventType
}

// PipelineOutput is a channel of events
type PipelineOutput <-chan Event

// PipelineInput is a channel for sending events to a pipeline
type PipelineInput chan<- Event
