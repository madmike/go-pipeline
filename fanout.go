package pipeline

import (
	"context"
	"sync"

	"github.com/madmike/go-pipeline/core"
)

// FanOutRouter routes events from a single input to multiple downstream branches
// with support for event filtering and configurable error handling policies
type FanOutRouter struct {
	config  *core.FanOutConfig
	inputs  []chan core.Event
	outputs []chan core.Event
	wg      sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc
}

// NewFanOutRouter creates a new fan-out router with the given configuration
func NewFanOutRouter(config *core.FanOutConfig) *FanOutRouter {
	ctx, cancel := context.WithCancel(context.Background())

	// Initialize input and output channels for each branch
	inputs := make([]chan core.Event, len(config.Branches))
	outputs := make([]chan core.Event, len(config.Branches))

	for i := range config.Branches {
		inputs[i] = make(chan core.Event, 100)
		outputs[i] = make(chan core.Event, 100)
	}

	return &FanOutRouter{
		config:  config,
		inputs:  inputs,
		outputs: outputs,
		ctx:     ctx,
		cancel:  cancel,
	}
}

// Route distributes events from the input channel to all downstream branches
// according to the configured error policy and event filters
func (fr *FanOutRouter) Route(ctx context.Context, input <-chan core.Event) error {
	// Create a merged context that respects both the router's context and the provided context
	mergedCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Track all branch goroutines
	var branchWg sync.WaitGroup
	errorChan := make(chan error, len(fr.config.Branches))

	// Start all branch processors
	for i, branch := range fr.config.Branches {
		branchWg.Add(1)
		go fr.processBranch(mergedCtx, i, branch, &branchWg, errorChan)
	}

	// Start the event distributor
	fr.wg.Add(1)
	go func() {
		defer fr.wg.Done()
		fr.distributeEvents(mergedCtx, input, errorChan)
	}()

	// Wait for all branches to complete
	branchWg.Wait()

	// Close error channel and collect errors
	close(errorChan)
	var errors []error
	for err := range errorChan {
		if err != nil {
			errors = append(errors, err)
		}
	}

	// Return first error if any occurred
	if len(errors) > 0 {
		return errors[0]
	}

	return nil
}

// distributeEvents reads from the input channel and forwards events to all branches
// according to their event filters
func (fr *FanOutRouter) distributeEvents(ctx context.Context, input <-chan core.Event, errorChan chan<- error) {
	defer func() {
		// Close all input channels when distribution is complete
		for _, ch := range fr.inputs {
			close(ch)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-input:
			if !ok {
				// Input channel closed, stop distribution
				return
			}

			// Forward event to each branch according to its filter
			for i, branch := range fr.config.Branches {
				// Check if this branch should receive this event type
				if !fr.shouldForwardEvent(branch, event) {
					continue
				}

				// Send event to branch input (non-blocking with context check)
				select {
				case <-ctx.Done():
					return
				case fr.inputs[i] <- event:
					// Event sent successfully
				}
			}
		}
	}
}

// processBranch processes events for a single downstream branch
func (fr *FanOutRouter) processBranch(ctx context.Context, branchIndex int, branch core.BranchConfig, wg *sync.WaitGroup, errorChan chan<- error) {
	defer wg.Done()

	// Execute the branch stage
	err := branch.Stage.Process(ctx, fr.inputs[branchIndex], fr.outputs[branchIndex])

	if err != nil {
		// Send error to error channel
		select {
		case errorChan <- err:
		default:
			// Error channel full, continue
		}

		// Handle error according to policy
		fr.handleBranchError(ctx, err)
	}

	// Close the output channel for this branch
	close(fr.outputs[branchIndex])
}

// handleBranchError handles errors according to the configured error policy
func (fr *FanOutRouter) handleBranchError(ctx context.Context, err error) {
	if fr.config.ErrorPolicy == core.ErrorPolicyCancelAll {
		// Cancel all branches
		fr.cancel()
	}
	// For ErrorPolicyIsolated, we don't cancel - other branches continue
}

// shouldForwardEvent checks if an event should be forwarded to a branch
// based on the branch's event filter
func (fr *FanOutRouter) shouldForwardEvent(branch core.BranchConfig, event core.Event) bool {
	// If no filter is specified, forward all events
	if len(branch.EventFilter) == 0 {
		return true
	}

	// Check if the event type is in the filter
	eventType := event.EventType()
	for _, filterType := range branch.EventFilter {
		if filterType == eventType {
			return true
		}
	}

	return false
}

// GetOutputs returns the output channels for all branches
// Each output channel receives events that passed the branch's filter
func (fr *FanOutRouter) GetOutputs() []<-chan core.Event {
	outputs := make([]<-chan core.Event, len(fr.outputs))
	for i, ch := range fr.outputs {
		outputs[i] = ch
	}
	return outputs
}

// Cancel cancels the fan-out router and all its branches
func (fr *FanOutRouter) Cancel() {
	fr.cancel()
}

// Wait waits for all fan-out operations to complete
func (fr *FanOutRouter) Wait() {
	fr.wg.Wait()
}

// FanOutStage is a stage that implements the Stage interface for fan-out routing
type FanOutStage struct {
	name   string
	config *core.FanOutConfig
	router *FanOutRouter
}

// NewFanOutStage creates a new fan-out stage
func NewFanOutStage(name string, config *core.FanOutConfig) *FanOutStage {
	return &FanOutStage{
		name:   name,
		config: config,
		router: NewFanOutRouter(config),
	}
}

// Name returns the stage name
func (fs *FanOutStage) Name() string {
	return fs.name
}

// Process implements the Stage interface
// It routes events from input to multiple downstream branches
func (fs *FanOutStage) Process(ctx context.Context, input <-chan core.Event, output chan<- core.Event) error {
	// Route events to all branches
	err := fs.router.Route(ctx, input)

	// Merge outputs from all branches back to the single output channel
	fs.mergeOutputs(ctx, output)

	return err
}

// mergeOutputs merges events from all branch outputs into a single output channel
func (fs *FanOutStage) mergeOutputs(ctx context.Context, output chan<- core.Event) {
	var wg sync.WaitGroup

	// Start a goroutine for each branch output
	for _, branchOutput := range fs.router.GetOutputs() {
		wg.Add(1)
		go func(ch <-chan core.Event) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case event, ok := <-ch:
					if !ok {
						return
					}
					select {
					case <-ctx.Done():
						return
					case output <- event:
						// Event sent successfully
					}
				}
			}
		}(branchOutput)
	}

	// Wait for all branch outputs to be consumed
	wg.Wait()
}

// InputTypes returns the input event types this stage accepts
func (fs *FanOutStage) InputTypes() []core.EventType {
	// Fan-out accepts all event types
	return []core.EventType{}
}

// OutputTypes returns the output event types this stage produces
func (fs *FanOutStage) OutputTypes() []core.EventType {
	// Collect all output types from all branches
	outputTypes := make(map[core.EventType]bool)

	for _, branch := range fs.config.Branches {
		for _, outputType := range branch.Stage.OutputTypes() {
			outputTypes[outputType] = true
		}
	}

	// Convert map to slice
	result := make([]core.EventType, 0, len(outputTypes))
	for outputType := range outputTypes {
		result = append(result, outputType)
	}

	return result
}
