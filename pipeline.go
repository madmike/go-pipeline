package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"sync"

	"github.com/madmike/go-pipeline/core"
)

// Pipeline represents a composable processing pipeline with graph-based execution
type Pipeline struct {
	graph  *PipelineGraph
	mu     sync.Mutex
	ctx    context.Context
	cancel context.CancelFunc
}

// NewPipeline creates a new pipeline from a validated graph
func NewPipeline(graph *PipelineGraph) *Pipeline {
	return &Pipeline{
		graph: graph,
	}
}

// Execute processes the pipeline DAG starting from the entry node
// Returns a channel of events from all exit nodes
func (p *Pipeline) Execute(ctx context.Context, input <-chan core.Event) core.PipelineOutput {
	outputChan := make(chan core.Event, 100)

	go func() {
		defer close(outputChan)

		// Create a cancellable context
		pipelineCtx, cancel := context.WithCancel(ctx)
		p.mu.Lock()
		p.ctx = pipelineCtx
		p.cancel = cancel
		p.mu.Unlock()

		defer func() {
			p.mu.Lock()
			p.ctx = nil
			p.cancel = nil
			p.mu.Unlock()
		}()

		// Execute the graph
		if err := p.executeGraph(pipelineCtx, input, outputChan); err != nil {
			slog.Error("pipeline execution failed", "error", err)
			return
		}
	}()

	return outputChan
}

// executeGraph executes the pipeline DAG with proper synchronization and error handling
func (p *Pipeline) executeGraph(ctx context.Context, input <-chan core.Event, output chan<- core.Event) error {
	// Create execution state with cancellation support
	pipelineCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	state := &executionState{
		ctx:        pipelineCtx,
		cancel:     cancel,
		nodeStates: make(map[string]*nodeState),
		wg:         sync.WaitGroup{},
		mu:         sync.Mutex{},
		errorChan:  make(chan error, len(p.graph.AllNodes())),
	}

	// Initialize node states for all nodes in the graph
	for _, node := range p.graph.AllNodes() {
		state.nodeStates[node.Name()] = &nodeState{
			input:  make(chan core.Event, 100),
			output: make(chan core.Event, 100),
			done:   make(chan struct{}),
		}
	}

	// Start all stages
	for _, node := range p.graph.AllNodes() {
		state.wg.Add(1)
		go p.runStage(node, state)
	}

	// Send input to entry node
	entryNode := p.graph.GetEntryNode()
	if entryNode != nil {
		state.wg.Add(1)
		go func() {
			defer state.wg.Done()
			defer close(state.nodeStates[entryNode.Name()].input)
			for event := range input {
				select {
				case <-pipelineCtx.Done():
					return
				case state.nodeStates[entryNode.Name()].input <- event:
				}
			}
		}()
	}

	// Collect output from exit nodes
	exitNodes := p.graph.GetExitNodes()
	state.wg.Add(1)
	go func() {
		defer state.wg.Done()
		var wg sync.WaitGroup
		for _, exitNode := range exitNodes {
			wg.Add(1)
			go func(node *graphNode) {
				defer wg.Done()
				for event := range state.nodeStates[node.Name()].output {
					select {
					case <-pipelineCtx.Done():
						return
					case output <- event:
					}
				}
			}(exitNode)
		}
		wg.Wait()
	}()

	// Wait for all stages to complete
	state.wg.Wait()

	// Close error channel and check for errors
	close(state.errorChan)
	for err := range state.errorChan {
		if err != nil {
			return err
		}
	}

	return nil
}

// runStage executes a single stage with proper error handling and event routing
func (p *Pipeline) runStage(node *graphNode, state *executionState) {
	defer state.wg.Done()

	nodeState := state.nodeStates[node.Name()]

	// Start a goroutine to route output events as they arrive
	state.wg.Add(1)
	go func() {
		defer state.wg.Done()
		p.routeOutputsStreaming(node, state)
	}()

	defer close(nodeState.output)
	defer close(nodeState.done)

	// Recover from panics
	defer func() {
		if r := recover(); r != nil {
			// Log panic with stack trace
			buf := make([]byte, 4096)
			n := runtime.Stack(buf, false)
			stackTrace := string(buf[:n])

			err := fmt.Errorf("stage %s panicked: %v\nStack trace:\n%s", node.Name(), r, stackTrace)
			errEvent := core.ErrorEvent{
				Error:     err,
				Retryable: false,
			}
			// Try to send error event before cancelling
			select {
			case <-state.ctx.Done():
			case nodeState.output <- errEvent:
			}
			// Propagate error and cancel pipeline
			select {
			case state.errorChan <- err:
			default:
			}
			state.cancel()
		}
	}()

	// Execute the stage
	err := node.Stage().Process(state.ctx, nodeState.input, nodeState.output)

	if err != nil {
		// Emit error event
		errEvent := core.ErrorEvent{
			Error:     err,
			Retryable: false,
		}
		select {
		case <-state.ctx.Done():
		case nodeState.output <- errEvent:
		}
		// Propagate error and cancel pipeline
		select {
		case state.errorChan <- err:
		default:
		}
		state.cancel()
		return
	}
}

// routeOutputsStreaming routes events from a stage to its downstream nodes as they arrive
// This is used for stages that produce events while still running
func (p *Pipeline) routeOutputsStreaming(node *graphNode, state *executionState) {
	nodeState := state.nodeStates[node.Name()]

	// Route events as they arrive
	for event := range nodeState.output {
		for _, edge := range node.Outputs() {
			downstreamNode := edge.To()
			downstreamState := state.nodeStates[downstreamNode.Name()]

			// Check if event should be forwarded based on filter
			shouldForward := edge.ShouldForwardEvent(event.EventType())

			if !shouldForward {
				continue
			}

			select {
			case <-state.ctx.Done():
				return
			case downstreamState.input <- event:
			}
		}
	}

	// Close input channels for downstream nodes that have no more inputs
	for _, edge := range node.Outputs() {
		downstreamNode := edge.To()
		downstreamState := state.nodeStates[downstreamNode.Name()]

		// Check if all upstream nodes have completed
		allUpstreamDone := true
	UpstreamLoop:
		for _, inEdge := range downstreamNode.Inputs() {
			upstreamState := state.nodeStates[inEdge.From().Name()]
			select {
			case <-upstreamState.done:
			default:
				allUpstreamDone = false
				break UpstreamLoop
			}
		}

		if allUpstreamDone {
			// Safely close the channel exactly once
			downstreamState.closeInput.Do(func() {
				close(downstreamState.input)
			})
		}
	}
}

// Cancel cancels the pipeline execution
func (p *Pipeline) Cancel() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.cancel != nil {
		p.cancel()
	}
}

// executionState tracks runtime state during pipeline execution
type executionState struct {
	ctx        context.Context
	cancel     context.CancelFunc
	nodeStates map[string]*nodeState
	wg         sync.WaitGroup
	mu         sync.Mutex
	errorChan  chan error
}

// nodeState tracks the state of a single node during execution
type nodeState struct {
	input      chan core.Event
	output     chan core.Event
	done       chan struct{}
	closeInput sync.Once
}
