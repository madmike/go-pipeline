package pipeline

import (
	"fmt"

	"github.com/madmike/go-pipeline/core"
)

// GraphBuilder constructs pipeline DAGs with a fluent API
type GraphBuilder struct {
	graph       *PipelineGraph
	nodeConfigs map[string]*nodeConfig
	edges       []edgeConfig
	entryNode   string
	exitNodes   []string
}

// nodeConfig holds configuration for a node
type nodeConfig struct {
	stage   core.Stage
	fanOut  *core.FanOutConfig
	barrier *core.BarrierConfig
}

// edgeConfig holds configuration for an edge
type edgeConfig struct {
	from        string
	to          string
	eventFilter []core.EventType
}

// NewBuilder creates a new graph-based pipeline builder
func NewBuilder() *GraphBuilder {
	return &GraphBuilder{
		graph:       NewPipelineGraph(),
		nodeConfigs: make(map[string]*nodeConfig),
		edges:       make([]edgeConfig, 0),
		exitNodes:   make([]string, 0),
	}
}

// AddStage adds a stage node to the pipeline
func (b *GraphBuilder) AddStage(name string, stage core.Stage) *GraphBuilder {
	b.nodeConfigs[name] = &nodeConfig{
		stage:   stage,
		fanOut:  nil,
		barrier: nil,
	}
	return b
}

// AddFanOut adds a fan-out node that routes to multiple branches
func (b *GraphBuilder) AddFanOut(name string, config core.FanOutConfig) *GraphBuilder {
	// Create a synthetic stage for the fan-out node
	// The fan-out node itself doesn't process events, it just routes them
	b.nodeConfigs[name] = &nodeConfig{
		stage:   nil, // Fan-out nodes don't have a stage
		fanOut:  &config,
		barrier: nil,
	}
	return b
}

// AddBarrier adds a barrier/join node that synchronizes multiple branches
func (b *GraphBuilder) AddBarrier(name string, config core.BarrierConfig) *GraphBuilder {
	// Create a synthetic stage for the barrier node
	// The barrier node itself doesn't process events, it just synchronizes them
	b.nodeConfigs[name] = &nodeConfig{
		stage:   nil, // Barrier nodes don't have a stage
		fanOut:  nil,
		barrier: &config,
	}
	return b
}

// Connect creates an edge from one node to another with optional event filtering
func (b *GraphBuilder) Connect(from, to string, eventFilter ...core.EventType) *GraphBuilder {
	b.edges = append(b.edges, edgeConfig{
		from:        from,
		to:          to,
		eventFilter: eventFilter,
	})
	return b
}

// SetErrorPolicy sets the error policy for a fan-out node
func (b *GraphBuilder) SetErrorPolicy(nodeName string, policy core.ErrorPolicy) *GraphBuilder {
	if config, exists := b.nodeConfigs[nodeName]; exists && config.fanOut != nil {
		config.fanOut.ErrorPolicy = policy
	}
	return b
}

// SetEntryNode sets the entry point for the pipeline
func (b *GraphBuilder) SetEntryNode(name string) *GraphBuilder {
	b.entryNode = name
	return b
}

// AddExitNode marks a node as a terminal/exit node
func (b *GraphBuilder) AddExitNode(name string) *GraphBuilder {
	b.exitNodes = append(b.exitNodes, name)
	return b
}

// Build creates and validates the pipeline graph
func (b *GraphBuilder) Build() (*Pipeline, error) {
	// Validate that we have at least one node
	if len(b.nodeConfigs) == 0 {
		return nil, fmt.Errorf("pipeline must have at least one stage")
	}

	// Validate that entry node is set
	if b.entryNode == "" {
		return nil, fmt.Errorf("entry node must be set")
	}

	// Add all nodes to the graph
	for name, config := range b.nodeConfigs {
		if err := b.graph.AddNode(name, config.stage, config.fanOut, config.barrier); err != nil {
			return nil, fmt.Errorf("failed to add node %q: %w", name, err)
		}
	}

	// Add all edges to the graph
	for _, edge := range b.edges {
		if err := b.graph.AddEdge(edge.from, edge.to, edge.eventFilter); err != nil {
			return nil, fmt.Errorf("failed to add edge from %q to %q: %w", edge.from, edge.to, err)
		}
	}

	// Set entry node
	if err := b.graph.SetEntryNode(b.entryNode); err != nil {
		return nil, fmt.Errorf("failed to set entry node: %w", err)
	}

	// Add exit nodes
	for _, exitNode := range b.exitNodes {
		if err := b.graph.AddExitNode(exitNode); err != nil {
			return nil, fmt.Errorf("failed to add exit node %q: %w", exitNode, err)
		}
	}

	// Validate the graph structure
	if err := ValidateGraph(b.graph); err != nil {
		return nil, fmt.Errorf("graph validation failed: %w", err)
	}

	// Create and return the pipeline
	return &Pipeline{
		graph: b.graph,
	}, nil
}
