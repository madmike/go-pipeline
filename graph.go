package pipeline

import (
	"fmt"
	"github.com/madmike/go-pipeline/core"
)

// PipelineGraph represents the compiled pipeline topology as a directed acyclic graph (DAG)
type PipelineGraph struct {
	// nodes maps stage names to their graph node representations
	nodes map[string]*graphNode

	// entryNode is the name of the entry point stage
	entryNode string

	// exitNodes are the names of terminal stages
	exitNodes []string
}

// graphNode represents a stage in the pipeline graph
type graphNode struct {
	// name is the unique identifier for this node
	name string

	// stage is the actual processing stage
	stage core.Stage

	// outputs are the outgoing edges from this node
	outputs []*graphEdge

	// inputs are the incoming edges to this node
	inputs []*graphEdge

	// fanOut configuration if this node routes to multiple branches
	fanOut *core.FanOutConfig

	// barrier configuration if this node synchronizes multiple branches
	barrier *core.BarrierConfig
}

// graphEdge represents a directed edge in the pipeline graph
type graphEdge struct {
	// from is the source node
	from *graphNode

	// to is the destination node
	to *graphNode

	// eventFilter maps event types to whether they should be forwarded
	// nil means forward all events
	eventFilter map[core.EventType]bool
}

// NewPipelineGraph creates a new empty pipeline graph
func NewPipelineGraph() *PipelineGraph {
	return &PipelineGraph{
		nodes:     make(map[string]*graphNode),
		exitNodes: make([]string, 0),
	}
}

// AddNode adds a stage node to the graph
func (pg *PipelineGraph) AddNode(name string, stage core.Stage, fanOut *core.FanOutConfig, barrier *core.BarrierConfig) error {
	if _, exists := pg.nodes[name]; exists {
		return fmt.Errorf("node %q already exists in graph", name)
	}

	pg.nodes[name] = &graphNode{
		name:    name,
		stage:   stage,
		outputs: make([]*graphEdge, 0),
		inputs:  make([]*graphEdge, 0),
		fanOut:  fanOut,
		barrier: barrier,
	}

	return nil
}

// AddEdge adds a directed edge from source to destination with optional event filtering
func (pg *PipelineGraph) AddEdge(fromName, toName string, eventFilter []core.EventType) error {
	fromNode, exists := pg.nodes[fromName]
	if !exists {
		return fmt.Errorf("source node %q does not exist", fromName)
	}

	toNode, exists := pg.nodes[toName]
	if !exists {
		return fmt.Errorf("destination node %q does not exist", toName)
	}

	// Build event filter map
	var filterMap map[core.EventType]bool
	if len(eventFilter) > 0 {
		filterMap = make(map[core.EventType]bool)
		for _, et := range eventFilter {
			filterMap[et] = true
		}
	}

	edge := &graphEdge{
		from:        fromNode,
		to:          toNode,
		eventFilter: filterMap,
	}

	fromNode.outputs = append(fromNode.outputs, edge)
	toNode.inputs = append(toNode.inputs, edge)

	return nil
}

// SetEntryNode sets the entry point for the pipeline
func (pg *PipelineGraph) SetEntryNode(name string) error {
	if _, exists := pg.nodes[name]; !exists {
		return fmt.Errorf("entry node %q does not exist", name)
	}
	pg.entryNode = name
	return nil
}

// AddExitNode marks a node as a terminal/exit node
func (pg *PipelineGraph) AddExitNode(name string) error {
	if _, exists := pg.nodes[name]; !exists {
		return fmt.Errorf("exit node %q does not exist", name)
	}
	pg.exitNodes = append(pg.exitNodes, name)
	return nil
}

// GetNode retrieves a node by name
func (pg *PipelineGraph) GetNode(name string) *graphNode {
	return pg.nodes[name]
}

// GetEntryNode returns the entry node
func (pg *PipelineGraph) GetEntryNode() *graphNode {
	if pg.entryNode == "" {
		return nil
	}
	return pg.nodes[pg.entryNode]
}

// GetExitNodes returns all exit nodes
func (pg *PipelineGraph) GetExitNodes() []*graphNode {
	exitNodes := make([]*graphNode, 0, len(pg.exitNodes))
	for _, name := range pg.exitNodes {
		exitNodes = append(exitNodes, pg.nodes[name])
	}
	return exitNodes
}

// AllNodes returns all nodes in the graph
func (pg *PipelineGraph) AllNodes() []*graphNode {
	nodes := make([]*graphNode, 0, len(pg.nodes))
	for _, node := range pg.nodes {
		nodes = append(nodes, node)
	}
	return nodes
}

// graphNode methods

// Name returns the node's name
func (n *graphNode) Name() string {
	return n.name
}

// Stage returns the stage associated with this node
func (n *graphNode) Stage() core.Stage {
	return n.stage
}

// Outputs returns all outgoing edges
func (n *graphNode) Outputs() []*graphEdge {
	return n.outputs
}

// Inputs returns all incoming edges
func (n *graphNode) Inputs() []*graphEdge {
	return n.inputs
}

// FanOut returns the fan-out configuration if present
func (n *graphNode) FanOut() *core.FanOutConfig {
	return n.fanOut
}

// Barrier returns the barrier configuration if present
func (n *graphNode) Barrier() *core.BarrierConfig {
	return n.barrier
}

// graphEdge methods

// From returns the source node
func (e *graphEdge) From() *graphNode {
	return e.from
}

// To returns the destination node
func (e *graphEdge) To() *graphNode {
	return e.to
}

// ShouldForwardEvent checks if an event type should be forwarded on this edge
func (e *graphEdge) ShouldForwardEvent(eventType core.EventType) bool {
	// If no filter is set, forward all events
	if e.eventFilter == nil {
		return true
	}

	// Check if the event type is in the filter
	return e.eventFilter[eventType]
}

// EventFilter returns the event filter map
func (e *graphEdge) EventFilter() map[core.EventType]bool {
	return e.eventFilter
}
