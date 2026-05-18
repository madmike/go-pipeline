package pipeline

import (
	"testing"

	"github.com/madmike/go-pipeline/core"
)

// TestGraphNodeCreationStandalone tests basic node creation and retrieval
func TestGraphNodeCreationStandalone(t *testing.T) {
	graph := NewPipelineGraph()

	stage := &MockStage{
		name:        "test-stage",
		inputTypes:  []core.EventType{core.EventTypeSTT},
		outputTypes: []core.EventType{core.EventTypeLLM},
	}

	err := graph.AddNode("stage1", stage, nil, nil)
	if err != nil {
		t.Fatalf("failed to add node: %v", err)
	}

	node := graph.GetNode("stage1")
	if node == nil {
		t.Fatal("node not found")
	}

	if node.Name() != "stage1" {
		t.Errorf("expected node name 'stage1', got %q", node.Name())
	}

	if node.Stage() != stage {
		t.Error("stage mismatch")
	}
}

// TestGraphDuplicateNodeStandalone tests that duplicate nodes are rejected
func TestGraphDuplicateNodeStandalone(t *testing.T) {
	graph := NewPipelineGraph()
	stage := &MockStage{name: "test"}

	err := graph.AddNode("stage1", stage, nil, nil)
	if err != nil {
		t.Fatalf("first add failed: %v", err)
	}

	err = graph.AddNode("stage1", stage, nil, nil)
	if err == nil {
		t.Fatal("expected error for duplicate node")
	}
}

// TestGraphEdgeCreationStandalone tests edge creation and filtering
func TestGraphEdgeCreationStandalone(t *testing.T) {
	graph := NewPipelineGraph()

	stage1 := &MockStage{name: "stage1", outputTypes: []core.EventType{core.EventTypeSTT}}
	stage2 := &MockStage{name: "stage2", inputTypes: []core.EventType{core.EventTypeSTT}}

	graph.AddNode("stage1", stage1, nil, nil)
	graph.AddNode("stage2", stage2, nil, nil)

	// Add edge with filter
	err := graph.AddEdge("stage1", "stage2", []core.EventType{core.EventTypeSTT})
	if err != nil {
		t.Fatalf("failed to add edge: %v", err)
	}

	node1 := graph.GetNode("stage1")
	if len(node1.Outputs()) != 1 {
		t.Errorf("expected 1 output, got %d", len(node1.Outputs()))
	}

	edge := node1.Outputs()[0]
	if !edge.ShouldForwardEvent(core.EventTypeSTT) {
		t.Error("edge should forward STT events")
	}

	if edge.ShouldForwardEvent(core.EventTypeLLM) {
		t.Error("edge should not forward LLM events")
	}
}

// TestGraphEdgeNoFilterStandalone tests edge creation without filter (forward all)
func TestGraphEdgeNoFilterStandalone(t *testing.T) {
	graph := NewPipelineGraph()

	stage1 := &MockStage{name: "stage1"}
	stage2 := &MockStage{name: "stage2"}

	graph.AddNode("stage1", stage1, nil, nil)
	graph.AddNode("stage2", stage2, nil, nil)

	// Add edge without filter
	err := graph.AddEdge("stage1", "stage2", nil)
	if err != nil {
		t.Fatalf("failed to add edge: %v", err)
	}

	edge := graph.GetNode("stage1").Outputs()[0]

	// Should forward all event types
	if !edge.ShouldForwardEvent(core.EventTypeSTT) {
		t.Error("edge should forward STT events")
	}
	if !edge.ShouldForwardEvent(core.EventTypeLLM) {
		t.Error("edge should forward LLM events")
	}
	if !edge.ShouldForwardEvent(core.EventTypeAudio) {
		t.Error("edge should forward Audio events")
	}
}

// TestGraphEdgeInvalidNodeStandalone tests edge creation with non-existent nodes
func TestGraphEdgeInvalidNodeStandalone(t *testing.T) {
	graph := NewPipelineGraph()
	stage := &MockStage{name: "stage1"}
	graph.AddNode("stage1", stage, nil, nil)

	// Try to add edge from non-existent node
	err := graph.AddEdge("nonexistent", "stage1", nil)
	if err == nil {
		t.Fatal("expected error for non-existent source node")
	}

	// Try to add edge to non-existent node
	err = graph.AddEdge("stage1", "nonexistent", nil)
	if err == nil {
		t.Fatal("expected error for non-existent destination node")
	}
}

// TestGraphEntryNodeStandalone tests entry node setting
func TestGraphEntryNodeStandalone(t *testing.T) {
	graph := NewPipelineGraph()
	stage := &MockStage{name: "entry"}
	graph.AddNode("entry", stage, nil, nil)

	err := graph.SetEntryNode("entry")
	if err != nil {
		t.Fatalf("failed to set entry node: %v", err)
	}

	entryNode := graph.GetEntryNode()
	if entryNode == nil {
		t.Fatal("entry node is nil")
	}

	if entryNode.Name() != "entry" {
		t.Errorf("expected entry node name 'entry', got %q", entryNode.Name())
	}
}

// TestGraphExitNodesStandalone tests exit node management
func TestGraphExitNodesStandalone(t *testing.T) {
	graph := NewPipelineGraph()
	stage1 := &MockStage{name: "stage1"}
	stage2 := &MockStage{name: "stage2"}

	graph.AddNode("stage1", stage1, nil, nil)
	graph.AddNode("stage2", stage2, nil, nil)

	err := graph.AddExitNode("stage1")
	if err != nil {
		t.Fatalf("failed to add exit node: %v", err)
	}

	err = graph.AddExitNode("stage2")
	if err != nil {
		t.Fatalf("failed to add exit node: %v", err)
	}

	exitNodes := graph.GetExitNodes()
	if len(exitNodes) != 2 {
		t.Errorf("expected 2 exit nodes, got %d", len(exitNodes))
	}
}

// TestValidateGraphRejectsCyclesStandalone tests cycle detection
func TestValidateGraphRejectsCyclesStandalone(t *testing.T) {
	graph := NewPipelineGraph()

	// Create a simple cycle: A -> B -> C -> A
	stageA := &MockStage{name: "A"}
	stageB := &MockStage{name: "B"}
	stageC := &MockStage{name: "C"}

	graph.AddNode("A", stageA, nil, nil)
	graph.AddNode("B", stageB, nil, nil)
	graph.AddNode("C", stageC, nil, nil)

	graph.AddEdge("A", "B", nil)
	graph.AddEdge("B", "C", nil)
	graph.AddEdge("C", "A", nil) // Creates cycle

	graph.SetEntryNode("A")

	err := ValidateGraph(graph)
	if err == nil {
		t.Fatal("expected cycle detection error, got nil")
	}
}

// TestValidateGraphRejectsUnreachableStandalone tests unreachable stage detection
func TestValidateGraphRejectsUnreachableStandalone(t *testing.T) {
	graph := NewPipelineGraph()

	// Create disconnected stages
	stageA := &MockStage{name: "A"}
	stageB := &MockStage{name: "B"}
	stageC := &MockStage{name: "C"}

	graph.AddNode("A", stageA, nil, nil)
	graph.AddNode("B", stageB, nil, nil)
	graph.AddNode("C", stageC, nil, nil)

	// Only connect A -> B, leaving C unreachable
	graph.AddEdge("A", "B", nil)

	graph.SetEntryNode("A")

	err := ValidateGraph(graph)
	if err == nil {
		t.Fatal("expected unreachable stage error, got nil")
	}
}

// TestValidateGraphConstructsValidTopologyStandalone tests valid graph construction
func TestValidateGraphConstructsValidTopologyStandalone(t *testing.T) {
	graph := NewPipelineGraph()

	// Create a valid linear pipeline: A -> B -> C
	stageA := &MockStage{name: "A"}
	stageB := &MockStage{name: "B"}
	stageC := &MockStage{name: "C"}

	graph.AddNode("A", stageA, nil, nil)
	graph.AddNode("B", stageB, nil, nil)
	graph.AddNode("C", stageC, nil, nil)

	graph.AddEdge("A", "B", nil)
	graph.AddEdge("B", "C", nil)

	graph.SetEntryNode("A")
	graph.AddExitNode("C")

	err := ValidateGraph(graph)
	if err != nil {
		t.Fatalf("valid graph failed validation: %v", err)
	}

	// Verify structure
	nodeA := graph.GetNode("A")
	if len(nodeA.Outputs()) != 1 {
		t.Fatalf("node A should have 1 output, got %d", len(nodeA.Outputs()))
	}

	nodeB := graph.GetNode("B")
	if len(nodeB.Inputs()) != 1 {
		t.Fatalf("node B should have 1 input, got %d", len(nodeB.Inputs()))
	}
	if len(nodeB.Outputs()) != 1 {
		t.Fatalf("node B should have 1 output, got %d", len(nodeB.Outputs()))
	}
}

// TestTypeCompatibilityValidationStandalone tests type compatibility
func TestTypeCompatibilityValidationStandalone(t *testing.T) {
	graph := NewPipelineGraph()

	// Create stages with incompatible types
	stageA := &MockStage{
		name:        "A",
		outputTypes: []core.EventType{core.EventTypeSTT},
	}
	stageB := &MockStage{
		name:       "B",
		inputTypes: []core.EventType{core.EventTypeLLM}, // Incompatible
	}

	graph.AddNode("A", stageA, nil, nil)
	graph.AddNode("B", stageB, nil, nil)

	// Connect with filter that doesn't match downstream input
	graph.AddEdge("A", "B", []core.EventType{core.EventTypeSTT})

	graph.SetEntryNode("A")

	err := ValidateGraph(graph)
	if err == nil {
		t.Fatal("expected type compatibility error, got nil")
	}
}

// TestTypeCompatibilityWithWildcardStandalone tests that wildcard types are compatible
func TestTypeCompatibilityWithWildcardStandalone(t *testing.T) {
	graph := NewPipelineGraph()

	// Stage A produces STT
	stageA := &MockStage{
		name:        "A",
		outputTypes: []core.EventType{core.EventTypeSTT},
	}

	// Stage B accepts all types (empty input types)
	stageB := &MockStage{
		name:       "B",
		inputTypes: []core.EventType{}, // Accepts all
	}

	graph.AddNode("A", stageA, nil, nil)
	graph.AddNode("B", stageB, nil, nil)

	graph.AddEdge("A", "B", nil)
	graph.SetEntryNode("A")

	err := ValidateGraph(graph)
	if err != nil {
		t.Fatalf("wildcard input should be compatible: %v", err)
	}
}

// TestTypeCompatibilityWithFilterStandalone tests type compatibility with event filters
func TestTypeCompatibilityWithFilterStandalone(t *testing.T) {
	graph := NewPipelineGraph()

	// Stage A produces multiple types
	stageA := &MockStage{
		name:        "A",
		outputTypes: []core.EventType{core.EventTypeSTT, core.EventTypeLLM},
	}

	// Stage B accepts LLM
	stageB := &MockStage{
		name:       "B",
		inputTypes: []core.EventType{core.EventTypeLLM},
	}

	graph.AddNode("A", stageA, nil, nil)
	graph.AddNode("B", stageB, nil, nil)

	// Filter to only forward LLM events
	graph.AddEdge("A", "B", []core.EventType{core.EventTypeLLM})
	graph.SetEntryNode("A")

	err := ValidateGraph(graph)
	if err != nil {
		t.Fatalf("compatible types with filter should pass: %v", err)
	}
}
