package pipeline

import (
	"testing"

	"github.com/madmike/go-pipeline/core"
)

// TestGraphBuilderAddStage tests adding stages to the builder
func TestGraphBuilderAddStage(t *testing.T) {
	builder := NewBuilder()

	stage := &MockStage{
		name:        "test-stage",
		inputTypes:  []core.EventType{core.EventTypeSTT},
		outputTypes: []core.EventType{core.EventTypeLLM},
	}

	builder.AddStage("stage1", stage)
	builder.SetEntryNode("stage1")
	builder.AddExitNode("stage1")

	pipeline, err := builder.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	if pipeline == nil {
		t.Fatal("Pipeline is nil")
	}

	if pipeline.graph == nil {
		t.Fatal("Pipeline graph is nil")
	}
}

// TestGraphBuilderConnect tests connecting stages
func TestGraphBuilderConnect(t *testing.T) {
	builder := NewBuilder()

	stage1 := &MockStage{
		name:        "stage1",
		outputTypes: []core.EventType{core.EventTypeSTT},
	}
	stage2 := &MockStage{
		name:       "stage2",
		inputTypes: []core.EventType{core.EventTypeSTT},
	}

	builder.AddStage("stage1", stage1)
	builder.AddStage("stage2", stage2)
	builder.Connect("stage1", "stage2")
	builder.SetEntryNode("stage1")
	builder.AddExitNode("stage2")

	pipeline, err := builder.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	if pipeline == nil {
		t.Fatal("Pipeline is nil")
	}
}

// TestGraphBuilderConnectWithFilter tests connecting stages with event filter
func TestGraphBuilderConnectWithFilter(t *testing.T) {
	builder := NewBuilder()

	stage1 := &MockStage{
		name:        "stage1",
		outputTypes: []core.EventType{core.EventTypeSTT, core.EventTypeLLM},
	}
	stage2 := &MockStage{
		name:       "stage2",
		inputTypes: []core.EventType{core.EventTypeLLM},
	}

	builder.AddStage("stage1", stage1)
	builder.AddStage("stage2", stage2)
	builder.Connect("stage1", "stage2", core.EventTypeLLM)
	builder.SetEntryNode("stage1")
	builder.AddExitNode("stage2")

	pipeline, err := builder.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	if pipeline == nil {
		t.Fatal("Pipeline is nil")
	}
}

// TestGraphBuilderEmptyPipeline tests that empty pipeline fails
func TestGraphBuilderEmptyPipeline(t *testing.T) {
	builder := NewBuilder()

	_, err := builder.Build()
	if err == nil {
		t.Fatal("Expected error for empty pipeline, got nil")
	}
}

// TestGraphBuilderNoEntryNode tests that missing entry node fails
func TestGraphBuilderNoEntryNode(t *testing.T) {
	builder := NewBuilder()

	stage := &MockStage{name: "stage1"}
	builder.AddStage("stage1", stage)

	_, err := builder.Build()
	if err == nil {
		t.Fatal("Expected error for missing entry node, got nil")
	}
}

// TestGraphBuilderFluentAPI tests fluent API chaining
func TestGraphBuilderFluentAPI(t *testing.T) {
	stage1 := &MockStage{
		name:        "stage1",
		outputTypes: []core.EventType{core.EventTypeSTT},
	}
	stage2 := &MockStage{
		name:       "stage2",
		inputTypes: []core.EventType{core.EventTypeSTT},
	}

	pipeline, err := NewBuilder().
		AddStage("stage1", stage1).
		AddStage("stage2", stage2).
		Connect("stage1", "stage2").
		SetEntryNode("stage1").
		AddExitNode("stage2").
		Build()

	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	if pipeline == nil {
		t.Fatal("Pipeline is nil")
	}
}

// TestGraphBuilderFanOut tests adding a fan-out node
func TestGraphBuilderFanOut(t *testing.T) {
	builder := NewBuilder()

	stage1 := &MockStage{
		name:        "stage1",
		outputTypes: []core.EventType{core.EventTypeSTT},
	}
	stage2 := &MockStage{
		name:       "stage2",
		inputTypes: []core.EventType{core.EventTypeSTT},
	}
	stage3 := &MockStage{
		name:       "stage3",
		inputTypes: []core.EventType{core.EventTypeSTT},
	}

	fanOutConfig := core.FanOutConfig{
		ErrorPolicy: core.ErrorPolicyCancelAll,
		Branches: []core.BranchConfig{
			{Stage: stage2, EventFilter: nil},
			{Stage: stage3, EventFilter: nil},
		},
	}

	builder.AddStage("stage1", stage1)
	builder.AddFanOut("fanout", fanOutConfig)
	builder.Connect("stage1", "fanout")
	builder.SetEntryNode("stage1")
	builder.AddExitNode("fanout")

	pipeline, err := builder.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	if pipeline == nil {
		t.Fatal("Pipeline is nil")
	}
}

// TestGraphBuilderBarrier tests adding a barrier node
func TestGraphBuilderBarrier(t *testing.T) {
	builder := NewBuilder()

	stage1 := &MockStage{
		name:        "stage1",
		outputTypes: []core.EventType{core.EventTypeSTT},
	}
	stage2 := &MockStage{
		name:       "stage2",
		inputTypes: []core.EventType{core.EventTypeSTT},
	}

	barrierConfig := core.BarrierConfig{
		UpstreamCount: 1,
		MergeStrategy: core.MergeStrategyCollect,
	}

	builder.AddStage("stage1", stage1)
	builder.AddStage("stage2", stage2)
	builder.AddBarrier("barrier", barrierConfig)
	builder.Connect("stage1", "barrier")
	builder.Connect("barrier", "stage2")
	builder.SetEntryNode("stage1")
	builder.AddExitNode("stage2")

	pipeline, err := builder.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	if pipeline == nil {
		t.Fatal("Pipeline is nil")
	}
}

// TestGraphBuilderSetErrorPolicy tests setting error policy
func TestGraphBuilderSetErrorPolicy(t *testing.T) {
	builder := NewBuilder()

	stage1 := &MockStage{
		name:        "stage1",
		outputTypes: []core.EventType{core.EventTypeSTT},
	}

	fanOutConfig := core.FanOutConfig{
		ErrorPolicy: core.ErrorPolicyCancelAll,
		Branches:    []core.BranchConfig{},
	}

	builder.AddStage("stage1", stage1)
	builder.AddFanOut("fanout", fanOutConfig)
	builder.SetErrorPolicy("fanout", core.ErrorPolicyIsolated)
	builder.Connect("stage1", "fanout")
	builder.SetEntryNode("stage1")
	builder.AddExitNode("fanout")

	pipeline, err := builder.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	if pipeline == nil {
		t.Fatal("Pipeline is nil")
	}
}
