package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/madmike/go-pipeline/core"
	"pgregory.net/rapid"
)

// TestBarrierBasicSynchronization tests that barrier waits for all upstream branches
func TestBarrierBasicSynchronization(t *testing.T) {
	config := &core.BarrierConfig{
		UpstreamCount: 2,
		MergeStrategy: core.MergeStrategyCollect,
	}

	barrier := NewBarrierStage("barrier", config)

	// Create input channel with events from 2 branches
	input := make(chan core.Event, 10)
	output := make(chan core.Event, 10)

	go func() {
		// Events from branch 1
		input <- core.StatusEvent{Status: core.StatusListening}
		input <- core.STTEvent{Text: "hello"}
		input <- core.DoneEvent{}

		// Events from branch 2
		input <- core.LLMEvent{Delta: "world"}
		input <- core.DoneEvent{}

		close(input)
	}()

	// Run barrier
	err := barrier.Process(context.Background(), input, output)
	if err != nil {
		t.Fatalf("barrier process failed: %v", err)
	}

	// Collect output events
	var outputEvents []core.Event
	for event := range output {
		outputEvents = append(outputEvents, event)
	}

	// Should have received all non-Done events plus one consolidated Done
	if len(outputEvents) < 4 {
		t.Errorf("expected at least 4 events, got %d", len(outputEvents))
	}

	// Last event should be DoneEvent
	if _, ok := outputEvents[len(outputEvents)-1].(core.DoneEvent); !ok {
		t.Error("last event should be DoneEvent")
	}
}

// TestBarrierFailFastOnError tests that barrier propagates errors immediately
func TestBarrierFailFastOnError(t *testing.T) {
	config := &core.BarrierConfig{
		UpstreamCount: 2,
		MergeStrategy: core.MergeStrategyCollect,
	}

	barrier := NewBarrierStage("barrier", config)

	// Create input channel with an error event
	input := make(chan core.Event, 10)
	output := make(chan core.Event, 10)

	go func() {
		// Events from branch 1
		input <- core.StatusEvent{Status: core.StatusListening}
		// Error from branch 1
		input <- core.ErrorEvent{Error: errors.New("branch 1 failed"), Retryable: false}
		// This DoneEvent should not be processed
		input <- core.DoneEvent{}

		close(input)
	}()

	// Run barrier
	err := barrier.Process(context.Background(), input, output)
	if err == nil {
		t.Fatal("expected error from barrier")
	}

	if err.Error() != "branch 1 failed" {
		t.Errorf("expected 'branch 1 failed', got %q", err.Error())
	}
}

// TestBarrierCollectsEvents tests that barrier collects events from all branches
func TestBarrierCollectsEvents(t *testing.T) {
	config := &core.BarrierConfig{
		UpstreamCount: 2,
		MergeStrategy: core.MergeStrategyCollect,
	}

	barrier := NewBarrierStage("barrier", config)

	// Create input channel
	input := make(chan core.Event, 10)
	output := make(chan core.Event, 10)

	go func() {
		// Events from branch 1
		input <- core.StatusEvent{Status: core.StatusListening, Message: "branch1"}
		input <- core.STTEvent{Text: "hello"}
		input <- core.DoneEvent{}

		// Events from branch 2
		input <- core.LLMEvent{Delta: "response"}
		input <- core.DoneEvent{}

		close(input)
	}()

	// Run barrier
	err := barrier.Process(context.Background(), input, output)
	if err != nil {
		t.Fatalf("barrier process failed: %v", err)
	}

	// Collect output events
	var outputEvents []core.Event
	for event := range output {
		outputEvents = append(outputEvents, event)
	}

	// Verify we got events from both branches
	hasStatus := false
	hasSTT := false
	hasLLM := false
	hasDone := false

	for _, event := range outputEvents {
		switch event.(type) {
		case core.StatusEvent:
			hasStatus = true
		case core.STTEvent:
			hasSTT = true
		case core.LLMEvent:
			hasLLM = true
		case core.DoneEvent:
			hasDone = true
		}
	}

	if !hasStatus {
		t.Error("missing StatusEvent from branch 1")
	}
	if !hasSTT {
		t.Error("missing STTEvent from branch 1")
	}
	if !hasLLM {
		t.Error("missing LLMEvent from branch 2")
	}
	if !hasDone {
		t.Error("missing consolidated DoneEvent")
	}
}

// TestBarrierConsolidatesDoneEvents tests that barrier emits single DoneEvent
func TestBarrierConsolidatesDoneEvents(t *testing.T) {
	config := &core.BarrierConfig{
		UpstreamCount: 3,
		MergeStrategy: core.MergeStrategyCollect,
	}

	barrier := NewBarrierStage("barrier", config)

	// Create input channel
	input := make(chan core.Event, 10)
	output := make(chan core.Event, 10)

	go func() {
		// Events from branch 1
		input <- core.DoneEvent{FullText: "text1"}
		// Events from branch 2
		input <- core.DoneEvent{FullText: "text2"}
		// Events from branch 3
		input <- core.DoneEvent{FullText: "text3"}

		close(input)
	}()

	// Run barrier
	err := barrier.Process(context.Background(), input, output)
	if err != nil {
		t.Fatalf("barrier process failed: %v", err)
	}

	// Collect output events
	var outputEvents []core.Event
	for event := range output {
		outputEvents = append(outputEvents, event)
	}

	// Should have exactly one DoneEvent
	doneCount := 0
	for _, event := range outputEvents {
		if _, ok := event.(core.DoneEvent); ok {
			doneCount++
		}
	}

	if doneCount != 1 {
		t.Errorf("expected 1 DoneEvent, got %d", doneCount)
	}
}

// TestBarrierMissingDoneEvent tests that barrier fails if not all branches send DoneEvent
func TestBarrierMissingDoneEvent(t *testing.T) {
	config := &core.BarrierConfig{
		UpstreamCount: 2,
		MergeStrategy: core.MergeStrategyCollect,
	}

	barrier := NewBarrierStage("barrier", config)

	// Create input channel with only one DoneEvent
	input := make(chan core.Event, 10)
	output := make(chan core.Event, 10)

	go func() {
		input <- core.StatusEvent{Status: core.StatusListening}
		input <- core.DoneEvent{} // Only one DoneEvent, but expecting 2

		close(input)
	}()

	// Run barrier
	err := barrier.Process(context.Background(), input, output)
	if err == nil {
		t.Fatal("expected error when DoneEvent count doesn't match")
	}
}

// TestBarrierContextCancellation tests that barrier respects context cancellation
func TestBarrierContextCancellation(t *testing.T) {
	config := &core.BarrierConfig{
		UpstreamCount: 2,
		MergeStrategy: core.MergeStrategyCollect,
	}

	barrier := NewBarrierStage("barrier", config)

	// Create input channel
	input := make(chan core.Event, 10)
	output := make(chan core.Event, 10)

	// Create a cancellable context
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		input <- core.StatusEvent{Status: core.StatusListening}
		// Cancel context before sending more events
		time.Sleep(10 * time.Millisecond)
		cancel()
		close(input)
	}()

	// Run barrier
	err := barrier.Process(ctx, input, output)
	if err != context.Canceled {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

// Property 4: Barrier waits for all upstream branches
// **Feature: pipeline-parallel-routing, Property 4: Barrier waits for all upstream branches**
// **Validates: Requirements 2.1, 1.4**
func TestPropertyBarrierWaitsForAllUpstream(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		upstreamCount := rapid.IntRange(1, 5).Draw(rt, "upstreamCount")

		config := &core.BarrierConfig{
			UpstreamCount: upstreamCount,
			MergeStrategy: core.MergeStrategyCollect,
		}

		barrier := NewBarrierStage("barrier", config)

		// Create input channel
		input := make(chan core.Event, 100)
		output := make(chan core.Event, 100)

		go func() {
			// Send DoneEvent from each upstream branch
			for i := 0; i < upstreamCount; i++ {
				input <- core.DoneEvent{}
			}
			close(input)
		}()

		// Run barrier
		err := barrier.Process(context.Background(), input, output)
		if err != nil {
			rt.Fatalf("barrier process failed: %v", err)
		}

		// Collect output events
		var outputEvents []core.Event
		for event := range output {
			outputEvents = append(outputEvents, event)
		}

		// Should have exactly one DoneEvent
		doneCount := 0
		for _, event := range outputEvents {
			if _, ok := event.(core.DoneEvent); ok {
				doneCount++
			}
		}

		if doneCount != 1 {
			rt.Fatalf("expected 1 DoneEvent, got %d", doneCount)
		}
	})
}

// Property 5: Barrier collects events from all branches
// **Feature: pipeline-parallel-routing, Property 5: Barrier collects events from all branches**
// **Validates: Requirements 2.2**
func TestPropertyBarrierCollectsFromAllBranches(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		upstreamCount := rapid.IntRange(1, 5).Draw(rt, "upstreamCount")
		eventsPerBranch := rapid.IntRange(1, 5).Draw(rt, "eventsPerBranch")

		config := &core.BarrierConfig{
			UpstreamCount: upstreamCount,
			MergeStrategy: core.MergeStrategyCollect,
		}

		barrier := NewBarrierStage("barrier", config)

		// Create input channel
		input := make(chan core.Event, 1000)
		output := make(chan core.Event, 1000)

		go func() {
			// Send events from each upstream branch
			for i := 0; i < upstreamCount; i++ {
				for j := 0; j < eventsPerBranch; j++ {
					input <- core.StatusEvent{
						Status:  core.StatusListening,
						Message: "event",
					}
				}
				input <- core.DoneEvent{}
			}
			close(input)
		}()

		// Run barrier
		err := barrier.Process(context.Background(), input, output)
		if err != nil {
			rt.Fatalf("barrier process failed: %v", err)
		}

		// Collect output events
		var outputEvents []core.Event
		for event := range output {
			outputEvents = append(outputEvents, event)
		}

		// Should have events from all branches
		statusCount := 0
		for _, event := range outputEvents {
			if _, ok := event.(core.StatusEvent); ok {
				statusCount++
			}
		}

		expectedStatusCount := upstreamCount * eventsPerBranch
		if statusCount != expectedStatusCount {
			rt.Fatalf("expected %d StatusEvents, got %d", expectedStatusCount, statusCount)
		}
	})
}

// Barrier fail-fast on upstream error
func TestPropertyBarrierFailFastOnError(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		config := &core.BarrierConfig{
			UpstreamCount: 2,
			MergeStrategy: core.MergeStrategyCollect,
		}

		barrier := NewBarrierStage("barrier", config)

		// Create input channel
		input := make(chan core.Event, 10)
		output := make(chan core.Event, 10)

		go func() {
			// Send some events
			input <- core.StatusEvent{Status: core.StatusListening}
			// Send error
			input <- core.ErrorEvent{Error: errors.New("upstream failed"), Retryable: false}
			// These should not be processed
			input <- core.DoneEvent{}
			input <- core.DoneEvent{}
			close(input)
		}()

		// Run barrier
		err := barrier.Process(context.Background(), input, output)
		if err == nil {
			rt.Fatalf("expected error from barrier")
		}

		if err.Error() != "upstream failed" {
			rt.Fatalf("expected 'upstream failed', got %q", err.Error())
		}
	})
}

// Barrier consolidates DoneEvents
func TestPropertyBarrierConsolidatesDoneEvents(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		upstreamCount := rapid.IntRange(1, 5).Draw(rt, "upstreamCount")

		config := &core.BarrierConfig{
			UpstreamCount: upstreamCount,
			MergeStrategy: core.MergeStrategyCollect,
		}

		barrier := NewBarrierStage("barrier", config)

		// Create input channel
		input := make(chan core.Event, 100)
		output := make(chan core.Event, 100)

		go func() {
			// Send DoneEvent from each upstream branch with different data
			for i := 0; i < upstreamCount; i++ {
				input <- core.DoneEvent{
					FullText:      "text",
					TokensUsed:    i,
					AudioDuration: float64(i),
					ActionsCount:  i,
				}
			}
			close(input)
		}()

		// Run barrier
		err := barrier.Process(context.Background(), input, output)
		if err != nil {
			rt.Fatalf("barrier process failed: %v", err)
		}

		// Collect output events
		var outputEvents []core.Event
		for event := range output {
			outputEvents = append(outputEvents, event)
		}

		// Should have exactly one DoneEvent
		doneCount := 0
		for _, event := range outputEvents {
			if _, ok := event.(core.DoneEvent); ok {
				doneCount++
			}
		}

		if doneCount != 1 {
			rt.Fatalf("expected 1 consolidated DoneEvent, got %d", doneCount)
		}
	})
}
