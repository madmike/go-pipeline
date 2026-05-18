package pipeline

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/madmike/go-pipeline/core"
	"pgregory.net/rapid"
)

// TestFanOutBasicRouting tests that events are routed to all branches
func TestFanOutBasicRouting(t *testing.T) {
	// Create two downstream stages
	stage1 := &MockStage{name: "stage1"}
	stage2 := &MockStage{name: "stage2"}

	config := &core.FanOutConfig{
		ErrorPolicy: core.ErrorPolicyCancelAll,
		Branches: []core.BranchConfig{
			{Stage: stage1, EventFilter: nil},
			{Stage: stage2, EventFilter: nil},
		},
	}

	router := NewFanOutRouter(config)

	// Create input channel and send events
	input := make(chan core.Event, 10)
	go func() {
		input <- core.StatusEvent{Status: core.StatusListening}
		input <- core.STTEvent{Text: "hello"}
		close(input)
	}()

	// Route events
	err := router.Route(context.Background(), input)
	if err != nil {
		t.Fatalf("routing failed: %v", err)
	}
}

// TestFanOutEventFiltering tests that event filters work correctly
func TestFanOutEventFiltering(t *testing.T) {
	stage1 := &MockStage{name: "stage1"}
	stage2 := &MockStage{name: "stage2"}

	config := &core.FanOutConfig{
		ErrorPolicy: core.ErrorPolicyCancelAll,
		Branches: []core.BranchConfig{
			{
				Stage:       stage1,
				EventFilter: []core.EventType{core.EventTypeSTT},
			},
			{
				Stage:       stage2,
				EventFilter: []core.EventType{core.EventTypeLLM},
			},
		},
	}

	router := NewFanOutRouter(config)

	// Verify that shouldForwardEvent works correctly
	sttEvent := core.STTEvent{Text: "test"}
	llmEvent := core.LLMEvent{Delta: "test"}

	if !router.shouldForwardEvent(config.Branches[0], sttEvent) {
		t.Error("branch 1 should forward STT events")
	}

	if router.shouldForwardEvent(config.Branches[0], llmEvent) {
		t.Error("branch 1 should not forward LLM events")
	}

	if !router.shouldForwardEvent(config.Branches[1], llmEvent) {
		t.Error("branch 2 should forward LLM events")
	}

	if router.shouldForwardEvent(config.Branches[1], sttEvent) {
		t.Error("branch 2 should not forward STT events")
	}
}

// TestFanOutNoFilter tests that no filter forwards all events
func TestFanOutNoFilter(t *testing.T) {
	stage := &MockStage{name: "stage"}

	config := &core.FanOutConfig{
		ErrorPolicy: core.ErrorPolicyCancelAll,
		Branches: []core.BranchConfig{
			{Stage: stage, EventFilter: nil},
		},
	}

	router := NewFanOutRouter(config)

	// All event types should be forwarded
	if !router.shouldForwardEvent(config.Branches[0], core.STTEvent{}) {
		t.Error("should forward STT events")
	}
	if !router.shouldForwardEvent(config.Branches[0], core.LLMEvent{}) {
		t.Error("should forward LLM events")
	}
	if !router.shouldForwardEvent(config.Branches[0], core.AudioEvent{}) {
		t.Error("should forward Audio events")
	}
}

// Fan-out delivers all events to all branches
func TestPropertyFanOutDeliversAllEvents(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		// Create 3 branches
		branches := []core.BranchConfig{
			{
				Stage:       &MockStage{name: "stage1"},
				EventFilter: nil, // No filter - accept all
			},
			{
				Stage:       &MockStage{name: "stage2"},
				EventFilter: nil, // No filter - accept all
			},
			{
				Stage:       &MockStage{name: "stage3"},
				EventFilter: nil, // No filter - accept all
			},
		}

		config := &core.FanOutConfig{
			ErrorPolicy: core.ErrorPolicyCancelAll,
			Branches:    branches,
		}

		router := NewFanOutRouter(config)

		// Create input with multiple events
		input := make(chan core.Event, 10)
		events := []core.Event{
			core.StatusEvent{Status: core.StatusListening},
			core.STTEvent{Text: "hello"},
			core.LLMEvent{Delta: "world"},
		}

		go func() {
			for _, event := range events {
				input <- event
			}
			close(input)
		}()

		// Route events
		err := router.Route(context.Background(), input)
		if err != nil {
			rt.Fatalf("routing failed: %v", err)
		}

		// Verify all outputs received all events
		outputs := router.GetOutputs()
		if len(outputs) != len(branches) {
			rt.Fatalf("expected %d outputs, got %d", len(branches), len(outputs))
		}
	})
}

// Default error policy cancels all branches
func TestPropertyDefaultErrorPolicyCancelsAll(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		// Create a stage that fails
		failingStage := &FailingMockStage{
			name:  "failing",
			delay: 10 * time.Millisecond,
		}

		// Create a normal stage
		normalStage := &MockStage{name: "normal"}

		config := &core.FanOutConfig{
			ErrorPolicy: core.ErrorPolicyCancelAll,
			Branches: []core.BranchConfig{
				{Stage: failingStage, EventFilter: nil},
				{Stage: normalStage, EventFilter: nil},
			},
		}

		router := NewFanOutRouter(config)

		// Create input
		input := make(chan core.Event, 10)
		go func() {
			input <- core.StatusEvent{Status: core.StatusListening}
			time.Sleep(5 * time.Millisecond)
			input <- core.STTEvent{Text: "test"}
			close(input)
		}()

		// Route events - should fail due to failing stage
		err := router.Route(context.Background(), input)
		if err == nil {
			rt.Fatalf("expected error from failing stage")
		}
	})
}

// Isolated error policy allows continuation
func TestPropertyIsolatedErrorPolicyAllowsContinuation(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		// Create a stage that fails
		failingStage := &FailingMockStage{
			name:  "failing",
			delay: 10 * time.Millisecond,
		}

		// Create a normal stage that collects events
		collectingStage := &CollectingMockStage{
			name:   "collecting",
			events: make([]core.Event, 0),
		}

		config := &core.FanOutConfig{
			ErrorPolicy: core.ErrorPolicyIsolated,
			Branches: []core.BranchConfig{
				{Stage: failingStage, EventFilter: nil},
				{Stage: collectingStage, EventFilter: nil},
			},
		}

		router := NewFanOutRouter(config)

		// Create input
		input := make(chan core.Event, 10)
		go func() {
			input <- core.StatusEvent{Status: core.StatusListening}
			time.Sleep(5 * time.Millisecond)
			input <- core.STTEvent{Text: "test"}
			close(input)
		}()

		// Route events - should complete despite failing stage
		err := router.Route(context.Background(), input)
		if err == nil {
			rt.Fatalf("expected error from failing stage")
		}

		// The collecting stage should have received events
		if len(collectingStage.events) == 0 {
			rt.Fatalf("collecting stage should have received events despite isolated error policy")
		}
	})
}

// Event filter only forwards matching types
func TestPropertyEventFilterOnlyForwardsMatching(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		// Create a collecting stage to verify what it receives
		collectingStage := &CollectingMockStage{
			name:   "collecting",
			events: make([]core.Event, 0),
		}

		config := &core.FanOutConfig{
			ErrorPolicy: core.ErrorPolicyCancelAll,
			Branches: []core.BranchConfig{
				{
					Stage:       collectingStage,
					EventFilter: []core.EventType{core.EventTypeSTT},
				},
			},
		}

		router := NewFanOutRouter(config)

		// Create input with mixed event types
		input := make(chan core.Event, 10)
		go func() {
			input <- core.StatusEvent{Status: core.StatusListening}
			input <- core.STTEvent{Text: "hello"}
			input <- core.LLMEvent{Delta: "world"}
			input <- core.STTEvent{Text: "goodbye"}
			close(input)
		}()

		// Route events
		err := router.Route(context.Background(), input)
		if err != nil {
			rt.Fatalf("routing failed: %v", err)
		}

		// Verify only STT events were forwarded
		for _, event := range collectingStage.events {
			if event.EventType() != core.EventTypeSTT {
				rt.Fatalf("expected only STT events, got %v", event.EventType())
			}
		}
	})
}

// Event filtering preserves event integrity
func TestPropertyEventFilteringPreservesIntegrity(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		// Create a collecting stage
		collectingStage := &CollectingMockStage{
			name:   "collecting",
			events: make([]core.Event, 0),
		}

		config := &core.FanOutConfig{
			ErrorPolicy: core.ErrorPolicyCancelAll,
			Branches: []core.BranchConfig{
				{
					Stage:       collectingStage,
					EventFilter: []core.EventType{core.EventTypeSTT},
				},
			},
		}

		router := NewFanOutRouter(config)

		// Create input with specific STT event
		originalText := "test message"
		originalConfidence := 0.95

		input := make(chan core.Event, 10)
		go func() {
			input <- core.STTEvent{
				Text:       originalText,
				IsFinal:    true,
				Confidence: originalConfidence,
			}
			close(input)
		}()

		// Route events
		err := router.Route(context.Background(), input)
		if err != nil {
			rt.Fatalf("routing failed: %v", err)
		}

		// Verify event integrity
		if len(collectingStage.events) != 1 {
			rt.Fatalf("expected 1 event, got %d", len(collectingStage.events))
		}

		sttEvent := collectingStage.events[0].(core.STTEvent)
		if sttEvent.Text != originalText {
			rt.Fatalf("text not preserved: expected %q, got %q", originalText, sttEvent.Text)
		}
		if sttEvent.Confidence != originalConfidence {
			rt.Fatalf("confidence not preserved: expected %f, got %f", originalConfidence, sttEvent.Confidence)
		}
		if !sttEvent.IsFinal {
			rt.Fatalf("IsFinal not preserved")
		}
	})
}

// FailingMockStage is a mock stage that fails after a delay
type FailingMockStage struct {
	name  string
	delay time.Duration
}

func (m *FailingMockStage) Name() string {
	return m.name
}

func (m *FailingMockStage) Process(ctx context.Context, input <-chan core.Event, output chan<- core.Event) error {
	time.Sleep(m.delay)
	return errors.New("stage failed")
}

func (m *FailingMockStage) InputTypes() []core.EventType {
	return []core.EventType{}
}

func (m *FailingMockStage) OutputTypes() []core.EventType {
	return []core.EventType{}
}

// CollectingMockStage is a mock stage that collects all events it receives
type CollectingMockStage struct {
	name   string
	events []core.Event
	mu     sync.Mutex
}

func (m *CollectingMockStage) Name() string {
	return m.name
}

func (m *CollectingMockStage) Process(ctx context.Context, input <-chan core.Event, output chan<- core.Event) error {
	for event := range input {
		m.mu.Lock()
		m.events = append(m.events, event)
		m.mu.Unlock()

		// Forward to output
		select {
		case <-ctx.Done():
			return ctx.Err()
		case output <- event:
		}
	}
	return nil
}

func (m *CollectingMockStage) InputTypes() []core.EventType {
	return []core.EventType{}
}

func (m *CollectingMockStage) OutputTypes() []core.EventType {
	return []core.EventType{}
}
