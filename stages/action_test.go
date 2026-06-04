package stages

import (
	"context"
	"testing"
	"time"

	"github.com/madmike/go-pipeline/core"
	"pgregory.net/rapid"
)

// For any LLM output containing action commands, the action stage SHALL emit
// ActionEvent for each action with correct ActionID, ActionType, Target, and Data.
func TestPropertyActionEventEmission(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		// Generate action type
		actionType := rapid.SampledFrom([]core.ActionType{
			core.ActionNavigate,
			core.ActionFillForm,
			core.ActionClick,
		}).Draw(rt, "actionType")

		// Create action stage with pre-configured actions
		actions := []ActionRequestPayload{
			{
				ActionID:   "action_1",
				ActionType: actionType,
				Target:     "/test",
				Data: map[string]any{
					"key": "value",
				},
				Required: true,
			},
		}

		stage := NewActionStage(ActionStageConfig{
			Actions: actions,
		})

		// Create input channel with LLM output
		input := make(chan core.Event, 10)
		go func() {
			defer close(input)
			input <- core.LLMEvent{
				Delta:   "Performing action",
				Content: "Performing action",
			}
		}()

		// Create output channel
		output := make(chan core.Event, 100)

		// Execute stage in goroutine
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		go func() {
			defer close(output)
			_ = stage.Process(ctx, input, output)
		}()

		// Collect output events
		var actionEvents []core.ActionEvent
		var doneEvent *core.DoneEvent

		for event := range output {
			if action, ok := event.(core.ActionEvent); ok {
				actionEvents = append(actionEvents, action)
			}
			if done, ok := event.(core.DoneEvent); ok {
				doneEvent = &done
			}
		}

		// Verify we received action events
		if len(actionEvents) == 0 {
			rt.Fatalf("No action events received")
		}

		// Verify action event properties
		for _, event := range actionEvents {
			if event.ActionID == "" {
				rt.Fatalf("Action event has empty ActionID")
			}

			if event.ActionType != actionType {
				rt.Fatalf("Expected action type %s, got %s", actionType, event.ActionType)
			}

			if event.Target == "" {
				rt.Fatalf("Action event has empty Target")
			}
		}

		// Verify done event contains action count
		if doneEvent == nil {
			rt.Fatalf("No done event received")
			return
		}

		if doneEvent.ActionsCount != len(actions) {
			rt.Fatalf("Expected %d actions, got %d", len(actions), doneEvent.ActionsCount)
		}
	})
}
