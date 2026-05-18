package stages

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/madmike/go-pipeline/core"
)

// ActionStageConfig holds action stage configuration
type ActionStageConfig struct {
	// Actions can be pre-defined or parsed from LLM output
	Actions []ActionRequestPayload
}

// ActionRequestPayload represents an action to be executed by the client
type ActionRequestPayload struct {
	ActionID   string          `json:"actionId"`
	ActionType core.ActionType `json:"actionType"`
	Target     string          `json:"target,omitempty"`
	Data       map[string]any  `json:"data,omitempty"`
	Required   bool            `json:"required"`
	Timeout    int             `json:"timeout,omitempty"`
}

// ActionStage represents an action execution stage
type ActionStage struct {
	config ActionStageConfig
}

// NewActionStage creates a new action stage
func NewActionStage(config ActionStageConfig) *ActionStage {
	return &ActionStage{
		config: config,
	}
}

// Name returns the stage name
func (s *ActionStage) Name() string {
	return "action"
}

// InputTypes returns the event types this stage accepts
func (s *ActionStage) InputTypes() []core.EventType {
	return []core.EventType{core.EventTypeLLM}
}

// OutputTypes returns the event types this stage produces
func (s *ActionStage) OutputTypes() []core.EventType {
	return []core.EventType{core.EventTypeAction, core.EventTypeStatus, core.EventTypeDone}
}

// Process implements the Stage interface
// It reads LLM output, parses action commands, and emits ActionEvents
func (s *ActionStage) Process(ctx context.Context, input <-chan core.Event, output chan<- core.Event) error {

	// Emit executing status
	output <- core.StatusEvent{
		Status:  core.StatusExecuting,
		Target:  core.StatusTargetBot,
		Message: "Executing actions...",
	}

	// Collect all LLM output to parse for actions
	var fullText string
	for event := range input {
		if llmEvent, ok := event.(core.LLMEvent); ok {
			fullText += llmEvent.Delta
		}
	}

	// Parse actions from LLM output
	actions, err := s.parseActions(fullText)
	if err != nil {
		output <- core.ErrorEvent{
			Error:     fmt.Errorf("failed to parse actions from LLM output: %w", err),
			Retryable: false,
		}
		return err
	}

	// If no actions were parsed, use pre-configured actions
	if len(actions) == 0 {
		actions = s.config.Actions
	}

	// Emit each action
	actionsCount := 0
	for _, action := range actions {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// Emit action event
			output <- core.ActionEvent{
				ActionID:   action.ActionID,
				ActionType: action.ActionType,
				Target:     action.Target,
				Data:       action.Data,
				Required:   action.Required,
			}
			actionsCount++
		}
	}

	// Emit done event with action count
	output <- core.DoneEvent{
		ActionsCount: actionsCount,
	}

	return nil
}

// parseActions attempts to parse action commands from LLM output
// It looks for JSON structures containing action definitions
func (s *ActionStage) parseActions(text string) ([]ActionRequestPayload, error) {
	var actions []ActionRequestPayload

	// Try to find JSON object or array in the text
	// Look for patterns like {"actions": [...]} or just [...]

	// First, try to find a JSON object with "actions" key
	if idx := strings.Index(text, `"actions"`); idx != -1 {
		// Find the start of the JSON object
		startIdx := strings.LastIndex(text[:idx], "{")
		if startIdx == -1 {
			return actions, nil
		}

		// Find the end of the JSON object
		endIdx := findJSONObjectEnd(text, startIdx)
		if endIdx == -1 {
			return actions, nil
		}

		jsonStr := text[startIdx : endIdx+1]
		var parsed struct {
			Actions []ActionRequestPayload `json:"actions"`
		}

		if err := json.Unmarshal([]byte(jsonStr), &parsed); err == nil {
			actions = parsed.Actions
		}
		return actions, nil
	}

	// Try to find a JSON array directly
	if idx := strings.Index(text, "["); idx != -1 {
		endIdx := findJSONArrayEnd(text, idx)
		if endIdx != -1 {
			jsonStr := text[idx : endIdx+1]
			if err := json.Unmarshal([]byte(jsonStr), &actions); err == nil {
				return actions, nil
			}
		}
	}

	return actions, nil
}

// findJSONObjectEnd finds the closing brace of a JSON object starting at startIdx
func findJSONObjectEnd(text string, startIdx int) int {
	depth := 0
	inString := false
	escaped := false

	for i := startIdx; i < len(text); i++ {
		char := text[i]

		if escaped {
			escaped = false
			continue
		}

		if char == '\\' {
			escaped = true
			continue
		}

		if char == '"' {
			inString = !inString
			continue
		}

		if inString {
			continue
		}

		switch char {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return i
			}
		}
	}

	return -1
}

// findJSONArrayEnd finds the closing bracket of a JSON array starting at startIdx
func findJSONArrayEnd(text string, startIdx int) int {
	depth := 0
	inString := false
	escaped := false

	for i := startIdx; i < len(text); i++ {
		char := text[i]

		if escaped {
			escaped = false
			continue
		}

		if char == '\\' {
			escaped = true
			continue
		}

		if char == '"' {
			inString = !inString
			continue
		}

		if inString {
			continue
		}

		switch char {
		case '[':
			depth++
		case ']':
			depth--
			if depth == 0 {
				return i
			}
		}
	}

	return -1
}
