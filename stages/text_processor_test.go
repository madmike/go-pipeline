package stages

import (
	"context"
	"testing"

	"github.com/madmike/go-pipeline/core"
)

func TestTextProcessorStage_StripMarkdown(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "bold text",
			input:    "This is **bold** text.",
			expected: "This is bold text.",
		},
		{
			name:     "italic text",
			input:    "This is *italic* text.",
			expected: "This is italic text.",
		},
		{
			name:     "markdown link",
			input:    "Check [this link](https://example.com) for info.",
			expected: "Check this link for info.",
		},
		{
			name:     "code block",
			input:    "Here is code:\n```go\nfunc main() {}\n```\nEnd.",
			expected: "Here is code:\nEnd.",
		},
		{
			name:     "inline code",
			input:    "Use `fmt.Println()` for output.",
			expected: "Use  for output.",
		},
		{
			name:     "header",
			input:    "## Section Title\nContent here.",
			expected: "Section Title\nContent here.",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stage := NewTextProcessorStage(TextProcessorStageConfig{
				StripCodeBlocks: true,
				StripMarkdown:   true,
			})

			input := make(chan core.Event, 1)
			output := make(chan core.Event, 10)

			go func() {
				input <- core.LLMEvent{Delta: tt.input}
				close(input)
			}()

			go stage.Process(context.Background(), input, output)

			var result string
			for event := range output {
				if llmEvent, ok := event.(core.LLMEvent); ok {
					result += llmEvent.Delta
				}
				if _, ok := event.(core.DoneEvent); ok {
					break
				}
			}

			if result != tt.expected {
				t.Errorf("got %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestTextProcessorStage_ExpandAbbreviations(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Dr. abbreviation",
			input:    "Dr. Smith is here.",
			expected: "Doctor Smith is here.",
		},
		{
			name:     "Mr. abbreviation",
			input:    "Mr. Jones arrived.",
			expected: "Mister Jones arrived.",
		},
		{
			name:     "etc. abbreviation",
			input:    "Apples, oranges, et cetera are fruits.",
			expected: "Apples, oranges, et cetera are fruits.",
		},
		{
			name:     "No. abbreviation",
			input:    "No. 5 is the answer.",
			expected: "Number 5 is the answer.",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stage := NewTextProcessorStage(TextProcessorStageConfig{
				ExpandAbbreviations: true,
			})

			input := make(chan core.Event, 1)
			output := make(chan core.Event, 10)

			go func() {
				input <- core.LLMEvent{Delta: tt.input}
				close(input)
			}()

			go stage.Process(context.Background(), input, output)

			var result string
			for event := range output {
				if llmEvent, ok := event.(core.LLMEvent); ok {
					result += llmEvent.Delta
				}
				if _, ok := event.(core.DoneEvent); ok {
					break
				}
			}

			if result != tt.expected {
				t.Errorf("got %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestTextProcessorStage_ExpandSymbols(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "ampersand",
			input:    "Cats & dogs.",
			expected: "Cats and dogs.",
		},
		{
			name:     "at symbol",
			input:    "Email me @ example.com.",
			expected: "Email me at example.com.",
		},
		{
			name:     "hash symbol",
			input:    "Use #1 approach.",
			expected: "Use number1 approach.",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stage := NewTextProcessorStage(TextProcessorStageConfig{
				ExpandSymbols: true,
			})

			input := make(chan core.Event, 1)
			output := make(chan core.Event, 10)

			go func() {
				input <- core.LLMEvent{Delta: tt.input}
				close(input)
			}()

			go stage.Process(context.Background(), input, output)

			var result string
			for event := range output {
				if llmEvent, ok := event.(core.LLMEvent); ok {
					result += llmEvent.Delta
				}
				if _, ok := event.(core.DoneEvent); ok {
					break
				}
			}

			if result != tt.expected {
				t.Errorf("got %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestTextProcessorStage_SentenceBuffering(t *testing.T) {
	tests := []struct {
		name     string
		inputs   []string
		expected []string
	}{
		{
			name:     "single sentence",
			inputs:   []string{"Hello ", "world."},
			expected: []string{"Hello world."},
		},
		{
			name:     "multiple sentences",
			inputs:   []string{"First. ", "Second."},
			expected: []string{"First.", "Second."},
		},
		{
			name:     "question mark",
			inputs:   []string{"What is ", "this?"},
			expected: []string{"What is this?"},
		},
		{
			name:     "exclamation",
			inputs:   []string{"Wow", "!"},
			expected: []string{"Wow!"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stage := NewTextProcessorStage(TextProcessorStageConfig{})

			input := make(chan core.Event, len(tt.inputs))
			output := make(chan core.Event, 100)

			go func() {
				for _, inp := range tt.inputs {
					input <- core.LLMEvent{Delta: inp}
				}
				close(input)
			}()

			go stage.Process(context.Background(), input, output)

			var results []string
			for event := range output {
				if llmEvent, ok := event.(core.LLMEvent); ok {
					results = append(results, llmEvent.Delta)
				}
				if _, ok := event.(core.DoneEvent); ok {
					break
				}
			}

			if len(results) != len(tt.expected) {
				t.Errorf("got %d events, want %d", len(results), len(tt.expected))
			}

			for i, result := range results {
				if i < len(tt.expected) && result != tt.expected[i] {
					t.Errorf("event %d: got %q, want %q", i, result, tt.expected[i])
				}
			}
		})
	}
}

func TestTextProcessorStage_PassThroughStatusEvents(t *testing.T) {
	stage := NewTextProcessorStage(TextProcessorStageConfig{})

	input := make(chan core.Event, 2)
	output := make(chan core.Event, 10)

	go func() {
		input <- core.StatusEvent{
			Status:  core.StatusThinking,
			Target:  core.StatusTargetBot,
			Message: "Processing...",
		}
		input <- core.LLMEvent{Delta: "Hello."}
		close(input)
	}()

	go stage.Process(context.Background(), input, output)

	var statusCount int
	var llmCount int

	for event := range output {
		if _, ok := event.(core.StatusEvent); ok {
			statusCount++
		}
		if _, ok := event.(core.LLMEvent); ok {
			llmCount++
		}
		if _, ok := event.(core.DoneEvent); ok {
			break
		}
	}

	if statusCount != 1 {
		t.Errorf("expected 1 status event, got %d", statusCount)
	}
	if llmCount != 1 {
		t.Errorf("expected 1 LLM event, got %d", llmCount)
	}
}

func TestTextProcessorStage_EmptyInput(t *testing.T) {
	stage := NewTextProcessorStage(TextProcessorStageConfig{})

	input := make(chan core.Event)
	output := make(chan core.Event, 10)

	go func() {
		close(input)
	}()

	go stage.Process(context.Background(), input, output)

	var doneCount int
	for event := range output {
		if _, ok := event.(core.DoneEvent); ok {
			doneCount++
			break
		}
	}

	if doneCount != 1 {
		t.Errorf("expected 1 done event, got %d", doneCount)
	}
}

func TestTextProcessorStage_SkipsEmptyDeltas(t *testing.T) {
	stage := NewTextProcessorStage(TextProcessorStageConfig{})

	input := make(chan core.Event, 3)
	output := make(chan core.Event, 10)

	go func() {
		input <- core.LLMEvent{Delta: "  "}
		input <- core.LLMEvent{Delta: "\n"}
		input <- core.LLMEvent{Delta: "Hello."}
		close(input)
	}()

	go stage.Process(context.Background(), input, output)

	var llmCount int
	for event := range output {
		if _, ok := event.(core.LLMEvent); ok {
			llmCount++
		}
		if _, ok := event.(core.DoneEvent); ok {
			break
		}
	}

	if llmCount != 1 {
		t.Errorf("expected 1 LLM event (empty deltas skipped), got %d", llmCount)
	}
}
