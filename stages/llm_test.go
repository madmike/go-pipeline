package stages

import (
	"context"
	"strings"
	"testing"
	"time"

	providers "github.com/madmike/go-ai-providers/core"
	"github.com/madmike/go-pipeline/core"
	"pgregory.net/rapid"
)

// For any text input to a text pipeline, the output stream SHALL contain LLM chunks
// that together form a coherent response.
func TestPropertyLLMTextPipelineOutput(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		// Generate test input text
		inputText := rapid.StringN(1, 50, 255).Draw(rt, "inputText")

		// Create LLM stage with streaming provider
		stage := NewLLMStage(LLMStageConfig{
			Provider: &TestStreamingLLMProvider{
				responseText: "This is a test response.",
			},
			Model: "gpt-4",
		})

		// Create input channel and send text
		input := make(chan core.Event, 10)
		go func() {
			defer close(input)
			input <- core.LLMEvent{
				Delta:   inputText,
				Content: inputText,
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
		var llmEvents []core.LLMEvent
		var doneEvent *core.DoneEvent
		var serviceMessage *core.ServiceMessageEvent

		for event := range output {
			if llmEvent, ok := event.(core.LLMEvent); ok {
				llmEvents = append(llmEvents, llmEvent)
			}
			if done, ok := event.(core.DoneEvent); ok {
				doneEvent = &done
			}
			if svc, ok := event.(core.ServiceMessageEvent); ok {
				serviceMessage = &svc
			}
		}

		// If input is whitespace-only, we should get a service message instead
		trimmedInput := strings.TrimSpace(inputText)

		if trimmedInput == "" {
			// Whitespace-only input should terminate cleanly without LLM output.
			if serviceMessage != nil {
				rt.Fatalf("Did not expect service message for whitespace-only input")
			}
			if doneEvent == nil {
				rt.Fatalf("Expected done event after service message")
			}
		} else {
			// Valid input should produce LLM events
			if len(llmEvents) == 0 {
				rt.Fatalf("No LLM events received for valid input")
			}

			// Verify chunks form coherent response
			var fullResponse string
			for _, event := range llmEvents {
				fullResponse += event.Delta
			}

			if fullResponse == "" {
				rt.Fatalf("Empty response from LLM")
			}

			// Verify done event contains full text
			if doneEvent == nil {
				rt.Fatalf("No done event received")
				return
			}

			if doneEvent.FullText == "" {
				rt.Fatalf("Done event has empty full text")
			}
		}
	})
}

// TestStreamingLLMProvider provides streaming responses for testing
type TestStreamingLLMProvider struct {
	responseText string
}

func (m *TestStreamingLLMProvider) Name() string                 { return "test-streaming-llm" }
func (m *TestStreamingLLMProvider) Type() providers.ProviderType { return "test" }
func (m *TestStreamingLLMProvider) Initialize(ctx context.Context, config providers.ProviderConfig) error {
	return nil
}
func (m *TestStreamingLLMProvider) Close() error                          { return nil }
func (m *TestStreamingLLMProvider) HealthCheck(ctx context.Context) error { return nil }
func (m *TestStreamingLLMProvider) Capabilities() []providers.Capability {
	return []providers.Capability{providers.CapabilityLLM}
}
func (m *TestStreamingLLMProvider) SupportsCapability(capability providers.Capability) bool {
	return capability == providers.CapabilityLLM
}
func (m *TestStreamingLLMProvider) ChatCompletion(ctx context.Context, req providers.ChatRequest) (*providers.ChatResponse, error) {
	return nil, nil
}
func (m *TestStreamingLLMProvider) StreamChatCompletion(ctx context.Context, req providers.ChatRequest) (providers.ChatStream, error) {
	return &TestChatStream{
		responseText: m.responseText,
	}, nil
}

// TestChatStream provides streaming chat responses
type TestChatStream struct {
	responseText string
	chunks       int
	words        []string
}

func (s *TestChatStream) Send(ctx context.Context, data []byte) error {
	return nil
}

func (s *TestChatStream) Receive(ctx context.Context) (*providers.ChatChunk, error) {
	// Split response into words for streaming
	if len(s.words) == 0 {
		s.words = []string{}
		word := ""
		for _, char := range s.responseText {
			if char == ' ' {
				if word != "" {
					s.words = append(s.words, word)
					word = ""
				}
				s.words = append(s.words, " ")
			} else {
				word += string(char)
			}
		}
		if word != "" {
			s.words = append(s.words, word)
		}
	}

	if s.chunks >= len(s.words) {
		return &providers.ChatChunk{Done: true}, nil
	}

	chunk := s.words[s.chunks]
	s.chunks++

	return &providers.ChatChunk{
		Content: chunk,
		Done:    false,
	}, nil
}

func (s *TestChatStream) Close() error {
	return nil
}
