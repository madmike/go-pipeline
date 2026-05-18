package stages

import (
	"context"
	"fmt"
	"testing"
	"time"

	providers "github.com/madmike/go-ai-providers/core"
	"github.com/madmike/go-infra/telemetry"
	"github.com/madmike/go-pipeline/core"
	"pgregory.net/rapid"
)

// For any audio input to an audio pipeline, the output stream SHALL contain both
// interim and final STT chunks, followed by LLM event with final transcription.
func TestPropertySTTAudioPipelineOutput(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		// Generate test audio data
		audioChunks := rapid.IntRange(1, 5).Draw(rt, "numChunks")

		// Create logger
		logger := telemetry.New(telemetry.Config{
			Level: "error",
		})

		// Create STT stage with streaming provider
		stage := NewSTTStage(STTStageConfig{
			Provider:       &TestStreamingSTTProvider{},
			Language:       "en",
			SampleRate:     16000,
			InterimResults: true,
			Logger:         logger,
		})

		// Create input channel and send audio chunks
		input := make(chan core.Event, 10)
		go func() {
			defer close(input)
			for i := 0; i < audioChunks; i++ {
				input <- core.AudioEvent{
					Data: []byte(fmt.Sprintf("audio_chunk_%d", i)),
				}
			}
		}()

		// Create output channel
		output := make(chan core.Event, 100)

		// Execute stage in goroutine
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		go func() {
			defer close(output)
			stage.Process(ctx, input, output)
		}()

		// Collect output events
		var sttEvents []core.STTEvent
		var llmEvent *core.LLMEvent
		var statusEvent *core.StatusEvent

		for event := range output {
			if stt, ok := event.(core.STTEvent); ok {
				sttEvents = append(sttEvents, stt)
			}
			if llm, ok := event.(core.LLMEvent); ok {
				llmEvent = &llm
			}
			if status, ok := event.(core.StatusEvent); ok {
				statusEvent = &status
			}
		}

		// Verify we received status event
		if statusEvent == nil {
			rt.Fatalf("No status event received")
		}

		if statusEvent.Status != core.StatusTranscribing {
			rt.Fatalf("Expected transcribing status, got %s", statusEvent.Status)
		}

		// Verify we received STT events
		if len(sttEvents) == 0 {
			rt.Fatalf("No STT events received")
		}

		// Verify we have at least one final result
		hasFinal := false
		for _, event := range sttEvents {
			if event.IsFinal {
				hasFinal = true
				break
			}
		}

		if !hasFinal {
			rt.Fatalf("No final STT result received")
		}

		// Verify LLM event with final transcription
		if llmEvent == nil {
			rt.Fatalf("No LLM event with transcription received")
		}

		if llmEvent.Content == "" {
			rt.Fatalf("LLM event has empty content")
		}
	})
}

// TestStreamingSTTProvider provides streaming STT responses for testing
type TestStreamingSTTProvider struct{}

func (m *TestStreamingSTTProvider) Name() string                 { return "test-streaming-stt" }
func (m *TestStreamingSTTProvider) Type() providers.ProviderType { return "test" }
func (m *TestStreamingSTTProvider) Initialize(ctx context.Context, config providers.ProviderConfig) error {
	return nil
}
func (m *TestStreamingSTTProvider) Close() error                          { return nil }
func (m *TestStreamingSTTProvider) HealthCheck(ctx context.Context) error { return nil }
func (m *TestStreamingSTTProvider) Capabilities() []providers.Capability {
	return []providers.Capability{providers.CapabilitySTT}
}
func (m *TestStreamingSTTProvider) SupportsCapability(capability providers.Capability) bool {
	return capability == providers.CapabilitySTT
}
func (m *TestStreamingSTTProvider) Transcribe(ctx context.Context, req providers.STTRequest) (*providers.STTResponse, error) {
	return nil, nil
}
func (m *TestStreamingSTTProvider) StreamTranscribe(ctx context.Context, req providers.STTRequest) (providers.STTStream, error) {
	return &TestSTTStream{}, nil
}

// TestSTTStream provides streaming STT responses
type TestSTTStream struct {
	chunks int
}

func (s *TestSTTStream) Send(ctx context.Context, data []byte) error {
	return nil
}

func (s *TestSTTStream) Receive(ctx context.Context) (*providers.STTChunk, error) {
	s.chunks++

	if s.chunks == 1 {
		// Interim result
		return &providers.STTChunk{
			Text:       "Hello",
			IsFinal:    false,
			Confidence: 0.8,
			Done:       false,
		}, nil
	} else if s.chunks == 2 {
		// Another interim result
		return &providers.STTChunk{
			Text:       "Hello world",
			IsFinal:    false,
			Confidence: 0.85,
			Done:       false,
		}, nil
	} else if s.chunks == 3 {
		// Final result
		return &providers.STTChunk{
			Text:       "Hello world",
			IsFinal:    true,
			Confidence: 0.95,
			Done:       false,
		}, nil
	}

	// End of stream
	return &providers.STTChunk{
		Done: true,
	}, nil
}

func (s *TestSTTStream) Close() error {
	return nil
}
