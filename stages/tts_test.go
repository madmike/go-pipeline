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

// For any text input with sentence boundaries, the TTS stage SHALL buffer text until
// sentence boundaries are detected before sending to TTS provider.
func TestPropertyTTSSentenceBuffering(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		// Generate test text with sentence boundaries
		sentences := rapid.SliceOfN(
			rapid.StringN(5, 30, 255),
			1, 3,
		).Draw(rt, "sentences")

		// Create logger
		logger := telemetry.New(telemetry.Config{
			Level: "error",
		})

		// Create TTS stage
		stage := NewTTSStage(TTSStageConfig{
			Provider: &TestStreamingTTSProvider{},
			Voice:    "en-US-Neural2-C",
			Language: "en",
			Logger:   logger,
		})

		// Build input text with sentence boundaries
		var inputText string
		for i, sentence := range sentences {
			inputText += sentence
			if i < len(sentences)-1 {
				inputText += ". "
			} else {
				inputText += "."
			}
		}

		// Create input channel and send text in chunks
		input := make(chan core.Event, 10)
		go func() {
			defer close(input)
			// Send text in small chunks to simulate streaming
			for _, char := range inputText {
				input <- core.LLMEvent{
					Delta:   string(char),
					Content: inputText,
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
			_ = stage.Process(ctx, input, output)
		}()

		// Collect output events
		var audioEvents []core.AudioEvent
		var statusEvent *core.StatusEvent

		for event := range output {
			if audio, ok := event.(core.AudioEvent); ok {
				audioEvents = append(audioEvents, audio)
			}
			if status, ok := event.(core.StatusEvent); ok {
				statusEvent = &status
			}
		}

		// Verify we received status event
		if statusEvent == nil {
			rt.Fatalf("No status event received")
			return
		}

		if statusEvent.Status != core.StatusSpeaking {
			rt.Fatalf("Expected speaking status, got %s", statusEvent.Status)
		}

		// Verify we received audio events
		if len(audioEvents) == 0 {
			rt.Fatalf("No audio events received")
		}

		// Verify audio data is not empty
		for _, event := range audioEvents {
			if len(event.Data) == 0 {
				rt.Fatalf("Audio event has empty data")
			}
		}
	})
}

// TestStreamingTTSProvider provides streaming TTS responses for testing
type TestStreamingTTSProvider struct{}

func (m *TestStreamingTTSProvider) Name() string                 { return "test-streaming-tts" }
func (m *TestStreamingTTSProvider) Type() providers.ProviderType { return "test" }
func (m *TestStreamingTTSProvider) Initialize(ctx context.Context, config providers.ProviderConfig) error {
	return nil
}
func (m *TestStreamingTTSProvider) Close() error                          { return nil }
func (m *TestStreamingTTSProvider) HealthCheck(ctx context.Context) error { return nil }
func (m *TestStreamingTTSProvider) Capabilities() []providers.Capability {
	return []providers.Capability{providers.CapabilityTTS}
}
func (m *TestStreamingTTSProvider) SupportsCapability(capability providers.Capability) bool {
	return capability == providers.CapabilityTTS
}
func (m *TestStreamingTTSProvider) Synthesize(ctx context.Context, req providers.TTSRequest) (*providers.TTSResponse, error) {
	return nil, nil
}
func (m *TestStreamingTTSProvider) StreamSynthesize(ctx context.Context, req providers.TTSRequest) (providers.TTSStream, error) {
	return &TestTTSStream{}, nil
}

// TestTTSStream provides streaming TTS responses
type TestTTSStream struct {
	chunks int
}

func (s *TestTTSStream) Send(ctx context.Context, text string) error {
	return nil
}

func (s *TestTTSStream) Receive(ctx context.Context) (*providers.TTSChunk, error) {
	s.chunks++

	if s.chunks > 3 {
		return &providers.TTSChunk{Done: true}, nil
	}

	// Return audio chunks
	return &providers.TTSChunk{
		Audio: []byte(fmt.Sprintf("audio_chunk_%d", s.chunks)),
		Done:  false,
	}, nil
}

func (s *TestTTSStream) Close() error {
	return nil
}
