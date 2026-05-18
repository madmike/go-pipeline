package stages

import (
	"context"
	"io"

	providers "github.com/madmike/go-ai-providers/core"
	"github.com/madmike/go-infra/telemetry"
	"github.com/madmike/go-pipeline/core"
)

// STTStageConfig holds STT stage configuration
type STTStageConfig struct {
	Provider       providers.STTProvider
	Language       string
	Encoding       string
	SampleRate     int
	InterimResults bool
	Logger         telemetry.Logger
}

// STTStage represents a speech-to-text processing stage
type STTStage struct {
	config STTStageConfig
}

// NewSTTStage creates a new STT stage
func NewSTTStage(config STTStageConfig) *STTStage {
	return &STTStage{
		config: config,
	}
}

// Name returns the stage name
func (s *STTStage) Name() string {
	return "stt"
}

// InputTypes returns the event types this stage accepts
func (s *STTStage) InputTypes() []core.EventType {
	return []core.EventType{core.EventTypeAudio}
}

// OutputTypes returns the event types this stage produces
func (s *STTStage) OutputTypes() []core.EventType {
	return []core.EventType{core.EventTypeSTT, core.EventTypeLLM, core.EventTypeStatus}
}

// Process implements the Stage interface
// It reads audio chunks from the input channel and streams transcription to the output channel
func (s *STTStage) Process(ctx context.Context, input <-chan core.Event, output chan<- core.Event) error {
	logger := s.config.Logger.WithModule(s.Name())
	logger.Info("Starting STT stage", telemetry.String("provider", s.config.Provider.Name()), telemetry.String("language", s.config.Language))
	logger.Info("Emitting transcribing status")

	// Emit listening status
	output <- core.StatusEvent{
		Status:  core.StatusListening,
		Target:  core.StatusTargetUser,
		Message: "Listening...",
	}

	// Create STT request
	req := providers.STTRequest{
		Language:   s.config.Language,
		Encoding:   s.config.Encoding,
		SampleRate: s.config.SampleRate,
		Options: map[string]any{
			"interim_results": s.config.InterimResults,
		},
	}

	logger.Info("Starting STT stream", telemetry.String("encoding", s.config.Encoding), telemetry.Int("sample_rate", s.config.SampleRate))

	// Start streaming transcription
	stream, err := s.config.Provider.StreamTranscribe(ctx, req)
	if err != nil {
		logger.Error("Failed to start STT stream", telemetry.Err(err))
		// Send user-friendly message instead of error
		output <- core.ServiceMessageEvent{
			MessageType: core.ServiceMessageRetryRequest,
			Content:     "Error transcribing audio. Please try again.",
			Localized: map[string]string{
				"en": "Error transcribing audio. Please try again.",
				"es": "Error al transcribir audio. Por favor, intenta de nuevo.",
				"fr": "Erreur lors de la transcription audio. Veuillez réessayer.",
			},
		}
		// Emit DoneEvent to properly close the pipeline
		logger.Info("Emitting done event after STT stream start error")
		output <- core.DoneEvent{}
		return nil
	}
	defer stream.Close()

	// Process input audio chunks and send to stream
	go func() {
		audioChunkCount := 0
		for event := range input {
			if audioEvent, ok := event.(core.AudioEvent); ok {
				audioChunkCount++
				logger.Debug("Sending audio chunk to STT provider", telemetry.Int("size", len(audioEvent.Data)), telemetry.Int("chunk_number", audioChunkCount))
				err := stream.Send(ctx, audioEvent.Data)
				if err != nil {
					logger.Error("Failed to send audio to STT stream", telemetry.Err(err), telemetry.Int("chunks_sent", audioChunkCount))
					// Log error but don't send to client - handled by stream.Receive error
					return
				}
			}
		}
		// Send empty chunk to signal end-of-stream to the provider
		logger.Info("Sending end-of-stream signal to STT provider", telemetry.Int("total_audio_chunks_sent", audioChunkCount))
		err := stream.Send(ctx, []byte{})
		if err != nil {
			logger.Error("Failed to send end-of-stream signal", telemetry.Err(err))
		}
		// Close the stream when input is done
		logger.Info("Closing STT stream", telemetry.Int("total_audio_chunks_sent", audioChunkCount))
		stream.Close()
	}()

	// Process stream and emit events
	var fullTranscription string
	chunkCount := 0

	for {
		chunk, err := stream.Receive(ctx)
		if err != nil {
			if err == io.EOF {
				logger.Info("STT stream finished (EOF)", telemetry.Int("chunks_received", chunkCount), telemetry.String("full_transcription", fullTranscription))
				break
			}
			logger.Error("Error receiving STT chunk", telemetry.Err(err), telemetry.Int("chunks_received", chunkCount))
			// Send user-friendly message instead of error
			output <- core.ServiceMessageEvent{
				MessageType: core.ServiceMessageRetryRequest,
				Content:     "Error transcribing audio. Please try again.",
				Localized: map[string]string{
					"en": "Error transcribing audio. Please try again.",
					"es": "Error al transcribir audio. Por favor, intenta de nuevo.",
					"fr": "Erreur lors de la transcription audio. Veuillez réessayer.",
				},
			}
			// Emit DoneEvent to properly close the pipeline
			logger.Info("Emitting done event after STT error")
			output <- core.DoneEvent{}
			return nil
		}

		if chunk == nil || chunk.Done {
			logger.Info("STT stream finished", telemetry.Int("chunks_received", chunkCount), telemetry.String("full_transcription", fullTranscription))
			break
		}

		chunkCount++
		logger.Debug("Received STT chunk",
			telemetry.String("text", chunk.Text),
			telemetry.Bool("is_final", chunk.IsFinal),
			telemetry.Int("chunk_number", chunkCount))

		// Skip empty text chunks
		if chunk.Text == "" {
			logger.Debug("Skipping empty STT chunk", telemetry.Int("chunk_number", chunkCount))
			continue
		}

		// Emit STT event for each chunk (interim and final)
		logger.Debug("Emitting STT event", telemetry.String("text", chunk.Text), telemetry.Bool("is_final", chunk.IsFinal))
		output <- core.STTEvent{
			Text:       chunk.Text,
			IsFinal:    chunk.IsFinal,
			Confidence: chunk.Confidence,
		}

		// If final, append to full transcription and emit LLM event immediately
		if chunk.IsFinal {
			if fullTranscription != "" {
				fullTranscription += " "
			}
			fullTranscription += chunk.Text

			logger.Info("Emitting LLM event for final chunk", telemetry.String("text", chunk.Text))
			output <- core.LLMEvent{
				Delta:   chunk.Text,
				Content: chunk.Text,
			}
			logger.Info("Emitted LLM event for final chunk")
		}
	}

	// Check if we got any transcription
	if fullTranscription == "" {
		logger.Warn("No transcription received from STT provider")
		// Emit service message asking user to repeat
		output <- core.ServiceMessageEvent{
			MessageType: core.ServiceMessageRetryRequest,
			Content:     "Could not understand your input. Please try again.",
			Localized: map[string]string{
				"en": "Could not understand your input. Please try again.",
				"es": "No pude entender tu entrada. Por favor, intenta de nuevo.",
				"fr": "Je n'ai pas pu comprendre votre entrée. Veuillez réessayer.",
			},
		}
		// Emit DoneEvent to close the pipeline without any query text
		// Downstream stages will handle the empty query gracefully
		logger.Info("Emitting done event with no transcription")
		output <- core.DoneEvent{}
		return nil
	}

	// Emit DoneEvent to properly terminate the pipeline branch
	logger.Info("Emitting done event", telemetry.String("full_transcription", fullTranscription))
	output <- core.DoneEvent{}

	return nil
}
