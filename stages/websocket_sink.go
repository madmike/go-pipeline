package stages

import (
	"context"
	"encoding/json"

	"github.com/gorilla/websocket"
	"github.com/madmike/go-infra/telemetry"
	"github.com/madmike/go-pipeline/core"
	"github.com/madmike/go-pipeline/protocol"
)

// WebSocketSinkConfig holds WebSocket sink configuration
type WebSocketSinkConfig struct {
	Conn       *websocket.Conn
	SessionID  string
	ResponseID string // ID to correlate response.start and response.end
	Logger     telemetry.Logger
}

// WebSocketSink sends pipeline events to a WebSocket connection
type WebSocketSink struct {
	config       WebSocketSinkConfig
	audioStarted bool
}

// NewWebSocketSink creates a new WebSocket sink stage
func NewWebSocketSink(config WebSocketSinkConfig) *WebSocketSink {
	return &WebSocketSink{
		config: config,
	}
}

// Name returns the stage name
func (ws *WebSocketSink) Name() string {
	return "websocket_sink"
}

// Process implements the Stage interface
// It reads events from the input channel and sends them to the WebSocket connection
func (ws *WebSocketSink) Process(ctx context.Context, input <-chan core.Event, output chan<- core.Event) error {
	logger := ws.config.Logger.WithModule(ws.Name())
	logger.Info("Starting WebSocket sink stage", telemetry.String("session_id", ws.config.SessionID))

	for {
		select {
		case <-ctx.Done():
			logger.Info("WebSocket sink context cancelled", telemetry.String("session_id", ws.config.SessionID))
			return ctx.Err()

		case event, ok := <-input:
			if !ok {
				logger.Info("WebSocket sink input channel closed", telemetry.String("session_id", ws.config.SessionID))
				return nil
			}

			// Special handling for AudioEvent to send only binary
			if audioEvent, ok := event.(core.AudioEvent); ok {
				// Send audio start message if this is the first chunk
				if !ws.audioStarted {
					// Default to PCM 24kHz if not specified (should be passed from config, but for now hardcoded or inferred)
					// Ideally this info comes from the event or stage config.
					// For now, we'll assume 24kHz PCM as per common defaults, or extract from event if possible.
					// The AudioEvent struct has Format, but not SampleRate.
					// We'll send what we have.
					startMsg := protocol.NewResponseAudioStartMessage(
						ws.config.SessionID,
						ws.config.ResponseID,
						ws.config.ResponseID,
						audioEvent.Format,
						24000, // TODO: Get this from config/event
					)
					if data, err := json.Marshal(startMsg); err == nil {
						ws.config.Conn.WriteMessage(websocket.TextMessage, data)
						logger.Info("Sent audio start message", telemetry.String("session_id", ws.config.SessionID))
					}
					ws.audioStarted = true
				}

				if err := ws.config.Conn.WriteMessage(websocket.BinaryMessage, audioEvent.Data); err != nil {
					logger.Error("Failed to send audio to WebSocket", telemetry.Err(err), telemetry.String("session_id", ws.config.SessionID))
					// WebSocket connection closed or failed - gracefully drain input without failing pipeline
					for range input {
						// Drain remaining events
					}
					return nil
				}
				logger.Debug("Sent audio to WebSocket", telemetry.Int("size", len(audioEvent.Data)), telemetry.String("session_id", ws.config.SessionID))
				continue
			}

			// Check for DoneEvent to send audio end if audio was started
			if doneEvent, ok := event.(core.DoneEvent); ok {
				if ws.audioStarted {
					endMsg := protocol.NewResponseAudioEndMessage(
						ws.config.SessionID,
						ws.config.ResponseID,
						ws.config.ResponseID,
						0, // Duration not tracked here yet
					)
					if data, err := json.Marshal(endMsg); err == nil {
						ws.config.Conn.WriteMessage(websocket.TextMessage, data)
						logger.Debug("Sent audio end message", telemetry.String("session_id", ws.config.SessionID))
					}
					ws.audioStarted = false
				}

				// Forward DoneEvent to client
				logger.Debug("Forwarding DoneEvent to client", telemetry.String("session_id", ws.config.SessionID), telemetry.Float64("audio_duration", doneEvent.AudioDuration))
				// Convert event to protocol message
				msg := protocol.EventToMessage(event, ws.config.SessionID, ws.config.ResponseID)
				if msg != nil {
					data, err := json.Marshal(msg)
					if err == nil {
						ws.config.Conn.WriteMessage(websocket.TextMessage, data)
						logger.Debug("Sent event to WebSocket", telemetry.String("type", string(msg.Type)), telemetry.String("session_id", ws.config.SessionID))
					}
				}
				continue
			}

			// Convert event to protocol message
			msg := protocol.EventToMessage(event, ws.config.SessionID, ws.config.ResponseID)
			if msg == nil {
				logger.Debug("Skipping unknown event type", telemetry.String("session_id", ws.config.SessionID))
				continue
			}

			// Serialize message to JSON
			data, err := json.Marshal(msg)
			if err != nil {
				logger.Error("Failed to marshal message", telemetry.Err(err), telemetry.String("session_id", ws.config.SessionID), telemetry.String("event_type", string(msg.Type)))
				// Log error but continue processing - don't fail the pipeline
				continue
			}

			// Send JSON message to WebSocket
			if err := ws.config.Conn.WriteMessage(websocket.TextMessage, data); err != nil {
				logger.Error("Failed to send message to WebSocket", telemetry.Err(err), telemetry.String("session_id", ws.config.SessionID), telemetry.String("event_type", string(msg.Type)))
				// WebSocket connection closed or failed - gracefully drain input without failing pipeline
				// This allows upstream stages to complete their work
				for range input {
					// Drain remaining events
				}
				return nil
			}

			logger.Debug("Sent event to WebSocket", telemetry.String("type", string(msg.Type)), telemetry.String("session_id", ws.config.SessionID))
		}
	}
}

// InputTypes returns the input event types this stage accepts
func (ws *WebSocketSink) InputTypes() []core.EventType {
	// WebSocket sink accepts all event types
	return []core.EventType{}
}

// OutputTypes returns the output event types this stage produces
func (ws *WebSocketSink) OutputTypes() []core.EventType {
	// WebSocket sink is a terminal stage, it only produces error events
	return []core.EventType{core.EventTypeError}
}
