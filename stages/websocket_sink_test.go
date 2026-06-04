package stages

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/madmike/go-infra/telemetry"
	"github.com/madmike/go-pipeline/core"
	"github.com/madmike/go-pipeline/protocol"
)

func TestWebSocketSink_AudioEvent(t *testing.T) {
	// Setup WebSocket server
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{}
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		for {
			mt, _, err := c.ReadMessage()
			if err != nil {
				break
			}
			// Echo back for keepalive if needed, but we just consume here
			_ = mt
		}
	}))
	defer s.Close()

	// Connect to server
	u := "ws" + strings.TrimPrefix(s.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(u, nil)
	if err != nil {
		t.Fatalf("Failed to dial: %v", err)
	}
	defer conn.Close()

	// Create sink
	sink := NewWebSocketSink(WebSocketSinkConfig{
		Conn:      conn,
		SessionID: "test-session",
		Logger:    telemetry.New(telemetry.Config{Level: "error"}),
	})

	// Create channels
	input := make(chan core.Event)
	output := make(chan core.Event)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start sink
	go func() { _ = sink.Process(ctx, input, output) }()

	// Send AudioEvent
	audioData := []byte{0x01, 0x02, 0x03, 0x04}
	input <- core.AudioEvent{
		Data:   audioData,
		Format: "pcm",
	}

	// Read from server side to verify what was sent
	// We need to hijack the server side connection to read messages
	// Re-implement server to capture messages

	// Channel to receive messages on server side
	serverMessages := make(chan any, 10)

	s2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{}
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		for {
			mt, message, err := c.ReadMessage()
			if err != nil {
				break
			}
			if mt == websocket.BinaryMessage {
				serverMessages <- message // []byte
			} else if mt == websocket.TextMessage {
				var msg protocol.OutputMessage
				_ = json.Unmarshal(message, &msg)
				serverMessages <- msg
			}
		}
	}))
	defer s2.Close()

	u2 := "ws" + strings.TrimPrefix(s2.URL, "http")
	conn2, _, err := websocket.DefaultDialer.Dial(u2, nil)
	if err != nil {
		t.Fatalf("Failed to dial 2: %v", err)
	}
	defer conn2.Close()

	sink2 := NewWebSocketSink(WebSocketSinkConfig{
		Conn:      conn2,
		SessionID: "test-session",
		Logger:    telemetry.New(telemetry.Config{Level: "error"}),
	})

	// Create new input channel for second sink
	input2 := make(chan core.Event)
	go func() { _ = sink2.Process(ctx, input2, output) }()

	// Send AudioEvent again to the new setup
	input2 <- core.AudioEvent{
		Data:   audioData,
		Format: "pcm",
	}

	// Verify messages
	// We expect:
	// 1. response.audio_start (JSON)
	// 2. Binary audio message
	// 3. response.audio_end (JSON) - triggered by DoneEvent (which we need to send)

	// Send DoneEvent to trigger audio end
	input2 <- core.DoneEvent{}

	timeout := time.After(1 * time.Second)
	var receivedBinary []byte
	var receivedStart *protocol.OutputMessage
	var receivedEnd *protocol.OutputMessage

	for i := 0; i < 3; i++ {
		select {
		case msg := <-serverMessages:
			switch v := msg.(type) {
			case []byte:
				receivedBinary = v
			case protocol.OutputMessage:
				if v.Type == protocol.OutputResponseAudioStart {
					receivedStart = &v
				} else if v.Type == protocol.OutputResponseAudioEnd {
					receivedEnd = &v
				}
			}
		case <-timeout:
			// Break if timeout
		}
	}

	// Assertions
	if receivedBinary == nil {
		t.Error("Should receive binary message")
	} else if string(receivedBinary) != string(audioData) {
		t.Errorf("Binary message mismatch. Expected %v, got %v", audioData, receivedBinary)
	}

	if receivedStart == nil {
		t.Error("Should receive response.audio_start message")
	} else {
		// Verify payload
		payloadMap, ok := receivedStart.Payload.(map[string]any)
		if !ok {
			// Try to marshal/unmarshal to check structure if it came as map from JSON
			// In test it comes as struct from our mock server if we unmarshal it there?
			// Wait, our mock server unmarshals to OutputMessage, but Payload is any.
			// json.Unmarshal will unmarshal payload to map[string]any
			t.Errorf("Payload is not a map: %T", receivedStart.Payload)
		} else {
			if payloadMap["encoding"] != "pcm" {
				t.Errorf("Expected encoding pcm, got %v", payloadMap["encoding"])
			}
		}
	}

	if receivedEnd == nil {
		t.Error("Should receive response.audio_end message")
	}
}
