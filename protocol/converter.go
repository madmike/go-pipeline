package protocol

import (
	"time"

	"github.com/madmike/go-pipeline/core"
)

// EventToMessage converts a pipeline event to an output message
func EventToMessage(event core.Event, sessionID, replyTo string) *OutputMessage {
	msg := &OutputMessage{
		ID:        generateMessageID(),
		SessionID: sessionID,
		ReplyTo:   replyTo,
		Timestamp: time.Now().UnixMilli(),
	}

	switch e := event.(type) {
	case core.StatusEvent:
		msg.Type = OutputStatus
		msg.Payload = StatusPayload{
			Status:  mapStatusType(e.Status),
			Target:  mapStatusTarget(e.Target),
			Message: e.Message,
			Details: e.Details,
		}

	case core.STTEvent:
		msg.Type = OutputStreamSTT
		msg.Payload = STTStreamPayload{
			Text:       e.Text,
			IsFinal:    e.IsFinal,
			Confidence: e.Confidence,
		}

	case core.LLMEvent:
		msg.Type = OutputStreamLLM
		msg.Payload = LLMStreamPayload{
			Delta:   e.Delta,
			Content: e.Content,
		}

	case core.AudioEvent:
		msg.Type = OutputStreamAudio
		msg.Payload = AudioStreamPayload{
			Data:   e.Data,
			Format: e.Format,
		}

	case core.ActionEvent:
		msg.Type = OutputActionRequest
		msg.Payload = ActionRequestPayload{
			ActionID:   e.ActionID,
			ActionType: mapActionType(e.ActionType),
			Target:     e.Target,
			Data:       e.Data,
			Required:   e.Required,
		}

	case core.ErrorEvent:
		msg.Type = OutputError
		errMsg := ""
		if e.Error != nil {
			errMsg = e.Error.Error()
		}
		msg.Payload = ErrorPayload{
			Code:      "PIPELINE_ERROR",
			Message:   errMsg,
			Retryable: e.Retryable,
		}

	case core.DoneEvent:
		msg.Type = OutputResponseEnd
		msg.Payload = ResponseEndPayload{
			ResponseID:    replyTo,
			FullText:      e.FullText,
			TokensUsed:    e.TokensUsed,
			AudioDuration: e.AudioDuration,
			ActionsCount:  e.ActionsCount,
		}

	case core.ServiceMessageEvent:
		msg.Type = OutputServiceMessage
		msg.Payload = ServiceMessagePayload{
			MessageType: string(e.MessageType),
			Content:     e.Content,
			Localized:   e.Localized,
		}

	default:
		// Unknown event type, skip
		return nil
	}

	return msg
}

// NewResponseAudioStartMessage creates a response.audio_start message
func NewResponseAudioStartMessage(sessionID, replyTo, responseID, encoding string, sampleRate int) *OutputMessage {
	return &OutputMessage{
		Type:      OutputResponseAudioStart,
		ID:        generateMessageID(),
		SessionID: sessionID,
		ReplyTo:   replyTo,
		Payload: ResponseAudioStartPayload{
			ResponseID: responseID,
			Encoding:   encoding,
			SampleRate: sampleRate,
		},
		Timestamp: time.Now().UnixMilli(),
	}
}

// NewResponseAudioEndMessage creates a response.audio_end message
func NewResponseAudioEndMessage(sessionID, replyTo, responseID string, duration float64) *OutputMessage {
	return &OutputMessage{
		Type:      OutputResponseAudioEnd,
		ID:        generateMessageID(),
		SessionID: sessionID,
		ReplyTo:   replyTo,
		Payload: ResponseAudioEndPayload{
			ResponseID: responseID,
			Duration:   duration,
		},
		Timestamp: time.Now().UnixMilli(),
	}
}

// NewResponseStartMessage creates a response.start message
func NewResponseStartMessage(sessionID, replyTo, responseID string, sources []string) *OutputMessage {
	return &OutputMessage{
		Type:      OutputResponseStart,
		ID:        generateMessageID(),
		SessionID: sessionID,
		ReplyTo:   replyTo,
		Payload: ResponseStartPayload{
			ResponseID: responseID,
			Sources:    sources,
		},
		Timestamp: time.Now().UnixMilli(),
	}
}

// NewStatusMessage creates a status message
func NewStatusMessage(sessionID string, status StatusType, target StatusTarget, message string) *OutputMessage {
	return &OutputMessage{
		Type:      OutputStatus,
		ID:        generateMessageID(),
		SessionID: sessionID,
		Payload: StatusPayload{
			Status:  status,
			Target:  target,
			Message: message,
		},
		Timestamp: time.Now().UnixMilli(),
	}
}

// NewErrorMessage creates an error message
func NewErrorMessage(sessionID, replyTo, code, message string, retryable bool, details any) *OutputMessage {
	return &OutputMessage{
		Type:      OutputError,
		ID:        generateMessageID(),
		SessionID: sessionID,
		ReplyTo:   replyTo,
		Payload: ErrorPayload{
			Code:      code,
			Message:   message,
			Retryable: retryable,
			Details:   details,
		},
		Timestamp: time.Now().UnixMilli(),
	}
}

// mapStatusType maps core.StatusType to protocol.StatusType
func mapStatusType(s core.StatusType) StatusType {
	switch s {
	case core.StatusListening:
		return StatusListening
	case core.StatusTranscribing:
		return StatusTranscribing
	case core.StatusSearching:
		return StatusSearching
	case core.StatusThinking:
		return StatusThinking
	case core.StatusSpeaking:
		return StatusSpeaking
	case core.StatusExecuting:
		return StatusExecuting
	case core.StatusIdle:
		return StatusIdle
	default:
		return StatusIdle
	}
}

// mapStatusTarget maps core.StatusTarget to protocol.StatusTarget
func mapStatusTarget(t core.StatusTarget) StatusTarget {
	switch t {
	case core.StatusTargetUser:
		return StatusTargetUser
	case core.StatusTargetBot:
		return StatusTargetBot
	default:
		return StatusTargetBot
	}
}

// mapActionType maps core.ActionType to protocol.ActionType
func mapActionType(a core.ActionType) ActionType {
	switch a {
	case core.ActionNavigate:
		return ActionNavigate
	case core.ActionFillForm:
		return ActionFillForm
	case core.ActionClick:
		return ActionClick
	case core.ActionScroll:
		return ActionScroll
	case core.ActionShowModal:
		return ActionShowModal
	case core.ActionHideModal:
		return ActionHideModal
	case core.ActionNotify:
		return ActionNotify
	case core.ActionDownload:
		return ActionDownload
	case core.ActionCopy:
		return ActionCopy
	case core.ActionCustom:
		return ActionCustom
	default:
		return ActionCustom
	}
}

// generateMessageID generates a unique message ID
func generateMessageID() string {
	return "msg-" + time.Now().Format("20060102150405.000000")
}
