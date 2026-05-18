package core

// EventType categorizes pipeline events
type EventType string

const (
	EventTypeStatus         EventType = "status"
	EventTypeSTT            EventType = "stt"
	EventTypeLLM            EventType = "llm"
	EventTypeAudio          EventType = "audio"
	EventTypeAction         EventType = "action"
	EventTypeError          EventType = "error"
	EventTypeDone           EventType = "done"
	EventTypeServiceMessage EventType = "service_message"
	EventTypeDocument       EventType = "document"
	EventTypeRAG            EventType = "rag"
	EventTypeUserMessage    EventType = "user_message"
)

// StatusType defines the current processing status
type StatusType string

const (
	StatusListening    StatusType = "listening"
	StatusTranscribing StatusType = "transcribing"
	StatusSearching    StatusType = "searching"
	StatusThinking     StatusType = "thinking"
	StatusSpeaking     StatusType = "speaking"
	StatusExecuting    StatusType = "executing"
	StatusIdle         StatusType = "idle"
)

// StatusTarget defines where the status should be displayed
type StatusTarget string

const (
	StatusTargetUser StatusTarget = "user"
	StatusTargetBot  StatusTarget = "bot"
)

// ActionType defines the type of action
type ActionType string

const (
	ActionNavigate  ActionType = "navigate"
	ActionFillForm  ActionType = "fill_form"
	ActionClick     ActionType = "click"
	ActionScroll    ActionType = "scroll"
	ActionShowModal ActionType = "show_modal"
	ActionHideModal ActionType = "hide_modal"
	ActionNotify    ActionType = "notify"
	ActionDownload  ActionType = "download"
	ActionCopy      ActionType = "copy"
	ActionCustom    ActionType = "custom"
)

// ServiceMessageType defines the type of service message
type ServiceMessageType string

const (
	ServiceMessageRetryRequest ServiceMessageType = "retry_request"
	ServiceMessageInfo         ServiceMessageType = "info"
	ServiceMessageWarning      ServiceMessageType = "warning"
	ServiceMessageError        ServiceMessageType = "error"
)
