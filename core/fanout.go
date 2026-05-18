package core

// ErrorPolicy defines how fan-out handles errors in parallel branches
type ErrorPolicy string

const (
	// ErrorPolicyCancelAll cancels all branches when one fails (default)
	ErrorPolicyCancelAll ErrorPolicy = "cancel-all"

	// ErrorPolicyIsolated allows other branches to continue when one fails
	ErrorPolicyIsolated ErrorPolicy = "isolated"
)

// BranchConfig defines a single fan-out branch
type BranchConfig struct {
	// Stage is the downstream stage for this branch
	Stage Stage

	// EventFilter specifies which event types to forward to this branch.
	// Empty slice means forward all events.
	EventFilter []EventType
}

// FanOutConfig configures parallel routing behavior
type FanOutConfig struct {
	// ErrorPolicy determines behavior when a branch fails
	ErrorPolicy ErrorPolicy

	// Branches defines the downstream routing for each branch
	Branches []BranchConfig
}
