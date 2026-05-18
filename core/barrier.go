package core

// MergeStrategy defines how a barrier combines events from multiple upstream branches
type MergeStrategy string

const (
	// MergeStrategyCollect collects all events from all branches in arrival order
	MergeStrategyCollect MergeStrategy = "collect"

	// MergeStrategyLastOnly emits only the final event from each branch
	MergeStrategyLastOnly MergeStrategy = "last-only"
)

// BarrierConfig configures synchronization behavior for a barrier stage
type BarrierConfig struct {
	// UpstreamCount is the number of branches to wait for
	UpstreamCount int

	// MergeStrategy defines how to combine events from branches
	MergeStrategy MergeStrategy
}
