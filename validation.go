package pipeline

import (
	"fmt"
	"github.com/madmike/go-pipeline/core"
)

// ValidationError represents a validation error with context
type ValidationError struct {
	Message string
	Details string
}

func (e ValidationError) Error() string {
	if e.Details != "" {
		return fmt.Sprintf("%s: %s", e.Message, e.Details)
	}
	return e.Message
}

// ValidateGraph performs comprehensive validation on a pipeline graph
func ValidateGraph(graph *PipelineGraph) error {
	// Check that entry node exists
	if graph.GetEntryNode() == nil {
		return ValidationError{
			Message: "graph validation failed",
			Details: "no entry node defined",
		}
	}

	// Check for cycles
	if err := detectCycles(graph); err != nil {
		return err
	}

	// Check for unreachable stages
	if err := checkReachability(graph); err != nil {
		return err
	}

	// Check type compatibility
	if err := validateTypeCompatibility(graph); err != nil {
		return err
	}

	return nil
}

// detectCycles uses depth-first search to detect cycles in the graph
func detectCycles(graph *PipelineGraph) error {
	visited := make(map[string]bool)
	recStack := make(map[string]bool)

	for _, node := range graph.AllNodes() {
		if !visited[node.Name()] {
			if hasCycle(node, visited, recStack) {
				return ValidationError{
					Message: "graph validation failed",
					Details: "cycle detected in pipeline graph",
				}
			}
		}
	}

	return nil
}

// hasCycle performs DFS to detect cycles
func hasCycle(node *graphNode, visited, recStack map[string]bool) bool {
	visited[node.Name()] = true
	recStack[node.Name()] = true

	// Visit all adjacent nodes
	for _, edge := range node.Outputs() {
		neighbor := edge.To()

		if !visited[neighbor.Name()] {
			if hasCycle(neighbor, visited, recStack) {
				return true
			}
		} else if recStack[neighbor.Name()] {
			// Back edge found - cycle detected
			return true
		}
	}

	recStack[node.Name()] = false
	return false
}

// checkReachability verifies that all nodes are reachable from the entry node
func checkReachability(graph *PipelineGraph) error {
	entryNode := graph.GetEntryNode()
	if entryNode == nil {
		return ValidationError{
			Message: "graph validation failed",
			Details: "no entry node defined",
		}
	}

	reachable := make(map[string]bool)
	dfsReachability(entryNode, reachable)

	// Check if all nodes are reachable
	for _, node := range graph.AllNodes() {
		if !reachable[node.Name()] {
			return ValidationError{
				Message: "graph validation failed",
				Details: fmt.Sprintf("stage %q is unreachable from entry node", node.Name()),
			}
		}
	}

	return nil
}

// dfsReachability performs DFS to mark all reachable nodes
func dfsReachability(node *graphNode, reachable map[string]bool) {
	if reachable[node.Name()] {
		return
	}

	reachable[node.Name()] = true

	for _, edge := range node.Outputs() {
		dfsReachability(edge.To(), reachable)
	}
}

// validateTypeCompatibility checks that connected stages have compatible types
func validateTypeCompatibility(graph *PipelineGraph) error {
	for _, node := range graph.AllNodes() {
		// Skip validation for synthetic nodes (fan-out, barrier) that don't have stages
		if node.Stage() == nil {
			continue
		}

		// Get output types from this stage
		outputTypes := node.Stage().OutputTypes()

		// For each outgoing edge, check compatibility with downstream stage
		for _, edge := range node.Outputs() {
			downstreamNode := edge.To()

			// Skip validation if downstream is a synthetic node
			if downstreamNode.Stage() == nil {
				continue
			}

			downstreamInputTypes := downstreamNode.Stage().InputTypes()

			// If downstream accepts all types (empty input types), it's compatible
			if len(downstreamInputTypes) == 0 {
				continue
			}

			// If upstream produces all types (empty output types), it's compatible
			if len(outputTypes) == 0 {
				continue
			}

			// Check if there's at least one compatible type
			if !hasCompatibleType(outputTypes, downstreamInputTypes, edge.EventFilter()) {
				return ValidationError{
					Message: "graph validation failed",
					Details: fmt.Sprintf(
						"incompatible types between stage %q (outputs: %v) and stage %q (inputs: %v)",
						node.Name(), outputTypes,
						downstreamNode.Name(), downstreamInputTypes,
					),
				}
			}
		}
	}

	return nil
}

// hasCompatibleType checks if there's at least one compatible type between upstream and downstream
// considering the edge filter
func hasCompatibleType(upstreamTypes, downstreamTypes []core.EventType, filter map[core.EventType]bool) bool {
	// Build a set of upstream types that would be forwarded
	forwardedTypes := make(map[core.EventType]bool)

	if filter == nil {
		// No filter - all upstream types are forwarded
		for _, t := range upstreamTypes {
			forwardedTypes[t] = true
		}
	} else {
		// Filter is present - only forwarded types matter
		for _, t := range upstreamTypes {
			if filter[t] {
				forwardedTypes[t] = true
			}
		}
	}

	// Check if any forwarded type is accepted downstream
	for _, downstreamType := range downstreamTypes {
		if forwardedTypes[downstreamType] {
			return true
		}

		// Check for wildcard acceptance
		if downstreamType == core.EventTypeWildcard {
			return true
		}
	}

	// Check if downstream accepts wildcard
	for _, downstreamType := range downstreamTypes {
		if downstreamType == core.EventTypeWildcard {
			return true
		}
	}

	return false
}
