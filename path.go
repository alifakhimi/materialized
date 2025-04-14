// Package materialized provides utilities for working with materialized paths
// in tree structures. Materialized paths are a technique for representing
// hierarchical data in relational databases that optimizes read operations.
package materialized

import (
	"errors"
	"fmt"
	"strings"
)

const (
	// PathSeparator is the character used to separate node IDs in a path
	PathSeparator = "/"

	// RootPath represents the path of a root node
	RootPath = Path(PathSeparator)
)

var (
	// ErrInvalidPath is returned when an invalid path is provided
	ErrInvalidPath = errors.New("invalid materialized path")

	// ErrInvalidNodeID is returned when an invalid node ID is provided
	ErrInvalidNodeID = errors.New("invalid node ID")
)

// Path represents a materialized path in the tree
type Path string

// NewPath creates a new materialized path
func NewPath(path string) Path {
	return Path(path)
}

// IsRoot checks if the path represents a root node
func (p Path) IsRoot() bool {
	return p == RootPath
}

// Depth returns the depth of the node in the tree
// Root nodes have a depth of 0
func (p Path) Depth() int {
	if p.IsRoot() {
		return 0
	}
	return strings.Count(string(p), PathSeparator)
}

// Parent returns the path of the parent node
func (p Path) Parent() (Path, error) {
	if p.IsRoot() {
		return "", errors.New("root node has no parent")
	}

	lastSepIndex := strings.LastIndex(string(p), PathSeparator)
	if lastSepIndex == 0 {
		// This is a direct child of root
		return Path(RootPath), nil
	}

	return Path(string(p)[:lastSepIndex]), nil
}

// AppendNode creates a new path by appending a node ID to the current path
func (p Path) AppendNode(nodeID NodeID) (Path, error) {
	if nodeID == "" {
		return "", ErrInvalidNodeID
	}

	// Ensure nodeID doesn't contain the path separator
	if strings.Contains(string(nodeID), PathSeparator) {
		return "", fmt.Errorf("node ID cannot contain the path separator '%s'", PathSeparator)
	}

	return Path(fmt.Sprintf("%s%s%s", strings.TrimSuffix(string(p), PathSeparator), PathSeparator, nodeID)), nil
}

// Contains checks if the current path contains another path.
// This is used to determine if a node is an ancestor of another node.
//
// For example:
// - path "a/b" contains "a/b/c" (true)
// - path "a/b" contains "a/b" (false, not a descendant)
// - path "a/b" contains "a/c" (false, different branch)
// - root "/" contains "a/b" (true, root contains all)
// - path "a/b" contains "/" (false, cannot contain root)
//
// The function handles three cases:
// 1. If current path is root - contains all non-root paths
// 2. If sub path is root - nothing can contain root
// 3. Otherwise checks if sub path starts with current path + separator
func (p Path) Contains(sub Path) bool {
	if p.IsRoot() {
		return !sub.IsRoot() // Root contains all non-root nodes
	}

	if sub.IsRoot() {
		return false // Non-root cannot contain root
	}

	// Check if 'sub' starts with 'p' followed by a separator
	return strings.HasPrefix(string(sub), string(p)+PathSeparator)
}

// IsDirectParentOf checks if the current path is the direct parent of another path
func (p Path) IsDirectParentOf(child Path) bool {
	if child.IsRoot() {
		return false // Nothing is a parent of root
	}

	otherParent, err := child.Parent()
	if err != nil {
		return false
	}

	return p == otherParent
}

// GetNodeIDs returns a slice of NodeIDs extracted from the path.
// For a root path, it returns an empty slice. For non-root paths, it splits
// the path and converts each segment into a NodeID.
// GetNodeIDs returns all node IDs in the path
func (p Path) GetNodeIDs() NodeIDs {
	if p.IsRoot() {
		return NodeIDs{}
	}

	// Split path by separator and convert each part to NodeID
	parts := strings.Split(strings.TrimSuffix(string(p), PathSeparator), PathSeparator)
	nodeIDs := make(NodeIDs, len(parts))
	for i, part := range parts {
		nodeIDs[i] = NodeID(part)
	}
	return nodeIDs
}

// GetLastNodeID returns the ID of the last node in the path
func (p Path) GetLastNodeID() (NodeID, error) {
	if p.IsRoot() {
		return "", errors.New("root path has no node ID")
	}

	parts := p.GetNodeIDs()
	return parts[len(parts)-1], nil
}

// IsDescendantOf checks if the current path is a descendant of another path
func (p Path) IsDescendantOf(ancestor Path) bool {
	return ancestor.Contains(p)
}

// GetAncestorAtDepth returns the ancestor path at the specified depth
func (p Path) GetAncestorAtDepth(depth int) (Path, error) {
	if depth < 0 {
		return "", fmt.Errorf("depth cannot be negative: %d", depth)
	}

	currentDepth := p.Depth()
	if depth > currentDepth {
		return "", fmt.Errorf("requested depth %d is greater than path depth %d", depth, currentDepth)
	}

	if depth == 0 {
		return RootPath, nil
	}

	if depth == currentDepth {
		return p, nil
	}

	// Get node IDs and take only up to the requested depth
	nodeIDs := p.GetNodeIDs()
	return NodeIDs(nodeIDs[:depth]).ToPath(), nil
}

// GetPathPrefix returns a SQL LIKE pattern for finding all descendants of this path
func (p Path) GetPathPrefix() string {
	if p.IsRoot() {
		return "%" // All nodes
	}
	return strings.TrimSuffix(string(p), PathSeparator) + PathSeparator + "%"
}

// ValidatePath checks if a path is valid
func ValidatePath(path string) error {
	// Path should not be empty (root)
	if path == "" {
		return ErrInvalidPath
	}

	if path == string(RootPath) {
		return nil // Root path is valid
	}

	// Path should not end with separator
	if strings.HasSuffix(path, PathSeparator) {
		return ErrInvalidPath
	}

	// Path should not have empty segments
	if strings.Contains(path, PathSeparator+PathSeparator) {
		return ErrInvalidPath
	}

	return nil
}
