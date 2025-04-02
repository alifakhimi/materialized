package materialized

import (
	"math/rand"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"
)

// NodeID represents a unique identifier for a node in the tree
type NodeID string

type NodeIDs []NodeID

// ToPath converts NodeIIDs to a materialized path
func (nids NodeIDs) ToPath() Path {
	if len(nids) == 0 {
		return RootPath
	}

	strs := make([]string, len(nids))
	for i, nid := range nids {
		strs[i] = string(nid)
	}
	return Path(string(RootPath) + strings.Join(strs, PathSeparator))
}

// NewNodeID generates a new ULID-based NodeID
func NewNodeID() NodeID {
	entropy := ulid.Monotonic(rand.New(rand.NewSource(time.Now().UnixNano())), 0)
	id := ulid.MustNew(ulid.Timestamp(time.Now()), entropy)
	return NodeID(id.String())
}
