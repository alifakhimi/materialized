# Materialized Path Tree Library

The `materialized` library is a Go module that provides tools to manage hierarchical (tree-like) data structures in a relational database using the materialized path pattern. It integrates seamlessly with [GORM](https://gorm.io/) for ORM functionality and uses [ULID](https://github.com/oklog/ulid) for generating unique node identifiers. The library is designed with multi-tenancy support, allowing isolated tree structures for different tenants (e.g., organizations or users).

Materialized paths represent a tree by storing the full path from the root to each node as a string (e.g., `/node1/node2`). This approach optimizes queries for hierarchical operations like finding ancestors or descendants.

## Features

- **Node Management**: Create, update, move, and delete nodes in the tree.
- **Querying**: Retrieve nodes by ID, code, or path; fetch ancestors, descendants, or direct children.
- **Multi-Tenancy**: Isolate trees by tenant using `TenantID` and `TenantType`.
- **Polymorphic Ownership**: Associate nodes with owners using `OwnerID` and `OwnerType`.
- **Metadata**: Store arbitrary key-value data with nodes using JSON-serialized `Metadata`.
- **Efficient Hierarchical Queries**: Leverage materialized paths for fast tree traversal.
- **Unique Identifiers**: Generate ULIDs for each node via the `Code` field.

## Installation

To install the library, run:

```bash
go get github.com/alifakhimi/materialized
```

## Usage

### Setting Up the Database

First, establish a database connection using GORM and create a `TreeQuery` instance to interact with the tree.

```go
package main

import (
 "github.com/alifakhimi/materialized"
 "gorm.io/driver/sqlite" // Use your preferred database driver
 "gorm.io/gorm"
)

func main() {
 // Connect to the database (SQLite in this example)
 db, err := gorm.Open(sqlite.Open("test.db"), &gorm.Config{})
 if err != nil {
  panic("failed to connect to database")
 }

 // Initialize TreeQuery with default configuration
 config := materialized.DefaultTableConfig()
 treeQuery, err := materialized.NewTreeQuery(db, config)
 if err != nil {
  panic("failed to create tree query")
 }

 // Create the database schema (table and indexes)
 if err := treeQuery.CreateSchema(); err != nil {
  panic("failed to create schema")
 }
}
```

Supported databases include any GORM-compatible database (e.g., SQLite, PostgreSQL, MySQL). Adjust the driver and DSN accordingly.

### Creating Nodes

Nodes are created under a parent path. The root node for each tenant is automatically created when accessed via `GetRootNode`.

```go
// Define tenant identifiers
tenantID := uint(1)
tenantType := "organizations"

// Get or create the root node
rootNode, err := treeQuery.GetRootNode(tenantID, tenantType)
if err != nil {
 panic("failed to get root node")
}

// Create a child node under the root
childNode, err := treeQuery.CreateNode(
 "Child Node",
 rootNode.Path, // "/"
 tenantID,
 tenantType,
 0, // ownerID (optional)
 "", // ownerType (optional)
 map[string]interface{}{
  "description": "A child node under the root",
 },
)
if err != nil {
 panic("failed to create child node")
}
```

- **Root Path**: The root node's path is `/`.
- **Child Paths**: A child of the root has a path like `nodeID`, and deeper nodes have paths like `nodeID1/nodeID2`.

### Querying the Tree

The library provides methods to query the tree structure:

```go
// Get a node by its unique code
node, err := treeQuery.GetNodeByCode(childNode.Code, tenantID, tenantType)
if err != nil {
 panic("failed to get node by code")
}

// Get direct children of the root
children, err := treeQuery.GetChildrenByPath(rootNode.Path, tenantID, tenantType)
if err != nil {
 panic("failed to get children")
}

// Get all descendants of the root
descendants, err := treeQuery.GetDescendants(rootNode.Path, tenantID, tenantType)
if err != nil {
 panic("failed to get descendants")
}

// Get ancestors of a node
ancestors, err := treeQuery.GetAncestors(childNode.Path, tenantID, tenantType)
if err != nil {
 panic("failed to get ancestors")
}
```

### Moving Nodes

Move a node and its subtree to a new parent:

```go
// Move childNode to a new parent (e.g., root)
err = treeQuery.MoveNode(childNode.Path, rootNode.Path, tenantID, tenantType)
if err != nil {
 panic("failed to move node")
}
```

### Deleting Nodes

Delete a node with or without its descendants:

```go
// Delete childNode and its descendants
err = treeQuery.DeleteNode(childNode.Path, tenantID, tenantType, true)
if err != nil {
 panic("failed to delete node")
}
```

### Searching Nodes

Search nodes by name:

```go
nodes, total, err := treeQuery.SearchNodes("Child", tenantID, tenantType, 10, 0)
if err != nil {
 panic("failed to search nodes")
}
for _, n := range nodes {
 println(n.Name, string(n.Path))
}
println("Total matches:", total)
```

## Configuration

Customize the table and column names using `TableConfig`:

```go
config := materialized.TableConfig{
 TableName:        "custom_tree",
 PathColumn:       "hierarchy_path",
 TenantIDColumn:   "org_id",
 TenantTypeColumn: "org_type",
 OwnerIDColumn:    "user_id",
 OwnerTypeColumn:  "user_type",
}

treeQuery, err := materialized.NewTreeQuery(db, config)
if err != nil {
 panic("invalid configuration")
}
```

The default configuration (`DefaultTableConfig`) uses:

- Table: `tree_nodes`
- Columns: `path`, `tenant_id`, `tenant_type`, `owner_id`, `owner_type`

## Comprehensive Example

This example demonstrates setting up a tree, creating nodes, moving them, and querying the structure:

```go
package main

import (
 "fmt"

 "github.com/alifakhimi/materialized"
 "gorm.io/driver/sqlite"
 "gorm.io/gorm"
)

func main() {
 // Database setup
 db, err := gorm.Open(sqlite.Open("tree.db"), &gorm.Config{})
 if err != nil {
  panic("failed to connect")
 }

 treeQuery, err := materialized.NewTreeQuery(db, materialized.DefaultTableConfig())
 if err != nil {
  panic("failed to initialize")
 }

 if err := treeQuery.CreateSchema(); err != nil {
  panic("failed to create schema")
 }

 // Tenant setup
 tenantID := uint(1)
 tenantType := "organizations"

 // Get root node
 root, err := treeQuery.GetRootNode(tenantID, tenantType)
 if err != nil {
  panic("failed to get root")
 }
 fmt.Printf("Root: %s (%s)\n", root.Name, root.Path)

 // Create nodes
 nodeA, err := treeQuery.CreateNode("Node A", root.Path, tenantID, tenantType, 0, "", nil)
 if err != nil {
  panic("failed to create Node A")
 }
 fmt.Printf("Node A: %s (%s)\n", nodeA.Name, nodeA.Path)

 nodeB, err := treeQuery.CreateNode("Node B", root.Path, tenantID, tenantType, 0, "", nil)
 if err != nil {
  panic("failed to create Node B")
 }
 fmt.Printf("Node B: %s (%s)\n", nodeB.Name, nodeB.Path)

 nodeC, err := treeQuery.CreateNode("Node C", nodeA.Path, tenantID, tenantType, 0, "", nil)
 if err != nil {
  panic("failed to create Node C")
 }
 fmt.Printf("Node C: %s (%s)\n", nodeC.Name, nodeC.Path)

 // List descendants of root
 descendants, err := treeQuery.GetDescendants(root.Path, tenantID, tenantType)
 if err != nil {
  panic("failed to get descendants")
 }
 fmt.Println("Descendants of root:")
 for _, d := range descendants {
  fmt.Printf("- %s (%s)\n", d.Name, d.Path)
 }

 // Move Node C under Node B
 err = treeQuery.MoveNode(nodeC.Path, nodeB.Path, tenantID, tenantType)
 if err != nil {
  panic("failed to move Node C")
 }
 movedC, err := treeQuery.GetNodeByCode(nodeC.Code, tenantID, tenantType)
 if err != nil {
  panic("failed to get moved Node C")
 }
 fmt.Printf("Node C moved to: %s\n", movedC.Path)

 // List children of Node B
 children, err := treeQuery.GetChildrenByCode(nodeB.Code, tenantID, tenantType)
 if err != nil {
  panic("failed to get children")
 }
 fmt.Println("Children of Node B:")
 for _, c := range children {
  fmt.Printf("- %s (%s)\n", c.Name, c.Path)
 }
}
```

### Expected Output

```bash
Root: Root (/)
Node A: Node A (abc123)
Node B: Node B (def456)
Node C: Node C (abc123/ghi789)
Descendants of root:
- Node A (abc123)
- Node B (def456)
- Node C (abc123/ghi789)
Node C moved to: def456/ghi789
Children of Node B:
- Node C (def456/ghi789)
```

(Note: Actual ULIDs will differ with each run.)

## Contributing

Contributions are welcome! Please submit issues or pull requests to the [GitHub repository](https://github.com/alifakhimi/materialized). Ensure tests are included with any new features or bug fixes.

## License

This library is released under the [MIT License](LICENSE). See the LICENSE file for details.
