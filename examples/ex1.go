package main

import (
	"fmt"

	"github.com/alifakhimi/materialized"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type TreeNode struct {
	gorm.Model
	materialized.TreeNode
}

func main() {
	// Database setup
	db, err := gorm.Open(sqlite.Open("tree.db"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Info),
	})
	if err != nil {
		panic("failed to connect")
	}

	treeQuery, err := materialized.NewTreeQuery(db, materialized.DefaultTableConfig())
	if err != nil {
		panic("failed to initialize")
	}

	if err := treeQuery.MigrateDefault(); err != nil {
		panic("failed to create schema")
	}

	// Tenant setup
	tenantID := "1"
	tenantType := "organizations"

	// Get root node
	root, err := treeQuery.GetRootNode(tenantID, tenantType)
	if err != nil {
		panic("failed to get root")
	}
	fmt.Printf("Root: %s (%s)\n", root.Name, root.Path)

	// Create nodes
	nodeA, err := treeQuery.CreateNode("Node A", root.Path, tenantID, tenantType, "123", "users")
	if err != nil {
		panic("failed to create Node A")
	}
	fmt.Printf("Node A: %s (%s)\n", nodeA.Name, nodeA.Path)

	nodeB, err := treeQuery.CreateNode("Node B", root.Path, tenantID, tenantType, "124", "users")
	if err != nil {
		panic("failed to create Node B")
	}
	fmt.Printf("Node B: %s (%s)\n", nodeB.Name, nodeB.Path)

	nodeC, err := treeQuery.CreateNode("Node C", nodeA.Path, tenantID, tenantType, "125", "users")
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
