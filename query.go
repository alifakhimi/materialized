package materialized

import (
	"errors"
	"fmt"

	"gorm.io/gorm"
)

var (
	// ErrUnauthorized is returned when a tenant tries to access unauthorized data
	ErrUnauthorized = errors.New("unauthorized access")

	// ErrInvalidTableConfig is returned when table configuration is invalid
	ErrInvalidTableConfig = errors.New("invalid table configuration")
)

const (
	CondID      = "id = ?"
	CondPathCol = "%s = ?"
)

// TreeNode represents a node in the materialized path tree
type TreeNode struct {
	gorm.Model

	// Code is a unique node identifier
	Code NodeID `gorm:"type:varchar(26);uniqueIndex"`

	// Multi-tenancy fields
	TenantID   uint   `gorm:"index:idx_tenant"`
	TenantType string `gorm:"index:idx_tenant"`

	// Parent-child relationship
	ParentID uint        `gorm:"index:idx_parent"`
	Parent   *TreeNode   `gorm:"foreignKey:ParentID"`
	Children []*TreeNode `gorm:"foreignKey:ParentID"`

	Path Path `gorm:"index:idx_path"`
	Name string

	// Polymorphic owner association
	OwnerID   uint   `gorm:"index:idx_owner"`
	OwnerType string `gorm:"index:idx_owner"`

	// Metadata
	Metadata map[string]interface{} `gorm:"serializer:json"`
}

// TableConfig holds configuration for the tree table
type TableConfig struct {
	// TableName is the name of the table in the database
	TableName string

	// PathColumn is the name of the column that stores the materialized path
	PathColumn string

	// TenantIDColumn is the name of the column that stores the tenant ID
	TenantIDColumn string

	// TenantTypeColumn is the name of the column that stores the tenant type
	TenantTypeColumn string

	// OwnerIDColumn is the name of the column that stores the owner ID
	OwnerIDColumn string

	// OwnerTypeColumn is the name of the column that stores the owner type
	OwnerTypeColumn string
}

// DefaultTableConfig returns the default table configuration
func DefaultTableConfig() TableConfig {
	return TableConfig{
		TableName:        "tree_nodes",
		PathColumn:       "path",
		TenantIDColumn:   "tenant_id",
		TenantTypeColumn: "tenant_type",
		OwnerIDColumn:    "owner_id",
		OwnerTypeColumn:  "owner_type",
	}
}

// TreeQuery provides methods for querying the tree structure
type TreeQuery struct {
	db     *gorm.DB
	config TableConfig
}

// NewTreeQuery creates a new TreeQuery instance
func NewTreeQuery(db *gorm.DB, config TableConfig) (*TreeQuery, error) {
	if config.TableName == "" || config.PathColumn == "" ||
		config.TenantIDColumn == "" || config.TenantTypeColumn == "" ||
		config.OwnerIDColumn == "" || config.OwnerTypeColumn == "" {
		return nil, ErrInvalidTableConfig
	}

	return &TreeQuery{
		db:     db,
		config: config,
	}, nil
}

// tenantScope adds tenant-based security scope to queries
func (tq *TreeQuery) tenantScope(tenantID uint, tenantType string) func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		return db.Where(fmt.Sprintf("%s = ? AND %s = ?",
			tq.config.TenantIDColumn, tq.config.TenantTypeColumn),
			tenantID, tenantType)
	}
}

// GetNodeByID retrieves a node by its ID with tenant security
func (tq *TreeQuery) GetNodeByID(id uint, tenantID uint, tenantType string) (*TreeNode, error) {
	var node TreeNode
	result := tq.db.Table(tq.config.TableName).
		Scopes(tq.tenantScope(tenantID, tenantType)).
		Where(CondID, id).
		First(&node)

	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, ErrUnauthorized
		}
		return nil, result.Error
	}

	return &node, nil
}

// GetNodeByCode retrieves a node by its code with tenant security
func (tq *TreeQuery) GetNodeByCode(code NodeID, tenantID uint, tenantType string) (*TreeNode, error) {
	var node TreeNode
	result := tq.db.Table(tq.config.TableName).
		Scopes(tq.tenantScope(tenantID, tenantType)).
		Where("code = ?", code).
		First(&node)

	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, ErrUnauthorized
		}
		return nil, result.Error
	}

	return &node, nil
}

// GetNodeByPath retrieves a node by its path with tenant security
func (tq *TreeQuery) GetNodeByPath(path Path, tenantID uint, tenantType string) (*TreeNode, error) {
	var node TreeNode
	result := tq.db.Table(tq.config.TableName).
		Scopes(tq.tenantScope(tenantID, tenantType)).
		Where(fmt.Sprintf(CondPathCol, tq.config.PathColumn), string(path)).
		First(&node)

	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, ErrUnauthorized
		}
		return nil, result.Error
	}

	return &node, nil
}

// GetParent retrieves the parent of a node
func (tq *TreeQuery) GetParent(nodeID uint, tenantID uint, tenantType string) (*TreeNode, error) {
	var node TreeNode

	// First get the node with its parent ID
	result := tq.db.Table(tq.config.TableName).
		Scopes(tq.tenantScope(tenantID, tenantType)).
		Where(CondID, nodeID).
		First(&node)

	if result.Error != nil {
		return nil, result.Error
	}

	// If it's a root node (parent_id is 0 or null)
	if node.ParentID == 0 {
		return nil, errors.New("root node has no parent")
	}

	// Get the parent node
	var parent TreeNode
	result = tq.db.Table(tq.config.TableName).
		Scopes(tq.tenantScope(tenantID, tenantType)).
		Where(CondID, node.ParentID).
		First(&parent)

	if result.Error != nil {
		return nil, result.Error
	}

	return &parent, nil
}

// GetParentByCode retrieves the parent of a node by its code
func (tq *TreeQuery) GetParentByCode(code NodeID, tenantID uint, tenantType string) (*TreeNode, error) {
	// First get the node with its parent ID using the code
	node, err := tq.GetNodeByCode(code, tenantID, tenantType)
	if err != nil {
		return nil, err
	}

	// If it's a root node (parent_id is 0 or null)
	if node.ParentID == 0 {
		return nil, errors.New("root node has no parent")
	}

	// Get the parent node
	var parent TreeNode
	result := tq.db.Table(tq.config.TableName).
		Scopes(tq.tenantScope(tenantID, tenantType)).
		Where(CondID, node.ParentID).
		First(&parent)

	if result.Error != nil {
		return nil, result.Error
	}

	return &parent, nil
}

// GetParentByPath retrieves the parent of a node by its path
func (tq *TreeQuery) GetParentByPath(nodePath Path, tenantID uint, tenantType string) (*TreeNode, error) {
	// First get the node ID from the path
	node, err := tq.GetNodeByPath(nodePath, tenantID, tenantType)
	if err != nil {
		return nil, err
	}

	return tq.GetParent(node.ID, tenantID, tenantType)
}

// GetChildren retrieves all direct children of a node
func (tq *TreeQuery) GetChildren(nodeID uint, tenantID uint, tenantType string) ([]*TreeNode, error) {
	var children []*TreeNode

	// Get all nodes where parent_id matches the given node ID
	result := tq.db.Table(tq.config.TableName).
		Scopes(tq.tenantScope(tenantID, tenantType)).
		Where("parent_id = ?", nodeID).
		Find(&children)

	if result.Error != nil {
		return nil, result.Error
	}

	return children, nil
}

// GetChildrenByCode retrieves all direct children of a node by its code
func (tq *TreeQuery) GetChildrenByCode(code NodeID, tenantID uint, tenantType string) ([]*TreeNode, error) {
	// First get the node ID from the code
	node, err := tq.GetNodeByCode(code, tenantID, tenantType)
	if err != nil {
		return nil, err
	}

	return tq.GetChildren(node.ID, tenantID, tenantType)
}

// GetChildrenByPath retrieves all direct children of a node by its path
// This is an alternative method that uses the path when node ID is not available
func (tq *TreeQuery) GetChildrenByPath(parentPath Path, tenantID uint, tenantType string) ([]*TreeNode, error) {
	// First get the node ID from the path
	node, err := tq.GetNodeByPath(parentPath, tenantID, tenantType)
	if err != nil {
		return nil, err
	}

	return tq.GetChildren(node.ID, tenantID, tenantType)
}

// GetDescendants retrieves all descendants of a node
func (tq *TreeQuery) GetDescendants(parentPath Path, tenantID uint, tenantType string) ([]*TreeNode, error) {
	var descendants []*TreeNode

	result := tq.db.Table(tq.config.TableName).
		Scopes(tq.tenantScope(tenantID, tenantType)).
		Where(fmt.Sprintf("%s LIKE ? AND %s != ?",
			tq.config.PathColumn, tq.config.PathColumn),
			parentPath.GetPathPrefix(), string(parentPath)).
		Find(&descendants)

	if result.Error != nil {
		return nil, result.Error
	}

	return descendants, nil
}

// GetAncestors retrieves all ancestors of a node
func (tq *TreeQuery) GetAncestors(nodePath Path, tenantID uint, tenantType string) ([]*TreeNode, error) {
	if nodePath.IsRoot() {
		return []*TreeNode{}, nil
	}

	var ancestors []*TreeNode

	parentPath, err := nodePath.Parent()
	if err != nil {
		return nil, err
	}

	nodeID, err := nodePath.GetLastNodeID()
	if err != nil {
		return nil, err
	}

	// Get all possible ancestor paths in a single query
	result := tq.db.Table(tq.config.TableName).
		Scopes(tq.tenantScope(tenantID, tenantType)).
		Where("code IN (?) and code != ?", parentPath.GetNodeIDs(), nodeID).
		Find(&ancestors)

	if result.Error != nil {
		return nil, result.Error
	}

	// Sort ancestors by depth to maintain order
	sortedAncestors := make([]*TreeNode, 0, len(ancestors))
	ancestorMap := make(map[Path]*TreeNode)
	for _, a := range ancestors {
		ancestorMap[a.Path] = a
	}

	// Build the ordered ancestor list
	for i := 1; i <= len(nodePath.GetNodeIDs()); i++ {
		ancestorPath, err := nodePath.GetAncestorAtDepth(i)
		if err != nil {
			continue
		}
		if ancestor, ok := ancestorMap[ancestorPath]; ok {
			sortedAncestors = append(sortedAncestors, ancestor)
		}
	}

	return sortedAncestors, nil
}

// GetNestedAncestors retrieves all ancestors of a node in a nested structure
func (tq *TreeQuery) GetNestedAncestors(nodePath Path, tenantID uint, tenantType string) (*TreeNode, error) {
	ancestors, err := tq.GetAncestors(nodePath, tenantID, tenantType)
	if err != nil {
		return nil, err
	}

	if len(ancestors) == 0 {
		return nil, nil
	}

	// Start with the root ancestor
	root := ancestors[0]
	current := root

	// Build the nested structure
	for i := 1; i < len(ancestors); i++ {
		current.Children = []*TreeNode{ancestors[i]}
		ancestors[i].Parent = current
		current = ancestors[i]
	}

	return root, nil
}

// CreateNode creates a new node in the tree
func (tq *TreeQuery) CreateNode(
	name string,
	parentPath Path,
	tenantID uint,
	tenantType string,
	ownerID uint,
	ownerType string,
	metadata map[string]interface{},
) (*TreeNode, error) {
	// Start transaction
	tx := tq.db.Begin()
	if tx.Error != nil {
		return nil, tx.Error
	}
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		}
	}()

	var parentID uint = 0 // Default to 0 for root

	// Verify parent exists if not root
	if !parentPath.IsRoot() {
		parent, err := tq.GetNodeByPath(parentPath, tenantID, tenantType)
		if err != nil {
			tx.Rollback()
			return nil, fmt.Errorf("parent node not found: %w", err)
		}
		parentID = parent.ID
	}

	// Generate a unique NodeID
	newNodeID := NewNodeID()

	// Create path for new node
	nodePath, err := parentPath.AppendNode(newNodeID)
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	// Create the node with all required fields
	node := &TreeNode{
		Code:       newNodeID,
		Name:       name,
		Path:       nodePath,
		TenantID:   tenantID,
		TenantType: tenantType,
		ParentID:   parentID,
		OwnerID:    ownerID,
		OwnerType:  ownerType,
		Metadata:   metadata,
	}

	// Create the node in the database
	result := tx.Table(tq.config.TableName).Create(node)
	if result.Error != nil {
		tx.Rollback()
		return nil, result.Error
	}

	// Commit transaction
	if err := tx.Commit().Error; err != nil {
		tx.Rollback()
		return nil, err
	}

	return node, nil
}

// UpdateNode updates a node's properties
func (tq *TreeQuery) UpdateNode(
	id uint,
	tenantID uint,
	tenantType string,
	updates map[string]interface{},
) error {
	// First check if node exists and belongs to tenant
	_, err := tq.GetNodeByID(id, tenantID, tenantType)
	if err != nil {
		return err
	}

	// Remove protected fields from updates
	delete(updates, "id")
	delete(updates, "created_at")
	delete(updates, "updated_at")
	delete(updates, "deleted_at")
	delete(updates, "parent_id")
	delete(updates, "code")
	delete(updates, "path")
	delete(updates, "tenant_id")
	delete(updates, "tenant_type")

	// Apply updates
	result := tq.db.Table(tq.config.TableName).
		Scopes(tq.tenantScope(tenantID, tenantType)).
		Where(CondID, id).
		Updates(updates)

	return result.Error
}

// MoveNode moves a node and all its descendants to a new parent
func (tq *TreeQuery) MoveNode(
	nodePath Path,
	newParentPath Path,
	tenantID uint,
	tenantType string,
) error {
	// Start a transaction
	tx := tq.db.Begin()
	if tx.Error != nil {
		return tx.Error
	}
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		}
	}()

	// Get the node to move
	node, err := tq.GetNodeByPath(nodePath, tenantID, tenantType)
	if err != nil {
		tx.Rollback()
		return err
	}

	// Get new parent ID
	var newParentID uint = 0 // Default to 0 for root
	if !newParentPath.IsRoot() {
		newParent, err := tq.GetNodeByPath(newParentPath, tenantID, tenantType)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("new parent node not found: %w", err)
		}
		newParentID = newParent.ID

		// Check that new parent is not a descendant of the node being moved
		if nodePath.Contains(newParentPath) {
			tx.Rollback()
			return errors.New("cannot move a node to its own descendant")
		}
	}

	// Get the node ID of the node being moved
	nodeID, err := nodePath.GetLastNodeID()
	if err != nil {
		tx.Rollback()
		return err
	}

	// Create new path for the node
	newPath, err := newParentPath.AppendNode(nodeID)
	if err != nil {
		tx.Rollback()
		return err
	}

	// Get all descendants to update their paths
	descendants, err := tq.GetDescendants(nodePath, tenantID, tenantType)
	if err != nil {
		tx.Rollback()
		return err
	}

	// Update the node's path and parent_id
	if err := tx.Table(tq.config.TableName).
		Scopes(tq.tenantScope(tenantID, tenantType)).
		Where("id = ?", node.ID).
		Updates(map[string]interface{}{
			tq.config.PathColumn: string(newPath),
			"parent_id":          newParentID,
		}).Error; err != nil {
		tx.Rollback()
		return err
	}

	// Update all descendants' paths
	for _, descendant := range descendants {
		descendantPath := Path(descendant.Path)
		relPath := descendantPath[len(string(nodePath)):]
		newDescPath := newPath + relPath

		if err := tx.Table(tq.config.TableName).
			Scopes(tq.tenantScope(tenantID, tenantType)).
			Where("id = ?", descendant.ID).
			Update(tq.config.PathColumn, string(newDescPath)).Error; err != nil {
			tx.Rollback()
			return err
		}
	}

	return tx.Commit().Error
}

// DeleteNode deletes a node and optionally its descendants
func (tq *TreeQuery) DeleteNode(
	nodePath Path,
	tenantID uint,
	tenantType string,
	deleteDescendants bool,
) error {
	// Start a transaction
	tx := tq.db.Begin()
	if tx.Error != nil {
		return tx.Error
	}
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		}
	}()

	// Verify node exists and belongs to tenant
	_, err := tq.GetNodeByPath(nodePath, tenantID, tenantType)
	if err != nil {
		tx.Rollback()
		return err
	}

	// Check if node has descendants without loading them all into memory
	var count int64
	if err := tx.Table(tq.config.TableName).
		Scopes(tq.tenantScope(tenantID, tenantType)).
		Where(fmt.Sprintf("%s LIKE ? AND %s != ?",
			tq.config.PathColumn, tq.config.PathColumn),
			nodePath.GetPathPrefix(), string(nodePath)).
		Count(&count).Error; err != nil {
		tx.Rollback()
		return err
	}

	// Delete the node and its descendants if requested
	query := tx.Table(tq.config.TableName).
		Scopes(tq.tenantScope(tenantID, tenantType))

	if !deleteDescendants {
		if count > 0 {
			tx.Rollback()
			return errors.New("cannot delete node with descendants, set deleteDescendants to true")
		}

		query = query.Where(fmt.Sprintf("%s = ?", tq.config.PathColumn),
			string(nodePath))
	} else {
		query = query.Where(fmt.Sprintf("%s = ? OR %s LIKE ?",
			tq.config.PathColumn, tq.config.PathColumn),
			string(nodePath), nodePath.GetPathPrefix())
	}

	if err := query.Delete(&TreeNode{}).Error; err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit().Error
}

// SearchNodes searches for nodes by name or metadata with tenant security
func (tq *TreeQuery) SearchNodes(
	query string,
	tenantID uint,
	tenantType string,
	limit int,
	offset int,
) ([]*TreeNode, int64, error) {
	var nodes []*TreeNode
	var count int64

	// Count total matches
	countQuery := tq.db.Table(tq.config.TableName).
		Scopes(tq.tenantScope(tenantID, tenantType)).
		Where("name LIKE ?", "%"+query+"%")

	if err := countQuery.Count(&count).Error; err != nil {
		return nil, 0, err
	}

	// Get paginated results
	result := tq.db.Table(tq.config.TableName).
		Scopes(tq.tenantScope(tenantID, tenantType)).
		Where("name LIKE ?", "%"+query+"%").
		Limit(limit).
		Offset(offset).
		Find(&nodes)

	if result.Error != nil {
		return nil, 0, result.Error
	}

	return nodes, count, nil
}

// GetNodesByOwner retrieves nodes associated with a specific owner
func (tq *TreeQuery) GetNodesByOwner(
	ownerID uint,
	ownerType string,
	tenantID uint,
	tenantType string,
	limit int,
	offset int,
) ([]*TreeNode, int64, error) {
	var nodes []*TreeNode
	var count int64

	// Count total matches
	query := tq.db.Table(tq.config.TableName).
		Scopes(tq.tenantScope(tenantID, tenantType)).
		Where(fmt.Sprintf("%s = ? AND %s = ?",
			tq.config.OwnerIDColumn, tq.config.OwnerTypeColumn),
			ownerID, ownerType)

	if err := query.Count(&count).Error; err != nil {
		return nil, 0, err
	}

	// Get paginated results
	result := query.
		Limit(limit).
		Offset(offset).
		Find(&nodes)

	if result.Error != nil {
		return nil, 0, result.Error
	}

	return nodes, count, nil
}

// GetNodesByDepth retrieves nodes at a specific depth in the tree
func (tq *TreeQuery) GetNodesByDepth(
	depth int,
	tenantID uint,
	tenantType string,
) ([]*TreeNode, error) {
	var nodes []*TreeNode

	// For depth 0, return just the root node
	if depth == 0 {
		rootNode, err := tq.GetRootNode(tenantID, tenantType)
		if err != nil {
			return nil, err
		}

		return []*TreeNode{rootNode}, nil
	}

	// For other depths, we need to count path separators
	result := tq.db.Table(tq.config.TableName).
		Scopes(tq.tenantScope(tenantID, tenantType)).
		Where(fmt.Sprintf("(LENGTH(%s) - LENGTH(REPLACE(%s, ?, ''))) / ? = ?",
			tq.config.PathColumn, tq.config.PathColumn),
			PathSeparator, len(PathSeparator), depth).
		Find(&nodes)

	if result.Error != nil {
		return nil, result.Error
	}

	return nodes, nil
}

// GetRootNode retrieves the root node for a tenant
func (tq *TreeQuery) GetRootNode(tenantID uint, tenantType string) (*TreeNode, error) {
	var rootNode TreeNode

	result := tq.db.Table(tq.config.TableName).
		Scopes(tq.tenantScope(tenantID, tenantType)).
		Where(fmt.Sprintf(CondPathCol, tq.config.PathColumn), string(RootPath)).
		First(&rootNode)

	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			// Create root node if it doesn't exist
			rootNode = TreeNode{
				Path:       RootPath,
				Name:       "Root",
				TenantID:   tenantID,
				TenantType: tenantType,
				Metadata:   map[string]interface{}{"isRoot": true},
			}

			if err := tq.db.Table(tq.config.TableName).Create(&rootNode).Error; err != nil {
				return nil, err
			}

			return &rootNode, nil
		}
		return nil, result.Error
	}

	return &rootNode, nil
}

// BatchCreateNodes creates multiple nodes in a single transaction
func (tq *TreeQuery) BatchCreateNodes(
	nodes []struct {
		Name       string
		ParentPath Path
		OwnerID    uint
		OwnerType  string
		Metadata   map[string]interface{}
	},
	tenantID uint,
	tenantType string,
) ([]*TreeNode, error) {
	tx := tq.db.Begin()
	if tx.Error != nil {
		return nil, tx.Error
	}
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		}
	}()

	createdNodes := make([]*TreeNode, 0, len(nodes))
	batchNodes := make([]*TreeNode, 0, len(nodes))

	// Create a map to store parent paths and their IDs
	parentPathMap := make(map[Path]uint)
	uniqueParentPaths := make([]Path, 0)

	// Collect unique parent paths
	for _, nodeInfo := range nodes {
		if !nodeInfo.ParentPath.IsRoot() {
			parentPathStr := nodeInfo.ParentPath
			if _, exists := parentPathMap[parentPathStr]; !exists {
				uniqueParentPaths = append(uniqueParentPaths, nodeInfo.ParentPath)
			}
		}
	}

	// Fetch all parent nodes in a single query
	if len(uniqueParentPaths) > 0 {
		var parentNodes []*TreeNode
		result := tx.Table(tq.config.TableName).
			Scopes(tq.tenantScope(tenantID, tenantType)).
			Where(fmt.Sprintf("%s IN ?", tq.config.PathColumn), uniqueParentPaths).
			Find(&parentNodes)

		if result.Error != nil {
			tx.Rollback()
			return nil, result.Error
		}

		// Build parent path to ID map
		for _, parent := range parentNodes {
			parentPathMap[parent.Path] = parent.ID
		}
	}

	// Create nodes using the parent path map
	for _, nodeInfo := range nodes {
		parentID := uint(0)
		if !nodeInfo.ParentPath.IsRoot() {
			var exists bool
			parentID, exists = parentPathMap[nodeInfo.ParentPath]
			if !exists {
				tx.Rollback()
				return nil, fmt.Errorf("parent node not found for path %s", nodeInfo.ParentPath)
			}
		}

		newNodeID := NewNodeID()
		nodePath, err := nodeInfo.ParentPath.AppendNode(newNodeID)
		if err != nil {
			tx.Rollback()
			return nil, err
		}

		node := &TreeNode{
			Code:       newNodeID,
			Path:       nodePath,
			Name:       nodeInfo.Name,
			TenantID:   tenantID,
			TenantType: tenantType,
			ParentID:   parentID,
			OwnerID:    nodeInfo.OwnerID,
			OwnerType:  nodeInfo.OwnerType,
			Metadata:   nodeInfo.Metadata,
		}

		batchNodes = append(batchNodes, node)
	}

	if err := tx.Table(tq.config.TableName).CreateInBatches(batchNodes, 100).Error; err != nil {
		tx.Rollback()
		return nil, err
	}

	if err := tx.Commit().Error; err != nil {
		return nil, err
	}

	createdNodes = append(createdNodes, batchNodes...)
	return createdNodes, nil
}

// CreateSchema creates the database schema for the tree table
func (tq *TreeQuery) CreateSchema() error {
	// Define table creation SQL
	tableSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			code VARCHAR(26) NOT NULL UNIQUE,
			parent_id INTEGER DEFAULT 0,
			%s TEXT NOT NULL,
			name TEXT NOT NULL,
			%s INTEGER NOT NULL,
			%s TEXT NOT NULL,
			%s INTEGER NOT NULL,
			%s TEXT NOT NULL,
			metadata JSONB,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			deleted_at TIMESTAMP WITH TIME ZONE
		)`,
		tq.config.TableName,
		tq.config.PathColumn,
		tq.config.TenantIDColumn,
		tq.config.TenantTypeColumn,
		tq.config.OwnerIDColumn,
		tq.config.OwnerTypeColumn)

	// Define indexes as separate statements for better maintainability
	indexStatements := []string{
		// Node ID index
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_code ON %s (code)",
			tq.config.TableName, tq.config.TableName),

		// Path index
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_%s ON %s (%s)",
			tq.config.TableName, tq.config.PathColumn, tq.config.TableName, tq.config.PathColumn),

		// Tenant composite index
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_tenant ON %s (%s, %s)",
			tq.config.TableName, tq.config.TableName, tq.config.TenantIDColumn, tq.config.TenantTypeColumn),

		// Owner composite index
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_owner ON %s (%s, %s)",
			tq.config.TableName, tq.config.TableName, tq.config.OwnerIDColumn, tq.config.OwnerTypeColumn),

		// Parent ID index
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_parent ON %s (parent_id)",
			tq.config.TableName, tq.config.TableName),

		// Path prefix index for hierarchical queries
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_path_prefix ON %s (%s text_pattern_ops)",
			tq.config.TableName, tq.config.TableName, tq.config.PathColumn),
	}

	// Execute table creation in a transaction
	tx := tq.db.Begin()
	if tx.Error != nil {
		return tx.Error
	}
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		}
	}()

	// Create table
	if err := tx.Exec(tableSQL).Error; err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to create table: %w", err)
	}

	// Create indexes
	for i, indexSQL := range indexStatements {
		if err := tx.Exec(indexSQL).Error; err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to create index #%d: %w", i+1, err)
		}
	}

	return tx.Commit().Error
}

// WithTransaction allows executing operations within an existing transaction
func (tq *TreeQuery) WithTransaction(tx *gorm.DB) *TreeQuery {
	return &TreeQuery{
		db:     tx,
		config: tq.config,
	}
}

// GetNodeWithChildren retrieves a node with its direct children preloaded
func (tq *TreeQuery) GetNodeWithChildren(
	nodeID uint,
	tenantID uint,
	tenantType string,
	limit int,
	offset int,
) (*TreeNode, int64, error) {
	var node TreeNode

	result := tq.db.Table(tq.config.TableName).
		Scopes(tq.tenantScope(tenantID, tenantType)).
		Where(CondID, nodeID).
		First(&node)

	return tq.loadNodeChildren(&node, tenantID, tenantType, limit, offset, result.Error)
}

// GetNodeWithChildrenByCode retrieves a node with its direct children preloaded using the node code
// with pagination support for the children
func (tq *TreeQuery) GetNodeWithChildrenByCode(
	code NodeID,
	tenantID uint,
	tenantType string,
	limit int,
	offset int,
) (*TreeNode, int64, error) {
	var node TreeNode
	result := tq.db.Table(tq.config.TableName).
		Scopes(tq.tenantScope(tenantID, tenantType)).
		Where("code = ?", code).
		First(&node)

	return tq.loadNodeChildren(&node, tenantID, tenantType, limit, offset, result.Error)
}

func (tq *TreeQuery) loadNodeChildren(
	node *TreeNode,
	tenantID uint,
	tenantType string,
	limit int,
	offset int,
	loadErr error,
) (*TreeNode, int64, error) {
	if loadErr != nil {
		if errors.Is(loadErr, gorm.ErrRecordNotFound) {
			return nil, 0, ErrUnauthorized
		}

		return nil, 0, loadErr
	}

	query := tq.db.Table(tq.config.TableName).
		Scopes(tq.tenantScope(tenantID, tenantType)).
		Where("parent_id = ?", node.ID)

	var totalChildren int64
	if err := query.Count(&totalChildren).Error; err != nil {
		return nil, 0, err
	}

	var children []*TreeNode
	if err := query.
		Limit(limit).
		Offset(offset).
		Find(&children).Error; err != nil {
		return nil, 0, err
	}

	node.Children = children
	return node, totalChildren, nil
}
