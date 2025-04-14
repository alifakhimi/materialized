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

type Code = NodeID

// TreeNode represents a node in the materialized path tree
type TreeNode struct {
	gorm.Model

	// Code is a unique node identifier
	Code Code `json:"code,omitempty" gorm:"column:code;uniqueIndex:idx_code,sort:asc"`

	// Tenancy fields
	Tenant TenantFields `json:"tenant_fields,omitempty" gorm:"embedded"`

	// Parent-child relationship
	ParentID *Code       `json:"parent_id,omitempty" gorm:"column:parent_id;size:26;index:idx_parent_id;default:null"`
	Parent   *TreeNode   `json:"parent,omitempty" gorm:"foreignKey:ParentID;references:Code"`
	Children []*TreeNode `json:"children,omitempty" gorm:"foreignKey:ParentID;references:Code"`

	Path Path   `json:"path,omitempty" gorm:"column:path;index:idx_path"`
	Name string `json:"name,omitempty" gorm:"column:name"`

	// Owner fields
	Owner OwnerFields `json:"owner_fields,omitempty" gorm:"embedded"`
}

type TenantFields struct {
	// Multi-tenancy fields
	ID   string `json:"id,omitempty" gorm:"column:tenant_id;index:idx_tenant"`
	Type string `json:"type,omitempty" gorm:"column:tenant_type;index:idx_tenant"`
}

type OwnerFields struct {
	// Polymorphic owner association
	ID   string `json:"id,omitempty" gorm:"column:owner_id;index:idx_owner"`
	Type string `json:"type,omitempty" gorm:"column:owner_type;index:idx_owner"`
}

// TableConfig holds configuration for the tree table
type TableConfig struct {
	// TableName is the name of the table in the database
	TableName string
}

// DefaultTableConfig returns the default table configuration
func DefaultTableConfig() TableConfig {
	return TableConfig{
		TableName: "tree_nodes",
	}
}

// TreeQuery provides methods for querying the tree structure
type TreeQuery struct {
	db     *gorm.DB
	config TableConfig
}

// NewTreeQuery creates a new TreeQuery instance
func NewTreeQuery(db *gorm.DB, config TableConfig) (*TreeQuery, error) {
	if config.TableName == "" {
		return nil, ErrInvalidTableConfig
	}

	return &TreeQuery{
		db:     db,
		config: config,
	}, nil
}

// tenantScope adds tenant-based security scope to queries
func (tq *TreeQuery) tenantScope(tenantID, tenantType string) func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		return db.Where(TenantFields{tenantID, tenantType})
	}
}

// ownerScope adds owner-based scope to queries
func (tq *TreeQuery) ownerScope(ownerID, ownerType string) func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		return db.Where(OwnerFields{ownerID, ownerType})
	}
}

func (tq *TreeQuery) GetNodeByCodeQuery(tx *gorm.DB, code Code, tenantID, tenantType string) *gorm.DB {
	if err := code.Validate(); err != nil {
		tx.AddError(fmt.Errorf("invalid code: %w", err))
		return tx
	}

	return tq.db.Table(tq.config.TableName).
		Scopes(tq.tenantScope(tenantID, tenantType)).
		Where(TreeNode{Code: code})
}

// GetNodeByCode retrieves a node by its code with tenant security
func (tq *TreeQuery) GetNodeByCode(code Code, tenantID, tenantType string) (*TreeNode, error) {
	var node TreeNode
	result := tq.GetNodeByCodeQuery(tq.db, code, tenantID, tenantType).First(&node)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, ErrUnauthorized
		}
		return nil, result.Error
	}

	return &node, nil
}

func (tq *TreeQuery) GetNodeByIDQuery(tx *gorm.DB, id any, tenantID, tenantType string) *gorm.DB {
	return tq.db.Table(tq.config.TableName).
		Scopes(tq.tenantScope(tenantID, tenantType))
}

// GetNodeByID retrieves a node by its ID with tenant security
func (tq *TreeQuery) GetNodeByID(id any, tenantID, tenantType string) (*TreeNode, error) {
	var node TreeNode
	result := tq.GetNodeByIDQuery(tq.db, id, tenantID, tenantType).First(&node, id)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, ErrUnauthorized
		}
		return nil, result.Error
	}

	return &node, nil
}

func (tq *TreeQuery) GetNodeByPathQuery(tx *gorm.DB, path Path, tenantID, tenantType string) *gorm.DB {
	return tq.db.Table(tq.config.TableName).
		Scopes(tq.tenantScope(tenantID, tenantType)).
		Where(TreeNode{Path: path})
}

// GetNodeByPath retrieves a node by its path with tenant security
func (tq *TreeQuery) GetNodeByPath(path Path, tenantID, tenantType string) (*TreeNode, error) {
	var node TreeNode
	result := tq.GetNodeByPathQuery(tq.db, path, tenantID, tenantType).First(&node)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, ErrUnauthorized
		}
		return nil, result.Error
	}

	return &node, nil
}

func (tq *TreeQuery) GetParentByNodeQuery(tx *gorm.DB, node *TreeNode, tenantID, tenantType string) *gorm.DB {
	if node == nil {
		tx.AddError(errors.New("node is nil"))
		return tx
	}

	// If it's a root node (parent_id is null)
	if node.ParentID == nil {
		tx.AddError(errors.New("node is a root node"))
		return tx
	}
	if err := node.ParentID.Validate(); err != nil {
		tx.AddError(errors.New("invalid parent code"))
		return tx
	}

	return tq.db.Table(tq.config.TableName).
		Scopes(tq.tenantScope(tenantID, tenantType)).
		Where(TreeNode{Code: *node.ParentID})
}

func (tq *TreeQuery) GetParentByNode(node *TreeNode, tenantID, tenantType string) (*TreeNode, error) {
	// Get the parent node
	var parent TreeNode
	result := tq.GetParentByNodeQuery(tq.db, node, tenantID, tenantType).First(&parent)
	if result.Error != nil {
		return nil, result.Error
	}

	return &parent, nil
}

func (tq *TreeQuery) GetParentByCodeQuery(tx *gorm.DB, code Code, tenantID, tenantType string) *gorm.DB {
	// First get the node with its parent ID using the code
	node, err := tq.GetNodeByCode(code, tenantID, tenantType)
	if err != nil {
		tx.AddError(err)
		return tx
	}

	return tq.GetParentByNodeQuery(tx, node, tenantID, tenantType)
}

// GetParentByCode retrieves the parent of a node by its code
func (tq *TreeQuery) GetParentByCode(code Code, tenantID, tenantType string) (*TreeNode, error) {
	// First get the node with its parent ID using the code
	node, err := tq.GetNodeByCode(code, tenantID, tenantType)
	if err != nil {
		return nil, err
	}

	return tq.GetParentByNode(node, tenantID, tenantType)
}

func (tq *TreeQuery) GetParentByIDQuery(tx *gorm.DB, id any, tenantID, tenantType string) *gorm.DB {
	// First get the node with its parent ID using the id
	node, err := tq.GetNodeByID(id, tenantID, tenantType)
	if err != nil {
		tx.AddError(err)
		return tx
	}

	return tq.GetParentByNodeQuery(tx, node, tenantID, tenantType)
}

// GetParentByID retrieves the parent of a node
func (tq *TreeQuery) GetParentByID(id any, tenantID, tenantType string) (*TreeNode, error) {
	// First get the node ID from the id
	node, err := tq.GetNodeByID(id, tenantID, tenantType)
	if err != nil {
		return nil, err
	}

	return tq.GetParentByNode(node, tenantID, tenantType)
}

func (tq *TreeQuery) GetParentByPathQuery(tx *gorm.DB, nodePath Path, tenantID, tenantType string) *gorm.DB {
	// First get the node with its parent ID using the path
	node, err := tq.GetNodeByPath(nodePath, tenantID, tenantType)
	if err != nil {
		tx.AddError(err)
		return tx
	}

	return tq.GetParentByNodeQuery(tx, node, tenantID, tenantType)
}

// GetParentByPath retrieves the parent of a node by its path
func (tq *TreeQuery) GetParentByPath(nodePath Path, tenantID, tenantType string) (*TreeNode, error) {
	// First get the node ID from the path
	node, err := tq.GetNodeByPath(nodePath, tenantID, tenantType)
	if err != nil {
		return nil, err
	}

	return tq.GetParentByNode(node, tenantID, tenantType)
}

func (tq *TreeQuery) GetChildrenByParentIDQuery(tx *gorm.DB, code *Code, tenantID, tenantType string) *gorm.DB {
	if err := ValidateNil(code); err != nil {
		tx.AddError(err)
		return tx
	}

	return tx.Table(tq.config.TableName).
		Scopes(tq.tenantScope(tenantID, tenantType)).
		Where(TreeNode{ParentID: code})
}

// GetChildrenByParentID retrieves all direct children of a node
func (tq *TreeQuery) GetChildrenByParentID(code *Code, tenantID, tenantType string) ([]*TreeNode, error) {
	var children []*TreeNode

	// Get all nodes where parent_id matches the given node ID
	result := tq.GetChildrenByParentIDQuery(tq.db, code, tenantID, tenantType).Find(&children)
	if result.Error != nil {
		return nil, result.Error
	}

	return children, nil
}

func (tq *TreeQuery) GetChildrenByCodeQuery(tx *gorm.DB, code Code, tenantID, tenantType string) *gorm.DB {
	// First get the node from the code
	node, err := tq.GetNodeByCode(code, tenantID, tenantType)
	if err != nil {
		tx.AddError(err)
		return tx
	}

	return tq.GetChildrenByParentIDQuery(tx, &node.Code, tenantID, tenantType)
}

// GetChildrenByCode retrieves all direct children of a node by its code
func (tq *TreeQuery) GetChildrenByCode(code Code, tenantID, tenantType string) ([]*TreeNode, error) {
	// First get the node ID from the code
	node, err := tq.GetNodeByCode(code, tenantID, tenantType)
	if err != nil {
		return nil, err
	}

	return tq.GetChildrenByParentID(&node.Code, tenantID, tenantType)
}

// GetChildrenByPathQuery returns a query builder for retrieving all direct children of a node by its path
func (tq *TreeQuery) GetChildrenByPathQuery(tx *gorm.DB, parentPath Path, tenantID, tenantType string) *gorm.DB {
	// First get the node from the path
	node, err := tq.GetNodeByPath(parentPath, tenantID, tenantType)
	if err != nil {
		tx.AddError(err)
		return tx
	}

	return tq.GetChildrenByParentIDQuery(tx, &node.Code, tenantID, tenantType)
}

// GetChildrenByPath retrieves all direct children of a node by its path
// This is an alternative method that uses the path when node ID is not available
func (tq *TreeQuery) GetChildrenByPath(parentPath Path, tenantID, tenantType string) ([]*TreeNode, error) {
	// First get the node ID from the path
	node, err := tq.GetNodeByPath(parentPath, tenantID, tenantType)
	if err != nil {
		return nil, err
	}

	return tq.GetChildrenByParentID(&node.Code, tenantID, tenantType)
}

// GetDescendantsQuery returns a query builder for retrieving all descendants of a node
func (tq *TreeQuery) GetDescendantsQuery(tx *gorm.DB, parentPath Path, tenantID, tenantType string) *gorm.DB {
	return tx.Table(tq.config.TableName).
		Scopes(tq.tenantScope(tenantID, tenantType)).
		Where("path LIKE ? AND path != ?", parentPath.GetPathPrefix(), string(parentPath))
}

// GetDescendants retrieves all descendants of a node
func (tq *TreeQuery) GetDescendants(parentPath Path, tenantID, tenantType string) ([]*TreeNode, error) {
	var descendants []*TreeNode

	result := tq.GetDescendantsQuery(tq.db, parentPath, tenantID, tenantType).
		Find(&descendants)

	if result.Error != nil {
		return nil, result.Error
	}

	return descendants, nil
}

// GetAncestorsQuery returns a query builder for retrieving all ancestors of a node
func (tq *TreeQuery) GetAncestorsQuery(tx *gorm.DB, nodePath Path, tenantID, tenantType string) *gorm.DB {
	if nodePath.IsRoot() {
		return tx.Where("1 = 0") // Return empty query for root node
	}

	ancestorPaths := make([]Path, 0)
	for i := 1; i <= len(nodePath.GetNodeIDs()); i++ {
		if ancestorPath, err := nodePath.GetAncestorAtDepth(i); err == nil {
			ancestorPaths = append(ancestorPaths, ancestorPath)
		}
	}

	return tx.Table(tq.config.TableName).
		Scopes(tq.tenantScope(tenantID, tenantType)).
		Where("path IN (?)", ancestorPaths).
		Order("LENGTH(path), path")
}

// GetAncestors retrieves all ancestors of a node
func (tq *TreeQuery) GetAncestors(nodePath Path, tenantID, tenantType string) ([]*TreeNode, error) {
	if nodePath.IsRoot() {
		return []*TreeNode{}, nil
	}

	var ancestors []*TreeNode

	result := tq.GetAncestorsQuery(tq.db, nodePath, tenantID, tenantType).
		Find(&ancestors)

	if result.Error != nil {
		return nil, result.Error
	}

	// Since GetAncestorsQuery already orders by LENGTH(path) and path,
	// we can return the ancestors directly without additional sorting
	return ancestors, nil
}

// GetAncestorsNested retrieves all ancestors of a node in a nested structure
func (tq *TreeQuery) GetAncestorsNested(nodePath Path, tenantID, tenantType string) (*TreeNode, error) {
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

// CreateNodeQuery creates a new node query in the tree
func (tq *TreeQuery) CreateNodeQuery(
	tx *gorm.DB,
	name string,
	parentPath Path,
	tenantID,
	tenantType string,
	ownerID,
	ownerType string,
) (*TreeNode, *gorm.DB, error) {
	var parent *TreeNode
	var parentID *Code // Default to nil for root

	// Generate a unique NodeID
	newNodeID := NewNodeID()

	// Create path for new node
	nodePath, err := parentPath.AppendNode(newNodeID)
	if err != nil {
		return nil, nil, err
	}

	// If not root, get parent
	if !parentPath.IsRoot() {
		parent, err = tq.GetNodeByPath(parentPath, tenantID, tenantType)
		if err != nil {
			return nil, nil, fmt.Errorf("parent node not found: %w", err)
		}
		parentID = &parent.Code
	}

	// Create the node with all required fields
	node := &TreeNode{
		Code:     newNodeID,
		Name:     name,
		Path:     nodePath,
		Parent:   parent,
		ParentID: parentID,
		Tenant: TenantFields{
			ID:   tenantID,
			Type: tenantType,
		},
		Owner: OwnerFields{
			ID:   ownerID,
			Type: ownerType,
		},
	}

	db := tx
	if db == nil {
		db = tq.db
	}

	return node, db.Table(tq.config.TableName), nil
}

// CreateNode creates a new node in the tree
func (tq *TreeQuery) CreateNode(
	name string,
	parentPath Path,
	tenantID,
	tenantType string,
	ownerID,
	ownerType string,
) (node *TreeNode, err error) {
	err = tq.db.Transaction(func(tx *gorm.DB) error {
		var txErr error
		node, tx, txErr = tq.CreateNodeQuery(tx, name, parentPath, tenantID, tenantType, ownerID, ownerType)
		if txErr != nil {
			return txErr
		}

		if err := tx.Create(node).Error; err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return node, nil
}

// UpdateNodeQuery prepares the query for updating a node
func (tq *TreeQuery) UpdateNodeQuery(
	tx *gorm.DB,
	code Code,
	tenantID,
	tenantType string,
	updates map[string]interface{},
) (*gorm.DB, error) {
	// First check if node exists and belongs to tenant
	_, err := tq.GetNodeByCode(code, tenantID, tenantType)
	if err != nil {
		return nil, err
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

	db := tx
	if db == nil {
		db = tq.db
	}

	// Prepare update query
	query := db.Table(tq.config.TableName).
		Scopes(tq.tenantScope(tenantID, tenantType)).
		Where(TreeNode{Code: code})

	return query, nil
}

// UpdateNode updates a node's properties
func (tq *TreeQuery) UpdateNode(
	code Code,
	tenantID,
	tenantType string,
	updates map[string]interface{},
) error {
	query, err := tq.UpdateNodeQuery(tq.db, code, tenantID, tenantType, updates)
	if err != nil {
		return err
	}

	result := query.Updates(updates)
	return result.Error
}

// MoveNode moves a node and all its descendants to a new parent
func (tq *TreeQuery) MoveNode(
	nodePath Path,
	newParentPath Path,
	tenantID,
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
	var newParentID *Code // Default to nil for root
	if !newParentPath.IsRoot() {
		newParent, err := tq.GetNodeByPath(newParentPath, tenantID, tenantType)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("new parent node not found: %w", err)
		}
		newParentID = newParent.ParentID

		// Check that new parent is not a descendant of the node being moved
		if nodePath.Contains(newParentPath) {
			tx.Rollback()
			return errors.New("cannot move a node to its own descendant")
		}
	}

	// Create new path for the node
	newPath, err := newParentPath.AppendNode(node.Code)
	if err != nil {
		tx.Rollback()
		return err
	}

	// Update the node and all its descendants in a single query
	if err := tx.Table(tq.config.TableName).
		Scopes(tq.tenantScope(tenantID, tenantType)).
		Where("path = ? OR path LIKE ?", string(nodePath), nodePath.GetPathPrefix()).
		Updates(map[string]interface{}{
			"path": gorm.Expr("CONCAT(?, SUBSTRING(path, ?))",
				string(newPath),
				len(string(nodePath))+1,
			),
		}).Error; err != nil {
		tx.Rollback()
		return err
	}

	// Update the moved node's parent_id separately
	if err := tx.Table(tq.config.TableName).
		Scopes(tq.tenantScope(tenantID, tenantType)).
		Where(TreeNode{Code: node.Code}).
		Updates(&TreeNode{ParentID: newParentID}).Error; err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit().Error
}

// DeleteNode deletes a node and optionally its descendants
func (tq *TreeQuery) DeleteNode(
	nodePath Path,
	tenantID,
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
		Where("path LIKE ? AND path != ?", nodePath.GetPathPrefix(), string(nodePath)).
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

		query = query.Where("path = ?", string(nodePath))
	} else {
		query = query.Where("path = ? OR path LIKE ?", string(nodePath), nodePath.GetPathPrefix())
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
	tenantID,
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

// GetNodesByOwnerQuery returns a query builder for nodes associated with a specific owner
func (tq *TreeQuery) GetNodesByOwnerQuery(
	ownerID,
	ownerType,
	tenantID,
	tenantType string,
) *gorm.DB {
	return tq.db.Table(tq.config.TableName).
		Scopes(tq.tenantScope(tenantID, tenantType)).
		Scopes(tq.ownerScope(ownerID, ownerType))
}

// GetNodesByOwner retrieves nodes associated with a specific owner
func (tq *TreeQuery) GetNodesByOwner(
	ownerID,
	ownerType,
	tenantID,
	tenantType string,
	limit int,
	offset int,
) ([]*TreeNode, int64, error) {
	var nodes []*TreeNode
	var count int64

	query := tq.GetNodesByOwnerQuery(ownerID, ownerType, tenantID, tenantType)

	// Count total matches
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

// GetNodesByDepthQuery returns a query builder for nodes at a specific depth in the tree
func (tq *TreeQuery) GetNodesByDepthQuery(
	tx *gorm.DB,
	depth int,
	tenantID,
	tenantType string,
) *gorm.DB {
	// For depth 0, return just the root node query
	if depth == 0 {
		return tq.GetRootNodeQuery(tx, tenantID, tenantType)
	}

	// For other depths, we need to count path separators
	return tx.Table(tq.config.TableName).
		Scopes(tq.tenantScope(tenantID, tenantType)).
		Where("(LENGTH(path) - LENGTH(REPLACE(path, ?, ''))) / ? = ?", PathSeparator, len(PathSeparator), depth)
}

// GetNodesByDepth retrieves nodes at a specific depth in the tree
func (tq *TreeQuery) GetNodesByDepth(
	depth int,
	tenantID,
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

	result := tq.GetNodesByDepthQuery(tq.db, depth, tenantID, tenantType).
		Find(&nodes)

	if result.Error != nil {
		return nil, result.Error
	}

	return nodes, nil
}

// GetRootNodeQuery returns a query builder for the root node
func (tq *TreeQuery) GetRootNodeQuery(
	tx *gorm.DB,
	tenantID,
	tenantType string,
) *gorm.DB {
	return tx.Table(tq.config.TableName).
		Scopes(tq.tenantScope(tenantID, tenantType)).
		Where(TreeNode{Path: RootPath})
}

// GetRootNode retrieves the root node for a tenant
func (tq *TreeQuery) GetRootNode(tenantID, tenantType string) (*TreeNode, error) {
	var rootNode TreeNode

	result := tq.db.Table(tq.config.TableName).
		Scopes(tq.tenantScope(tenantID, tenantType)).
		Where(TreeNode{Path: RootPath}).
		First(&rootNode)

	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			// Create root node if it doesn't exist
			rootNode = TreeNode{
				Path:   RootPath,
				Name:   "root",
				Tenant: TenantFields{tenantID, tenantType},
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
		OwnerID    string
		OwnerType  string
	},
	tenantID,
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
	parentPathMap := make(map[Path]*Code)
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
			Where("path IN (?)", uniqueParentPaths).
			Find(&parentNodes)

		if result.Error != nil {
			tx.Rollback()
			return nil, result.Error
		}

		// Build parent path to ID map
		for _, parent := range parentNodes {
			parentPathMap[parent.Path] = &parent.Code
		}
	}

	// Create nodes using the parent path map
	for _, nodeInfo := range nodes {
		var parentID *Code
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
			Code:     newNodeID,
			Path:     nodePath,
			Name:     nodeInfo.Name,
			ParentID: parentID,
			Tenant:   TenantFields{tenantID, tenantType},
			Owner:    OwnerFields{nodeInfo.OwnerID, nodeInfo.OwnerType},
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

// MigrateDefault creates the database schema for the tree table
func (tq *TreeQuery) MigrateDefault() error {
	return tq.db.AutoMigrate(&TreeNode{})
}

func (tq *TreeQuery) Migrate(m any) error {
	return tq.db.AutoMigrate(m)
}

// WithTransaction allows executing operations within an existing transaction
func (tq *TreeQuery) WithTransaction(tx *gorm.DB) *TreeQuery {
	return &TreeQuery{
		db:     tx,
		config: tq.config,
	}
}

// GetNodeWithChildrenByPathQuery returns a query builder for retrieving a node with its direct children by path
func (tq *TreeQuery) GetNodeWithChildrenByPathQuery(
	tx *gorm.DB,
	tenantID,
	tenantType string,
	path Path,
	limit,
	offset int,
) (*gorm.DB, *TreeNode, int64, error) {
	node, err := tq.GetNodeByPath(path, tenantID, tenantType)
	tx, count, err := tq.loadNodeChildrenQuery(tx, tenantID, tenantType, node.Code, limit, offset, err)
	if err != nil {
		return tx, nil, 0, err
	}

	return tx, node, count, nil
}

// GetNodeWithChildrenByPath retrieves a node with its direct children preloaded using the node path
// with pagination support for the children
func (tq *TreeQuery) GetNodeWithChildrenByPath(
	path Path,
	tenantID,
	tenantType string,
	limit int,
	offset int,
) (*TreeNode, int64, error) {
	query, node, count, err := tq.GetNodeWithChildrenByPathQuery(tq.db, tenantID, tenantType, path, limit, offset)
	err = tq.setNodeChildren(query, node, err)
	if err != nil {
		return nil, 0, err
	}

	return node, count, nil
}

// GetNodeWithChildrenByCodeQuery returns a query builder for retrieving a node with its direct children by code
func (tq *TreeQuery) GetNodeWithChildrenByCodeQuery(
	tx *gorm.DB,
	tenantID,
	tenantType string,
	code Code,
	limit,
	offset int,
) (*gorm.DB, *TreeNode, int64, error) {
	node, err := tq.GetNodeByCode(code, tenantID, tenantType)
	tx, count, err := tq.loadNodeChildrenQuery(tx, tenantID, tenantType, code, limit, offset, err)
	if err != nil {
		return tx, nil, 0, err
	}

	return tx, node, count, nil
}

// GetNodeWithChildrenByCode retrieves a node with its direct children preloaded using the node code
// with pagination support for the children
func (tq *TreeQuery) GetNodeWithChildrenByCode(
	code Code,
	tenantID,
	tenantType string,
	limit int,
	offset int,
) (node *TreeNode, count int64, err error) {
	query, node, count, err := tq.GetNodeWithChildrenByCodeQuery(tq.db, tenantID, tenantType, code, limit, offset)
	err = tq.setNodeChildren(query, node, err)
	if err != nil {
		return nil, 0, err
	}

	return node, count, nil
}

// loadNodeChildrenQuery returns a query builder for loading children of a node
func (tq *TreeQuery) loadNodeChildrenQuery(
	tx *gorm.DB,
	tenantID,
	tenantType string,
	parentCode Code,
	limit int,
	offset int,
	loadErr error,
) (*gorm.DB, int64, error) {
	if loadErr != nil {
		if errors.Is(loadErr, gorm.ErrRecordNotFound) {
			return nil, 0, ErrUnauthorized
		}

		return nil, 0, loadErr
	}

	query := tx.Table(tq.config.TableName).
		Scopes(tq.tenantScope(tenantID, tenantType)).
		Where(TreeNode{ParentID: &parentCode})

	var count int64
	if err := query.Count(&count).Error; err != nil {
		return nil, 0, err
	}

	return query.Limit(limit).Offset(offset), count, nil
}

func (tq *TreeQuery) setNodeChildren(
	query *gorm.DB,
	node *TreeNode,
	err error,
) error {
	if err != nil {
		return err
	}

	var children []*TreeNode
	if err := query.Find(&children).Error; err != nil {
		return err
	}

	node.Children = children
	return nil
}
