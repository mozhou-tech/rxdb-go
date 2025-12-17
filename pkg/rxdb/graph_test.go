package rxdb

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"
)

// TestDatabase_Graph_Init 测试图数据库初始化
func TestDatabase_Graph_Init(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_graph_init.db"
	defer os.RemoveAll(dbPath)

	// 测试启用图数据库
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "test_graph",
		Path: dbPath,
		GraphOptions: &GraphOptions{
			Enabled: true,
			Backend: "memory",
		},
	})
	if err != nil {
		t.Fatalf("Failed to create database with graph: %v", err)
	}
	defer db.Close(ctx)

	graphDB := db.Graph()
	if graphDB == nil {
		t.Fatal("Graph database should not be nil")
	}

	// 测试未启用图数据库
	db2, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "test_graph_disabled",
		Path: dbPath + "_disabled",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer os.RemoveAll(dbPath + "_disabled")
	defer db2.Close(ctx)

	graphDB2 := db2.Graph()
	if graphDB2 != nil {
		t.Error("Graph database should be nil when not enabled")
	}
}

// TestGraphDatabase_Link 测试创建链接
func TestGraphDatabase_Link(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_graph_link.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "test_link",
		Path: dbPath,
		GraphOptions: &GraphOptions{
			Enabled: true,
			Backend: "memory",
		},
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	graphDB := db.Graph()

	// 创建链接
	err = graphDB.Link(ctx, "user1", "follows", "user2")
	if err != nil {
		t.Fatalf("Failed to create link: %v", err)
	}

	// 验证链接存在
	neighbors, err := graphDB.GetNeighbors(ctx, "user1", "follows")
	if err != nil {
		t.Fatalf("Failed to get neighbors: %v", err)
	}

	if len(neighbors) == 0 {
		t.Error("Expected at least one neighbor")
	}

	found := false
	for _, n := range neighbors {
		if n == "user2" {
			found = true
			break
		}
	}
	if !found {
		t.Error("Expected to find user2 as neighbor")
	}
}

// TestGraphDatabase_Unlink 测试删除链接
func TestGraphDatabase_Unlink(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_graph_unlink.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "test_unlink",
		Path: dbPath,
		GraphOptions: &GraphOptions{
			Enabled: true,
			Backend: "memory",
		},
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	graphDB := db.Graph()

	// 创建链接
	err = graphDB.Link(ctx, "user1", "follows", "user2")
	if err != nil {
		t.Fatalf("Failed to create link: %v", err)
	}

	// 验证链接存在
	neighbors, err := graphDB.GetNeighbors(ctx, "user1", "follows")
	if err != nil {
		t.Fatalf("Failed to get neighbors: %v", err)
	}
	if len(neighbors) == 0 {
		t.Error("Expected link to exist")
	}

	// 删除链接
	err = graphDB.Unlink(ctx, "user1", "follows", "user2")
	if err != nil {
		t.Fatalf("Failed to unlink: %v", err)
	}

	// 验证链接已删除
	neighbors, err = graphDB.GetNeighbors(ctx, "user1", "follows")
	if err != nil {
		t.Fatalf("Failed to get neighbors: %v", err)
	}

	found := false
	for _, n := range neighbors {
		if n == "user2" {
			found = true
			break
		}
	}
	if found {
		t.Error("Expected link to be removed")
	}
}

// TestGraphDatabase_GetNeighbors 测试获取邻居节点
func TestGraphDatabase_GetNeighbors(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_graph_neighbors.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "test_neighbors",
		Path: dbPath,
		GraphOptions: &GraphOptions{
			Enabled: true,
			Backend: "memory",
		},
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	graphDB := db.Graph()

	// 创建多个链接
	graphDB.Link(ctx, "user1", "follows", "user2")
	graphDB.Link(ctx, "user1", "follows", "user3")
	graphDB.Link(ctx, "user1", "likes", "post1")
	graphDB.Link(ctx, "user2", "follows", "user1") // 反向链接

	// 测试获取指定关系的邻居
	neighbors, err := graphDB.GetNeighbors(ctx, "user1", "follows")
	if err != nil {
		t.Fatalf("Failed to get neighbors: %v", err)
	}

	if len(neighbors) != 2 {
		t.Errorf("Expected 2 neighbors, got %d", len(neighbors))
	}

	// 验证包含正确的邻居
	expected := []string{"user2", "user3"}
	sort.Strings(neighbors)
	sort.Strings(expected)
	if !reflect.DeepEqual(neighbors, expected) {
		t.Errorf("Expected neighbors %v, got %v", expected, neighbors)
	}

	// 测试获取所有邻居（不指定关系）
	allNeighbors, err := graphDB.GetNeighbors(ctx, "user1", "")
	if err != nil {
		t.Fatalf("Failed to get all neighbors: %v", err)
	}

	if len(allNeighbors) < 3 {
		t.Errorf("Expected at least 3 neighbors, got %d", len(allNeighbors))
	}
}

// TestGraphDatabase_FindPath 测试路径查找
func TestGraphDatabase_FindPath(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_graph_path.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "test_path",
		Path: dbPath,
		GraphOptions: &GraphOptions{
			Enabled: true,
			Backend: "memory",
		},
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	graphDB := db.Graph()

	// 创建链式关系：user1 -> user2 -> user3 -> user4
	graphDB.Link(ctx, "user1", "follows", "user2")
	graphDB.Link(ctx, "user2", "follows", "user3")
	graphDB.Link(ctx, "user3", "follows", "user4")

	// 查找路径
	paths, err := graphDB.FindPath(ctx, "user1", "user4", 10, "follows")
	if err != nil {
		t.Fatalf("Failed to find path: %v", err)
	}

	if len(paths) == 0 {
		t.Error("Expected to find at least one path")
	}

	// 验证路径正确性
	found := false
	for _, path := range paths {
		if len(path) >= 4 && path[0] == "user1" && path[len(path)-1] == "user4" {
			found = true
			break
		}
	}
	if !found {
		t.Error("Expected to find path from user1 to user4")
	}

	// 测试不存在的路径
	paths, err = graphDB.FindPath(ctx, "user1", "user999", 10, "follows")
	if err != nil {
		t.Fatalf("Failed to find path: %v", err)
	}
	if len(paths) > 0 {
		t.Error("Expected no path to non-existent node")
	}
}

// TestGraphDatabase_Query_V 测试查询 API - V
func TestGraphDatabase_Query_V(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_graph_query_v.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "test_query_v",
		Path: dbPath,
		GraphOptions: &GraphOptions{
			Enabled: true,
			Backend: "memory",
		},
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	graphDB := db.Graph()

	// 创建链接
	graphDB.Link(ctx, "user1", "follows", "user2")
	graphDB.Link(ctx, "user1", "follows", "user3")

	// 测试 V 查询
	query := graphDB.Query()
	if query == nil {
		t.Fatal("Query should not be nil")
	}

	queryImpl := query.V("user1")
	if queryImpl == nil {
		t.Fatal("Query result should not be nil")
	}
}

// TestGraphDatabase_Query_Out 测试查询 API - Out
func TestGraphDatabase_Query_Out(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_graph_query_out.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "test_query_out",
		Path: dbPath,
		GraphOptions: &GraphOptions{
			Enabled: true,
			Backend: "memory",
		},
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	graphDB := db.Graph()

	// 创建链接
	graphDB.Link(ctx, "user1", "follows", "user2")
	graphDB.Link(ctx, "user1", "follows", "user3")
	graphDB.Link(ctx, "user1", "likes", "post1")

	// 测试 Out 查询
	query := graphDB.Query()
	queryImpl := query.V("user1")
	if queryImpl == nil {
		t.Fatal("Query result should not be nil")
	}
	results, err := queryImpl.Out("follows").All(ctx)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(results) == 0 {
		t.Error("Expected query results")
	}

	// 验证结果
	foundUser2 := false
	foundUser3 := false
	for _, r := range results {
		if r.Subject == "user1" && r.Predicate == "follows" && r.Object == "user2" {
			foundUser2 = true
		}
		if r.Subject == "user1" && r.Predicate == "follows" && r.Object == "user3" {
			foundUser3 = true
		}
	}

	if !foundUser2 || !foundUser3 {
		t.Error("Expected to find both user2 and user3 in results")
	}
}

// TestGraphDatabase_Query_In 测试查询 API - In
func TestGraphDatabase_Query_In(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_graph_query_in.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "test_query_in",
		Path: dbPath,
		GraphOptions: &GraphOptions{
			Enabled: true,
			Backend: "memory",
		},
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	graphDB := db.Graph()

	// 创建链接（反向）
	graphDB.Link(ctx, "user2", "follows", "user1")
	graphDB.Link(ctx, "user3", "follows", "user1")

	// 测试 In 查询
	query := graphDB.Query()
	queryImpl := query.V("user1")
	if queryImpl == nil {
		t.Fatal("Query result should not be nil")
	}
	results, err := queryImpl.In("follows").All(ctx)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(results) == 0 {
		t.Error("Expected query results")
	}

	// 验证结果包含反向链接
	found := false
	for _, r := range results {
		if (r.Subject == "user2" || r.Subject == "user3") && r.Object == "user1" {
			found = true
			break
		}
	}
	if !found {
		t.Error("Expected to find incoming links")
	}
}

// TestGraphDatabase_Query_Both 测试查询 API - Both
func TestGraphDatabase_Query_Both(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_graph_query_both.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "test_query_both",
		Path: dbPath,
		GraphOptions: &GraphOptions{
			Enabled: true,
			Backend: "memory",
		},
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	graphDB := db.Graph()

	// 创建双向链接
	graphDB.Link(ctx, "user1", "follows", "user2")
	graphDB.Link(ctx, "user2", "follows", "user1")

	// 测试 Both 查询
	query := graphDB.Query()
	queryImpl := query.V("user1")
	if queryImpl == nil {
		t.Fatal("Query result should not be nil")
	}
	results, err := queryImpl.Both("follows").All(ctx)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(results) == 0 {
		t.Error("Expected query results")
	}

	// 验证结果包含双向链接
	foundOut := false
	foundIn := false
	for _, r := range results {
		if r.Subject == "user1" && r.Object == "user2" {
			foundOut = true
		}
		if r.Subject == "user2" && r.Object == "user1" {
			foundIn = true
		}
	}

	if !foundOut || !foundIn {
		t.Error("Expected to find both outgoing and incoming links")
	}
}

// TestGraphDatabase_Query_Limit 测试查询 API - Limit
func TestGraphDatabase_Query_Limit(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_graph_query_limit.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "test_query_limit",
		Path: dbPath,
		GraphOptions: &GraphOptions{
			Enabled: true,
			Backend: "memory",
		},
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	graphDB := db.Graph()

	// 创建多个链接
	for i := 2; i <= 10; i++ {
		graphDB.Link(ctx, "user1", "follows", "user"+strconv.Itoa(i))
	}

	// 测试 Limit 查询
	query := graphDB.Query()
	queryImpl := query.V("user1")
	if queryImpl == nil {
		t.Fatal("Query result should not be nil")
	}
	results, err := queryImpl.Out("follows").Limit(3).All(ctx)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(results) > 3 {
		t.Errorf("Expected at most 3 results, got %d", len(results))
	}
}

// TestGraphDatabase_Query_AllNodes 测试查询 API - AllNodes
func TestGraphDatabase_Query_AllNodes(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_graph_query_allnodes.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "test_query_allnodes",
		Path: dbPath,
		GraphOptions: &GraphOptions{
			Enabled: true,
			Backend: "memory",
		},
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	graphDB := db.Graph()

	// 创建链接
	graphDB.Link(ctx, "user1", "follows", "user2")
	graphDB.Link(ctx, "user1", "follows", "user3")

	// 测试 AllNodes
	query := graphDB.Query()
	queryImpl := query.V("user1")
	if queryImpl == nil {
		t.Fatal("Query result should not be nil")
	}
	nodes, err := queryImpl.Out("follows").AllNodes(ctx)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(nodes) == 0 {
		t.Error("Expected nodes")
	}

	// 验证节点
	foundUser2 := false
	foundUser3 := false
	for _, node := range nodes {
		if node == "user2" {
			foundUser2 = true
		}
		if node == "user3" {
			foundUser3 = true
		}
	}

	if !foundUser2 || !foundUser3 {
		t.Error("Expected to find both user2 and user3")
	}
}

// TestGraphDatabase_Query_Count 测试查询 API - Count
func TestGraphDatabase_Query_Count(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_graph_query_count.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "test_query_count",
		Path: dbPath,
		GraphOptions: &GraphOptions{
			Enabled: true,
			Backend: "memory",
		},
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	graphDB := db.Graph()

	// 创建多个链接
	graphDB.Link(ctx, "user1", "follows", "user2")
	graphDB.Link(ctx, "user1", "follows", "user3")
	graphDB.Link(ctx, "user1", "follows", "user4")

	// 测试 Count
	query := graphDB.Query()
	queryImpl := query.V("user1")
	if queryImpl == nil {
		t.Fatal("Query result should not be nil")
	}
	count, err := queryImpl.Out("follows").Count(ctx)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if count != 3 {
		t.Errorf("Expected count 3, got %d", count)
	}
}

// TestGraphDatabase_Query_First 测试查询 API - First
func TestGraphDatabase_Query_First(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_graph_query_first.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "test_query_first",
		Path: dbPath,
		GraphOptions: &GraphOptions{
			Enabled: true,
			Backend: "memory",
		},
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	graphDB := db.Graph()

	// 创建链接
	graphDB.Link(ctx, "user1", "follows", "user2")
	graphDB.Link(ctx, "user1", "follows", "user3")

	// 测试 First
	query := graphDB.Query()
	queryImpl := query.V("user1")
	if queryImpl == nil {
		t.Fatal("Query result should not be nil")
	}
	result, err := queryImpl.Out("follows").First(ctx)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if result == nil {
		t.Error("Expected a result")
		return
	}

	if result.Subject != "user1" {
		t.Errorf("Expected subject 'user1', got '%s'", result.Subject)
	}

	if result.Predicate != "follows" {
		t.Errorf("Expected predicate 'follows', got '%s'", result.Predicate)
	}
}

// TestGraphDatabase_Query_Chain 测试链式查询
func TestGraphDatabase_Query_Chain(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_graph_query_chain.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "test_query_chain",
		Path: dbPath,
		GraphOptions: &GraphOptions{
			Enabled: true,
			Backend: "memory",
		},
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	graphDB := db.Graph()

	// 创建复杂关系
	graphDB.Link(ctx, "user1", "follows", "user2")
	graphDB.Link(ctx, "user2", "follows", "user3")
	graphDB.Link(ctx, "user3", "follows", "user4")

	// 测试链式查询
	query := graphDB.Query()
	queryImpl := query.V("user1")
	if queryImpl == nil {
		t.Fatal("Query result should not be nil")
	}
	queryImpl2 := queryImpl.Out("follows")
	if queryImpl2 == nil {
		t.Fatal("Query result should not be nil")
	}
	results, err := queryImpl2.Out("follows").All(ctx)
	if err != nil {
		t.Fatalf("Failed to execute chain query: %v", err)
	}

	// 应该能找到 user3（user1 -> user2 -> user3）
	found := false
	for _, r := range results {
		if r.Object == "user3" {
			found = true
			break
		}
	}
	if !found {
		t.Error("Expected to find user3 through chain query")
	}
}

// TestGraphBridge_Basic 测试图数据库桥接基本功能
func TestGraphBridge_Basic(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_graph_bridge.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "test_bridge",
		Path: dbPath,
		GraphOptions: &GraphOptions{
			Enabled:  true,
			Backend:  "memory",
			AutoSync: true,
		},
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	bridge := db.GraphBridge()
	if bridge == nil {
		t.Skip("Graph bridge not available (AutoSync disabled)")
	}

	// 测试启用/禁用
	if !bridge.IsEnabled() {
		t.Error("Bridge should be enabled by default")
	}

	bridge.Disable()
	if bridge.IsEnabled() {
		t.Error("Bridge should be disabled")
	}

	bridge.Enable()
	if !bridge.IsEnabled() {
		t.Error("Bridge should be enabled")
	}
}

// TestGraphBridge_RelationMapping 测试关系映射
func TestGraphBridge_RelationMapping(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_graph_mapping.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "test_mapping",
		Path: dbPath,
		GraphOptions: &GraphOptions{
			Enabled:  true,
			Backend:  "memory",
			AutoSync: true,
		},
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	bridge := db.GraphBridge()
	if bridge == nil {
		t.Skip("Graph bridge not available (AutoSync disabled)")
	}

	// 添加关系映射
	mapping := &GraphRelationMapping{
		Collection:  "users",
		Field:       "follows",
		Relation:    "follows",
		TargetField: "id",
		AutoLink:    true,
	}
	bridge.AddRelationMapping(mapping)

	// 移除关系映射
	bridge.RemoveRelationMapping("users", "follows")
}

// TestGraphDatabase_Close 测试关闭图数据库
func TestGraphDatabase_Close(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_graph_close.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "test_close",
		Path: dbPath,
		GraphOptions: &GraphOptions{
			Enabled: true,
			Backend: "memory",
		},
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	graphDB := db.Graph()
	if graphDB == nil {
		t.Fatal("Graph database should not be nil")
	}

	// 关闭数据库（应该同时关闭图数据库）
	err = db.Close(ctx)
	if err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}

	// 尝试在关闭后使用图数据库（应该失败）
	err = graphDB.Link(ctx, "user1", "follows", "user2")
	if err == nil {
		t.Error("Expected error when using closed graph database")
	}
}

// TestGraphDatabase_ErrorHandling 测试错误处理
func TestGraphDatabase_ErrorHandling(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_graph_errors.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "test_errors",
		Path: dbPath,
		GraphOptions: &GraphOptions{
			Enabled: true,
			Backend: "memory",
		},
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	graphDB := db.Graph()

	// 测试空查询
	query := graphDB.Query()
	if query == nil {
		t.Fatal("Query should not be nil")
	}

	// 测试无效查询
	queryImpl := query.V("nonexistent")
	if queryImpl != nil {
		_, err = queryImpl.Out("follows").All(ctx)
		if err != nil {
			// 这是预期的，因为节点不存在
		}

		// 测试 Count 在空结果上
		count, err := queryImpl.Out("follows").Count(ctx)
		if err != nil {
			t.Fatalf("Count should not error on empty result: %v", err)
		}
		if count != 0 {
			t.Errorf("Expected count 0, got %d", count)
		}
	}
}

// TestGraphDatabase_Concurrent 测试并发操作
func TestGraphDatabase_Concurrent(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_graph_concurrent.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "test_concurrent",
		Path: dbPath,
		GraphOptions: &GraphOptions{
			Enabled: true,
			Backend: "memory",
		},
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	graphDB := db.Graph()

	// 并发创建链接（创建10个不同的邻居节点，跳过自环）
	var wg sync.WaitGroup
	errChan := make(chan error, 10)
	expectedNodes := make(map[string]bool)

	for i := 0; i < 10; i++ {
		// 跳过 user1 到 user1 的自环，创建 user2 到 user11
		targetID := i + 2
		nodeID := "user" + strconv.Itoa(targetID)
		expectedNodes[nodeID] = true
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			err := graphDB.Link(ctx, "user1", "follows", "user"+strconv.Itoa(id))
			if err != nil {
				errChan <- fmt.Errorf("Failed to create link user%d: %v", id, err)
			}
		}(targetID)
	}

	// 等待所有 goroutine 完成
	wg.Wait()
	close(errChan)

	// 检查是否有错误
	for err := range errChan {
		t.Error(err)
	}

	// 验证所有链接都已创建
	neighbors, err := graphDB.GetNeighbors(ctx, "user1", "follows")
	if err != nil {
		t.Fatalf("Failed to get neighbors: %v", err)
	}

	if len(neighbors) != 10 {
		t.Errorf("Expected 10 neighbors, got %d: %v", len(neighbors), neighbors)
		// 检查哪些节点缺失
		actualNodes := make(map[string]bool)
		for _, n := range neighbors {
			actualNodes[n] = true
		}
		for node := range expectedNodes {
			if !actualNodes[node] {
				t.Errorf("Missing node: %s", node)
			}
		}
	}
}

// TestGraphDatabase_ComplexScenario 测试复杂场景
func TestGraphDatabase_ComplexScenario(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_graph_complex.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "test_complex",
		Path: dbPath,
		GraphOptions: &GraphOptions{
			Enabled: true,
			Backend: "memory",
		},
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	graphDB := db.Graph()

	// 创建复杂的社交网络图
	// user1 关注 user2, user3
	graphDB.Link(ctx, "user1", "follows", "user2")
	graphDB.Link(ctx, "user1", "follows", "user3")

	// user2 关注 user3, user4
	graphDB.Link(ctx, "user2", "follows", "user3")
	graphDB.Link(ctx, "user2", "follows", "user4")

	// user3 关注 user4
	graphDB.Link(ctx, "user3", "follows", "user4")

	// user1 点赞 post1
	graphDB.Link(ctx, "user1", "likes", "post1")
	graphDB.Link(ctx, "user2", "likes", "post1")

	// 测试多层查询：查找 user1 关注的人关注的人
	query := graphDB.Query()
	queryImpl := query.V("user1")
	if queryImpl == nil {
		t.Fatal("Query result should not be nil")
	}
	results, err := queryImpl.Out("follows").Out("follows").All(ctx)
	if err != nil {
		t.Fatalf("Failed to execute complex query: %v", err)
	}

	// 应该能找到 user3, user4（通过 user2 和 user3）
	// 链式查询的结果中，Object 应该是最终节点
	foundUser3 := false
	foundUser4 := false
	for _, r := range results {
		// 检查 Object（最终节点）
		if r.Object == "user3" {
			foundUser3 = true
		}
		if r.Object == "user4" {
			foundUser4 = true
		}
	}

	if !foundUser3 || !foundUser4 {
		// 输出详细调试信息
		t.Logf("Query results count: %d", len(results))
		for i, r := range results {
			t.Logf("  Result %d: %s --%s--> %s", i, r.Subject, r.Predicate, r.Object)
		}
		t.Errorf("Expected to find user3 and user4 through complex query, got %d results", len(results))
	}

	// 测试路径查找
	paths, err := graphDB.FindPath(ctx, "user1", "user4", 5, "follows")
	if err != nil {
		t.Fatalf("Failed to find path: %v", err)
	}

	if len(paths) == 0 {
		t.Error("Expected to find path from user1 to user4")
	}

	// 验证路径正确性
	validPath := false
	for _, path := range paths {
		if len(path) >= 3 && path[0] == "user1" && path[len(path)-1] == "user4" {
			validPath = true
			break
		}
	}
	if !validPath {
		t.Error("Expected valid path from user1 to user4")
	}
}

// TestGraphDatabase_DefaultPath 测试默认路径设置
func TestGraphDatabase_DefaultPath(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_graph_default_path.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "test_default_path",
		Path: dbPath,
		GraphOptions: &GraphOptions{
			Enabled: true,
			Backend: "memory",
			// Path 未设置，应该使用默认路径
		},
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	graphDB := db.Graph()
	if graphDB == nil {
		t.Fatal("Graph database should not be nil")
	}

	// 验证图数据库可以正常使用
	err = graphDB.Link(ctx, "user1", "follows", "user2")
	if err != nil {
		t.Fatalf("Failed to create link: %v", err)
	}
}

// TestGraphDatabase_AutoSync 测试自动同步功能
func TestGraphDatabase_AutoSync(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dbPath := "../../data/test_graph_autosync.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "test_autosync",
		Path: dbPath,
		GraphOptions: &GraphOptions{
			Enabled:  true,
			Backend:  "memory",
			AutoSync: true,
		},
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	// 创建集合
	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}
	users, err := db.Collection(ctx, "users", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 配置关系映射
	bridge := db.GraphBridge()
	if bridge == nil {
		t.Skip("Graph bridge not available (AutoSync disabled)")
	}
	bridge.AddRelationMapping(&GraphRelationMapping{
		Collection:  "users",
		Field:       "follows",
		Relation:    "follows",
		TargetField: "id",
		AutoLink:    true,
	})

	// 插入文档（应该自动创建图关系）
	_, err = users.Insert(ctx, map[string]any{
		"id":      "user1",
		"name":    "Alice",
		"follows": "user2",
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	// 等待自动同步
	time.Sleep(100 * time.Millisecond)

	// 验证图关系已创建
	graphDB := db.Graph()
	neighbors, err := graphDB.GetNeighbors(ctx, "user1", "follows")
	if err != nil {
		t.Fatalf("Failed to get neighbors: %v", err)
	}

	found := false
	for _, n := range neighbors {
		if n == "user2" {
			found = true
			break
		}
	}
	if !found {
		t.Error("Expected auto-sync to create graph link")
	}
}
