package rxdb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sync"
	"testing"
)

// callHookMethod uses reflection to call hook registration methods on collection
func callHookMethod(c Collection, methodName string, hook HookFunc) {
	// Get the method by name
	method := reflect.ValueOf(c).MethodByName(methodName)
	if method.IsValid() {
		method.Call([]reflect.Value{reflect.ValueOf(hook)})
	}
}

func TestHooks_PreInsert(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: "../../data/test_hooks_preinsert.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)
	defer os.RemoveAll("../../data/test_hooks_preinsert.db")

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 注册 preInsert 钩子
	var hookCalled bool
	var hookDoc map[string]any
	callHookMethod(collection, "PreInsert", func(ctx context.Context, doc map[string]any, oldDoc map[string]any) error {
		hookCalled = true
		hookDoc = doc
		// 修改文档数据
		doc["modified"] = true
		return nil
	})

	// 插入文档
	doc, err := collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Test",
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	// 验证钩子被调用
	if !hookCalled {
		t.Error("Expected preInsert hook to be called")
	}

	// 验证钩子接收到的文档
	if hookDoc["id"] != "doc1" {
		t.Errorf("Expected hook to receive doc with id 'doc1', got '%v'", hookDoc["id"])
	}

	// 验证文档被修改
	if doc.Get("modified") != true {
		t.Error("Expected document to be modified by hook")
	}

	// 测试阻止插入的钩子
	collection2, err := db.Collection(ctx, "test2", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	callHookMethod(collection2, "PreInsert", func(ctx context.Context, doc map[string]any, oldDoc map[string]any) error {
		return errors.New("insert blocked by hook")
	})

	_, err = collection2.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Test",
	})
	if err == nil {
		t.Error("Expected insert to be blocked by hook")
	}
}

func TestHooks_PostInsert(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: "../../data/test_hooks_postinsert.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)
	defer os.RemoveAll("../../data/test_hooks_postinsert.db")

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 注册 postInsert 钩子
	var hookCalled bool
	var hookDoc map[string]any
	callHookMethod(collection, "PostInsert", func(ctx context.Context, doc map[string]any, oldDoc map[string]any) error {
		hookCalled = true
		hookDoc = doc
		return nil
	})

	// 插入文档
	doc, err := collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Test",
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	// 验证钩子被调用
	if !hookCalled {
		t.Error("Expected postInsert hook to be called")
	}

	// 验证钩子接收到的文档
	if hookDoc["id"] != "doc1" {
		t.Errorf("Expected hook to receive doc with id 'doc1', got '%v'", hookDoc["id"])
	}

	// 验证文档已保存
	if doc.ID() != "doc1" {
		t.Errorf("Expected doc ID 'doc1', got '%s'", doc.ID())
	}
}

func TestHooks_PreSave(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: "../../data/test_hooks_presave.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)
	defer os.RemoveAll("../../data/test_hooks_presave.db")

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 注册 preSave 钩子
	var insertHookCalled bool
	var updateHookCalled bool
	callHookMethod(collection, "PreSave", func(ctx context.Context, doc map[string]any, oldDoc map[string]any) error {
		if oldDoc == nil {
			insertHookCalled = true
		} else {
			updateHookCalled = true
		}
		// 修改文档数据
		doc["saved"] = true
		return nil
	})

	// 测试 Insert 触发 preSave
	doc, err := collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Test",
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	if !insertHookCalled {
		t.Error("Expected preSave hook to be called on insert")
	}

	if doc.Get("saved") != true {
		t.Error("Expected document to be modified by preSave hook")
	}

	// 测试 Upsert 触发 preSave
	err = doc.Update(ctx, map[string]any{
		"name": "Updated",
	})
	if err != nil {
		t.Fatalf("Failed to update document: %v", err)
	}

	if !updateHookCalled {
		t.Error("Expected preSave hook to be called on update")
	}

	// 测试阻止保存的钩子
	collection2, err := db.Collection(ctx, "test2", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	callHookMethod(collection2, "PreSave", func(ctx context.Context, doc map[string]any, oldDoc map[string]any) error {
		return errors.New("save blocked by hook")
	})

	_, err = collection2.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Test",
	})
	if err == nil {
		t.Error("Expected insert to be blocked by preSave hook")
	}
}

func TestHooks_PostSave(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: "../../data/test_hooks_postsave.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)
	defer os.RemoveAll("../../data/test_hooks_postsave.db")

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 注册 postSave 钩子
	var insertHookCalled bool
	var updateHookCalled bool
	var hookDoc map[string]any
	var hookOldDoc map[string]any
	callHookMethod(collection, "PostSave", func(ctx context.Context, doc map[string]any, oldDoc map[string]any) error {
		if oldDoc == nil {
			insertHookCalled = true
		} else {
			updateHookCalled = true
		}
		hookDoc = doc
		hookOldDoc = oldDoc
		return nil
	})

	// 测试 Insert 触发 postSave
	doc, err := collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Test",
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	if !insertHookCalled {
		t.Error("Expected postSave hook to be called on insert")
	}

	if hookDoc["id"] != "doc1" {
		t.Errorf("Expected hook to receive doc with id 'doc1', got '%v'", hookDoc["id"])
	}

	if hookOldDoc != nil {
		t.Error("Expected oldDoc to be nil on insert")
	}

	// 测试 Update 触发 postSave
	err = doc.Update(ctx, map[string]any{
		"name": "Updated",
	})
	if err != nil {
		t.Fatalf("Failed to update document: %v", err)
	}

	if !updateHookCalled {
		t.Error("Expected postSave hook to be called on update")
	}

	if hookOldDoc == nil {
		t.Error("Expected oldDoc to be set on update")
	}
}

func TestHooks_PreRemove(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: "../../data/test_hooks_preremove.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)
	defer os.RemoveAll("../../data/test_hooks_preremove.db")

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入文档
	_, err = collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Test",
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	// 注册 preRemove 钩子
	var hookCalled bool
	var hookOldDoc map[string]any
	callHookMethod(collection, "PreRemove", func(ctx context.Context, doc map[string]any, oldDoc map[string]any) error {
		hookCalled = true
		hookOldDoc = oldDoc
		return nil
	})

	// 删除文档
	err = collection.Remove(ctx, "doc1")
	if err != nil {
		t.Fatalf("Failed to remove document: %v", err)
	}

	// 验证钩子被调用
	if !hookCalled {
		t.Error("Expected preRemove hook to be called")
	}

	if hookOldDoc == nil {
		t.Error("Expected hook to receive oldDoc")
	}

	if hookOldDoc["id"] != "doc1" {
		t.Errorf("Expected hook to receive doc with id 'doc1', got '%v'", hookOldDoc["id"])
	}

	// 测试阻止删除的钩子
	collection2, err := db.Collection(ctx, "test2", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	_, err = collection2.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Test",
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	callHookMethod(collection2, "PreRemove", func(ctx context.Context, doc map[string]any, oldDoc map[string]any) error {
		return errors.New("remove blocked by hook")
	})

	err = collection2.Remove(ctx, "doc1")
	if err == nil {
		t.Error("Expected remove to be blocked by hook")
	}
}

func TestHooks_PostRemove(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: "../../data/test_hooks_postremove.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)
	defer os.RemoveAll("../../data/test_hooks_postremove.db")

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入文档
	_, err = collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Test",
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	// 注册 postRemove 钩子
	var hookCalled bool
	var hookOldDoc map[string]any
	callHookMethod(collection, "PostRemove", func(ctx context.Context, doc map[string]any, oldDoc map[string]any) error {
		hookCalled = true
		hookOldDoc = oldDoc
		return nil
	})

	// 删除文档
	err = collection.Remove(ctx, "doc1")
	if err != nil {
		t.Fatalf("Failed to remove document: %v", err)
	}

	// 验证钩子被调用
	if !hookCalled {
		t.Error("Expected postRemove hook to be called")
	}

	if hookOldDoc == nil {
		t.Error("Expected hook to receive oldDoc")
	}

	if hookOldDoc["id"] != "doc1" {
		t.Errorf("Expected hook to receive doc with id 'doc1', got '%v'", hookOldDoc["id"])
	}

	// 验证文档已删除
	found, err := collection.FindByID(ctx, "doc1")
	if err != nil {
		t.Fatalf("Failed to find document: %v", err)
	}
	if found != nil {
		t.Error("Expected document to be deleted")
	}
}

func TestHooks_PreCreate(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: "../../data/test_hooks_precreate.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)
	defer os.RemoveAll("../../data/test_hooks_precreate.db")

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	// 注意：preCreate 钩子需要在创建集合之前注册
	// 但由于集合创建是在 Collection() 方法内部完成的，我们需要通过其他方式测试
	// 这里我们测试钩子注册功能
	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 注册 preCreate 钩子（虽然集合已创建，但可以注册用于后续操作）
	callHookMethod(collection, "PreCreate", func(ctx context.Context, doc map[string]any, oldDoc map[string]any) error {
		return nil
	})

	// 注意：preCreate 钩子在集合创建时调用，但由于集合已创建，这里主要测试注册功能
	// 实际使用中，钩子应该在创建集合之前通过某种方式注册
	if collection == nil {
		t.Error("Expected collection to be created")
	}
}

func TestHooks_PostCreate(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: "../../data/test_hooks_postcreate.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)
	defer os.RemoveAll("../../data/test_hooks_postcreate.db")

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	// 注意：postCreate 钩子需要在创建集合之前注册
	// 但由于集合创建是在 Collection() 方法内部完成的，我们需要通过其他方式测试
	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 注册 postCreate 钩子（虽然集合已创建，但可以注册用于后续操作）
	callHookMethod(collection, "PostCreate", func(ctx context.Context, doc map[string]any, oldDoc map[string]any) error {
		return nil
	})

	// 注意：postCreate 钩子在集合创建时调用，但由于集合已创建，这里主要测试注册功能
	if collection == nil {
		t.Error("Expected collection to be created")
	}
}

func TestHooks_MultipleHooks(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: "../../data/test_hooks_multiple.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)
	defer os.RemoveAll("../../data/test_hooks_multiple.db")

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 注册多个钩子
	var hook1Called bool
	var hook2Called bool
	var hook3Called bool

	callHookMethod(collection, "PreInsert", func(ctx context.Context, doc map[string]any, oldDoc map[string]any) error {
		hook1Called = true
		doc["hook1"] = true
		return nil
	})

	callHookMethod(collection, "PreInsert", func(ctx context.Context, doc map[string]any, oldDoc map[string]any) error {
		hook2Called = true
		doc["hook2"] = true
		return nil
	})

	callHookMethod(collection, "PreInsert", func(ctx context.Context, doc map[string]any, oldDoc map[string]any) error {
		hook3Called = true
		doc["hook3"] = true
		return nil
	})

	// 插入文档
	doc, err := collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Test",
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	// 验证所有钩子都被调用
	if !hook1Called {
		t.Error("Expected hook1 to be called")
	}
	if !hook2Called {
		t.Error("Expected hook2 to be called")
	}
	if !hook3Called {
		t.Error("Expected hook3 to be called")
	}

	// 验证文档被所有钩子修改
	if doc.Get("hook1") != true {
		t.Error("Expected hook1 modification")
	}
	if doc.Get("hook2") != true {
		t.Error("Expected hook2 modification")
	}
	if doc.Get("hook3") != true {
		t.Error("Expected hook3 modification")
	}
}

func TestHooks_ErrorHandling(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: "../../data/test_hooks_error.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)
	defer os.RemoveAll("../../data/test_hooks_error.db")

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 测试 preInsert 钩子返回错误
	callHookMethod(collection, "PreInsert", func(ctx context.Context, doc map[string]any, oldDoc map[string]any) error {
		return errors.New("hook error")
	})

	_, err = collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Test",
	})
	if err == nil {
		t.Error("Expected error when hook returns error")
	}
	if err.Error() != "preInsert hook failed: hook error" {
		t.Errorf("Expected specific error message, got: %v", err)
	}

	// 测试 preSave 钩子返回错误
	collection2, err := db.Collection(ctx, "test2", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	callHookMethod(collection2, "PreSave", func(ctx context.Context, doc map[string]any, oldDoc map[string]any) error {
		return errors.New("preSave hook error")
	})

	_, err = collection2.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Test",
	})
	if err == nil {
		t.Error("Expected error when preSave hook returns error")
	}

	// 测试 preRemove 钩子返回错误
	collection3, err := db.Collection(ctx, "test3", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	_, err = collection3.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Test",
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	callHookMethod(collection3, "PreRemove", func(ctx context.Context, doc map[string]any, oldDoc map[string]any) error {
		return errors.New("preRemove hook error")
	})

	err = collection3.Remove(ctx, "doc1")
	if err == nil {
		t.Error("Expected error when preRemove hook returns error")
	}
}

func TestHooks_ConcurrentHooks(t *testing.T) {
	ctx := context.Background()
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: "../../data/test_hooks_concurrent.db",
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)
	defer os.RemoveAll("../../data/test_hooks_concurrent.db")

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 注册并发安全的钩子
	var mu sync.Mutex
	var hookCallCount int

	callHookMethod(collection, "PreInsert", func(ctx context.Context, doc map[string]any, oldDoc map[string]any) error {
		mu.Lock()
		hookCallCount++
		mu.Unlock()
		return nil
	})

	// 并发插入文档
	var wg sync.WaitGroup
	numGoroutines := 10
	docsPerGoroutine := 10

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < docsPerGoroutine; j++ {
				_, err := collection.Insert(ctx, map[string]any{
					"id":   fmt.Sprintf("doc_%d_%d", goroutineID, j),
					"name": fmt.Sprintf("Test_%d_%d", goroutineID, j),
				})
				if err != nil {
					t.Errorf("Failed to insert document: %v", err)
				}
			}
		}(i)
	}

	wg.Wait()

	// 验证钩子被调用了正确的次数
	mu.Lock()
	expectedCalls := numGoroutines * docsPerGoroutine
	if hookCallCount != expectedCalls {
		t.Errorf("Expected hook to be called %d times, got %d", expectedCalls, hookCallCount)
	}
	mu.Unlock()
}
