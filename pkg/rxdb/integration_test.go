package rxdb

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"
)

// TestIntegration_FullWorkflow 测试完整工作流
func TestIntegration_FullWorkflow(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_integration_workflow.db"
	defer os.RemoveAll(dbPath)

	// 1. 创建数据库和集合
	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: dbPath,
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"id": map[string]any{
					"type": "string",
				},
				"name": map[string]any{
					"type": "string",
				},
				"age": map[string]any{
					"type": "integer",
				},
			},
		},
	}

	collection, err := db.Collection(ctx, "users", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 2. 插入文档
	doc1, err := collection.Insert(ctx, map[string]any{
		"id":   "user1",
		"name": "Alice",
		"age":  30,
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	if doc1.ID() != "user1" {
		t.Errorf("Expected ID 'user1', got '%s'", doc1.ID())
	}

	_, err = collection.Insert(ctx, map[string]any{
		"id":   "user2",
		"name": "Bob",
		"age":  25,
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	// 3. 查询文档
	found, err := collection.FindByID(ctx, "user1")
	if err != nil {
		t.Fatalf("Failed to find document: %v", err)
	}

	if found == nil {
		t.Fatal("Document not found")
	}

	if found.GetString("name") != "Alice" {
		t.Errorf("Expected 'Alice', got '%s'", found.GetString("name"))
	}

	// 4. 查询多个文档
	qc := AsQueryCollection(collection)
	if qc == nil {
		t.Fatal("Failed to get QueryCollection")
	}

	results, err := qc.Find(map[string]any{
		"age": map[string]any{
			"$gte": 25,
		},
	}).Exec(ctx)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}

	// 5. 更新文档
	err = doc1.Update(ctx, map[string]any{
		"age": 31,
	})
	if err != nil {
		t.Fatalf("Failed to update document: %v", err)
	}

	// 验证更新
	updated, err := collection.FindByID(ctx, "user1")
	if err != nil {
		t.Fatalf("Failed to find updated document: %v", err)
	}

	if updated.GetInt("age") != 31 {
		t.Errorf("Expected age 31, got %d", updated.GetInt("age"))
	}

	// 6. 删除文档
	err = collection.Remove(ctx, "user2")
	if err != nil {
		t.Fatalf("Failed to remove document: %v", err)
	}

	// 验证删除
	deleted, err := collection.FindByID(ctx, "user2")
	if err == nil && deleted != nil {
		t.Error("Document should not exist after removal")
	}

	// 7. 验证数据一致性
	count, err := collection.Count(ctx)
	if err != nil {
		t.Fatalf("Failed to count documents: %v", err)
	}

	if count != 1 {
		t.Errorf("Expected 1 document, got %d", count)
	}

	// 8. 验证剩余文档
	remaining, err := collection.FindByID(ctx, "user1")
	if err != nil {
		t.Fatalf("Failed to find remaining document: %v", err)
	}

	if remaining == nil {
		t.Fatal("Remaining document should exist")
	}

	if remaining.GetString("name") != "Alice" {
		t.Errorf("Expected 'Alice', got '%s'", remaining.GetString("name"))
	}
}

// TestIntegration_ConcurrentOperations 测试并发操作
func TestIntegration_ConcurrentOperations(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_integration_concurrent.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: dbPath,
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 并发插入
	var wg sync.WaitGroup
	numGoroutines := 10
	numDocsPerGoroutine := 10

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for j := 0; j < numDocsPerGoroutine; j++ {
				docID := goroutineID*numDocsPerGoroutine + j
				_, err := collection.Insert(ctx, map[string]any{
					"id":   string(rune(docID)),
					"name": "Document",
					"val":  docID,
				})
				if err != nil {
					t.Errorf("Failed to insert document %d: %v", docID, err)
				}
			}
		}(i)
	}

	wg.Wait()

	// 验证所有文档都已插入
	count, err := collection.Count(ctx)
	if err != nil {
		t.Fatalf("Failed to count documents: %v", err)
	}

	expectedCount := numGoroutines * numDocsPerGoroutine
	if count != expectedCount {
		t.Errorf("Expected %d documents, got %d", expectedCount, count)
	}

	// 并发查询
	resultsChan := make(chan int, numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			qc := AsQueryCollection(collection)
			if qc == nil {
				t.Error("Failed to get QueryCollection")
				return
			}

			results, err := qc.Find(map[string]any{}).Exec(ctx)
			if err != nil {
				t.Errorf("Failed to execute query: %v", err)
				return
			}

			resultsChan <- len(results)
		}()
	}

	wg.Wait()
	close(resultsChan)

	// 验证查询结果一致性
	for resultCount := range resultsChan {
		if resultCount != expectedCount {
			t.Errorf("Expected %d results, got %d", expectedCount, resultCount)
		}
	}

	// 并发更新
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			docID := string(rune(goroutineID * numDocsPerGoroutine))
			doc, err := collection.FindByID(ctx, docID)
			if err != nil || doc == nil {
				t.Logf("Document %s not found for update", docID)
				return
			}

			err = doc.Update(ctx, map[string]any{
				"updated": true,
			})
			if err != nil {
				t.Errorf("Failed to update document %s: %v", docID, err)
			}
		}(i)
	}

	wg.Wait()

	// 验证数据一致性
	finalCount, err := collection.Count(ctx)
	if err != nil {
		t.Fatalf("Failed to count documents: %v", err)
	}

	if finalCount != expectedCount {
		t.Errorf("Expected %d documents after concurrent updates, got %d", expectedCount, finalCount)
	}
}

// TestIntegration_Transaction 测试事务性
func TestIntegration_Transaction(t *testing.T) {
	ctx := context.Background()
	dbPath := "../../data/test_integration_transaction.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: dbPath,
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
		JSON: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"id": map[string]any{
					"type": "string",
				},
				"name": map[string]any{
					"type":     "string",
					"required": true,
				},
			},
			"required": []any{"id", "name"},
		},
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 测试批量插入的事务性
	docs := []map[string]any{
		{"id": "doc1", "name": "Document 1"},
		{"id": "doc2", "name": "Document 2"},
		{"id": "doc3"}, // 缺少必需字段，应该失败
		{"id": "doc4", "name": "Document 4"},
	}

	// BulkInsert 应该全部成功或全部失败（取决于实现）
	result, err := collection.BulkInsert(ctx, docs)
	if err != nil {
		// 如果批量插入失败，验证没有部分插入
		count, countErr := collection.Count(ctx)
		if countErr != nil {
			t.Fatalf("Failed to count documents: %v", countErr)
		}

		if count != 0 {
			t.Logf("BulkInsert failed but %d documents were inserted (partial insert)", count)
		}
	} else {
		// 如果成功，验证所有文档都已插入
		if len(result) != len(docs) {
			t.Errorf("Expected %d documents, got %d", len(docs), len(result))
		}
	}

	// 测试批量删除的事务性
	validDocs := []map[string]any{
		{"id": "doc1", "name": "Document 1"},
		{"id": "doc2", "name": "Document 2"},
		{"id": "doc3", "name": "Document 3"},
	}

	// 先插入有效文档
	_, err = collection.BulkInsert(ctx, validDocs)
	if err != nil {
		t.Fatalf("Failed to bulk insert valid documents: %v", err)
	}

	// 批量删除
	idsToRemove := []string{"doc1", "doc2", "doc3"}
	err = collection.BulkRemove(ctx, idsToRemove)
	if err != nil {
		t.Fatalf("Failed to bulk remove: %v", err)
	}

	// 验证所有文档都已删除
	count, err := collection.Count(ctx)
	if err != nil {
		t.Fatalf("Failed to count documents: %v", err)
	}

	if count != 0 {
		t.Errorf("Expected 0 documents after bulk remove, got %d", count)
	}

	// 测试失败回滚场景
	// 插入一些文档
	_, err = collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Document 1",
	})
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	// 尝试插入重复文档（应该失败）
	_, err = collection.Insert(ctx, map[string]any{
		"id":   "doc1",
		"name": "Duplicate",
	})
	if err == nil {
		t.Error("Expected error for duplicate insert")
	}

	// 验证原始文档未被修改
	doc, err := collection.FindByID(ctx, "doc1")
	if err != nil {
		t.Fatalf("Failed to find document: %v", err)
	}

	if doc == nil {
		t.Fatal("Original document should still exist")
	}

	if doc.GetString("name") != "Document 1" {
		t.Errorf("Expected 'Document 1', got '%s'", doc.GetString("name"))
	}
}

// TestPerformance_LargeDataset 测试大数据集性能
func TestPerformance_LargeDataset(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	ctx := context.Background()
	dbPath := "../../data/test_performance_large.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: dbPath,
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入大量数据
	numDocs := 1000
	startTime := time.Now()

	docs := make([]map[string]any, numDocs)
	for i := 0; i < numDocs; i++ {
		docs[i] = map[string]any{
			"id":   string(rune(i)),
			"name": "Document",
			"val":  i,
		}
	}

	result, err := collection.BulkInsert(ctx, docs)
	if err != nil {
		t.Fatalf("Failed to bulk insert: %v", err)
	}

	insertDuration := time.Since(startTime)
	t.Logf("Inserted %d documents in %v", len(result), insertDuration)

	// 查询性能测试
	startTime = time.Now()
	qc := AsQueryCollection(collection)
	if qc == nil {
		t.Fatal("Failed to get QueryCollection")
	}

	results, err := qc.Find(map[string]any{
		"val": map[string]any{
			"$gte": 500,
		},
	}).Exec(ctx)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	queryDuration := time.Since(startTime)
	t.Logf("Queried %d documents in %v", len(results), queryDuration)

	if len(results) != 500 {
		t.Errorf("Expected 500 results, got %d", len(results))
	}

	// 验证数据完整性
	count, err := collection.Count(ctx)
	if err != nil {
		t.Fatalf("Failed to count documents: %v", err)
	}

	if count != numDocs {
		t.Errorf("Expected %d documents, got %d", numDocs, count)
	}
}

// TestPerformance_ConcurrentQueries 测试并发查询性能
func TestPerformance_ConcurrentQueries(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	ctx := context.Background()
	dbPath := "../../data/test_performance_concurrent_queries.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: dbPath,
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 插入测试数据
	numDocs := 100
	for i := 0; i < numDocs; i++ {
		_, err := collection.Insert(ctx, map[string]any{
			"id":   string(rune(i)),
			"name": "Document",
			"val":  i,
		})
		if err != nil {
			t.Fatalf("Failed to insert document: %v", err)
		}
	}

	// 并发查询
	numQueries := 20
	var wg sync.WaitGroup
	startTime := time.Now()

	for i := 0; i < numQueries; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			qc := AsQueryCollection(collection)
			if qc == nil {
				t.Error("Failed to get QueryCollection")
				return
			}

			_, err := qc.Find(map[string]any{}).Exec(ctx)
			if err != nil {
				t.Errorf("Failed to execute query: %v", err)
			}
		}()
	}

	wg.Wait()
	duration := time.Since(startTime)
	t.Logf("Executed %d concurrent queries in %v", numQueries, duration)
}

// TestStress_HighLoad 测试高负载
func TestStress_HighLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	ctx := context.Background()
	dbPath := "../../data/test_stress_highload.db"
	defer os.RemoveAll(dbPath)

	db, err := CreateDatabase(ctx, DatabaseOptions{
		Name: "testdb",
		Path: dbPath,
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}

	collection, err := db.Collection(ctx, "test", schema)
	if err != nil {
		t.Fatalf("Failed to create collection: %v", err)
	}

	// 持续高负载操作
	numOperations := 500
	var wg sync.WaitGroup
	startTime := time.Now()

	for i := 0; i < numOperations; i++ {
		wg.Add(1)
		go func(opID int) {
			defer wg.Done()

			// 插入
			_, err := collection.Insert(ctx, map[string]any{
				"id":   string(rune(opID)),
				"name": "Document",
				"val":  opID,
			})
			if err != nil {
				t.Errorf("Failed to insert: %v", err)
				return
			}

			// 查询
			_, err = collection.FindByID(ctx, string(rune(opID)))
			if err != nil {
				t.Errorf("Failed to find: %v", err)
			}

			// 更新
			doc, err := collection.FindByID(ctx, string(rune(opID)))
			if err == nil && doc != nil {
				err = doc.Update(ctx, map[string]any{
					"updated": true,
				})
				if err != nil {
					t.Errorf("Failed to update: %v", err)
				}
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(startTime)
	t.Logf("Completed %d operations in %v", numOperations, duration)

	// 验证稳定性
	count, err := collection.Count(ctx)
	if err != nil {
		t.Fatalf("Failed to count documents: %v", err)
	}

	if count != numOperations {
		t.Errorf("Expected %d documents, got %d", numOperations, count)
	}
}
