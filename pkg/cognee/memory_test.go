package cognee

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/mozy/rxdb-go/pkg/rxdb"
	"github.com/sirupsen/logrus"
)

// setupTestService 创建测试用的 MemoryService
func setupTestService(t *testing.T) (*MemoryService, string, func()) {
	ctx := context.Background()
	dbPath := filepath.Join(os.TempDir(), "test_cognee_"+t.Name())

	// 确保目录存在
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}

	db, err := rxdb.CreateDatabase(ctx, rxdb.DatabaseOptions{
		Name: "testdb",
		Path: dbPath,
		GraphOptions: &rxdb.GraphOptions{
			Enabled:  true,
			Backend:  "badger",
			Path:     filepath.Join(dbPath, "graph"),
			AutoSync: true,
		},
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	embedder := NewSimpleEmbedder(384)
	service, err := NewMemoryService(ctx, db, MemoryServiceOptions{
		Embedder: embedder,
	})
	if err != nil {
		db.Close(ctx)
		t.Fatalf("Failed to create memory service: %v", err)
	}

	cleanup := func() {
		service = nil
		db.Close(ctx)
		os.RemoveAll(dbPath)
	}

	return service, dbPath, cleanup
}

func TestNewMemoryService(t *testing.T) {
	// 设置日志级别为 Debug，以便查看详细日志
	logrus.SetLevel(logrus.DebugLevel)
	logrus.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
		ForceColors:   true,
	})

	ctx := context.Background()
	dbPath := filepath.Join(os.TempDir(), "test_new_service")
	defer os.RemoveAll(dbPath)

	logrus.WithField("dbPath", dbPath).Info("🧪 TestNewMemoryService: 开始测试")

	db, err := rxdb.CreateDatabase(ctx, rxdb.DatabaseOptions{
		Name: "testdb",
		Path: dbPath,
		GraphOptions: &rxdb.GraphOptions{
			Enabled: true,
			Backend: "badger",
			Path:    filepath.Join(dbPath, "graph"),
		},
	})
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	defer db.Close(ctx)

	tests := []struct {
		name      string
		opts      MemoryServiceOptions
		wantError bool
	}{
		{
			name: "有效配置",
			opts: MemoryServiceOptions{
				Embedder: NewSimpleEmbedder(384),
			},
			wantError: false,
		},
		{
			name: "缺少嵌入生成器",
			opts: MemoryServiceOptions{
				Embedder: nil,
			},
			wantError: true,
		},
		{
			name: "自定义全文搜索选项",
			opts: MemoryServiceOptions{
				Embedder: NewSimpleEmbedder(384),
				FulltextIndexOptions: &rxdb.FulltextIndexOptions{
					Tokenize:      "jieba",
					CaseSensitive: false,
				},
			},
			wantError: false,
		},
		{
			name: "自定义向量搜索选项",
			opts: MemoryServiceOptions{
				Embedder: NewSimpleEmbedder(384),
				VectorSearchOptions: &VectorSearchOptions{
					DistanceMetric: "cosine",
				},
			},
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logrus.WithFields(logrus.Fields{
				"testName":        tt.name,
				"wantError":       tt.wantError,
				"hasEmbedder":     tt.opts.Embedder != nil,
				"hasFulltextOpts": tt.opts.FulltextIndexOptions != nil,
				"hasVectorOpts":   tt.opts.VectorSearchOptions != nil,
			}).Info("🧪 开始测试用例")

			if tt.opts.FulltextIndexOptions != nil {
				logrus.WithFields(logrus.Fields{
					"tokenize":      tt.opts.FulltextIndexOptions.Tokenize,
					"caseSensitive": tt.opts.FulltextIndexOptions.CaseSensitive,
				}).Info("📝 测试用例使用自定义全文搜索选项")
			}

			service, err := NewMemoryService(ctx, db, tt.opts)

			if (err != nil) != tt.wantError {
				logrus.WithError(err).WithField("wantError", tt.wantError).Error("❌ 测试失败: 错误不符合预期")
				t.Errorf("NewMemoryService() error = %v, wantError %v", err, tt.wantError)
				return
			}
			if !tt.wantError && service == nil {
				logrus.Error("❌ 测试失败: 期望得到 service，但得到 nil")
				t.Error("Expected service, got nil")
			} else {
				logrus.WithField("testName", tt.name).Info("✅ 测试用例通过")
			}
		})
	}
}

func TestMemoryService_AddMemory(t *testing.T) {
	service, _, cleanup := setupTestService(t)
	defer cleanup()
	ctx := context.Background()

	tests := []struct {
		name      string
		content   string
		memType   string
		dataset   string
		metadata  map[string]interface{}
		wantError bool
	}{
		{
			name:      "正常添加",
			content:   "这是一个测试记忆",
			memType:   "text",
			dataset:   "test_dataset",
			metadata:  nil,
			wantError: false,
		},
		{
			name:    "带元数据",
			content: "带元数据的记忆",
			memType: "text",
			dataset: "test_dataset",
			metadata: map[string]interface{}{
				"author": "测试作者",
				"source": "测试来源",
			},
			wantError: false,
		},
		{
			name:      "空内容",
			content:   "",
			memType:   "text",
			dataset:   "test_dataset",
			metadata:  nil,
			wantError: false,
		},
		{
			name:      "长文本",
			content:   "这是一个很长的测试文本，用来测试系统是否能正确处理较长的输入内容。",
			memType:   "text",
			dataset:   "test_dataset",
			metadata:  nil,
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			memory, err := service.AddMemory(ctx, tt.content, tt.memType, tt.dataset, tt.metadata)
			if (err != nil) != tt.wantError {
				t.Errorf("AddMemory() error = %v, wantError %v", err, tt.wantError)
				return
			}
			if !tt.wantError {
				if memory == nil {
					t.Error("Expected memory, got nil")
					return
				}
				if memory.ID == "" {
					t.Error("Expected memory ID, got empty string")
				}
				if memory.Content != tt.content {
					t.Errorf("Expected content %q, got %q", tt.content, memory.Content)
				}
				if memory.Type != tt.memType {
					t.Errorf("Expected type %q, got %q", tt.memType, memory.Type)
				}
				if memory.Dataset != tt.dataset {
					t.Errorf("Expected dataset %q, got %q", tt.dataset, memory.Dataset)
				}
				if memory.CreatedAt == 0 {
					t.Error("Expected CreatedAt to be set")
				}
			}
		})
	}
}

func TestMemoryService_GetMemory(t *testing.T) {
	service, _, cleanup := setupTestService(t)
	defer cleanup()
	ctx := context.Background()

	// 先添加一个记忆
	memory, err := service.AddMemory(ctx, "测试内容", "text", "test_dataset", nil)
	if err != nil {
		t.Fatalf("Failed to add memory: %v", err)
	}

	// 测试获取记忆
	t.Run("获取存在的记忆", func(t *testing.T) {
		got, err := service.GetMemory(ctx, memory.ID)
		if err != nil {
			t.Errorf("GetMemory() error = %v", err)
			return
		}
		if got == nil {
			t.Error("Expected memory, got nil")
			return
		}
		if got.ID != memory.ID {
			t.Errorf("Expected ID %q, got %q", memory.ID, got.ID)
		}
		if got.Content != "测试内容" {
			t.Errorf("Expected content %q, got %q", "测试内容", got.Content)
		}
	})

	// 测试获取不存在的记忆
	t.Run("获取不存在的记忆", func(t *testing.T) {
		_, err := service.GetMemory(ctx, "non-existent-id")
		if err == nil {
			t.Error("Expected error for non-existent memory")
		}
	})
}

func TestMemoryService_DeleteMemory(t *testing.T) {
	service, _, cleanup := setupTestService(t)
	defer cleanup()
	ctx := context.Background()

	// 先添加一个记忆
	memory, err := service.AddMemory(ctx, "待删除的内容", "text", "test_dataset", nil)
	if err != nil {
		t.Fatalf("Failed to add memory: %v", err)
	}

	// 删除记忆
	err = service.DeleteMemory(ctx, memory.ID)
	if err != nil {
		t.Errorf("DeleteMemory() error = %v", err)
	}

	// 验证记忆已被删除
	_, err = service.GetMemory(ctx, memory.ID)
	if err == nil {
		t.Error("Expected error when getting deleted memory")
	}
}

func TestMemoryService_ProcessMemory(t *testing.T) {
	service, _, cleanup := setupTestService(t)
	defer cleanup()
	ctx := context.Background()

	// 先添加一个记忆
	memory, err := service.AddMemory(ctx, "AI 正在改变世界", "text", "test_dataset", nil)
	if err != nil {
		t.Fatalf("Failed to add memory: %v", err)
	}

	// 处理记忆
	err = service.ProcessMemory(ctx, memory.ID)
	if err != nil {
		t.Errorf("ProcessMemory() error = %v", err)
	}

	// 验证记忆已被标记为已处理
	got, err := service.GetMemory(ctx, memory.ID)
	if err != nil {
		t.Fatalf("Failed to get memory: %v", err)
	}
	if got.ProcessedAt == 0 {
		t.Error("Expected ProcessedAt to be set after processing")
	}
}

func TestMemoryService_Search(t *testing.T) {
	service, _, cleanup := setupTestService(t)
	defer cleanup()
	ctx := context.Background()

	// 添加一些测试记忆
	memories := []struct {
		content string
		dataset string
	}{
		{"人工智能正在改变世界", "test_dataset"},
		{"机器学习是 AI 的一个分支", "test_dataset"},
		{"深度学习使用神经网络", "test_dataset"},
		{"自然语言处理很重要", "test_dataset"},
	}

	for _, m := range memories {
		_, err := service.AddMemory(ctx, m.content, "text", m.dataset, nil)
		if err != nil {
			t.Fatalf("Failed to add memory: %v", err)
		}
	}

	// 等待索引建立（如果需要）
	// 在实际实现中，可能需要等待索引完成

	tests := []struct {
		name       string
		query      string
		searchType string
		limit      int
		wantError  bool
	}{
		{
			name:       "全文搜索",
			query:      "人工智能",
			searchType: "FULLTEXT",
			limit:      10,
			wantError:  false,
		},
		{
			name:       "向量搜索",
			query:      "AI",
			searchType: "VECTOR",
			limit:      10,
			wantError:  false,
		},
		{
			name:       "混合搜索",
			query:      "机器学习",
			searchType: "HYBRID",
			limit:      10,
			wantError:  false,
		},
		{
			name:       "图搜索",
			query:      "神经网络",
			searchType: "GRAPH",
			limit:      10,
			wantError:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results, err := service.Search(ctx, tt.query, tt.searchType, tt.limit)
			if (err != nil) != tt.wantError {
				t.Errorf("Search() error = %v, wantError %v", err, tt.wantError)
				return
			}
			if !tt.wantError {
				if results == nil {
					t.Error("Expected results, got nil")
					return
				}
				if len(results) > tt.limit {
					t.Errorf("Expected at most %d results, got %d", tt.limit, len(results))
				}
				// 验证结果结构
				for _, result := range results {
					if result.ID == "" {
						t.Error("Result ID should not be empty")
					}
					if result.Source == "" {
						t.Error("Result Source should not be empty")
					}
				}
			}
		})
	}
}

func TestMemoryService_ListDatasets(t *testing.T) {
	service, _, cleanup := setupTestService(t)
	defer cleanup()
	ctx := context.Background()

	// 添加不同数据集的记忆
	datasets := []string{"dataset1", "dataset2", "dataset3"}
	for _, dataset := range datasets {
		_, err := service.AddMemory(ctx, "测试内容", "text", dataset, nil)
		if err != nil {
			t.Fatalf("Failed to add memory: %v", err)
		}
	}

	// 列出数据集
	got, err := service.ListDatasets(ctx)
	if err != nil {
		t.Errorf("ListDatasets() error = %v", err)
		return
	}

	if len(got) < len(datasets) {
		t.Errorf("Expected at least %d datasets, got %d", len(datasets), len(got))
	}

	// 验证数据集名称
	datasetMap := make(map[string]bool)
	for _, ds := range got {
		datasetMap[ds.Name] = true
	}
	for _, expected := range datasets {
		if !datasetMap[expected] {
			t.Errorf("Expected dataset %q not found", expected)
		}
	}
}

func TestMemoryService_GetDatasetData(t *testing.T) {
	service, _, cleanup := setupTestService(t)
	defer cleanup()
	ctx := context.Background()

	dataset := "test_dataset"
	contents := []string{"内容1", "内容2", "内容3"}

	// 添加记忆
	for _, content := range contents {
		_, err := service.AddMemory(ctx, content, "text", dataset, nil)
		if err != nil {
			t.Fatalf("Failed to add memory: %v", err)
		}
	}

	// 获取数据集数据
	data, err := service.GetDatasetData(ctx, dataset)
	if err != nil {
		t.Errorf("GetDatasetData() error = %v", err)
		return
	}

	if len(data) < len(contents) {
		t.Errorf("Expected at least %d items, got %d", len(contents), len(data))
	}
}

func TestMemoryService_GetDatasetStatus(t *testing.T) {
	service, _, cleanup := setupTestService(t)
	defer cleanup()
	ctx := context.Background()

	dataset := "test_dataset"

	// 添加记忆
	memory1, err := service.AddMemory(ctx, "内容1", "text", dataset, nil)
	if err != nil {
		t.Fatalf("Failed to add memory1: %v", err)
	}
	memory2, err := service.AddMemory(ctx, "内容2", "text", dataset, nil)
	if err != nil {
		t.Fatalf("Failed to add memory2: %v", err)
	}

	// 处理一个记忆
	if err := service.ProcessMemory(ctx, memory1.ID); err != nil {
		t.Fatalf("Failed to process memory1: %v", err)
	}

	// 确保 memory2 存在但未处理
	_ = memory2

	// 获取数据集状态
	status, err := service.GetDatasetStatus(ctx, dataset)
	if err != nil {
		t.Errorf("GetDatasetStatus() error = %v", err)
		return
	}

	if status.Dataset != dataset {
		t.Errorf("Expected dataset %q, got %q", dataset, status.Dataset)
	}
	if status.Total != 2 {
		t.Errorf("Expected total 2, got %d", status.Total)
	}
	if status.Processed != 1 {
		t.Errorf("Expected processed 1, got %d", status.Processed)
	}
	if status.Pending != 1 {
		t.Errorf("Expected pending 1, got %d", status.Pending)
	}
}

func TestMemoryService_ProcessDataset(t *testing.T) {
	service, _, cleanup := setupTestService(t)
	defer cleanup()
	ctx := context.Background()

	dataset := "test_dataset"

	// 添加多个记忆
	for i := 0; i < 3; i++ {
		_, err := service.AddMemory(ctx, "测试内容", "text", dataset, nil)
		if err != nil {
			t.Fatalf("Failed to add memory: %v", err)
		}
	}

	// 处理整个数据集
	count, err := service.ProcessDataset(ctx, dataset)
	if err != nil {
		t.Errorf("ProcessDataset() error = %v", err)
		return
	}

	if count != 3 {
		t.Errorf("Expected processed count 3, got %d", count)
	}
}

func TestMemoryService_DeleteDataset(t *testing.T) {
	service, _, cleanup := setupTestService(t)
	defer cleanup()
	ctx := context.Background()

	dataset := "test_dataset"

	// 添加记忆
	memory, err := service.AddMemory(ctx, "测试内容", "text", dataset, nil)
	if err != nil {
		t.Fatalf("Failed to add memory: %v", err)
	}

	// 删除数据集
	err = service.DeleteDataset(ctx, dataset)
	if err != nil {
		t.Errorf("DeleteDataset() error = %v", err)
	}

	// 验证记忆已被删除
	_, err = service.GetMemory(ctx, memory.ID)
	if err == nil {
		t.Error("Expected error when getting deleted memory")
	}
}

func TestMemoryService_GetGraphData(t *testing.T) {
	service, _, cleanup := setupTestService(t)
	defer cleanup()
	ctx := context.Background()

	// 添加并处理记忆（以生成实体和关系）
	memory, err := service.AddMemory(ctx, "AI 正在改变世界", "text", "test_dataset", nil)
	if err != nil {
		t.Fatalf("Failed to add memory: %v", err)
	}

	// 处理记忆以生成实体和关系
	service.ProcessMemory(ctx, memory.ID)

	// 获取图谱数据
	graphData, err := service.GetGraphData(ctx)
	if err != nil {
		t.Errorf("GetGraphData() error = %v", err)
		return
	}

	if graphData == nil {
		t.Error("Expected graph data, got nil")
		return
	}

	// 验证图谱数据结构
	if graphData.Nodes == nil {
		t.Error("Expected nodes, got nil")
	}
	if graphData.Edges == nil {
		t.Error("Expected edges, got nil")
	}
}

func TestMemoryService_Health(t *testing.T) {
	service, _, cleanup := setupTestService(t)
	defer cleanup()
	ctx := context.Background()

	// 添加一些数据
	service.AddMemory(ctx, "测试内容1", "text", "test_dataset", nil)
	service.AddMemory(ctx, "测试内容2", "text", "test_dataset", nil)

	// 获取健康状态
	health, err := service.Health(ctx)
	if err != nil {
		t.Errorf("Health() error = %v", err)
		return
	}

	if health == nil {
		t.Error("Expected health status, got nil")
		return
	}

	if health.Status != "healthy" {
		t.Errorf("Expected status 'healthy', got %q", health.Status)
	}

	if health.Stats.Memories < 2 {
		t.Errorf("Expected at least 2 memories, got %d", health.Stats.Memories)
	}
}

func TestMemoryService_SearchResultStructure(t *testing.T) {
	service, _, cleanup := setupTestService(t)
	defer cleanup()
	ctx := context.Background()

	// 添加记忆
	_, err := service.AddMemory(ctx, "人工智能测试", "text", "test_dataset", nil)
	if err != nil {
		t.Fatalf("Failed to add memory: %v", err)
	}

	// 搜索
	results, err := service.Search(ctx, "人工智能", "HYBRID", 10)
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}

	if len(results) > 0 {
		result := results[0]
		// 验证结果结构
		if result.ID == "" {
			t.Error("Result ID should not be empty")
		}
		if result.Source == "" {
			t.Error("Result Source should not be empty")
		}
		// Score 应该是一个有效的浮点数
		if result.Score < 0 {
			t.Error("Result Score should be non-negative")
		}
	}
}
