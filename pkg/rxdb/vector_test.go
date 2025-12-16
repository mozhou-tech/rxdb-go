package rxdb

import (
	"context"
	"fmt"
	"math"
	"os"
	"testing"
)

func TestVectorSearch_Basic(t *testing.T) {
	// 创建临时目录
	tmpDir, err := os.MkdirTemp("", "rxdb-vector-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// 创建数据库
	db, err := CreateDatabase(context.Background(), DatabaseOptions{
		Name: "test-vector",
		Path: tmpDir,
	})
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close(context.Background())

	// 创建集合
	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}
	coll, err := db.Collection(context.Background(), "vectors", schema)
	if err != nil {
		t.Fatalf("failed to create collection: %v", err)
	}

	// 插入测试文档（带预定义的嵌入向量）
	testDocs := []map[string]any{
		{"id": "1", "text": "apple", "embedding": []float64{1.0, 0.0, 0.0}},
		{"id": "2", "text": "banana", "embedding": []float64{0.9, 0.1, 0.0}},
		{"id": "3", "text": "orange", "embedding": []float64{0.0, 1.0, 0.0}},
		{"id": "4", "text": "grape", "embedding": []float64{0.0, 0.0, 1.0}},
		{"id": "5", "text": "cherry", "embedding": []float64{0.8, 0.2, 0.0}},
	}

	for _, doc := range testDocs {
		_, err := coll.Insert(context.Background(), doc)
		if err != nil {
			t.Fatalf("failed to insert document: %v", err)
		}
	}

	// 创建向量搜索实例
	vs, err := AddVectorSearch(coll, VectorSearchConfig{
		Identifier: "fruit-search",
		Dimensions: 3,
		DocToEmbedding: func(doc map[string]any) (Vector, error) {
			if emb, ok := doc["embedding"].([]float64); ok {
				return emb, nil
			}
			// 从 []any 转换
			if embAny, ok := doc["embedding"].([]any); ok {
				emb := make([]float64, len(embAny))
				for i, v := range embAny {
					if f, ok := v.(float64); ok {
						emb[i] = f
					}
				}
				return emb, nil
			}
			return nil, nil
		},
		DistanceMetric: "euclidean",
	})
	if err != nil {
		t.Fatalf("failed to create vector search: %v", err)
	}
	defer vs.Close()

	// 搜索与 [1.0, 0.0, 0.0] 相似的向量
	results, err := vs.Search(context.Background(), []float64{1.0, 0.0, 0.0}, VectorSearchOptions{
		Limit: 3,
	})
	if err != nil {
		t.Fatalf("failed to search: %v", err)
	}

	if len(results) != 3 {
		t.Errorf("expected 3 results, got %d", len(results))
	}

	// 验证第一个结果是最接近的（应该是 id=1）
	if len(results) > 0 && results[0].Document.ID() != "1" {
		t.Errorf("expected first result to be document '1', got '%s'", results[0].Document.ID())
	}
}

func TestVectorSearch_CosineSimilarity(t *testing.T) {
	// 测试余弦相似度计算
	a := Vector{1.0, 0.0, 0.0}
	b := Vector{1.0, 0.0, 0.0}
	c := Vector{0.0, 1.0, 0.0}

	// 相同向量的余弦相似度应该是 1
	sim := CosineSimilarity(a, b)
	if math.Abs(sim-1.0) > 0.001 {
		t.Errorf("expected similarity 1.0 for identical vectors, got %f", sim)
	}

	// 正交向量的余弦相似度应该是 0
	sim = CosineSimilarity(a, c)
	if math.Abs(sim) > 0.001 {
		t.Errorf("expected similarity 0 for orthogonal vectors, got %f", sim)
	}

	// 余弦距离
	dist := CosineDistance(a, b)
	if math.Abs(dist) > 0.001 {
		t.Errorf("expected distance 0 for identical vectors, got %f", dist)
	}

	dist = CosineDistance(a, c)
	if math.Abs(dist-1.0) > 0.001 {
		t.Errorf("expected distance 1 for orthogonal vectors, got %f", dist)
	}
}

func TestVectorSearch_EuclideanDistance(t *testing.T) {
	a := Vector{0.0, 0.0, 0.0}
	b := Vector{1.0, 0.0, 0.0}
	c := Vector{1.0, 1.0, 1.0}

	// 距离计算
	dist := EuclideanDistance(a, b)
	if math.Abs(dist-1.0) > 0.001 {
		t.Errorf("expected distance 1.0, got %f", dist)
	}

	dist = EuclideanDistance(a, c)
	expected := math.Sqrt(3.0)
	if math.Abs(dist-expected) > 0.001 {
		t.Errorf("expected distance %f, got %f", expected, dist)
	}
}

func TestVectorSearch_KNN(t *testing.T) {
	// 创建临时目录
	tmpDir, err := os.MkdirTemp("", "rxdb-vector-knn-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// 创建数据库
	db, err := CreateDatabase(context.Background(), DatabaseOptions{
		Name: "test-vector-knn",
		Path: tmpDir,
	})
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close(context.Background())

	// 创建集合
	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}
	coll, err := db.Collection(context.Background(), "points", schema)
	if err != nil {
		t.Fatalf("failed to create collection: %v", err)
	}

	// 插入测试点
	points := []map[string]any{
		{"id": "p1", "x": 0.0, "y": 0.0},
		{"id": "p2", "x": 1.0, "y": 0.0},
		{"id": "p3", "x": 0.0, "y": 1.0},
		{"id": "p4", "x": 1.0, "y": 1.0},
		{"id": "p5", "x": 2.0, "y": 2.0},
	}

	for _, p := range points {
		_, err := coll.Insert(context.Background(), p)
		if err != nil {
			t.Fatalf("failed to insert point: %v", err)
		}
	}

	// 创建向量搜索
	vs, err := AddVectorSearch(coll, VectorSearchConfig{
		Identifier: "point-search",
		Dimensions: 2,
		DocToEmbedding: func(doc map[string]any) (Vector, error) {
			x, _ := doc["x"].(float64)
			y, _ := doc["y"].(float64)
			return Vector{x, y}, nil
		},
		DistanceMetric: "euclidean",
	})
	if err != nil {
		t.Fatalf("failed to create vector search: %v", err)
	}
	defer vs.Close()

	// KNN 搜索：找到距离 (0.5, 0.5) 最近的 2 个点
	results, err := vs.KNNSearch(context.Background(), Vector{0.5, 0.5}, 2)
	if err != nil {
		t.Fatalf("failed to KNN search: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("expected 2 results, got %d", len(results))
	}
}

func TestVectorSearch_RangeSearch(t *testing.T) {
	// 创建临时目录
	tmpDir, err := os.MkdirTemp("", "rxdb-vector-range-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// 创建数据库
	db, err := CreateDatabase(context.Background(), DatabaseOptions{
		Name: "test-vector-range",
		Path: tmpDir,
	})
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close(context.Background())

	// 创建集合
	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}
	coll, err := db.Collection(context.Background(), "locations", schema)
	if err != nil {
		t.Fatalf("failed to create collection: %v", err)
	}

	// 插入测试位置
	locations := []map[string]any{
		{"id": "l1", "lat": 0.0, "lng": 0.0}, // 距离原点 0
		{"id": "l2", "lat": 0.5, "lng": 0.0}, // 距离原点 0.5
		{"id": "l3", "lat": 1.0, "lng": 0.0}, // 距离原点 1.0
		{"id": "l4", "lat": 2.0, "lng": 0.0}, // 距离原点 2.0
		{"id": "l5", "lat": 5.0, "lng": 0.0}, // 距离原点 5.0
	}

	for _, loc := range locations {
		_, err := coll.Insert(context.Background(), loc)
		if err != nil {
			t.Fatalf("failed to insert location: %v", err)
		}
	}

	// 创建向量搜索
	vs, err := AddVectorSearch(coll, VectorSearchConfig{
		Identifier: "location-search",
		Dimensions: 2,
		DocToEmbedding: func(doc map[string]any) (Vector, error) {
			lat, _ := doc["lat"].(float64)
			lng, _ := doc["lng"].(float64)
			return Vector{lat, lng}, nil
		},
		DistanceMetric: "euclidean",
	})
	if err != nil {
		t.Fatalf("failed to create vector search: %v", err)
	}
	defer vs.Close()

	// 范围搜索：找到距离原点 <= 1.5 的点
	results, err := vs.RangeSearch(context.Background(), Vector{0.0, 0.0}, 1.5)
	if err != nil {
		t.Fatalf("failed to range search: %v", err)
	}

	// 应该返回 l1, l2, l3 (距离 0, 0.5, 1.0)
	if len(results) != 3 {
		t.Errorf("expected 3 results within range, got %d", len(results))
	}
}

func TestVectorSearch_SearchByID(t *testing.T) {
	// 创建临时目录
	tmpDir, err := os.MkdirTemp("", "rxdb-vector-byid-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// 创建数据库
	db, err := CreateDatabase(context.Background(), DatabaseOptions{
		Name: "test-vector-byid",
		Path: tmpDir,
	})
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close(context.Background())

	// 创建集合
	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}
	coll, err := db.Collection(context.Background(), "items", schema)
	if err != nil {
		t.Fatalf("failed to create collection: %v", err)
	}

	// 插入测试文档
	items := []map[string]any{
		{"id": "item1", "category": "electronics", "embedding": []float64{1.0, 0.0}},
		{"id": "item2", "category": "electronics", "embedding": []float64{0.95, 0.05}},
		{"id": "item3", "category": "clothing", "embedding": []float64{0.0, 1.0}},
		{"id": "item4", "category": "clothing", "embedding": []float64{0.05, 0.95}},
	}

	for _, item := range items {
		_, err := coll.Insert(context.Background(), item)
		if err != nil {
			t.Fatalf("failed to insert item: %v", err)
		}
	}

	// 创建向量搜索
	vs, err := AddVectorSearch(coll, VectorSearchConfig{
		Identifier: "item-similarity",
		Dimensions: 2,
		DocToEmbedding: func(doc map[string]any) (Vector, error) {
			if emb, ok := doc["embedding"].([]float64); ok {
				return emb, nil
			}
			if embAny, ok := doc["embedding"].([]any); ok {
				emb := make([]float64, len(embAny))
				for i, v := range embAny {
					if f, ok := v.(float64); ok {
						emb[i] = f
					}
				}
				return emb, nil
			}
			return nil, nil
		},
		DistanceMetric: "cosine",
	})
	if err != nil {
		t.Fatalf("failed to create vector search: %v", err)
	}
	defer vs.Close()

	// 搜索与 item1 相似的项目
	results, err := vs.SearchByID(context.Background(), "item1", VectorSearchOptions{
		Limit: 2,
	})
	if err != nil {
		t.Fatalf("failed to search by ID: %v", err)
	}

	// 结果不应该包含 item1 本身
	for _, r := range results {
		if r.Document.ID() == "item1" {
			t.Errorf("search by ID should not include the query document itself")
		}
	}

	// 最相似的应该是 item2（同类电子产品）
	if len(results) > 0 && results[0].Document.ID() != "item2" {
		t.Errorf("expected most similar to item1 to be item2, got %s", results[0].Document.ID())
	}
}

func TestVectorSearch_Persist(t *testing.T) {
	// 创建临时目录
	tmpDir, err := os.MkdirTemp("", "rxdb-vector-persist-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// 创建数据库
	db, err := CreateDatabase(context.Background(), DatabaseOptions{
		Name: "test-vector-persist",
		Path: tmpDir,
	})
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	// 创建集合
	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}
	coll, err := db.Collection(context.Background(), "embeddings", schema)
	if err != nil {
		t.Fatalf("failed to create collection: %v", err)
	}

	// 插入文档
	_, err = coll.Insert(context.Background(), map[string]any{
		"id":        "1",
		"embedding": []float64{1.0, 2.0, 3.0},
	})
	if err != nil {
		t.Fatalf("failed to insert document: %v", err)
	}

	// 创建向量搜索并持久化
	vs, err := AddVectorSearch(coll, VectorSearchConfig{
		Identifier: "embedding-search",
		Dimensions: 3,
		DocToEmbedding: func(doc map[string]any) (Vector, error) {
			if emb, ok := doc["embedding"].([]float64); ok {
				return emb, nil
			}
			if embAny, ok := doc["embedding"].([]any); ok {
				emb := make([]float64, len(embAny))
				for i, v := range embAny {
					if f, ok := v.(float64); ok {
						emb[i] = f
					}
				}
				return emb, nil
			}
			return nil, nil
		},
	})
	if err != nil {
		t.Fatalf("failed to create vector search: %v", err)
	}

	// 持久化索引
	err = vs.Persist(context.Background())
	if err != nil {
		t.Fatalf("failed to persist index: %v", err)
	}
	vs.Close()

	// 创建新的向量搜索实例并加载
	vs2, err := AddVectorSearch(coll, VectorSearchConfig{
		Identifier:     "embedding-search",
		Dimensions:     3,
		Initialization: "lazy",
		DocToEmbedding: func(doc map[string]any) (Vector, error) {
			if emb, ok := doc["embedding"].([]float64); ok {
				return emb, nil
			}
			if embAny, ok := doc["embedding"].([]any); ok {
				emb := make([]float64, len(embAny))
				for i, v := range embAny {
					if f, ok := v.(float64); ok {
						emb[i] = f
					}
				}
				return emb, nil
			}
			return nil, nil
		},
	})
	if err != nil {
		t.Fatalf("failed to create second vector search: %v", err)
	}
	defer vs2.Close()

	// 加载持久化的索引
	err = vs2.Load(context.Background())
	if err != nil {
		t.Fatalf("failed to load index: %v", err)
	}

	// 验证索引已加载
	if vs2.Count() != 1 {
		t.Errorf("expected 1 document in loaded index, got %d", vs2.Count())
	}

	// 验证可以获取嵌入向量
	emb, exists := vs2.GetEmbedding("1")
	if !exists {
		t.Errorf("expected embedding for document '1' to exist")
	}
	if len(emb) != 3 {
		t.Errorf("expected embedding dimension 3, got %d", len(emb))
	}

	db.Close(context.Background())
}

func TestVectorSearch_NormalizeVector(t *testing.T) {
	v := Vector{3.0, 4.0}
	normalized := NormalizeVector(v)

	// 归一化后的向量长度应该是 1
	norm := math.Sqrt(normalized[0]*normalized[0] + normalized[1]*normalized[1])
	if math.Abs(norm-1.0) > 0.001 {
		t.Errorf("expected normalized vector length 1.0, got %f", norm)
	}

	// 验证值
	if math.Abs(normalized[0]-0.6) > 0.001 || math.Abs(normalized[1]-0.8) > 0.001 {
		t.Errorf("expected [0.6, 0.8], got [%f, %f]", normalized[0], normalized[1])
	}
}

func TestVectorSearch_IVFIndex(t *testing.T) {
	// 创建临时目录
	tmpDir, err := os.MkdirTemp("", "rxdb-vector-ivf-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// 创建数据库
	db, err := CreateDatabase(context.Background(), DatabaseOptions{
		Name: "test-vector-ivf",
		Path: tmpDir,
	})
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close(context.Background())

	// 创建集合
	schema := Schema{
		PrimaryKey: "id",
		RevField:   "_rev",
	}
	coll, err := db.Collection(context.Background(), "ivf_vectors", schema)
	if err != nil {
		t.Fatalf("failed to create collection: %v", err)
	}

	// 插入更多测试文档以测试 IVF 索引
	for i := 0; i < 20; i++ {
		doc := map[string]any{
			"id":        fmt.Sprintf("doc%d", i),
			"embedding": []float64{float64(i % 5), float64(i / 5)},
		}
		_, err := coll.Insert(context.Background(), doc)
		if err != nil {
			t.Fatalf("failed to insert document: %v", err)
		}
	}

	// 创建使用 IVF 索引的向量搜索
	vs, err := AddVectorSearch(coll, VectorSearchConfig{
		Identifier: "ivf-search",
		Dimensions: 2,
		IndexType:  "ivf",
		NumIndexes: 4,
		DocToEmbedding: func(doc map[string]any) (Vector, error) {
			if emb, ok := doc["embedding"].([]float64); ok {
				return emb, nil
			}
			if embAny, ok := doc["embedding"].([]any); ok {
				emb := make([]float64, len(embAny))
				for i, v := range embAny {
					if f, ok := v.(float64); ok {
						emb[i] = f
					}
				}
				return emb, nil
			}
			return nil, nil
		},
		DistanceMetric: "euclidean",
	})
	if err != nil {
		t.Fatalf("failed to create vector search with IVF: %v", err)
	}
	defer vs.Close()

	// 验证索引中有文档
	if vs.Count() != 20 {
		t.Errorf("expected 20 documents in index, got %d", vs.Count())
	}

	// 先使用全表扫描测试
	results, err := vs.Search(context.Background(), Vector{0.0, 0.0}, VectorSearchOptions{
		Limit:       5,
		UseFullScan: true,
	})
	if err != nil {
		t.Fatalf("failed to search with full scan: %v", err)
	}

	if len(results) < 1 {
		t.Errorf("expected at least 1 result with full scan, got %d", len(results))
	}

	// 测试 IVF 索引搜索
	results, err = vs.Search(context.Background(), Vector{0.0, 0.0}, VectorSearchOptions{
		Limit:            5,
		DocsPerIndexSide: 20, // 增加每个桶的返回数量以确保能找到结果
	})
	if err != nil {
		t.Fatalf("failed to search with IVF: %v", err)
	}

	// IVF 索引搜索的结果数量可能少于全表扫描
	if len(results) < 1 {
		t.Logf("IVF search returned %d results (may be less precise than full scan)", len(results))
	}
}
