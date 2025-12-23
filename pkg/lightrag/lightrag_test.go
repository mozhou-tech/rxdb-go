package lightrag

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

func TestSimpleEmbedder(t *testing.T) {
	dims := 10
	embedder := NewSimpleEmbedder(dims)

	if embedder.Dimensions() != dims {
		t.Errorf("expected dimensions %d, got %d", dims, embedder.Dimensions())
	}

	text := "hello"
	vec, err := embedder.Embed(context.Background(), text)
	if err != nil {
		t.Fatalf("failed to embed: %v", err)
	}

	if len(vec) != dims {
		t.Errorf("expected vector length %d, got %d", dims, len(vec))
	}

	// Verify embedding values (極簡實現：取前 N 个字符的 ASCII 值)
	for i := 0; i < len(text) && i < dims; i++ {
		expected := float64(text[i]) / 255.0
		if vec[i] != expected {
			t.Errorf("at index %d: expected %f, got %f", i, expected, vec[i])
		}
	}
}

func TestSimpleLLM(t *testing.T) {
	llm := &SimpleLLM{}
	ctx := context.Background()

	prompt := "Context: some context\n\nQuestion: What is this?\n\nAnswer the question based on the context."
	resp, err := llm.Complete(ctx, prompt)
	if err != nil {
		t.Fatalf("failed to complete: %v", err)
	}

	if !strings.Contains(resp, "What is this?") || !strings.Contains(resp, "some context") {
		t.Errorf("response should contain question and context, got: %s", resp)
	}

	resp2, err := llm.Complete(ctx, "just a prompt")
	if err != nil {
		t.Fatalf("failed to complete: %v", err)
	}
	if resp2 != "Simple LLM response" {
		t.Errorf("expected 'Simple LLM response', got: %s", resp2)
	}
}

func TestLightRAG_Flow(t *testing.T) {
	ctx := context.Background()
	workingDir := "./test_rag_storage"
	defer os.RemoveAll(workingDir)

	embedder := NewSimpleEmbedder(768)
	llm := &SimpleLLM{}

	rag := New(Options{
		WorkingDir: workingDir,
		Embedder:   embedder,
		LLM:        llm,
	})

	// Test uninitialized call
	err := rag.Insert(ctx, "test content")
	if err == nil || !strings.Contains(err.Error(), "storages not initialized") {
		t.Errorf("expected error for uninitialized insert, got: %v", err)
	}

	// Initialize
	err = rag.InitializeStorages(ctx)
	if err != nil {
		t.Fatalf("failed to initialize storages: %v", err)
	}
	defer rag.FinalizeStorages(ctx)

	// Insert
	err = rag.Insert(ctx, "The capital of France is Paris.")
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	err = rag.Insert(ctx, "The capital of Germany is Berlin.")
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	// Give it a moment to index (asynchronous in FulltextSearch)
	time.Sleep(200 * time.Millisecond)

	// Query - Vector mode
	resp, err := rag.Query(ctx, "What is the capital of France?", QueryParam{
		Mode:  ModeVector,
		Limit: 1,
	})
	if err != nil {
		t.Fatalf("failed to query vector: %v", err)
	}
	if !strings.Contains(resp, "Paris") {
		t.Errorf("vector query response should contain 'Paris', got: %s", resp)
	}

	// Query - Fulltext mode
	resp, err = rag.Query(ctx, "Berlin", QueryParam{
		Mode:  ModeFulltext,
		Limit: 1,
	})
	if err != nil {
		t.Fatalf("failed to query fulltext: %v", err)
	}
	if !strings.Contains(resp, "Berlin") {
		t.Errorf("fulltext query response should contain 'Berlin', got: %s", resp)
	}

	// Query - Hybrid mode
	resp, err = rag.Query(ctx, "capital", QueryParam{
		Mode:  ModeHybrid,
		Limit: 2,
	})
	if err != nil {
		t.Fatalf("failed to query hybrid: %v", err)
	}
	if !strings.Contains(resp, "Paris") && !strings.Contains(resp, "Berlin") {
		t.Errorf("hybrid query response should contain relevant info, got: %s", resp)
	}

	// Query - No results
	resp, err = rag.Query(ctx, "Something totally unrelated", QueryParam{
		Mode:  ModeFulltext,
		Limit: 1,
	})
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	if resp != "No relevant information found." {
		t.Errorf("expected 'No relevant information found.', got: %s", resp)
	}
}

func TestLightRAG_NoEmbedder(t *testing.T) {
	ctx := context.Background()
	workingDir := "./test_rag_no_embed"
	defer os.RemoveAll(workingDir)

	rag := New(Options{
		WorkingDir: workingDir,
		// No Embedder
		LLM: &SimpleLLM{},
	})

	err := rag.InitializeStorages(ctx)
	if err != nil {
		t.Fatalf("failed to initialize: %v", err)
	}
	defer rag.FinalizeStorages(ctx)

	err = rag.Insert(ctx, "Only fulltext search is available here.")
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	// Vector query should fail
	_, err = rag.Query(ctx, "test", QueryParam{Mode: ModeVector})
	if err == nil || !strings.Contains(err.Error(), "vector search not available") {
		t.Errorf("expected error for missing vector search, got: %v", err)
	}

	// Fulltext query should still work
	resp, err := rag.Query(ctx, "fulltext", QueryParam{Mode: ModeFulltext})
	if err != nil {
		t.Fatalf("fulltext query failed: %v", err)
	}
	if !strings.Contains(resp, "available") {
		t.Errorf("expected response to contain 'available', got: %s", resp)
	}
}

func TestLightRAG_Persistence(t *testing.T) {
	ctx := context.Background()
	workingDir := "./test_rag_persistence"
	defer os.RemoveAll(workingDir)

	embedder := NewSimpleEmbedder(768)
	llm := &SimpleLLM{}

	// First session
	{
		rag := New(Options{
			WorkingDir: workingDir,
			Embedder:   embedder,
			LLM:        llm,
		})
		if err := rag.InitializeStorages(ctx); err != nil {
			t.Fatalf("failed to initialize: %v", err)
		}
		if err := rag.Insert(ctx, "Persisted content."); err != nil {
			t.Fatalf("failed to insert: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
		rag.FinalizeStorages(ctx)
	}

	// Second session
	{
		rag := New(Options{
			WorkingDir: workingDir,
			Embedder:   embedder,
			LLM:        llm,
		})
		if err := rag.InitializeStorages(ctx); err != nil {
			t.Fatalf("failed to initialize second session: %v", err)
		}
		defer rag.FinalizeStorages(ctx)

		resp, err := rag.Query(ctx, "Persisted", QueryParam{Mode: ModeFulltext})
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if !strings.Contains(resp, "Persisted content.") {
			t.Errorf("expected persisted content, got: %s", resp)
		}
	}
}

func TestLightRAG_Initialize_Twice(t *testing.T) {
	ctx := context.Background()
	workingDir := "./test_rag_init_twice"
	defer os.RemoveAll(workingDir)

	rag := New(Options{WorkingDir: workingDir})

	if err := rag.InitializeStorages(ctx); err != nil {
		t.Fatalf("first init failed: %v", err)
	}
	defer rag.FinalizeStorages(ctx)

	if err := rag.InitializeStorages(ctx); err != nil {
		t.Errorf("second init failed: %v", err)
	}
}

func TestLightRAG_NoLLM(t *testing.T) {
	ctx := context.Background()
	workingDir := "./test_rag_no_llm"
	defer os.RemoveAll(workingDir)

	rag := New(Options{WorkingDir: workingDir})
	if err := rag.InitializeStorages(ctx); err != nil {
		t.Fatalf("init failed: %v", err)
	}
	defer rag.FinalizeStorages(ctx)

	content := "This is some data."
	if err := rag.Insert(ctx, content); err != nil {
		t.Fatalf("insert failed: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	// No LLM provided, should return context text
	resp, err := rag.Query(ctx, "data", QueryParam{Mode: ModeFulltext})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if !strings.Contains(resp, content) {
		t.Errorf("expected response to contain content, got: %s", resp)
	}
}

func TestLightRAG_Query_Limit(t *testing.T) {
	ctx := context.Background()
	workingDir := "./test_rag_limit"
	defer os.RemoveAll(workingDir)

	rag := New(Options{WorkingDir: workingDir})
	if err := rag.InitializeStorages(ctx); err != nil {
		t.Fatalf("init failed: %v", err)
	}
	defer rag.FinalizeStorages(ctx)

	rag.Insert(ctx, "Doc 1")
	rag.Insert(ctx, "Doc 2")
	rag.Insert(ctx, "Doc 3")
	time.Sleep(200 * time.Millisecond)

	resp, err := rag.Query(ctx, "Doc", QueryParam{Mode: ModeFulltext, Limit: 2})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	// resp should contain [1] and [2] but not [3]
	if !strings.Contains(resp, "[1]") || !strings.Contains(resp, "[2]") {
		t.Errorf("expected 2 results, got: %s", resp)
	}
	if strings.Contains(resp, "[3]") {
		t.Errorf("did not expect 3rd result, got: %s", resp)
	}
}

func TestLightRAG_DefaultWorkingDir(t *testing.T) {
	ctx := context.Background()
	defaultDir := "./rag_storage"
	defer os.RemoveAll(defaultDir)

	rag := New(Options{}) // No WorkingDir
	if err := rag.InitializeStorages(ctx); err != nil {
		t.Fatalf("init failed: %v", err)
	}
	defer rag.FinalizeStorages(ctx)

	if _, err := os.Stat(defaultDir); os.IsNotExist(err) {
		t.Error("default working directory was not created")
	}
}
