/*
 * Copyright 2025 CloudWeGo Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/cloudwego/eino/components/embedding"
	"github.com/cloudwego/eino/schema"
	rr "github.com/mozhou-tech/rxdb-go/pkg/eino/retriever"
	"github.com/mozhou-tech/rxdb-go/pkg/rxdb"
)

const (
	customContentFieldName       = "my_content_field"
	customContentVectorFieldName = "my_vector_content_field"
	customExtraFieldName         = "extra_field_number"
)

func main() {
	ctx := context.Background()

	// Initialize RxDB database
	db, err := rxdb.CreateDatabase(ctx, rxdb.DatabaseOptions{
		Name: "eino_example",
		Path: "./data/eino_example.db",
	})
	if err != nil {
		panic(err)
	}
	defer db.Close(ctx)

	// Create collection
	col, err := db.Collection(ctx, "docs", rxdb.Schema{
		PrimaryKey: "id",
	})
	if err != nil {
		panic(err)
	}

	// Add vector search with customized field names
	vs, err := rxdb.AddVectorSearch(col, rxdb.VectorSearchConfig{
		Identifier: "docs-vector-custom",
		Dimensions: 1024,
		DocToEmbedding: func(doc map[string]any) (rxdb.Vector, error) {
			if v, ok := doc[customContentVectorFieldName].([]float64); ok {
				return v, nil
			}
			return nil, fmt.Errorf("custom vector field not found")
		},
	})
	if err != nil {
		panic(err)
	}

	b, err := os.ReadFile("./examples/eino_indexer/embeddings.json")
	if err != nil {
		panic(err)
	}

	var dense [][]float64
	if err = json.Unmarshal(b, &dense); err != nil {
		panic(err)
	}

	// customize with your own index structure.
	r, err := rr.NewRetriever(ctx, &rr.RetrieverConfig{
		VectorSearch: vs,
		TopK:         5,
		ReturnFields: []string{
			customContentFieldName,
			customContentVectorFieldName,
			customExtraFieldName,
		},
		DocumentConverter: func(ctx context.Context, doc rxdb.Document) (*schema.Document, error) {
			data := doc.Data()
			resp := &schema.Document{
				ID:       doc.ID(),
				MetaData: map[string]any{},
			}
			if v, ok := data[customContentFieldName].(string); ok {
				resp.Content = v
			}
			if v, ok := data[customContentVectorFieldName].([]float64); ok {
				resp.WithDenseVector(v)
			}
			if v, ok := data[customExtraFieldName]; ok {
				resp.MetaData["ext"] = v
			}
			return resp, nil
		},
		Embedding: &mockEmbedding{dense[7]},
	})
	if err != nil {
		panic(err)
	}

	docs, err := r.Retrieve(ctx, "tourist attraction")
	if err != nil {
		panic(err)
	}

	for _, doc := range docs {
		fmt.Printf("id:%s, ext_number:%v, content:%v\n", doc.ID, doc.MetaData["ext"], doc.Content)
	}
}

// mockEmbedding returns embeddings with 1024 dimensions
type mockEmbedding struct {
	vector []float64
}

func (m mockEmbedding) EmbedStrings(ctx context.Context, texts []string, opts ...embedding.Option) ([][]float64, error) {
	res := make([][]float64, len(texts))
	for i := range res {
		res[i] = m.vector
	}
	return res, nil
}
