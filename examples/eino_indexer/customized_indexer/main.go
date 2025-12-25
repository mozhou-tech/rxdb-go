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
	ri "github.com/mozhou-tech/rxdb-go/pkg/eino/indexer/rxdb"
	"os"
	"strconv"
	"strings"

	"github.com/cloudwego/eino/components/embedding"
	"github.com/cloudwego/eino/schema"
	"github.com/mozhou-tech/rxdb-go/pkg/rxdb"
)

// This example is related to example in https://github.com/cloudwego/eino-ext/tree/main/components/retriever/redis/examples/default_retriever

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

	b, err := os.ReadFile("./examples/eino_indexer/embeddings.json")
	if err != nil {
		panic(err)
	}

	var dense [][]float64
	if err = json.Unmarshal(b, &dense); err != nil {
		panic(err)
	}

	indexer, err := ri.NewIndexer(ctx, &ri.IndexerConfig{
		Collection:    col,
		DocumentToMap: nil, // use default convert method
		BatchSize:     5,
		Embedding:     &mockEmbedding{dense}, // replace with real embedding
	})
	if err != nil {
		panic(err)
	}

	contents := `1. Eiffel Tower: Located in Paris, France, it is one of the most famous landmarks in the world, designed by Gustave Eiffel and built in 1889.
2. The Great Wall: Located in China, it is one of the Seven Wonders of the World, built from the Qin Dynasty to the Ming Dynasty, with a total length of over 20000 kilometers.
3. Grand Canyon National Park: Located in Arizona, USA, it is famous for its deep canyons and magnificent scenery, which are cut by the Colorado River.
4. The Colosseum: Located in Rome, Italy, built between 70-80 AD, it was the largest circular arena in the ancient Roman Empire.
5. Taj Mahal: Located in Agra, India, it was completed by Mughal Emperor Shah Jahan in 1653 to commemorate his wife and is one of the New Seven Wonders of the World.
6. Sydney Opera House: Located in Sydney Harbour, Australia, it is one of the most iconic buildings of the 20th century, renowned for its unique sailboat design.
7. Louvre Museum: Located in Paris, France, it is one of the largest museums in the world with a rich collection, including Leonardo da Vinci's Mona Lisa and Greece's Venus de Milo.
8. Niagara Falls: located at the border of the United States and Canada, consisting of three main waterfalls, its spectacular scenery attracts millions of tourists every year.
9. St. Sophia Cathedral: located in Istanbul, Türkiye, originally built in 537 A.D., it used to be an Orthodox cathedral and mosque, and now it is a museum.
10. Machu Picchu: an ancient Inca site located on the plateau of the Andes Mountains in Peru, one of the New Seven Wonders of the World, with an altitude of over 2400 meters.`

	var docs []*schema.Document
	for idx, str := range strings.Split(contents, "\n") {
		docs = append(docs, &schema.Document{
			ID:      strconv.FormatInt(int64(idx+1), 10),
			Content: str,
		})
	}

	ids, err := indexer.Store(ctx, docs)
	if err != nil {
		panic(err)
	}

	fmt.Println(ids) // [1 2 3 4 5 6 7 8 9 10]
	// documents are stored in rxdb collection 'docs'
}

// mockEmbedding returns embeddings with 1024 dimensions
type mockEmbedding struct {
	dense [][]float64
}

func (m *mockEmbedding) EmbedStrings(ctx context.Context, texts []string, opts ...embedding.Option) ([][]float64, error) {
	if len(m.dense) < len(texts) {
		return nil, fmt.Errorf("not enough mock embeddings, need %d, have %d", len(texts), len(m.dense))
	}
	res := m.dense[:len(texts)]
	m.dense = m.dense[len(texts):]
	return res, nil
}
