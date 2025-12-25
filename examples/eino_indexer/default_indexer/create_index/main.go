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
	"fmt"

	"github.com/mozhou-tech/rxdb-go/pkg/rxdb"
)

func main() {
	createIndex()
}

func createIndex() {
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

	// In RxDB, indexes are defined in the schema when creating a collection.
	// We can also create them explicitly using CreateIndex.

	col, err := db.Collection(ctx, "docs", rxdb.Schema{
		PrimaryKey: "id",
		Indexes: []rxdb.Index{
			{Fields: []string{"extra_field_number"}},
		},
	})
	if err != nil {
		panic(err)
	}

	fmt.Println("Collection created with indexes:", col.Name())
}
