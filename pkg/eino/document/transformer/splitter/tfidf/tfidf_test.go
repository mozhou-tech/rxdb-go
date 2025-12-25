/*
 * Copyright 2024 CloudWeGo Authors
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

package tfidf

import (
	"context"
	"testing"

	"github.com/cloudwego/eino/schema"
	"github.com/smartystreets/goconvey/convey"
)

func TestTFIDFSplitter(t *testing.T) {
	convey.Convey("Test TFIDFSplitter", t, func() {
		ctx := context.Background()
		config := &Config{
			SimilarityThreshold: 0.1,
			MaxChunkSize:        2,
			MinChunkSize:        1,
		}

		splitter, err := NewTFIDFSplitter(ctx, config)
		convey.So(err, convey.ShouldBeNil)

		text := "This is the first sentence. It is about cats. This is the second sentence. It is about dogs. The third part is different. It discusses airplanes and rockets."
		docs := []*schema.Document{
			{
				ID:      "doc1",
				Content: text,
			},
		}

		splitDocs, err := splitter.Transform(ctx, docs)
		convey.So(err, convey.ShouldBeNil)

		// Since we set MaxChunkSize to 2, we expect at least 3 chunks (6 sentences total)
		convey.So(len(splitDocs), convey.ShouldBeGreaterThanOrEqualTo, 3)

		for _, d := range splitDocs {
			convey.So(d.Content, convey.ShouldNotBeEmpty)
			convey.So(d.ID, convey.ShouldEqual, "doc1") // Default ID generator keeps original ID
		}
	})
}
