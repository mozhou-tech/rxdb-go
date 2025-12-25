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
	"math"
	"strings"

	"github.com/cloudwego/eino/components/document"
	"github.com/cloudwego/eino/schema"
	"github.com/mozhou-tech/rxdb-go/pkg/sego"
	"github.com/rioloc/tfidf-go"
	"github.com/rioloc/tfidf-go/token"
)

var (
	globalSegoErr error
)

// SetSegoDict allows setting the dictionary data for sego tokenizer.
// Deprecated: now uses pkg/sego embedded dictionary.
func SetSegoDict(dict []byte) {
}

// IDGenerator generates new IDs for split chunks
type IDGenerator func(ctx context.Context, originalID string, splitIndex int) string

// defaultIDGenerator keeps the original ID
func defaultIDGenerator(ctx context.Context, originalID string, _ int) string {
	return originalID
}

type Config struct {
	// SimilarityThreshold is the minimum cosine similarity between sentences to keep them in the same chunk.
	// If similarity is below this, a new chunk is started.
	// Default is 0.2.
	SimilarityThreshold float64
	// MaxChunkSize is the maximum number of sentences in a chunk.
	// Default is 10.
	MaxChunkSize int
	// MinChunkSize is the minimum number of sentences in a chunk.
	// Default is 1.
	MinChunkSize int
	// UseSego specifies whether to use sego for Chinese tokenization.
	UseSego bool
	// SegoDictPath is the path to the sego dictionary file.
	// If empty and UseSego is true, it will try to use globalSegoDict or a default path.
	SegoDictPath string
	// IDGenerator is an optional function to generate new IDs for split chunks.
	// If nil, the original document ID will be used for all splits.
	IDGenerator IDGenerator
}

func NewTFIDFSplitter(ctx context.Context, config *Config) (document.Transformer, error) {
	if config == nil {
		config = &Config{}
	}
	if config.MaxChunkSize <= 0 {
		config.MaxChunkSize = 10
	}
	if config.MinChunkSize <= 0 {
		config.MinChunkSize = 1
	}
	if config.SimilarityThreshold <= 0 {
		config.SimilarityThreshold = 0.2
	}
	idGenerator := config.IDGenerator
	if idGenerator == nil {
		idGenerator = defaultIDGenerator
	}
	return &tfidfSplitter{
		config:      config,
		idGenerator: idGenerator,
	}, nil
}

type tfidfSplitter struct {
	config      *Config
	idGenerator IDGenerator
}

func (s *tfidfSplitter) Transform(ctx context.Context, docs []*schema.Document, opts ...document.TransformerOption) ([]*schema.Document, error) {
	var ret []*schema.Document
	for _, doc := range docs {
		chunks := s.splitText(doc.Content)
		for i, chunk := range chunks {
			nDoc := &schema.Document{
				ID:       s.idGenerator(ctx, doc.ID, i),
				Content:  chunk,
				MetaData: deepCopyAnyMap(doc.MetaData),
			}
			ret = append(ret, nDoc)
		}
	}
	return ret, nil
}

func (s *tfidfSplitter) GetType() string {
	return "TFIDFSplitter"
}

func (s *tfidfSplitter) splitText(text string) []string {
	// 1. Split into sentences
	sentences := splitIntoSentences(text)
	if len(sentences) == 0 {
		return nil
	}
	if len(sentences) == 1 {
		return sentences
	}

	// 2. Calculate TF-IDF for each sentence
	var vocabulary []string
	var tokens [][]string
	var err error

	if s.config.UseSego {
		vocabulary, tokens, err = s.segoTokenize(sentences)
	} else {
		tokenizer := token.NewTokenizer()
		vocabulary, tokens, err = tokenizer.Tokenize(sentences)
	}

	if err != nil {
		// Fallback to simple split if tokenization fails
		return sentences
	}

	tfMatrix := tfidf.Tf(vocabulary, tokens)
	idfVector := tfidf.Idf(vocabulary, tokens, true) // with smoothing

	vectorizer := tfidf.NewTfIdfVectorizer()
	tfidfMatrix, err := vectorizer.TfIdf(tfMatrix, idfVector)
	if err != nil {
		return sentences
	}

	// 3. Group sentences into chunks based on similarity
	var chunks []string
	var currentChunk []string

	currentChunk = append(currentChunk, sentences[0])

	for i := 1; i < len(sentences); i++ {
		sim := cosineSimilarity(tfidfMatrix[i-1], tfidfMatrix[i])

		shouldSplit := sim < s.config.SimilarityThreshold || len(currentChunk) >= s.config.MaxChunkSize

		if shouldSplit && len(currentChunk) >= s.config.MinChunkSize {
			chunks = append(chunks, strings.Join(currentChunk, " "))
			currentChunk = []string{sentences[i]}
		} else {
			currentChunk = append(currentChunk, sentences[i])
		}
	}

	if len(currentChunk) > 0 {
		chunks = append(chunks, strings.Join(currentChunk, " "))
	}

	return chunks
}

func (s *tfidfSplitter) initSego() error {
	return nil
}

func (s *tfidfSplitter) segoTokenize(sentences []string) ([]string, [][]string, error) {
	segmenter, err := sego.GetSegmenter()
	if err != nil {
		return nil, nil, err
	}

	var vocabulary []string
	vocabMap := make(map[string]bool)
	var tokens [][]string

	for _, sent := range sentences {
		segments := segmenter.Segment([]byte(sent))
		var sentTokens []string
		for _, seg := range segments {
			word := sent[seg.Start():seg.End()]
			word = strings.TrimSpace(word)
			if word == "" {
				continue
			}
			sentTokens = append(sentTokens, word)
			if !vocabMap[word] {
				vocabMap[word] = true
				vocabulary = append(vocabulary, word)
			}
		}
		tokens = append(tokens, sentTokens)
	}
	return vocabulary, tokens, nil
}

func splitIntoSentences(text string) []string {
	// Simple sentence splitter using common delimiters
	delimiters := []string{". ", "! ", "? ", "。\n", "！\n", "？\n", "\n\n", "。", "！", "？"}

	// Temporarily replace delimiters with a unique marker
	marker := "|||SENTENCE_BOUNDARY|||"
	temp := text
	for _, d := range delimiters {
		temp = strings.ReplaceAll(temp, d, d+marker)
	}

	rawSentences := strings.Split(temp, marker)
	var sentences []string
	for _, s := range rawSentences {
		trimmed := strings.TrimSpace(s)
		if trimmed != "" {
			sentences = append(sentences, trimmed)
		}
	}
	return sentences
}

func cosineSimilarity(v1, v2 []float64) float64 {
	if len(v1) != len(v2) || len(v1) == 0 {
		return 0
	}

	dotProduct := 0.0
	mag1 := 0.0
	mag2 := 0.0
	for i := 0; i < len(v1); i++ {
		dotProduct += v1[i] * v2[i]
		mag1 += v1[i] * v1[i]
		mag2 += v2[i] * v2[i]
	}

	mag1 = math.Sqrt(mag1)
	mag2 = math.Sqrt(mag2)

	if mag1 == 0 || mag2 == 0 {
		return 0
	}

	return dotProduct / (mag1 * mag2)
}

func deepCopyAnyMap(anyMap map[string]any) map[string]any {
	if anyMap == nil {
		return nil
	}
	ret := make(map[string]any)
	for k, v := range anyMap {
		ret[k] = v
	}
	return ret
}
