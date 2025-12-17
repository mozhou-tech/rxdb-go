package rxdb

import (
	"context"
	"sort"
)

// HybridSearchResult 混合搜索结果。
// 结合全文搜索和向量搜索的结果，提供综合的搜索分数。
type HybridSearchResult struct {
	// Document 匹配的文档。
	Document Document
	// FulltextScore 全文搜索分数（0-1）。
	FulltextScore float64
	// VectorScore 向量搜索相似度分数（0-1）。
	VectorScore float64
	// VectorDistance 向量搜索距离。
	VectorDistance float64
	// HybridScore 混合搜索综合分数。
	// 计算公式：FulltextScore * fulltextWeight + VectorScore * vectorWeight
	HybridScore float64
}

// HybridSearchOptions 混合搜索选项。
type HybridSearchOptions struct {
	// Limit 返回结果数量限制。
	Limit int
	// FulltextWeight 全文搜索权重（0-1）。
	// 与 VectorWeight 一起决定混合分数的计算方式。
	FulltextWeight float64
	// VectorWeight 向量搜索权重（0-1）。
	// 与 FulltextWeight 一起决定混合分数的计算方式。
	// 建议 FulltextWeight + VectorWeight = 1.0，但不强制要求。
	VectorWeight float64
}

// PerformHybridSearch 执行混合搜索。
// 结合全文搜索和向量搜索的结果，根据权重计算综合分数。
// fts 是全文搜索实例，vs 是向量搜索实例，query 是查询文本，queryVector 是查询向量。
func PerformHybridSearch(
	ctx context.Context,
	fts *FulltextSearch,
	vs *VectorSearch,
	query string,
	queryVector Vector,
	options HybridSearchOptions,
) ([]HybridSearchResult, error) {
	// 执行全文搜索
	fulltextResults, err := fts.FindWithScores(ctx, query, FulltextSearchOptions{
		Limit: options.Limit * 2, // 获取更多结果以便合并
	})
	if err != nil {
		fulltextResults = []FulltextSearchResult{}
	}

	// 执行向量搜索
	vectorResults, err := vs.Search(ctx, queryVector, VectorSearchOptions{
		Limit: options.Limit * 2,
	})
	if err != nil {
		vectorResults = []VectorSearchResult{}
	}

	// 合并结果
	resultMap := make(map[string]*HybridSearchResult)

	// 添加全文搜索结果
	for _, r := range fulltextResults {
		docID := r.Document.ID()
		if existing, ok := resultMap[docID]; ok {
			// 如果已存在，更新全文搜索分数（取较高值）
			if r.Score > existing.FulltextScore {
				existing.FulltextScore = r.Score
				// 重新计算混合分数
				existing.HybridScore = existing.FulltextScore*options.FulltextWeight + existing.VectorScore*options.VectorWeight
			}
		} else {
			resultMap[docID] = &HybridSearchResult{
				Document:      r.Document,
				FulltextScore: r.Score,
				VectorScore:   0,
				HybridScore:   r.Score * options.FulltextWeight,
			}
		}
	}

	// 添加向量搜索结果
	for _, r := range vectorResults {
		docID := r.Document.ID()
		if existing, ok := resultMap[docID]; ok {
			// 如果已存在，更新向量搜索分数
			existing.VectorScore = r.Score
			existing.VectorDistance = r.Distance
			// 重新计算混合分数
			existing.HybridScore = existing.FulltextScore*options.FulltextWeight + r.Score*options.VectorWeight
		} else {
			resultMap[docID] = &HybridSearchResult{
				Document:       r.Document,
				FulltextScore:  0,
				VectorScore:    r.Score,
				VectorDistance: r.Distance,
				HybridScore:    r.Score * options.VectorWeight,
			}
		}
	}

	// 转换为切片并排序
	results := make([]HybridSearchResult, 0, len(resultMap))
	for _, r := range resultMap {
		results = append(results, *r)
	}

	// 按混合分数降序排序
	sort.Slice(results, func(i, j int) bool {
		return results[i].HybridScore > results[j].HybridScore
	})

	// 限制结果数量
	if options.Limit > 0 && len(results) > options.Limit {
		results = results[:options.Limit]
	}

	return results, nil
}
