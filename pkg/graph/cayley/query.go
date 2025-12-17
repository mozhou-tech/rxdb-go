package cayley

import (
	"context"
	"fmt"

	"github.com/sirupsen/logrus"
)

// QueryResult 表示查询结果
type QueryResult struct {
	Subject   string
	Predicate string
	Object    string
	Label     string
}

// QueryStep 表示查询步骤
type QueryStep struct {
	direction  string // "out", "in", "both"
	predicates []string
}

// Query 封装图查询操作
type Query struct {
	client *Client
	// 查询条件
	fromNodes []string
	steps     []QueryStep // 查询步骤链
	limit     int
}

// NewQuery 创建新的查询对象
func NewQuery(client *Client) *Query {
	if client == nil {
		return nil
	}
	return &Query{
		client:    client,
		fromNodes: make([]string, 0),
		steps:     make([]QueryStep, 0),
		limit:     -1, // -1 表示不限制
	}
}

// V 从指定节点开始查询
func (q *Query) V(nodes ...string) *Query {
	if q == nil {
		return q
	}
	q.fromNodes = append(q.fromNodes, nodes...)
	logrus.WithFields(logrus.Fields{
		"nodes":     nodes,
		"fromNodes": q.fromNodes,
	}).Debug("[Graph Query] V")
	return q
}

// Out 沿着指定谓词向外查询
func (q *Query) Out(predicates ...string) *Query {
	if q == nil {
		return q
	}
	q.steps = append(q.steps, QueryStep{
		direction:  "out",
		predicates: predicates,
	})
	logrus.WithFields(logrus.Fields{
		"predicates": predicates,
		"stepsCount": len(q.steps),
	}).Debug("[Graph Query] Out")
	return q
}

// In 沿着指定谓词向内查询
func (q *Query) In(predicates ...string) *Query {
	if q == nil {
		return q
	}
	q.steps = append(q.steps, QueryStep{
		direction:  "in",
		predicates: predicates,
	})
	return q
}

// Both 双向查询（同时包含 In 和 Out）
func (q *Query) Both(predicates ...string) *Query {
	if q == nil {
		return q
	}
	q.steps = append(q.steps, QueryStep{
		direction:  "both",
		predicates: predicates,
	})
	return q
}

// Has 过滤具有指定谓词和对象的节点
func (q *Query) Has(predicate, object string) *Query {
	// 简化实现，实际应该支持更复杂的过滤
	return q
}

// Limit 限制返回结果数量
func (q *Query) Limit(n int) *Query {
	if q == nil {
		return q
	}
	q.limit = n
	return q
}

// executeStep 执行单个查询步骤，返回结果节点
func (q *Query) executeStep(ctx context.Context, fromNodes []string, step QueryStep) ([]string, []QueryResult, error) {
	var results []QueryResult
	var nextNodes []string
	nextNodeMap := make(map[string]bool)

	if q.client.closed {
		return nil, nil, fmt.Errorf("graph database is closed")
	}

	// 遍历所有起始节点
	for _, fromNode := range fromNodes {
		switch step.direction {
		case "out":
			// 查询出边
			quads, err := q.client.getQuadsBySubject(ctx, fromNode)
			if err != nil {
				return nil, nil, err
			}
			if quads == nil {
				continue
			}
			for pred, objects := range quads {
				// 如果指定了谓词，只查询匹配的
				if len(step.predicates) > 0 {
					matched := false
					for _, p := range step.predicates {
						if p == pred {
							matched = true
							break
						}
					}
					if !matched {
						continue
					}
				}

				for obj := range objects {
					results = append(results, QueryResult{
						Subject:   fromNode,
						Predicate: pred,
						Object:    obj,
					})
					if !nextNodeMap[obj] {
						nextNodes = append(nextNodes, obj)
						nextNodeMap[obj] = true
					}
					if q.limit > 0 && len(results) >= q.limit {
						return nextNodes, results, nil
					}
				}
			}

		case "in":
			// 查询入边（需要反向遍历）
			inResults, err := q.client.getQuadsByObject(ctx, fromNode)
			if err != nil {
				return nil, nil, err
			}
			for _, r := range inResults {
				// 如果指定了谓词，只查询匹配的
				if len(step.predicates) > 0 {
					matched := false
					for _, p := range step.predicates {
						if p == r.Predicate {
							matched = true
							break
						}
					}
					if !matched {
						continue
					}
				}

				results = append(results, r)
				if !nextNodeMap[r.Subject] {
					nextNodes = append(nextNodes, r.Subject)
					nextNodeMap[r.Subject] = true
				}
				if q.limit > 0 && len(results) >= q.limit {
					return nextNodes, results, nil
				}
			}

		case "both":
			// 双向查询
			// 先查询出边
			quads, err := q.client.getQuadsBySubject(ctx, fromNode)
			if err != nil {
				return nil, nil, err
			}
			for pred, objects := range quads {
				if len(step.predicates) > 0 {
					matched := false
					for _, p := range step.predicates {
						if p == pred {
							matched = true
							break
						}
					}
					if !matched {
						continue
					}
				}

				for obj := range objects {
					results = append(results, QueryResult{
						Subject:   fromNode,
						Predicate: pred,
						Object:    obj,
					})
					if !nextNodeMap[obj] {
						nextNodes = append(nextNodes, obj)
						nextNodeMap[obj] = true
					}
					if q.limit > 0 && len(results) >= q.limit {
						return nextNodes, results, nil
					}
				}
			}

			// 再查询入边
			inResults, err := q.client.getQuadsByObject(ctx, fromNode)
			if err != nil {
				return nil, nil, err
			}
			for _, r := range inResults {
				if len(step.predicates) > 0 {
					matched := false
					for _, p := range step.predicates {
						if p == r.Predicate {
							matched = true
							break
						}
					}
					if !matched {
						continue
					}
				}

				results = append(results, r)
				if !nextNodeMap[r.Subject] {
					nextNodes = append(nextNodes, r.Subject)
					nextNodeMap[r.Subject] = true
				}
				if q.limit > 0 && len(results) >= q.limit {
					return nextNodes, results, nil
				}
			}
		}
	}

	return nextNodes, results, nil
}

// All 执行查询并返回所有结果
func (q *Query) All(ctx context.Context) ([]QueryResult, error) {
	if q == nil || q.client == nil {
		return nil, fmt.Errorf("invalid query")
	}

	if len(q.fromNodes) == 0 {
		logrus.Debug("[Graph Query] All: no fromNodes, returning empty results")
		return []QueryResult{}, nil
	}

	// 如果没有步骤，返回空结果
	if len(q.steps) == 0 {
		logrus.Debug("[Graph Query] All: no steps, returning empty results")
		return []QueryResult{}, nil
	}

	logrus.WithFields(logrus.Fields{
		"fromNodesCount": len(q.fromNodes),
		"stepsCount":     len(q.steps),
	}).Debug("[Graph Query] All: executing query")
	currentNodes := q.fromNodes
	var allResults []QueryResult

	// 按步骤执行查询
	for i, step := range q.steps {
		logrus.WithFields(logrus.Fields{
			"step":         i + 1,
			"totalSteps":   len(q.steps),
			"direction":    step.direction,
			"predicates":   step.predicates,
			"currentNodes": currentNodes,
		}).Debug("[Graph Query] All: executing step")
		nextNodes, stepResults, err := q.executeStep(ctx, currentNodes, step)
		if err != nil {
			logrus.WithFields(logrus.Fields{
				"step":  i + 1,
				"error": err,
			}).Error("[Graph Query] All: step failed")
			return nil, err
		}

		logrus.WithFields(logrus.Fields{
			"step":         i + 1,
			"nextNodes":    nextNodes,
			"resultsCount": len(stepResults),
		}).Debug("[Graph Query] All: step completed")

		// 如果是最后一步，收集所有结果
		if i == len(q.steps)-1 {
			allResults = append(allResults, stepResults...)
		} else {
			// 中间步骤，更新当前节点为下一步的起始节点
			if len(nextNodes) == 0 {
				// 如果没有下一个节点，提前结束
				logrus.WithField("step", i+1).Debug("[Graph Query] All: no nextNodes, breaking")
				break
			}
			currentNodes = nextNodes
			// 继续下一步，不收集中间步骤的结果
			continue
		}

		// 检查限制（只在最后一步检查）
		if q.limit > 0 && len(allResults) >= q.limit {
			logrus.WithFields(logrus.Fields{
				"limit":   q.limit,
				"results": q.limit,
			}).Debug("[Graph Query] All: limit reached")
			return allResults[:q.limit], nil
		}
	}

	logrus.WithField("resultsCount", len(allResults)).Debug("[Graph Query] All: completed")
	return allResults, nil
}

// AllNodes 返回所有节点值（字符串数组）
func (q *Query) AllNodes(ctx context.Context) ([]string, error) {
	results, err := q.All(ctx)
	if err != nil {
		return nil, err
	}

	nodeMap := make(map[string]bool)
	for _, r := range results {
		if r.Subject != "" {
			nodeMap[r.Subject] = true
		}
		if r.Object != "" {
			nodeMap[r.Object] = true
		}
	}

	nodes := make([]string, 0, len(nodeMap))
	for node := range nodeMap {
		nodes = append(nodes, node)
	}

	return nodes, nil
}

// Count 返回结果数量
func (q *Query) Count(ctx context.Context) (int64, error) {
	results, err := q.All(ctx)
	if err != nil {
		return 0, err
	}
	return int64(len(results)), nil
}

// First 返回第一个结果
func (q *Query) First(ctx context.Context) (*QueryResult, error) {
	results, err := q.Limit(1).All(ctx)
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, nil
	}
	return &results[0], nil
}

// GetNeighbors 获取节点的所有邻居节点
func (c *Client) GetNeighbors(ctx context.Context, nodeID string, relation string) ([]string, error) {
	logrus.WithFields(logrus.Fields{
		"nodeID":   nodeID,
		"relation": relation,
	}).Debug("[Graph] GetNeighbors")
	q := NewQuery(c).V(nodeID)
	var results []QueryResult
	var err error

	if relation == "" {
		// 获取所有邻居（双向）
		results, err = q.Both().All(ctx)
	} else {
		// 获取指定关系的邻居（只返回出边，不包括入边）
		results, err = q.Out(relation).All(ctx)
	}

	if err != nil {
		logrus.WithFields(logrus.Fields{
			"nodeID":   nodeID,
			"relation": relation,
			"error":    err,
		}).Error("[Graph] GetNeighbors failed")
		return nil, err
	}

	// 只返回 Object 节点（邻居节点），不包含 Subject（当前节点）
	nodeMap := make(map[string]bool)
	for _, r := range results {
		// 对于 Out 查询，Object 是邻居节点
		// 对于 In 查询，Subject 是邻居节点
		// 对于 Both 查询，需要同时考虑
		if relation == "" {
			// 双向查询：如果 Object 不是当前节点，则 Object 是邻居；如果 Subject 不是当前节点，则 Subject 是邻居
			if r.Object != nodeID {
				nodeMap[r.Object] = true
			}
			if r.Subject != nodeID {
				nodeMap[r.Subject] = true
			}
		} else {
			// 单向查询（Out）：只返回 Object（邻居节点）
			if r.Object != nodeID {
				nodeMap[r.Object] = true
			}
		}
	}

	nodes := make([]string, 0, len(nodeMap))
	for node := range nodeMap {
		nodes = append(nodes, node)
	}

	logrus.WithFields(logrus.Fields{
		"nodeID":         nodeID,
		"relation":       relation,
		"neighborsCount": len(nodes),
		"neighbors":      nodes,
	}).Debug("[Graph] GetNeighbors completed")
	return nodes, nil
}

// FindPath 查找两个节点之间的路径
func (c *Client) FindPath(ctx context.Context, from, to string, maxDepth int, relations ...string) ([][]string, error) {
	logrus.WithFields(logrus.Fields{
		"from":      from,
		"to":        to,
		"maxDepth":  maxDepth,
		"relations": relations,
	}).Debug("[Graph] FindPath")
	if maxDepth <= 0 {
		maxDepth = 10 // 默认最大深度
	}

	var paths [][]string
	visited := make(map[string]bool)

	var dfs func(current string, path []string, depth int) bool
	dfs = func(current string, path []string, depth int) bool {
		if depth > maxDepth {
			return false
		}

		if current == to {
			paths = append(paths, append(path, to))
			return true
		}

		visited[current] = true
		defer func() {
			delete(visited, current)
		}()

		// 获取邻居节点
		var neighbors []string
		var err error
		if len(relations) > 0 {
			// 只查询指定关系
			for _, rel := range relations {
				neighs, e := c.GetNeighbors(ctx, current, rel)
				if e != nil {
					err = e
					break
				}
				neighbors = append(neighbors, neighs...)
			}
		} else {
			// 查询所有邻居
			neighbors, err = c.GetNeighbors(ctx, current, "")
		}

		if err != nil {
			return false
		}

		// 去重
		neighborMap := make(map[string]bool)
		for _, n := range neighbors {
			neighborMap[n] = true
		}

		for neighbor := range neighborMap {
			if visited[neighbor] {
				continue
			}
			newPath := append(path, current)
			dfs(neighbor, newPath, depth+1)
		}

		return false
	}

	dfs(from, []string{}, 0)
	logrus.WithFields(logrus.Fields{
		"from":       from,
		"to":         to,
		"pathsCount": len(paths),
	}).Debug("[Graph] FindPath completed")
	return paths, nil
}
