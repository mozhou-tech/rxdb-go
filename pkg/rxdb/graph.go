package rxdb

import (
	"context"
	"fmt"

	"github.com/mozy/rxdb-go/pkg/graph/cayley"
)

// initGraph 初始化图数据库
func (d *database) initGraph(ctx context.Context, opts *GraphOptions) error {
	if opts == nil || !opts.Enabled {
		return nil
	}

	// 设置默认后端
	backend := opts.Backend
	if backend == "" {
		backend = "bolt"
	}

	// 设置默认路径
	path := opts.Path
	if path == "" {
		path = d.store.Path() + "-graph"
	}

	// 创建 Cayley 客户端
	client, err := cayley.NewClient(cayley.Options{
		Backend: backend,
		Path:    path,
	})
	if err != nil {
		return fmt.Errorf("failed to create graph client: %w", err)
	}

	// 创建图数据库包装器
	graphDB := &graphDatabase{
		client: client,
	}

	d.graphClient = graphDB

	// 如果启用自动同步，创建桥接
	if opts.AutoSync {
		// 创建适配器以匹配 cayley.Database 接口
		dbAdapter := &databaseAdapter{db: d}
		bridge := cayley.NewBridge(dbAdapter, client)
		// 包装为 GraphBridge 接口
		graphBridge := &graphBridgeImpl{bridge: bridge}
		d.graphBridge = graphBridge

		// 启动自动同步
		if err := bridge.StartAutoSync(ctx); err != nil {
			return fmt.Errorf("failed to start graph auto sync: %w", err)
		}
	}

	return nil
}

// Graph 返回图数据库实例
func (d *database) Graph() GraphDatabase {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.graphClient
}

// GraphBridge 返回图数据库桥接实例
func (d *database) GraphBridge() GraphBridge {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.graphBridge
}

// graphDatabase 实现 GraphDatabase 接口
type graphDatabase struct {
	client *cayley.Client
}

func (g *graphDatabase) Link(ctx context.Context, from, relation, to string) error {
	return g.client.Link(ctx, from, relation, to)
}

func (g *graphDatabase) Unlink(ctx context.Context, from, relation, to string) error {
	return g.client.Unlink(ctx, from, relation, to)
}

func (g *graphDatabase) GetNeighbors(ctx context.Context, nodeID string, relation string) ([]string, error) {
	return g.client.GetNeighbors(ctx, nodeID, relation)
}

func (g *graphDatabase) FindPath(ctx context.Context, from, to string, maxDepth int, relations ...string) ([][]string, error) {
	return g.client.FindPath(ctx, from, to, maxDepth, relations...)
}

func (g *graphDatabase) Query() GraphQuery {
	return &graphQueryImpl{query: cayley.NewQuery(g.client)}
}

func (g *graphDatabase) Close() error {
	return g.client.Close()
}

// graphQueryImpl 实现 GraphQuery 接口
type graphQueryImpl struct {
	query *cayley.Query
}

func (q *graphQueryImpl) V(nodes ...string) *GraphQueryImpl {
	if q == nil {
		return nil
	}
	return &GraphQueryImpl{query: q.query.V(nodes...)}
}

func (q *graphQueryImpl) Out(predicates ...string) *GraphQueryImpl {
	if q == nil {
		return nil
	}
	return &GraphQueryImpl{query: q.query.Out(predicates...)}
}

func (q *graphQueryImpl) In(predicates ...string) *GraphQueryImpl {
	if q == nil {
		return nil
	}
	return &GraphQueryImpl{query: q.query.In(predicates...)}
}

func (q *graphQueryImpl) Both(predicates ...string) *GraphQueryImpl {
	if q == nil {
		return nil
	}
	return &GraphQueryImpl{query: q.query.Both(predicates...)}
}

func (q *graphQueryImpl) Has(predicate, object string) *GraphQueryImpl {
	if q == nil {
		return nil
	}
	return &GraphQueryImpl{query: q.query.Has(predicate, object)}
}

func (q *graphQueryImpl) Limit(n int) *GraphQueryImpl {
	if q == nil {
		return nil
	}
	return &GraphQueryImpl{query: q.query.Limit(n)}
}

func (q *graphQueryImpl) All(ctx context.Context) ([]GraphQueryResult, error) {
	if q == nil || q.query == nil {
		return nil, fmt.Errorf("invalid query")
	}
	results, err := q.query.All(ctx)
	if err != nil {
		return nil, err
	}

	graphResults := make([]GraphQueryResult, len(results))
	for i, r := range results {
		graphResults[i] = GraphQueryResult{
			Subject:   r.Subject,
			Predicate: r.Predicate,
			Object:    r.Object,
			Label:     r.Label,
		}
	}
	return graphResults, nil
}

func (q *graphQueryImpl) AllNodes(ctx context.Context) ([]string, error) {
	if q == nil || q.query == nil {
		return nil, fmt.Errorf("invalid query")
	}
	return q.query.AllNodes(ctx)
}

func (q *graphQueryImpl) Count(ctx context.Context) (int64, error) {
	if q == nil || q.query == nil {
		return 0, fmt.Errorf("invalid query")
	}
	return q.query.Count(ctx)
}

func (q *graphQueryImpl) First(ctx context.Context) (*GraphQueryResult, error) {
	if q == nil || q.query == nil {
		return nil, fmt.Errorf("invalid query")
	}
	result, err := q.query.First(ctx)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, nil
	}
	return &GraphQueryResult{
		Subject:   result.Subject,
		Predicate: result.Predicate,
		Object:    result.Object,
		Label:     result.Label,
	}, nil
}

// GraphQueryImpl 实现 GraphQuery 接口（公开类型）
func (q *GraphQueryImpl) V(nodes ...string) *GraphQueryImpl {
	if q == nil {
		return nil
	}
	cayleyQuery, ok := q.query.(*cayley.Query)
	if !ok || cayleyQuery == nil {
		return nil
	}
	return &GraphQueryImpl{query: cayleyQuery.V(nodes...)}
}

func (q *GraphQueryImpl) Out(predicates ...string) *GraphQueryImpl {
	if q == nil {
		return nil
	}
	cayleyQuery, ok := q.query.(*cayley.Query)
	if !ok || cayleyQuery == nil {
		return nil
	}
	return &GraphQueryImpl{query: cayleyQuery.Out(predicates...)}
}

func (q *GraphQueryImpl) In(predicates ...string) *GraphQueryImpl {
	if q == nil {
		return nil
	}
	cayleyQuery, ok := q.query.(*cayley.Query)
	if !ok || cayleyQuery == nil {
		return nil
	}
	return &GraphQueryImpl{query: cayleyQuery.In(predicates...)}
}

func (q *GraphQueryImpl) Both(predicates ...string) *GraphQueryImpl {
	if q == nil {
		return nil
	}
	cayleyQuery, ok := q.query.(*cayley.Query)
	if !ok || cayleyQuery == nil {
		return nil
	}
	return &GraphQueryImpl{query: cayleyQuery.Both(predicates...)}
}

func (q *GraphQueryImpl) Has(predicate, object string) *GraphQueryImpl {
	if q == nil {
		return nil
	}
	cayleyQuery, ok := q.query.(*cayley.Query)
	if !ok || cayleyQuery == nil {
		return nil
	}
	return &GraphQueryImpl{query: cayleyQuery.Has(predicate, object)}
}

func (q *GraphQueryImpl) Limit(n int) *GraphQueryImpl {
	if q == nil {
		return nil
	}
	cayleyQuery, ok := q.query.(*cayley.Query)
	if !ok || cayleyQuery == nil {
		return nil
	}
	return &GraphQueryImpl{query: cayleyQuery.Limit(n)}
}

func (q *GraphQueryImpl) All(ctx context.Context) ([]GraphQueryResult, error) {
	if q == nil {
		return nil, fmt.Errorf("invalid query")
	}
	cayleyQuery, ok := q.query.(*cayley.Query)
	if !ok || cayleyQuery == nil {
		return nil, fmt.Errorf("invalid query")
	}
	results, err := cayleyQuery.All(ctx)
	if err != nil {
		return nil, err
	}

	graphResults := make([]GraphQueryResult, len(results))
	for i, r := range results {
		graphResults[i] = GraphQueryResult{
			Subject:   r.Subject,
			Predicate: r.Predicate,
			Object:    r.Object,
			Label:     r.Label,
		}
	}
	return graphResults, nil
}

func (q *GraphQueryImpl) AllNodes(ctx context.Context) ([]string, error) {
	if q == nil {
		return nil, fmt.Errorf("invalid query")
	}
	cayleyQuery, ok := q.query.(*cayley.Query)
	if !ok || cayleyQuery == nil {
		return nil, fmt.Errorf("invalid query")
	}
	return cayleyQuery.AllNodes(ctx)
}

func (q *GraphQueryImpl) Count(ctx context.Context) (int64, error) {
	if q == nil {
		return 0, fmt.Errorf("invalid query")
	}
	cayleyQuery, ok := q.query.(*cayley.Query)
	if !ok || cayleyQuery == nil {
		return 0, fmt.Errorf("invalid query")
	}
	return cayleyQuery.Count(ctx)
}

func (q *GraphQueryImpl) First(ctx context.Context) (*GraphQueryResult, error) {
	if q == nil {
		return nil, fmt.Errorf("invalid query")
	}
	cayleyQuery, ok := q.query.(*cayley.Query)
	if !ok || cayleyQuery == nil {
		return nil, fmt.Errorf("invalid query")
	}
	result, err := cayleyQuery.First(ctx)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, nil
	}
	return &GraphQueryResult{
		Subject:   result.Subject,
		Predicate: result.Predicate,
		Object:    result.Object,
		Label:     result.Label,
	}, nil
}

// databaseAdapter 适配 database 以匹配 cayley.Database 接口
type databaseAdapter struct {
	db *database
}

func (da *databaseAdapter) Changes() <-chan cayley.ChangeEvent {
	ch := make(chan cayley.ChangeEvent, 100)
	go func() {
		defer close(ch)
		for event := range da.db.Changes() {
			ch <- cayley.ChangeEvent{
				Collection: event.Collection,
				ID:         event.ID,
				Op:         string(event.Op),
				Doc:        event.Doc,
				Old:        event.Old,
				Meta:       event.Meta,
			}
		}
	}()
	return ch
}

// graphBridgeImpl 包装 cayley.Bridge 以实现 GraphBridge 接口
type graphBridgeImpl struct {
	bridge *cayley.Bridge
}

func (gb *graphBridgeImpl) Enable() {
	if gb.bridge != nil {
		gb.bridge.Enable()
	}
}

func (gb *graphBridgeImpl) Disable() {
	if gb.bridge != nil {
		gb.bridge.Disable()
	}
}

func (gb *graphBridgeImpl) IsEnabled() bool {
	if gb.bridge != nil {
		return gb.bridge.IsEnabled()
	}
	return false
}

func (gb *graphBridgeImpl) AddRelationMapping(mapping *GraphRelationMapping) {
	if gb.bridge != nil {
		gb.bridge.AddRelationMapping(&cayley.RelationMapping{
			Collection:  mapping.Collection,
			Field:       mapping.Field,
			Relation:    mapping.Relation,
			TargetField: mapping.TargetField,
			AutoLink:    mapping.AutoLink,
		})
	}
}

func (gb *graphBridgeImpl) RemoveRelationMapping(collection, field string) {
	if gb.bridge != nil {
		gb.bridge.RemoveRelationMapping(collection, field)
	}
}

func (gb *graphBridgeImpl) StartAutoSync(ctx context.Context) error {
	if gb.bridge != nil {
		return gb.bridge.StartAutoSync(ctx)
	}
	return nil
}
