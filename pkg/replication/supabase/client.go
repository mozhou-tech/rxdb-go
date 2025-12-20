package supabase

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/mozhou-tech/rxdb-go/pkg/rxdb"
)

// ReplicationState 同步状态。
type ReplicationState string

const (
	StateIdle    ReplicationState = "idle"
	StatePulling ReplicationState = "pulling"
	StatePushing ReplicationState = "pushing"
	StateError   ReplicationState = "error"
	StateStopped ReplicationState = "stopped"
)

// ConflictHandler 冲突处理函数类型。
type ConflictHandler func(local, remote map[string]any) map[string]any

// ReplicationOptions 同步配置选项。
type ReplicationOptions struct {
	// SupabaseURL Supabase 项目 URL
	SupabaseURL string
	// SupabaseKey Supabase API Key (anon 或 service_role)
	SupabaseKey string
	// Table Supabase 表名
	Table string
	// PrimaryKey 主键字段名
	PrimaryKey string
	// UpdatedAtField 更新时间字段名（用于增量同步）
	UpdatedAtField string
	// PullInterval 拉取间隔
	PullInterval time.Duration
	// PushOnChange 是否在本地变更时立即推送
	PushOnChange bool
	// ConflictHandler 冲突处理函数
	ConflictHandler ConflictHandler
	// HTTPClient 自定义 HTTP 客户端
	HTTPClient *http.Client
}

// Replication 同步客户端。
type Replication struct {
	opts       ReplicationOptions
	collection rxdb.Collection
	state      ReplicationState
	lastPull   time.Time
	mu         sync.RWMutex
	stopChan   chan struct{}
	errChan    chan error
	httpClient *http.Client
}

// NewReplication 创建新的同步实例。
func NewReplication(collection rxdb.Collection, opts ReplicationOptions) (*Replication, error) {
	if opts.SupabaseURL == "" {
		return nil, fmt.Errorf("supabase URL is required")
	}
	if opts.SupabaseKey == "" {
		return nil, fmt.Errorf("supabase API key is required")
	}
	if opts.Table == "" {
		return nil, fmt.Errorf("table name is required")
	}
	if opts.PrimaryKey == "" {
		opts.PrimaryKey = "id"
	}
	if opts.UpdatedAtField == "" {
		opts.UpdatedAtField = "updated_at"
	}
	if opts.PullInterval == 0 {
		opts.PullInterval = 10 * time.Second
	}
	if opts.ConflictHandler == nil {
		opts.ConflictHandler = defaultConflictHandler
	}

	httpClient := opts.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}

	return &Replication{
		opts:       opts,
		collection: collection,
		state:      StateIdle,
		stopChan:   make(chan struct{}),
		errChan:    make(chan error, 10),
		httpClient: httpClient,
	}, nil
}

// defaultConflictHandler 默认冲突处理：远程优先。
func defaultConflictHandler(local, remote map[string]any) map[string]any {
	return remote
}

// Start 启动同步。
func (r *Replication) Start(ctx context.Context) error {
	r.mu.Lock()
	if r.state != StateIdle && r.state != StateStopped {
		r.mu.Unlock()
		return fmt.Errorf("replication already running")
	}
	r.state = StateIdle
	r.stopChan = make(chan struct{})
	r.mu.Unlock()

	// 启动拉取循环
	go r.pullLoop(ctx)

	// 如果配置了推送，监听本地变更
	if r.opts.PushOnChange {
		go r.pushLoop(ctx)
	}

	return nil
}

// Stop 停止同步。
func (r *Replication) Stop() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state == StateStopped {
		return
	}
	r.state = StateStopped
	close(r.stopChan)
}

// State 返回当前同步状态。
func (r *Replication) State() ReplicationState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.state
}

// Errors 返回错误通道。
func (r *Replication) Errors() <-chan error {
	return r.errChan
}

// pullLoop 定期从 Supabase 拉取数据。
func (r *Replication) pullLoop(ctx context.Context) {
	ticker := time.NewTicker(r.opts.PullInterval)
	defer ticker.Stop()

	// 立即执行一次拉取
	r.pull(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-r.stopChan:
			return
		case <-ticker.C:
			r.pull(ctx)
		}
	}
}

// pull 从 Supabase 拉取数据。
func (r *Replication) pull(ctx context.Context) {
	r.mu.Lock()
	r.state = StatePulling
	r.mu.Unlock()

	defer func() {
		r.mu.Lock()
		if r.state == StatePulling {
			r.state = StateIdle
		}
		r.mu.Unlock()
	}()

	// 构建请求 URL
	url := fmt.Sprintf("%s/rest/v1/%s", r.opts.SupabaseURL, r.opts.Table)

	// 如果有上次拉取时间，只拉取更新的数据
	if !r.lastPull.IsZero() {
		url += fmt.Sprintf("?%s=gte.%s", r.opts.UpdatedAtField, r.lastPull.Format(time.RFC3339))
	}

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		r.sendError(fmt.Errorf("failed to create pull request: %w", err))
		return
	}

	r.setHeaders(req)

	resp, err := r.httpClient.Do(req)
	if err != nil {
		r.sendError(fmt.Errorf("failed to pull from supabase: %w", err))
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		r.sendError(fmt.Errorf("supabase pull failed: %s - %s", resp.Status, string(body)))
		return
	}

	var remoteDocs []map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&remoteDocs); err != nil {
		r.sendError(fmt.Errorf("failed to decode pull response: %w", err))
		return
	}

	// 处理拉取的文档
	for _, remoteDoc := range remoteDocs {
		if err := r.processRemoteDoc(ctx, remoteDoc); err != nil {
			r.sendError(err)
		}
	}

	r.lastPull = time.Now()
}

// processRemoteDoc 处理远程文档。
func (r *Replication) processRemoteDoc(ctx context.Context, remoteDoc map[string]any) error {
	id, ok := remoteDoc[r.opts.PrimaryKey]
	if !ok {
		return fmt.Errorf("remote document missing primary key")
	}
	idStr := fmt.Sprintf("%v", id)

	// 查找本地文档
	localDoc, err := r.collection.FindByID(ctx, idStr)
	if err != nil {
		return fmt.Errorf("failed to find local document: %w", err)
	}

	if localDoc == nil {
		// 本地不存在，直接插入
		_, err := r.collection.Insert(ctx, remoteDoc)
		return err
	}

	// 本地存在，检查冲突
	localData := localDoc.Data()
	resolved := r.opts.ConflictHandler(localData, remoteDoc)
	if resolved != nil {
		_, err := r.collection.Upsert(ctx, resolved)
		return err
	}

	return nil
}

// pushLoop 监听本地变更并推送。
func (r *Replication) pushLoop(ctx context.Context) {
	changes := r.collection.Changes()
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.stopChan:
			return
		case event, ok := <-changes:
			if !ok {
				return
			}
			r.push(ctx, event)
		}
	}
}

// push 推送本地变更到 Supabase。
func (r *Replication) push(ctx context.Context, event rxdb.ChangeEvent) {
	r.mu.Lock()
	r.state = StatePushing
	r.mu.Unlock()

	defer func() {
		r.mu.Lock()
		if r.state == StatePushing {
			r.state = StateIdle
		}
		r.mu.Unlock()
	}()

	var err error
	switch event.Op {
	case rxdb.OperationInsert:
		err = r.pushInsert(ctx, event.Doc)
	case rxdb.OperationUpdate:
		err = r.pushUpdate(ctx, event.ID, event.Doc)
	case rxdb.OperationDelete:
		err = r.pushDelete(ctx, event.ID)
	}

	if err != nil {
		r.sendError(fmt.Errorf("failed to push %s: %w", event.Op, err))
	}
}

// pushInsert 推送插入操作。
func (r *Replication) pushInsert(ctx context.Context, doc map[string]any) error {
	url := fmt.Sprintf("%s/rest/v1/%s", r.opts.SupabaseURL, r.opts.Table)

	body, err := json.Marshal(doc)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return err
	}

	r.setHeaders(req)
	req.Header.Set("Prefer", "return=minimal")

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("insert failed: %s - %s", resp.Status, string(respBody))
	}

	return nil
}

// pushUpdate 推送更新操作。
func (r *Replication) pushUpdate(ctx context.Context, id string, doc map[string]any) error {
	url := fmt.Sprintf("%s/rest/v1/%s?%s=eq.%s", r.opts.SupabaseURL, r.opts.Table, r.opts.PrimaryKey, id)

	body, err := json.Marshal(doc)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "PATCH", url, bytes.NewReader(body))
	if err != nil {
		return err
	}

	r.setHeaders(req)
	req.Header.Set("Prefer", "return=minimal")

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("update failed: %s - %s", resp.Status, string(respBody))
	}

	return nil
}

// pushDelete 推送删除操作。
func (r *Replication) pushDelete(ctx context.Context, id string) error {
	url := fmt.Sprintf("%s/rest/v1/%s?%s=eq.%s", r.opts.SupabaseURL, r.opts.Table, r.opts.PrimaryKey, id)

	req, err := http.NewRequestWithContext(ctx, "DELETE", url, nil)
	if err != nil {
		return err
	}

	r.setHeaders(req)

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("delete failed: %s - %s", resp.Status, string(respBody))
	}

	return nil
}

// setHeaders 设置 Supabase 请求头。
func (r *Replication) setHeaders(req *http.Request) {
	req.Header.Set("apikey", r.opts.SupabaseKey)
	req.Header.Set("Authorization", "Bearer "+r.opts.SupabaseKey)
	req.Header.Set("Content-Type", "application/json")
}

// sendError 发送错误到错误通道。
func (r *Replication) sendError(err error) {
	r.mu.Lock()
	r.state = StateError
	r.mu.Unlock()

	select {
	case r.errChan <- err:
	default:
		// 通道满时丢弃
	}
}

// PullOnce 执行一次拉取（用于手动触发）。
func (r *Replication) PullOnce(ctx context.Context) error {
	r.pull(ctx)
	return nil
}

// PushOnce 推送所有本地数据（用于初始化同步）。
func (r *Replication) PushOnce(ctx context.Context) error {
	docs, err := r.collection.All(ctx)
	if err != nil {
		return err
	}

	for _, doc := range docs {
		if err := r.pushInsert(ctx, doc.Data()); err != nil {
			// 如果是冲突，尝试更新
			if err := r.pushUpdate(ctx, doc.ID(), doc.Data()); err != nil {
				return err
			}
		}
	}

	return nil
}
