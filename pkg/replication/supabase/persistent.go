package supabase

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"sync"
	"time"

	"github.com/mozhou-tech/rxdb-go/pkg/rxdb"
	"github.com/mozhou-tech/rxdb-go/pkg/storage/badger"
)

// PersistentReplicationOptions 持久化同步配置选项。
type PersistentReplicationOptions struct {
	ReplicationOptions
	// StatePath Badger 存储路径（用于存储同步状态和队列）
	StatePath string
	// MaxRetries 最大重试次数
	MaxRetries int
	// RetryInterval 重试间隔
	RetryInterval time.Duration
	// QueueProcessInterval 队列处理间隔
	QueueProcessInterval time.Duration
}

// QueueItem 队列项，表示待推送的操作。
type QueueItem struct {
	ID        string                 `json:"id"`        // 队列项唯一 ID
	Op        rxdb.Operation         `json:"op"`        // 操作类型
	DocID     string                 `json:"doc_id"`   // 文档 ID
	Doc       map[string]any         `json:"doc"`       // 文档数据（delete 时可为空）
	Retries   int                    `json:"retries"`   // 重试次数
	CreatedAt time.Time              `json:"created_at"` // 创建时间
	LastError string                 `json:"last_error"` // 最后一次错误
}

// PersistentReplication 持久化同步客户端，使用 Badger 存储同步状态和队列。
type PersistentReplication struct {
	*Replication
	opts          PersistentReplicationOptions
	store         *badger.Store
	queueMu       sync.RWMutex
	queueSize     int64 // 使用 atomic 操作
	stopChan chan struct{}
}

const (
	// Badger bucket 前缀
	bucketState = "replication:state"
	bucketQueue = "replication:queue"
	
	// State keys
	keyLastPull = "last_pull"
	keyState    = "state"
)

// NewPersistentReplication 创建新的持久化同步实例。
func NewPersistentReplication(collection rxdb.Collection, opts PersistentReplicationOptions) (*PersistentReplication, error) {
	// 创建基础 Replication 实例
	rep, err := NewReplication(collection, opts.ReplicationOptions)
	if err != nil {
		return nil, err
	}

	// 设置默认值
	if opts.StatePath == "" {
		opts.StatePath = "./.rxdb-sync-state"
	}
	if opts.MaxRetries == 0 {
		opts.MaxRetries = 10
	}
	if opts.RetryInterval == 0 {
		opts.RetryInterval = 5 * time.Second
	}
	if opts.QueueProcessInterval == 0 {
		opts.QueueProcessInterval = 5 * time.Second
	}

	// 确保目录存在
	absPath, err := filepath.Abs(opts.StatePath)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve state path: %w", err)
	}

	// 打开 Badger store
	store, err := badger.Open(absPath, badger.Options{
		InMemory:   false,
		SyncWrites: false, // 异步写入性能更好
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open badger store: %w", err)
	}

	pr := &PersistentReplication{
		Replication:   rep,
		opts:          opts,
		store:    store,
		stopChan: make(chan struct{}),
	}

	// 恢复状态
	if err := pr.restoreState(context.Background()); err != nil {
		store.Close()
		return nil, fmt.Errorf("failed to restore state: %w", err)
	}

	// 恢复队列
	if err := pr.restoreQueue(context.Background()); err != nil {
		store.Close()
		return nil, fmt.Errorf("failed to restore queue: %w", err)
	}

	return pr, nil
}

// restoreState 从 Badger 恢复同步状态。
func (pr *PersistentReplication) restoreState(ctx context.Context) error {
	// 恢复 lastPull 时间
	lastPullData, err := pr.store.Get(ctx, bucketState, keyLastPull)
	if err != nil {
		return err
	}
	if lastPullData != nil {
		var lastPull time.Time
		if err := json.Unmarshal(lastPullData, &lastPull); err == nil {
			pr.Replication.mu.Lock()
			pr.Replication.lastPull = lastPull
			pr.Replication.mu.Unlock()
		}
	}

	return nil
}

// restoreQueue 从 Badger 恢复队列。
func (pr *PersistentReplication) restoreQueue(ctx context.Context) error {
	var count int64
	err := pr.store.Iterate(ctx, bucketQueue, func(key, value []byte) error {
		count++
		return nil
	})
	if err != nil {
		return err
	}
	pr.queueSize = count
	return nil
}

// saveLastPull 保存 lastPull 时间到 Badger。
func (pr *PersistentReplication) saveLastPull(ctx context.Context, t time.Time) error {
	data, err := json.Marshal(t)
	if err != nil {
		return err
	}
	return pr.store.Set(ctx, bucketState, keyLastPull, data)
}

// enqueue 将操作加入队列。
func (pr *PersistentReplication) enqueue(ctx context.Context, item QueueItem) error {
	// 生成队列项 ID（使用时间戳 + 文档 ID）
	if item.ID == "" {
		item.ID = fmt.Sprintf("%d_%s", time.Now().UnixNano(), item.DocID)
	}
	if item.CreatedAt.IsZero() {
		item.CreatedAt = time.Now()
	}

	// 序列化队列项
	data, err := json.Marshal(item)
	if err != nil {
		return fmt.Errorf("failed to marshal queue item: %w", err)
	}

	// 存储到 Badger
	if err := pr.store.Set(ctx, bucketQueue, item.ID, data); err != nil {
		return fmt.Errorf("failed to store queue item: %w", err)
	}

	pr.queueMu.Lock()
	pr.queueSize++
	pr.queueMu.Unlock()

	return nil
}

// dequeue 从队列中移除项。
func (pr *PersistentReplication) dequeue(ctx context.Context, itemID string) error {
	if err := pr.store.Delete(ctx, bucketQueue, itemID); err != nil {
		return err
	}

	pr.queueMu.Lock()
	if pr.queueSize > 0 {
		pr.queueSize--
	}
	pr.queueMu.Unlock()

	return nil
}

// getNextQueueItem 获取下一个待处理的队列项。
func (pr *PersistentReplication) getNextQueueItem(ctx context.Context) (*QueueItem, error) {
	var oldestItem *QueueItem
	var oldestID string
	var oldestTime time.Time

	err := pr.store.Iterate(ctx, bucketQueue, func(key, value []byte) error {
		var item QueueItem
		if err := json.Unmarshal(value, &item); err != nil {
			return nil // 跳过损坏的项
		}

		// 检查是否超过最大重试次数
		if item.Retries >= pr.opts.MaxRetries {
			// 删除超过最大重试次数的项
			pr.dequeue(ctx, item.ID)
			return nil
		}

		// 检查是否到了重试时间
		nextRetryTime := item.CreatedAt.Add(time.Duration(item.Retries+1) * pr.opts.RetryInterval)
		if time.Now().Before(nextRetryTime) {
			return nil // 还没到重试时间
		}

		// 找到最早创建的项
		if oldestItem == nil || item.CreatedAt.Before(oldestTime) {
			oldestItem = &item
			oldestID = item.ID
			oldestTime = item.CreatedAt
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	if oldestItem == nil {
		return nil, nil // 没有待处理的项
	}

	// 更新重试次数
	oldestItem.Retries++
	updatedData, err := json.Marshal(oldestItem)
	if err == nil {
		pr.store.Set(ctx, bucketQueue, oldestID, updatedData)
	}

	return oldestItem, nil
}

// Start 启动持久化同步。
func (pr *PersistentReplication) Start(ctx context.Context) error {
	pr.Replication.mu.Lock()
	if pr.Replication.state != StateIdle && pr.Replication.state != StateStopped {
		pr.Replication.mu.Unlock()
		return fmt.Errorf("replication already running")
	}
	pr.Replication.state = StateIdle
	pr.Replication.stopChan = make(chan struct{})
	pr.Replication.mu.Unlock()

	// 启动拉取循环（使用新的 pullLoop）
	go pr.pullLoop(ctx)

	// 如果配置了推送，监听本地变更（使用新的 pushLoop）
	if pr.opts.PushOnChange {
		go pr.pushLoop(ctx)
	}

	// 启动队列处理循环
	go pr.queueProcessLoop(ctx)

	return nil
}

// Stop 停止持久化同步。
func (pr *PersistentReplication) Stop() {
	pr.Replication.Stop()
	close(pr.stopChan)

	// 保存状态
	ctx := context.Background()
	pr.Replication.mu.RLock()
	lastPull := pr.Replication.lastPull
	pr.Replication.mu.RUnlock()
	pr.saveLastPull(ctx, lastPull)

	// 关闭 Badger store
	pr.store.Close()
}

// queueProcessLoop 队列处理循环。
func (pr *PersistentReplication) queueProcessLoop(ctx context.Context) {
	ticker := time.NewTicker(pr.opts.QueueProcessInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-pr.stopChan:
			return
		case <-ticker.C:
			pr.processQueue(ctx)
		}
	}
}

// processQueue 处理队列中的项。
func (pr *PersistentReplication) processQueue(ctx context.Context) {
	for {
		item, err := pr.getNextQueueItem(ctx)
		if err != nil || item == nil {
			return // 没有更多待处理的项
		}

		// 尝试推送
		var pushErr error
		switch item.Op {
		case rxdb.OperationInsert:
			pushErr = pr.pushInsertItem(ctx, item.Doc)
		case rxdb.OperationUpdate:
			pushErr = pr.pushUpdateItem(ctx, item.DocID, item.Doc)
		case rxdb.OperationDelete:
			pushErr = pr.pushDeleteItem(ctx, item.DocID)
		}

		if pushErr != nil {
			// 更新错误信息
			item.LastError = pushErr.Error()
			updatedData, err := json.Marshal(item)
			if err == nil {
				pr.store.Set(ctx, bucketQueue, item.ID, updatedData)
			}
			pr.sendError(fmt.Errorf("queue item %s failed: %w", item.ID, pushErr))
			return // 等待下次重试
		}

		// 成功，从队列中移除
		if err := pr.dequeue(ctx, item.ID); err != nil {
			pr.sendError(fmt.Errorf("failed to dequeue item %s: %w", item.ID, err))
		}
	}
}

// push 推送本地变更到 Supabase（重写以支持队列）。
func (pr *PersistentReplication) push(ctx context.Context, event rxdb.ChangeEvent) {
	pr.Replication.mu.Lock()
	pr.Replication.state = StatePushing
	pr.Replication.mu.Unlock()

	defer func() {
		pr.Replication.mu.Lock()
		if pr.Replication.state == StatePushing {
			pr.Replication.state = StateIdle
		}
		pr.Replication.mu.Unlock()
	}()

	var err error
	switch event.Op {
	case rxdb.OperationInsert:
		err = pr.pushInsertItem(ctx, event.Doc)
	case rxdb.OperationUpdate:
		err = pr.pushUpdateItem(ctx, event.ID, event.Doc)
	case rxdb.OperationDelete:
		err = pr.pushDeleteItem(ctx, event.ID)
	}

	if err != nil {
		// 失败时加入队列
		queueItem := QueueItem{
			Op:    event.Op,
			DocID: event.ID,
			Doc:   event.Doc,
		}
		if enqueueErr := pr.enqueue(ctx, queueItem); enqueueErr != nil {
			pr.sendError(fmt.Errorf("failed to enqueue after push error: %w", enqueueErr))
		} else {
			pr.sendError(fmt.Errorf("push failed, queued for retry: %w", err))
		}
	}
}

// pushLoop 监听本地变更并推送（重写以使用新的 push 方法）。
func (pr *PersistentReplication) pushLoop(ctx context.Context) {
	changes := pr.Replication.collection.Changes()
	for {
		select {
		case <-ctx.Done():
			return
		case <-pr.stopChan:
			return
		case <-pr.Replication.stopChan:
			return
		case event, ok := <-changes:
			if !ok {
				return
			}
			pr.push(ctx, event)
		}
	}
}

// pull 从 Supabase 拉取数据（重写以保存 lastPull）。
func (pr *PersistentReplication) pull(ctx context.Context) {
	pr.Replication.pull(ctx)

	// 保存 lastPull 时间
	pr.Replication.mu.RLock()
	lastPull := pr.Replication.lastPull
	pr.Replication.mu.RUnlock()
	if !lastPull.IsZero() {
		pr.saveLastPull(ctx, lastPull)
	}
}

// pullLoop 定期从 Supabase 拉取数据（重写以使用新的 pull 方法）。
func (pr *PersistentReplication) pullLoop(ctx context.Context) {
	ticker := time.NewTicker(pr.opts.PullInterval)
	defer ticker.Stop()

	// 立即执行一次拉取
	pr.pull(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-pr.stopChan:
			return
		case <-pr.Replication.stopChan:
			return
		case <-ticker.C:
			pr.pull(ctx)
		}
	}
}

// GetPendingQueueSize 返回待推送队列大小。
func (pr *PersistentReplication) GetPendingQueueSize() int {
	pr.queueMu.RLock()
	defer pr.queueMu.RUnlock()
	return int(pr.queueSize)
}

// pushInsertItem 推送插入操作（内部方法，复用 Replication 的逻辑）。
func (pr *PersistentReplication) pushInsertItem(ctx context.Context, doc map[string]any) error {
	url := fmt.Sprintf("%s/rest/v1/%s", pr.opts.SupabaseURL, pr.opts.Table)

	body, err := json.Marshal(doc)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return err
	}

	pr.setHeaders(req)
	req.Header.Set("Prefer", "return=minimal")

	resp, err := pr.Replication.httpClient.Do(req)
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

// pushUpdateItem 推送更新操作（内部方法，复用 Replication 的逻辑）。
func (pr *PersistentReplication) pushUpdateItem(ctx context.Context, id string, doc map[string]any) error {
	url := fmt.Sprintf("%s/rest/v1/%s?%s=eq.%s", pr.opts.SupabaseURL, pr.opts.Table, pr.opts.PrimaryKey, id)

	body, err := json.Marshal(doc)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "PATCH", url, bytes.NewReader(body))
	if err != nil {
		return err
	}

	pr.setHeaders(req)
	req.Header.Set("Prefer", "return=minimal")

	resp, err := pr.Replication.httpClient.Do(req)
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

// pushDeleteItem 推送删除操作（内部方法，复用 Replication 的逻辑）。
func (pr *PersistentReplication) pushDeleteItem(ctx context.Context, id string) error {
	url := fmt.Sprintf("%s/rest/v1/%s?%s=eq.%s", pr.opts.SupabaseURL, pr.opts.Table, pr.opts.PrimaryKey, id)

	req, err := http.NewRequestWithContext(ctx, "DELETE", url, nil)
	if err != nil {
		return err
	}

	pr.setHeaders(req)

	resp, err := pr.Replication.httpClient.Do(req)
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
func (pr *PersistentReplication) setHeaders(req *http.Request) {
	req.Header.Set("apikey", pr.opts.SupabaseKey)
	req.Header.Set("Authorization", "Bearer "+pr.opts.SupabaseKey)
	req.Header.Set("Content-Type", "application/json")
}


