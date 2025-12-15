package rxdb

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/mozy/rxdb-go/pkg/storage/bolt"
)

var (
	dbRegistry   = make(map[string]*database)
	dbRegistryMu sync.Mutex
	// eventBroadcasters 按数据库名称组织的事件广播器，用于多实例事件共享
	eventBroadcasters   = make(map[string]*eventBroadcaster)
	eventBroadcastersMu sync.Mutex
)

// eventBroadcaster 用于在多实例间广播变更事件
type eventBroadcaster struct {
	name      string
	listeners []chan ChangeEvent
	mu        sync.RWMutex
	closed    bool
}

// newEventBroadcaster 创建或获取指定名称的事件广播器
func newEventBroadcaster(name string) *eventBroadcaster {
	eventBroadcastersMu.Lock()
	defer eventBroadcastersMu.Unlock()

	if bc, ok := eventBroadcasters[name]; ok {
		return bc
	}

	bc := &eventBroadcaster{
		name:      name,
		listeners: make([]chan ChangeEvent, 0),
	}
	eventBroadcasters[name] = bc
	return bc
}

// subscribe 订阅事件广播
func (eb *eventBroadcaster) subscribe() <-chan ChangeEvent {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	if eb.closed {
		return nil
	}

	ch := make(chan ChangeEvent, 100)
	eb.listeners = append(eb.listeners, ch)
	return ch
}

// unsubscribe 取消订阅
func (eb *eventBroadcaster) unsubscribe(ch <-chan ChangeEvent) {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	for i, listener := range eb.listeners {
		if listener == ch {
			eb.listeners = append(eb.listeners[:i], eb.listeners[i+1:]...)
			close(listener)
			break
		}
	}
}

// broadcast 广播事件到所有订阅者
func (eb *eventBroadcaster) broadcast(event ChangeEvent) {
	eb.mu.RLock()
	defer eb.mu.RUnlock()

	for _, listener := range eb.listeners {
		select {
		case listener <- event:
		default:
			// 通道满时跳过，避免阻塞
		}
	}
}

// close 关闭广播器
func (eb *eventBroadcaster) close() {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	if eb.closed {
		return
	}

	eb.closed = true
	for _, listener := range eb.listeners {
		close(listener)
	}
	eb.listeners = nil

	eventBroadcastersMu.Lock()
	delete(eventBroadcasters, eb.name)
	eventBroadcastersMu.Unlock()
}

// DatabaseOptions 配置数据库创建选项。
type DatabaseOptions struct {
	// Name 数据库名称
	Name string
	// Path 存储路径
	Path string
	// BoltOptions Bolt 存储选项
	BoltOptions bolt.Options
	// Password 数据库级密码（预留用于字段加密）
	Password string
	// MultiInstance 是否允许多实例（同名数据库多开）
	MultiInstance bool
	// EventReduce 是否启用 EventReduce 优化（预留）
	EventReduce bool
	// IgnoreDuplicate 是否忽略重复创建同名数据库并返回已存在实例
	IgnoreDuplicate bool
	// CloseDuplicates 是否在创建新实例前关闭已存在的同名实例
	CloseDuplicates bool
	// HashFunction 自定义哈希函数（预留）
	HashFunction func(data []byte) string
}

// database 是 Database 接口的默认实现。
type database struct {
	name        string
	store       *bolt.Store
	collections map[string]*collection
	mu          sync.RWMutex
	idleCond    *sync.Cond
	activeOps   int
	closed      bool
	password    string
	multiInst   bool
	hashFn      func([]byte) string
	broadcaster *eventBroadcaster // 多实例事件广播器
	lockFile    *os.File          // 文件锁（用于多实例选举）
	isLeader    bool              // 是否为领导实例
}

// CreateDatabase 创建新的数据库实例。
func CreateDatabase(ctx context.Context, opts DatabaseOptions) (Database, error) {
	logger := GetLogger()
	logger.Debug("Creating database: name=%s, path=%s", opts.Name, opts.Path)

	if opts.Name == "" {
		return nil, errors.New("database name required")
	}
	if opts.Path == "" {
		opts.Path = fmt.Sprintf("./%s.db", opts.Name)
	}

	dbRegistryMu.Lock()
	existing, exists := dbRegistry[opts.Name]
	if exists && existing != nil && !existing.closed {
		if opts.CloseDuplicates {
			logger.Info("Closing duplicate database: %s", opts.Name)
			_ = existing.Close(ctx)
		} else if opts.IgnoreDuplicate {
			dbRegistryMu.Unlock()
			logger.Debug("Returning existing database: %s", opts.Name)
			return existing, nil
		} else if !opts.MultiInstance {
			dbRegistryMu.Unlock()
			return nil, fmt.Errorf("database %s already opened", opts.Name)
		}
	}
	dbRegistryMu.Unlock()

	store, err := bolt.Open(opts.Path, opts.BoltOptions)
	if err != nil {
		logger.Error("Failed to open bolt store: %v", err)
		return nil, fmt.Errorf("failed to open bolt store: %w", err)
	}
	logger.Debug("Bolt store opened successfully: %s", opts.Path)

	hashFn := opts.HashFunction
	if hashFn == nil {
		hashFn = defaultHash
	}

	db := &database{
		name:        opts.Name,
		store:       store,
		collections: make(map[string]*collection),
		password:    opts.Password,
		multiInst:   opts.MultiInstance,
		hashFn:      hashFn,
	}
	db.idleCond = sync.NewCond(&db.mu)

	// 如果启用多实例，创建或获取事件广播器
	if opts.MultiInstance {
		db.broadcaster = newEventBroadcaster(opts.Name)
		// 创建文件锁用于多实例选举
		lockPath := opts.Path + ".lock"
		lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0644)
		if err == nil {
			db.lockFile = lockFile
		}
	}

	dbRegistryMu.Lock()
	dbRegistry[opts.Name] = db
	dbRegistryMu.Unlock()

	logger.Info("Database created successfully: %s", opts.Name)
	return db, nil
}

func (d *database) Name() string {
	return d.name
}

func (d *database) Close(ctx context.Context) error {
	logger := GetLogger()
	logger.Debug("Closing database: %s", d.name)

	d.mu.Lock()

	if d.closed {
		d.mu.Unlock()
		return nil
	}

	d.closed = true
	d.idleCond.Broadcast()
	broadcaster := d.broadcaster

	d.mu.Unlock()

	d.mu.Lock()
	// 关闭所有集合的变更通道
	for _, col := range d.collections {
		col.close()
	}
	d.mu.Unlock()

	// 如果这是最后一个实例，关闭广播器
	dbRegistryMu.Lock()
	instanceCount := 0
	for _, db := range dbRegistry {
		if db.name == d.name && !db.closed {
			instanceCount++
		}
	}
	if current, ok := dbRegistry[d.name]; ok && current == d {
		delete(dbRegistry, d.name)
	}
	dbRegistryMu.Unlock()

	// 如果没有其他实例了，关闭广播器
	if broadcaster != nil && instanceCount <= 1 {
		broadcaster.close()
	}

	// 释放文件锁
	if d.lockFile != nil {
		if d.isLeader {
			_ = syscall.Flock(int(d.lockFile.Fd()), syscall.LOCK_UN)
		}
		_ = d.lockFile.Close()
	}

	return d.store.Close()
}

func (d *database) Destroy(ctx context.Context) error {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil
	}
	d.closed = true
	d.idleCond.Broadcast()
	d.mu.Unlock()

	// 获取存储路径
	path := d.store.Path()
	if path == "" {
		return errors.New("database path not available")
	}

	// 关闭数据库
	if err := d.store.Close(); err != nil {
		return fmt.Errorf("failed to close database: %w", err)
	}

	// 删除存储文件
	if err := os.Remove(path); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("failed to remove database file: %w", err)
		}
	}

	dbRegistryMu.Lock()
	if current, ok := dbRegistry[d.name]; ok && current == d {
		delete(dbRegistry, d.name)
	}
	dbRegistryMu.Unlock()

	return nil
}

func (d *database) Collection(ctx context.Context, name string, schema Schema) (Collection, error) {
	if err := d.beginOp(ctx); err != nil {
		return nil, err
	}
	defer d.endOp()

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return nil, errors.New("database is closed")
	}

	// 如果集合已存在，直接返回
	if col, ok := d.collections[name]; ok {
		return col, nil
	}

	// 设置默认主键
	if schema.PrimaryKey == nil {
		schema.PrimaryKey = "id"
	}
	if schema.RevField == "" {
		schema.RevField = "_rev"
	}

	col, err := newCollection(ctx, d.store, name, schema, d.hashFn, d.broadcaster, d.password)
	if err != nil {
		return nil, err
	}

	d.collections[name] = col
	return col, nil
}

// GetStore 返回底层存储（供内部使用）。
func (d *database) GetStore() *bolt.Store {
	return d.store
}

// Changes 返回数据库级别的变更事件通道（所有集合的变更）。
func (d *database) Changes() <-chan ChangeEvent {
	d.mu.RLock()
	multiInst := d.multiInst
	broadcaster := d.broadcaster
	collections := make([]*collection, 0, len(d.collections))
	for _, col := range d.collections {
		collections = append(collections, col)
	}
	d.mu.RUnlock()

	// 如果启用多实例，从广播器接收事件
	if multiInst && broadcaster != nil {
		return broadcaster.subscribe()
	}

	// 否则，合并所有集合的变更事件
	merged := make(chan ChangeEvent, 100)

	go func() {
		defer close(merged)

		var wg sync.WaitGroup
		for _, col := range collections {
			wg.Add(1)
			go func(c *collection) {
				defer wg.Done()
				for event := range c.Changes() {
					select {
					case merged <- event:
					case <-c.closeChan:
						return
					}
				}
			}(col)
		}
		wg.Wait()
	}()

	return merged
}

// WaitForLeadership 等待成为领导实例。单实例场景下立即返回，多实例场景使用文件锁选举。
func (d *database) WaitForLeadership(ctx context.Context) error {
	d.mu.RLock()
	closed := d.closed
	multi := d.multiInst
	isLeader := d.isLeader
	lockFile := d.lockFile
	d.mu.RUnlock()

	if closed {
		return errors.New("database is closed")
	}

	// 单实例场景立即返回
	if !multi {
		return nil
	}

	// 如果已经是领导实例，直接返回
	if isLeader {
		return nil
	}

	// 多实例场景：尝试获取文件锁
	if lockFile == nil {
		return errors.New("lock file not available")
	}

	// 尝试获取排他锁（非阻塞）
	err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err == nil {
		// 成功获取锁，成为领导实例
		d.mu.Lock()
		d.isLeader = true
		d.mu.Unlock()
		return nil
	}

	// 如果锁被占用，等待并重试
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
			if err == nil {
				d.mu.Lock()
				d.isLeader = true
				d.mu.Unlock()
				return nil
			}
		}
	}
}

// RequestIdle 等待数据库空闲；当前实现为无阻塞占位符。
func (d *database) RequestIdle(ctx context.Context) error {
	for {
		d.mu.Lock()
		if d.closed {
			d.mu.Unlock()
			return errors.New("database is closed")
		}
		if d.activeOps == 0 {
			d.mu.Unlock()
			return nil
		}
		d.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// Password 返回数据库级密码（当前仅存储，不用于加密）。
func (d *database) Password() string {
	return d.password
}

// MultiInstance 表示是否允许多实例。
func (d *database) MultiInstance() bool {
	return d.multiInst
}

// ExportJSON 导出数据库的所有集合数据为 JSON。
// 返回格式: {"collections": {"collectionName": [doc1, doc2, ...]}}
func (d *database) ExportJSON(ctx context.Context) (map[string]any, error) {
	if err := d.beginOp(ctx); err != nil {
		return nil, err
	}
	defer d.endOp()

	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.closed {
		return nil, errors.New("database is closed")
	}

	result := make(map[string]any)
	collections := make(map[string]any)

	for name, col := range d.collections {
		docs, err := col.ExportJSON(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to export collection %s: %w", name, err)
		}
		collections[name] = docs
	}

	result["collections"] = collections
	result["name"] = d.name

	return result, nil
}

// ImportJSON 从 JSON 导入数据到数据库。
// 输入格式: {"collections": {"collectionName": [doc1, doc2, ...]}}
func (d *database) ImportJSON(ctx context.Context, data map[string]any) error {
	if err := d.beginOp(ctx); err != nil {
		return err
	}
	defer d.endOp()

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return errors.New("database is closed")
	}

	collectionsData, ok := data["collections"].(map[string]any)
	if !ok {
		return errors.New("invalid import data: missing 'collections' field")
	}

	for name, docsData := range collectionsData {
		docs, ok := docsData.([]any)
		if !ok {
			return fmt.Errorf("invalid import data: collection %s is not an array", name)
		}

		// 转换为 []map[string]any
		docMaps := make([]map[string]any, 0, len(docs))
		for _, doc := range docs {
			if docMap, ok := doc.(map[string]any); ok {
				docMaps = append(docMaps, docMap)
			} else {
				return fmt.Errorf("invalid import data: document in collection %s is not an object", name)
			}
		}

		// 获取或创建集合
		// 注意：这里需要 schema，但导入时可能没有 schema 信息
		// 使用一个基本的 schema
		schema := Schema{
			PrimaryKey: "id",
			RevField:   "_rev",
		}

		col, err := d.Collection(ctx, name, schema)
		if err != nil {
			return fmt.Errorf("failed to get collection %s: %w", name, err)
		}

		// 导入文档
		if err := col.ImportJSON(ctx, docMaps); err != nil {
			return fmt.Errorf("failed to import collection %s: %w", name, err)
		}
	}

	return nil
}

// Backup 备份数据库到指定文件路径。
func (d *database) Backup(ctx context.Context, backupPath string) error {
	if err := d.beginOp(ctx); err != nil {
		return err
	}
	defer d.endOp()

	d.mu.RLock()
	defer d.mu.RUnlock()

	if d.closed {
		return errors.New("database is closed")
	}

	// 确保备份路径的目录存在
	backupDir := filepath.Dir(backupPath)
	if err := os.MkdirAll(backupDir, 0755); err != nil {
		return fmt.Errorf("failed to create backup directory: %w", err)
	}

	// 使用 Bolt 的内置备份功能
	return d.store.Backup(ctx, backupPath)
}

// IsRxDatabase 检查对象是否为 RxDatabase 实例。
func IsRxDatabase(db interface{}) bool {
	_, ok := db.(*database)
	return ok
}

// defaultHash 提供默认的 SHA-256 哈希实现，用于生成修订号后缀。
func defaultHash(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// beginOp 标记数据库级活跃操作，供 RequestIdle 等待。
func (d *database) beginOp(ctx context.Context) error {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return errors.New("database is closed")
	}
	d.activeOps++
	d.mu.Unlock()
	return nil
}

// endOp 结束活跃操作。
func (d *database) endOp() {
	d.mu.Lock()
	if d.activeOps > 0 {
		d.activeOps--
		if d.activeOps == 0 {
			d.idleCond.Broadcast()
		}
	}
	d.mu.Unlock()
}

// RemoveDatabase 删除数据库文件（静态方法等价于 RxDB remove）。
func RemoveDatabase(ctx context.Context, name, path string) error {
	if name == "" {
		return errors.New("database name required")
	}
	if path == "" {
		path = fmt.Sprintf("./%s.db", name)
	}

	dbRegistryMu.Lock()
	if db, ok := dbRegistry[name]; ok && db != nil {
		_ = db.Close(ctx)
		delete(dbRegistry, name)
	}
	dbRegistryMu.Unlock()

	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove database file: %w", err)
	}
	return nil
}
