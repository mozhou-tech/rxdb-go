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
	"sync/atomic"
	"syscall"
	"time"

	"github.com/mozy/rxdb-go/pkg/storage/badger"
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
	// BadgerOptions Badger 存储选项
	BadgerOptions badger.Options
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
	store       *badger.Store
	collections map[string]*collection
	mu          sync.RWMutex
	activeOps   int32 // 使用 atomic 操作，避免为了计数而加锁
	closed      bool
	password    string
	multiInst   bool
	hashFn      func([]byte) string
	broadcaster *eventBroadcaster // 多实例事件广播器
	lockFile    *os.File          // 文件锁（用于多实例选举）
	isLeader    bool              // 是否为领导实例

	// 数据库级别订阅者管理
	dbSubscribersMu   sync.RWMutex
	dbSubscribers     map[uint64]chan ChangeEvent
	dbSubscriberIDGen uint64
	closeChan         chan struct{}
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
	var shouldCloseExisting bool
	if exists && existing != nil && !existing.closed {
		if opts.CloseDuplicates {
			shouldCloseExisting = true
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

	// 在释放锁后关闭已存在的数据库，避免死锁
	// Close 方法内部需要获取 dbRegistryMu 锁
	if shouldCloseExisting {
		logger.Info("Closing duplicate database: %s", opts.Name)
		_ = existing.Close(ctx)
	}

	store, err := badger.Open(opts.Path, opts.BadgerOptions)
	if err != nil {
		logger.Error("Failed to open badger store: %v", err)
		return nil, fmt.Errorf("failed to open badger store: %w", err)
	}
	logger.Debug("Badger store opened successfully: %s", opts.Path)

	hashFn := opts.HashFunction
	if hashFn == nil {
		hashFn = defaultHash
	}

	db := &database{
		name:          opts.Name,
		store:         store,
		collections:   make(map[string]*collection),
		password:      opts.Password,
		multiInst:     opts.MultiInstance,
		hashFn:        hashFn,
		dbSubscribers: make(map[uint64]chan ChangeEvent),
		closeChan:     make(chan struct{}),
	}

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
	broadcaster := d.broadcaster

	// 关闭 closeChan
	close(d.closeChan)

	// 关闭所有数据库级别的订阅者通道
	d.dbSubscribersMu.Lock()
	for id, ch := range d.dbSubscribers {
		close(ch)
		delete(d.dbSubscribers, id)
	}
	d.dbSubscribersMu.Unlock()

	// 在同一个锁内关闭所有集合的变更通道，避免双重加锁
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

	// 删除存储目录（Badger 使用目录存储）
	if err := os.RemoveAll(path); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("failed to remove database directory: %w", err)
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

	// 如果集合已存在，检查是否需要迁移
	if col, ok := d.collections[name]; ok {
		// 检查版本变化
		oldVersion := getSchemaVersion(col.schema)
		newVersion := getSchemaVersion(schema)
		if newVersion > oldVersion && len(schema.MigrationStrategies) > 0 {
			// 更新 schema 并执行迁移
			col.schema = schema
			if err := col.migrate(ctx, oldVersion, newVersion); err != nil {
				return nil, fmt.Errorf("schema migration failed: %w", err)
			}
		}
		return col, nil
	}

	// 设置默认主键
	if schema.PrimaryKey == nil {
		schema.PrimaryKey = "id"
	}
	if schema.RevField == "" {
		schema.RevField = "_rev"
	}

	col, err := newCollection(ctx, d.store, name, schema, d.hashFn, d.broadcaster, d.password, d.emitDatabaseChange)
	if err != nil {
		return nil, err
	}

	d.collections[name] = col
	return col, nil
}

// GetStore 返回底层存储（供内部使用）。
func (d *database) GetStore() *badger.Store {
	return d.store
}

// Changes 返回数据库级别的变更事件通道（所有集合的变更）。
func (d *database) Changes() <-chan ChangeEvent {
	d.mu.RLock()
	multiInst := d.multiInst
	broadcaster := d.broadcaster
	d.mu.RUnlock()

	// 如果启用多实例，从广播器接收事件
	if multiInst && broadcaster != nil {
		return broadcaster.subscribe()
	}

	// 创建新的订阅通道
	return d.subscribeChanges()
}

// subscribeChanges 创建一个新的数据库级别订阅通道。
func (d *database) subscribeChanges() <-chan ChangeEvent {
	d.dbSubscribersMu.Lock()
	defer d.dbSubscribersMu.Unlock()

	// 检查是否已关闭
	select {
	case <-d.closeChan:
		ch := make(chan ChangeEvent)
		close(ch)
		return ch
	default:
	}

	d.dbSubscriberIDGen++
	id := d.dbSubscriberIDGen
	ch := make(chan ChangeEvent, 100)
	d.dbSubscribers[id] = ch

	return ch
}

// emitDatabaseChange 向所有数据库级别的订阅者发送变更事件。
func (d *database) emitDatabaseChange(event ChangeEvent) {
	select {
	case <-d.closeChan:
		return
	default:
	}

	d.dbSubscribersMu.RLock()
	subscribers := make([]chan ChangeEvent, 0, len(d.dbSubscribers))
	for _, ch := range d.dbSubscribers {
		subscribers = append(subscribers, ch)
	}
	d.dbSubscribersMu.RUnlock()

	for _, ch := range subscribers {
		select {
		case ch <- event:
		case <-d.closeChan:
			return
		default:
			// 通道满时丢弃，避免阻塞
		}
	}
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

// RequestIdle 等待数据库空闲；使用 atomic 轮询检查，避免锁竞争。
func (d *database) RequestIdle(ctx context.Context) error {
	for {
		// 使用读锁检查 closed 状态
		d.mu.RLock()
		closed := d.closed
		d.mu.RUnlock()

		if closed {
			return errors.New("database is closed")
		}

		// 使用 atomic 检查活跃操作数
		if atomic.LoadInt32(&d.activeOps) == 0 {
			return nil
		}

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

	// 获取集合列表（需要读锁）
	d.mu.RLock()
	if d.closed {
		d.mu.RUnlock()
		return nil, errors.New("database is closed")
	}
	// 复制集合引用，避免在持有锁时调用可能阻塞的操作
	collectionList := make([]struct {
		name string
		col  *collection
	}, 0, len(d.collections))
	for name, col := range d.collections {
		collectionList = append(collectionList, struct {
			name string
			col  *collection
		}{name: name, col: col})
	}
	d.mu.RUnlock()

	result := make(map[string]any)
	collections := make(map[string]any)

	// 在释放锁后调用 ExportJSON，避免死锁
	for _, item := range collectionList {
		docs, err := item.col.ExportJSON(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to export collection %s: %w", item.name, err)
		}
		// 将 []map[string]any 转换为 []any
		docsAny := make([]any, len(docs))
		for i, doc := range docs {
			docsAny[i] = doc
		}
		collections[item.name] = docsAny
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

	// 先检查数据库是否关闭（需要读锁）
	d.mu.RLock()
	closed := d.closed
	d.mu.RUnlock()

	if closed {
		return errors.New("database is closed")
	}

	collectionsData, ok := data["collections"].(map[string]any)
	if !ok {
		return errors.New("invalid import data: missing 'collections' field")
	}

	// 准备所有集合的导入数据（不需要持有锁）
	type collectionImportData struct {
		name   string
		docs   []map[string]any
		schema Schema
	}
	importList := make([]collectionImportData, 0, len(collectionsData))

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

		// 使用一个基本的 schema
		schema := Schema{
			PrimaryKey: "id",
			RevField:   "_rev",
		}

		importList = append(importList, collectionImportData{
			name:   name,
			docs:   docMaps,
			schema: schema,
		})
	}

	// 释放锁后，逐个获取集合并导入数据
	// Collection 和 ImportJSON 方法内部会处理自己的锁
	for _, item := range importList {
		col, err := d.Collection(ctx, item.name, item.schema)
		if err != nil {
			return fmt.Errorf("failed to get collection %s: %w", item.name, err)
		}

		// 导入文档
		if err := col.ImportJSON(ctx, item.docs); err != nil {
			return fmt.Errorf("failed to import collection %s: %w", item.name, err)
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

	// 使用 Badger 的内置备份功能
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
// 使用 atomic 计数，避免为了简单的计数操作而加锁。
func (d *database) beginOp(ctx context.Context) error {
	d.mu.RLock()
	closed := d.closed
	d.mu.RUnlock()

	if closed {
		return errors.New("database is closed")
	}
	atomic.AddInt32(&d.activeOps, 1)
	return nil
}

// endOp 结束活跃操作。
// 使用 atomic 计数，避免加锁。
func (d *database) endOp() {
	atomic.AddInt32(&d.activeOps, -1)
}

// RemoveDatabase 删除数据库目录（静态方法等价于 RxDB remove）。
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

	// Badger 使用目录存储，需要删除整个目录
	if err := os.RemoveAll(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove database directory: %w", err)
	}
	return nil
}
