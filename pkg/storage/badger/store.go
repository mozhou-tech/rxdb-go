package badger

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/dgraph-io/badger/v4"
)

// sharedDB 封装共享的 badger 实例和引用计数
type sharedDB struct {
	db       *badger.DB
	refCount int32
	path     string
}

var (
	// 全局存储注册表，按路径管理共享的 badger 实例
	sharedDBRegistry   = make(map[string]*sharedDB)
	sharedDBRegistryMu sync.Mutex
)

// Store 封装 Badger 数据库句柄，提供集合级别的 CRUD/查询接口。
// 注意：Badger 本身是线程安全的，Store 层仅在 Close 时使用锁保护 db 指针。
type Store struct {
	path   string
	db     *badger.DB
	mu     sync.Mutex // 仅用于 Close 操作的同步
	shared *sharedDB  // 指向共享实例（如果使用共享模式）
}

// Options 控制 Badger 打开参数。
type Options struct {
	// InMemory 是否使用内存模式
	InMemory bool
	// SyncWrites 是否同步写入（默认 false，异步写入性能更好）
	SyncWrites bool
	// Logger 自定义日志
	Logger badger.Logger
	// EncryptionKey 加密密钥（32 字节，用于数据库级加密）
	EncryptionKey []byte
	// IndexCacheSize 索引缓存大小（字节）。启用加密时必须 > 0。
	IndexCacheSize int64
	// BlockCacheSize 数据块缓存大小（字节）。
	BlockCacheSize int64
}

// Open 创建或打开 Badger DB（使用共享模式，相同路径复用同一实例）。
func Open(path string, opts Options) (*Store, error) {
	if path == "" && !opts.InMemory {
		return nil, errors.New("badger store path required")
	}

	abs := path
	if path != "" && !filepath.IsAbs(path) {
		if p, err := filepath.Abs(path); err == nil {
			abs = p
		}
	}

	// 内存模式不共享，每次创建新实例
	if opts.InMemory {
		return openNewDB(abs, opts)
	}

	// 尝试获取共享实例
	sharedDBRegistryMu.Lock()
	defer sharedDBRegistryMu.Unlock()

	if shared, ok := sharedDBRegistry[abs]; ok {
		// 增加引用计数
		atomic.AddInt32(&shared.refCount, 1)
		return &Store{
			path:   abs,
			db:     shared.db,
			shared: shared,
		}, nil
	}

	// 创建新的共享实例
	store, err := openNewDB(abs, opts)
	if err != nil {
		return nil, err
	}

	shared := &sharedDB{
		db:       store.db,
		refCount: 1,
		path:     abs,
	}
	sharedDBRegistry[abs] = shared
	store.shared = shared

	return store, nil
}

// openNewDB 创建新的 badger 实例（内部使用）
func openNewDB(abs string, opts Options) (*Store, error) {
	// 配置 Badger 选项
	badgerOpts := badger.DefaultOptions(abs)
	if opts.InMemory {
		badgerOpts = badgerOpts.WithInMemory(true)
	}
	badgerOpts = badgerOpts.WithSyncWrites(opts.SyncWrites)

	// 配置加密
	if len(opts.EncryptionKey) > 0 {
		badgerOpts = badgerOpts.WithEncryptionKey(opts.EncryptionKey)
		// 启用加密时，Badger 要求必须设置 IndexCacheSize > 0
		if opts.IndexCacheSize > 0 {
			badgerOpts = badgerOpts.WithIndexCacheSize(opts.IndexCacheSize)
		} else {
			// 提供默认的索引缓存大小 (100MB)
			badgerOpts = badgerOpts.WithIndexCacheSize(100 << 20)
		}
	} else if opts.IndexCacheSize > 0 {
		badgerOpts = badgerOpts.WithIndexCacheSize(opts.IndexCacheSize)
	}

	if opts.BlockCacheSize > 0 {
		badgerOpts = badgerOpts.WithBlockCacheSize(opts.BlockCacheSize)
	}

	// 禁用 Badger 的默认日志输出
	if opts.Logger != nil {
		badgerOpts = badgerOpts.WithLogger(opts.Logger)
	} else {
		badgerOpts = badgerOpts.WithLogger(nil)
	}

	// 确保目录存在
	if !opts.InMemory && abs != "" {
		if err := os.MkdirAll(abs, 0755); err != nil {
			return nil, fmt.Errorf("failed to create directory: %w", err)
		}
	}

	db, err := badger.Open(badgerOpts)
	if err != nil {
		return nil, err
	}

	return &Store{
		path: abs,
		db:   db,
	}, nil
}

// Close 关闭 Badger DB。如果是共享实例，减少引用计数，只有计数为 0 时才真正关闭。
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.db == nil {
		return nil
	}

	// 非共享模式，直接关闭
	if s.shared == nil {
		err := s.db.Close()
		s.db = nil
		return err
	}

	// 共享模式，减少引用计数
	sharedDBRegistryMu.Lock()
	defer sharedDBRegistryMu.Unlock()

	newCount := atomic.AddInt32(&s.shared.refCount, -1)
	if newCount <= 0 {
		// 最后一个引用，真正关闭 badger 实例
		delete(sharedDBRegistry, s.shared.path)
		err := s.db.Close()
		s.db = nil
		return err
	}

	// 还有其他引用，不真正关闭
	s.db = nil
	return nil
}

// WithUpdate 在写事务中执行 fn。
// Badger 的 Update 是线程安全的，无需额外加锁。
func (s *Store) WithUpdate(ctx context.Context, fn func(txn *badger.Txn) error) error {
	// 检查 context
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	db := s.db
	if db == nil {
		return errors.New("badger store not opened")
	}

	return db.Update(func(txn *badger.Txn) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return fn(txn)
		}
	})
}

// WithView 在读事务中执行 fn。
// Badger 的 View 是线程安全的，无需额外加锁。
func (s *Store) WithView(ctx context.Context, fn func(txn *badger.Txn) error) error {
	db := s.db
	if db == nil {
		return errors.New("badger store not opened")
	}

	return db.View(func(txn *badger.Txn) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return fn(txn)
		}
	})
}

// Path 返回数据库文件路径。
func (s *Store) Path() string {
	return s.path
}

// Backup 备份数据库到指定文件路径。
// Badger 的 Backup 是线程安全的，无需额外加锁。
func (s *Store) Backup(ctx context.Context, backupPath string) error {
	db := s.db
	if db == nil {
		return errors.New("badger store not opened")
	}

	// 创建备份文件
	backupFile, err := os.Create(backupPath)
	if err != nil {
		return fmt.Errorf("failed to create backup file: %w", err)
	}
	defer backupFile.Close()

	// 使用 Badger 的 Backup 方法
	_, err = db.Backup(backupFile, 0)
	return err
}

// DB 返回底层 Badger 数据库实例（供高级用法）。
func (s *Store) DB() *badger.DB {
	return s.db
}

// RefCount 返回当前共享实例的引用计数（非共享模式返回 1）。
func (s *Store) RefCount() int32 {
	if s.shared == nil {
		return 1
	}
	return atomic.LoadInt32(&s.shared.refCount)
}

// IsShared 返回当前存储是否使用共享模式。
func (s *Store) IsShared() bool {
	return s.shared != nil
}

// GetSharedDBCount 返回当前共享的 badger 实例数量（全局）。
func GetSharedDBCount() int {
	sharedDBRegistryMu.Lock()
	defer sharedDBRegistryMu.Unlock()
	return len(sharedDBRegistry)
}

// CloseAllShared 关闭所有共享的 badger 实例（用于测试清理）。
func CloseAllShared() error {
	sharedDBRegistryMu.Lock()
	defer sharedDBRegistryMu.Unlock()

	var lastErr error
	for path, shared := range sharedDBRegistry {
		if err := shared.db.Close(); err != nil {
			lastErr = err
		}
		delete(sharedDBRegistry, path)
	}
	return lastErr
}

// BucketKey 生成带 bucket 前缀的 key（用于实现逻辑 bucket 分组）。
func BucketKey(bucket, key string) []byte {
	return []byte(bucket + ":" + key)
}

// BucketPrefix 返回 bucket 的前缀（用于迭代）。
func BucketPrefix(bucket string) []byte {
	return []byte(bucket + ":")
}

// Get 从指定 bucket 获取值。
func (s *Store) Get(ctx context.Context, bucket, key string) ([]byte, error) {
	var value []byte
	err := s.WithView(ctx, func(txn *badger.Txn) error {
		item, err := txn.Get(BucketKey(bucket, key))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return nil
			}
			return err
		}
		value, err = item.ValueCopy(nil)
		return err
	})
	return value, err
}

// Set 在指定 bucket 设置值。
func (s *Store) Set(ctx context.Context, bucket, key string, value []byte) error {
	return s.WithUpdate(ctx, func(txn *badger.Txn) error {
		return txn.Set(BucketKey(bucket, key), value)
	})
}

// Delete 从指定 bucket 删除值。
func (s *Store) Delete(ctx context.Context, bucket, key string) error {
	return s.WithUpdate(ctx, func(txn *badger.Txn) error {
		return txn.Delete(BucketKey(bucket, key))
	})
}

// Iterate 迭代指定 bucket 中的所有键值对。
func (s *Store) Iterate(ctx context.Context, bucket string, fn func(key, value []byte) error) error {
	prefix := BucketPrefix(bucket)
	prefixLen := len(prefix)

	return s.WithView(ctx, func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			item := it.Item()
			key := item.Key()[prefixLen:] // 去掉 bucket 前缀

			err := item.Value(func(val []byte) error {
				return fn(key, val)
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
}
