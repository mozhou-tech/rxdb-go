package bolt

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

// Store 封装 Bolt 数据库句柄，后续提供集合级别的 CRUD/查询接口。
type Store struct {
	path string

	db   *bolt.DB
	lock sync.RWMutex
}

// Options 控制 Bolt 打开参数。
type Options struct {
	// FileMode 设置 DB 文件权限，默认 0600。
	FileMode uint32
	// Timeout 控制获取文件锁等待时间。
	Timeout time.Duration
	// NoSync 允许禁用 fsync（换取性能但降低持久性）。
	NoSync bool
}

// Open 创建或打开 Bolt DB。
func Open(path string, opts Options) (*Store, error) {
	if path == "" {
		return nil, errors.New("bolt store path required")
	}
	if opts.FileMode == 0 {
		opts.FileMode = 0o600
	}
	if opts.Timeout == 0 {
		opts.Timeout = 1 * time.Second
	}

	abs := path
	if !filepath.IsAbs(path) {
		if p, err := filepath.Abs(path); err == nil {
			abs = p
		}
	}

	db, err := bolt.Open(abs, os.FileMode(opts.FileMode), &bolt.Options{
		Timeout: opts.Timeout,
		NoSync:  opts.NoSync,
	})
	if err != nil {
		return nil, err
	}

	return &Store{
		path: abs,
		db:   db,
	}, nil
}

// Close 关闭 Bolt DB。
func (s *Store) Close() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.db == nil {
		return nil
	}
	return s.db.Close()
}

// WithUpdate 在写事务中执行 fn。
func (s *Store) WithUpdate(ctx context.Context, fn func(tx *bolt.Tx) error) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.db == nil {
		return errors.New("bolt store not opened")
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return fn(tx)
		}
	})
}

// WithView 在读事务中执行 fn。
func (s *Store) WithView(ctx context.Context, fn func(tx *bolt.Tx) error) error {
	s.lock.RLock()
	defer s.lock.RUnlock()
	if s.db == nil {
		return errors.New("bolt store not opened")
	}
	return s.db.View(func(tx *bolt.Tx) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return fn(tx)
		}
	})
}

// Path 返回数据库文件路径。
func (s *Store) Path() string {
	return s.path
}

// Backup 备份数据库到指定文件路径。
func (s *Store) Backup(ctx context.Context, backupPath string) error {
	s.lock.RLock()
	defer s.lock.RUnlock()

	if s.db == nil {
		return errors.New("bolt store not opened")
	}

	// 创建备份文件
	backupFile, err := os.Create(backupPath)
	if err != nil {
		return fmt.Errorf("failed to create backup file: %w", err)
	}
	defer backupFile.Close()

	// 使用 Bolt 的 WriteTo 方法进行备份
	return s.db.View(func(tx *bolt.Tx) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			_, err := tx.WriteTo(backupFile)
			return err
		}
	})
}
