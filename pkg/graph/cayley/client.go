package cayley

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"sync"

	badgerdb "github.com/dgraph-io/badger/v4"
	"github.com/mozhou-tech/rxdb-go/pkg/storage/badger"
	"github.com/sirupsen/logrus"
)

// Client 封装图数据库客户端
// 支持内存和 Badger 持久化存储
type Client struct {
	// 内存存储（当 backend == "memory" 时使用）
	quads map[string]map[string]map[string]bool // subject -> predicate -> object -> exists
	// Badger 存储（当 backend == "badger" 时使用）
	store   *badger.Store
	backend string
	path    string
	mu      sync.RWMutex
	closed  bool
}

// Options 配置图数据库客户端选项
type Options struct {
	// Backend 存储后端类型：bolt, leveldb, badger, memory
	Backend string
	// Path 存储路径
	Path string
}

// NewClient 创建新的图数据库客户端
func NewClient(opts Options) (*Client, error) {
	if opts.Backend == "" {
		opts.Backend = "badger" // 默认使用 Badger 持久化存储
	}

	client := &Client{
		backend: opts.Backend,
		path:    opts.Path,
		closed:  false,
	}

	// 根据后端类型初始化存储
	switch opts.Backend {
	case "memory":
		client.quads = make(map[string]map[string]map[string]bool)
		logrus.Info("[Graph] Using memory backend")

	case "badger":
		// 如果是文件存储，确保目录存在
		if opts.Path == "" {
			return nil, fmt.Errorf("badger backend requires a path")
		}
		if err := os.MkdirAll(opts.Path, 0755); err != nil {
			return nil, fmt.Errorf("failed to create graph database directory: %w", err)
		}

		store, err := badger.Open(opts.Path, badger.Options{
			InMemory:   false,
			SyncWrites: false, // 异步写入性能更好
		})
		if err != nil {
			return nil, fmt.Errorf("failed to open badger store: %w", err)
		}
		client.store = store
		logrus.WithField("path", opts.Path).Info("[Graph] Using Badger backend")

	default:
		return nil, fmt.Errorf("unsupported backend: %s", opts.Backend)
	}

	return client, nil
}

// Close 关闭图数据库连接
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}

	if c.backend == "memory" {
		c.quads = nil
	} else if c.store != nil {
		if err := c.store.Close(); err != nil {
			return fmt.Errorf("failed to close badger store: %w", err)
		}
		c.store = nil
	}

	c.closed = true
	return nil
}

// quadKey 生成四元组的 key
func quadKey(subject, predicate, object string) []byte {
	// 格式: quad:{subject}:{predicate}:{object}
	return []byte(fmt.Sprintf("quad:%s:%s:%s", subject, predicate, object))
}

// indexKeySP 生成 subject-predicate 索引的 key
func indexKeySP(subject, predicate string) []byte {
	return []byte(fmt.Sprintf("idx:sp:%s:%s", subject, predicate))
}

// indexKeyPO 生成 predicate-object 索引的 key
func indexKeyPO(predicate, object string) []byte {
	return []byte(fmt.Sprintf("idx:po:%s:%s", predicate, object))
}

// AddQuad 添加四元组（三元组 + 标签）
func (c *Client) AddQuad(ctx context.Context, subject, predicate, object string, label ...string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return fmt.Errorf("graph database is closed")
	}

	if c.backend == "memory" {
		if c.quads[subject] == nil {
			c.quads[subject] = make(map[string]map[string]bool)
		}
		if c.quads[subject][predicate] == nil {
			c.quads[subject][predicate] = make(map[string]bool)
		}
		c.quads[subject][predicate][object] = true
		return nil
	}

	// Badger 后端
	if c.store == nil {
		return fmt.Errorf("badger store not initialized")
	}

	// 使用事务写入
	return c.store.WithUpdate(ctx, func(txn *badgerdb.Txn) error {
		// 存储四元组（值存储为 1，表示存在）
		key := quadKey(subject, predicate, object)
		value := make([]byte, 8)
		binary.BigEndian.PutUint64(value, 1)
		if err := txn.Set(key, value); err != nil {
			return fmt.Errorf("failed to set quad: %w", err)
		}

		// 创建索引以便快速查询
		// SP 索引：subject -> predicate -> objects
		spKey := append(indexKeySP(subject, predicate), []byte(":"+object)...)
		spValue := []byte(object)
		if err := txn.Set(spKey, spValue); err != nil {
			return fmt.Errorf("failed to set SP index: %w", err)
		}

		// PO 索引：predicate -> object -> subjects
		poKey := append(indexKeyPO(predicate, object), []byte(":"+subject)...)
		poValue := []byte(subject)
		if err := txn.Set(poKey, poValue); err != nil {
			return fmt.Errorf("failed to set PO index: %w", err)
		}

		return nil
	})
}

// RemoveQuad 删除四元组
func (c *Client) RemoveQuad(ctx context.Context, subject, predicate, object string, label ...string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		logrus.Error("[Graph] RemoveQuad failed - graph database is closed")
		return fmt.Errorf("graph database is closed")
	}

	if c.backend == "memory" {
		if c.quads[subject] != nil && c.quads[subject][predicate] != nil {
			delete(c.quads[subject][predicate], object)
			if len(c.quads[subject][predicate]) == 0 {
				delete(c.quads[subject], predicate)
			}
			if len(c.quads[subject]) == 0 {
				delete(c.quads, subject)
			}
			logrus.WithFields(logrus.Fields{
				"subject":   subject,
				"predicate": predicate,
				"object":    object,
			}).Debug("[Graph] RemoveQuad")
		}
		return nil
	}

	// Badger 后端
	if c.store == nil {
		return fmt.Errorf("badger store not initialized")
	}

	return c.store.WithUpdate(ctx, func(txn *badgerdb.Txn) error {
		// 删除四元组
		key := quadKey(subject, predicate, object)
		if err := txn.Delete(key); err != nil {
			return fmt.Errorf("failed to delete quad: %w", err)
		}

		// 删除索引
		spKey := append(indexKeySP(subject, predicate), []byte(":"+object)...)
		_ = txn.Delete(spKey) // 忽略错误，可能不存在

		poKey := append(indexKeyPO(predicate, object), []byte(":"+subject)...)
		_ = txn.Delete(poKey) // 忽略错误，可能不存在

		logrus.WithFields(logrus.Fields{
			"subject":   subject,
			"predicate": predicate,
			"object":    object,
		}).Debug("[Graph] RemoveQuad")
		return nil
	})
}

// Link 创建两个节点之间的链接（便捷方法）
func (c *Client) Link(ctx context.Context, from, relation, to string) error {
	logrus.WithFields(logrus.Fields{
		"from":     from,
		"relation": relation,
		"to":       to,
	}).Info("[Graph] Link")
	return c.AddQuad(ctx, from, relation, to)
}

// Unlink 删除两个节点之间的链接（便捷方法）
func (c *Client) Unlink(ctx context.Context, from, relation, to string) error {
	logrus.WithFields(logrus.Fields{
		"from":     from,
		"relation": relation,
		"to":       to,
	}).Info("[Graph] Unlink")
	return c.RemoveQuad(ctx, from, relation, to)
}

// Path 返回存储路径
func (c *Client) Path() string {
	return c.path
}

// IsClosed 检查客户端是否已关闭
func (c *Client) IsClosed() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.closed
}

// getQuadsBySubject 从存储获取指定 subject 的所有四元组
func (c *Client) getQuadsBySubject(ctx context.Context, subject string) (map[string]map[string]bool, error) {
	if c.backend == "memory" {
		c.mu.RLock()
		defer c.mu.RUnlock()
		if c.quads[subject] == nil {
			return make(map[string]map[string]bool), nil
		}
		// 深拷贝
		result := make(map[string]map[string]bool)
		for pred, objects := range c.quads[subject] {
			result[pred] = make(map[string]bool)
			for obj := range objects {
				result[pred][obj] = true
			}
		}
		return result, nil
	}

	// Badger 后端
	if c.store == nil {
		return nil, fmt.Errorf("badger store not initialized")
	}

	result := make(map[string]map[string]bool)
	prefix := []byte(fmt.Sprintf("quad:%s:", subject))

	err := c.store.WithView(ctx, func(txn *badgerdb.Txn) error {
		opts := badgerdb.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := string(item.Key())
			// 解析 key: quad:{subject}:{predicate}:{object}
			// 跳过 "quad:" 和 subject
			rest := key[len(prefix):]
			// 找到第一个冒号，分割 predicate 和 object
			predEnd := 0
			for i, ch := range rest {
				if ch == ':' {
					predEnd = i
					break
				}
			}
			if predEnd == 0 {
				continue
			}
			predicate := rest[:predEnd]
			object := rest[predEnd+1:]

			if result[predicate] == nil {
				result[predicate] = make(map[string]bool)
			}
			result[predicate][object] = true
		}
		return nil
	})

	return result, err
}

// getQuadsByObject 从存储获取指向指定 object 的所有四元组（反向查询）
func (c *Client) getQuadsByObject(ctx context.Context, object string) ([]QueryResult, error) {
	if c.backend == "memory" {
		c.mu.RLock()
		defer c.mu.RUnlock()
		var results []QueryResult
		for subject, preds := range c.quads {
			for pred, objects := range preds {
				if objects[object] {
					results = append(results, QueryResult{
						Subject:   subject,
						Predicate: pred,
						Object:    object,
					})
				}
			}
		}
		return results, nil
	}

	// Badger 后端：遍历所有四元组查找指向 object 的
	if c.store == nil {
		return nil, fmt.Errorf("badger store not initialized")
	}

	var results []QueryResult
	prefix := []byte("quad:")

	err := c.store.WithView(ctx, func(txn *badgerdb.Txn) error {
		opts := badgerdb.DefaultIteratorOptions
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := string(item.Key())
			// 解析 key: quad:{subject}:{predicate}:{object}
			if len(key) < len("quad:") {
				continue
			}
			rest := key[len("quad:"):]
			// 分割 subject:predicate:object
			parts := splitKey(rest, ":")
			if len(parts) < 3 {
				continue
			}
			subject := parts[0]
			predicate := parts[1]
			obj := parts[2]

			if obj == object {
				results = append(results, QueryResult{
					Subject:   subject,
					Predicate: predicate,
					Object:    object,
				})
			}
		}
		return nil
	})

	return results, err
}

// splitKey 分割 key 字符串
func splitKey(s string, sep string) []string {
	var parts []string
	start := 0
	for i := 0; i < len(s); i++ {
		if i+len(sep) <= len(s) && s[i:i+len(sep)] == sep {
			parts = append(parts, s[start:i])
			start = i + len(sep)
			i += len(sep) - 1
		}
	}
	if start < len(s) {
		parts = append(parts, s[start:])
	}
	return parts
}
