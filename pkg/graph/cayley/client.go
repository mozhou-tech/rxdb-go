package cayley

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Client 封装图数据库客户端（简化实现，可作为接口供后续集成 Cayley 服务）
// 当前提供内存实现，后续可以替换为 Cayley HTTP API 客户端
type Client struct {
	// 内存存储（简化实现）
	quads map[string]map[string]map[string]bool // subject -> predicate -> object -> exists
	path  string
	mu    sync.RWMutex
	closed bool
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
		opts.Backend = "memory" // 默认使用内存模式
	}

	// 如果是文件存储，确保目录存在
	if opts.Path != "" && opts.Backend != "memory" {
		if err := os.MkdirAll(filepath.Dir(opts.Path), 0755); err != nil {
			return nil, fmt.Errorf("failed to create graph database directory: %w", err)
		}
	}

	return &Client{
		quads:  make(map[string]map[string]map[string]bool),
		path:   opts.Path,
		closed: false,
	}, nil
}

// Close 关闭图数据库连接
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}

	c.quads = nil
	c.closed = true
	return nil
}

// AddQuad 添加四元组（三元组 + 标签）
func (c *Client) AddQuad(ctx context.Context, subject, predicate, object string, label ...string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return fmt.Errorf("graph database is closed")
	}

	if c.quads[subject] == nil {
		c.quads[subject] = make(map[string]map[string]bool)
	}
	if c.quads[subject][predicate] == nil {
		c.quads[subject][predicate] = make(map[string]bool)
	}
	c.quads[subject][predicate][object] = true

	return nil
}

// RemoveQuad 删除四元组
func (c *Client) RemoveQuad(ctx context.Context, subject, predicate, object string, label ...string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return fmt.Errorf("graph database is closed")
	}

	if c.quads[subject] != nil && c.quads[subject][predicate] != nil {
		delete(c.quads[subject][predicate], object)
		if len(c.quads[subject][predicate]) == 0 {
			delete(c.quads[subject], predicate)
		}
		if len(c.quads[subject]) == 0 {
			delete(c.quads, subject)
		}
	}

	return nil
}

// Link 创建两个节点之间的链接（便捷方法）
func (c *Client) Link(ctx context.Context, from, relation, to string) error {
	return c.AddQuad(ctx, from, relation, to)
}

// Unlink 删除两个节点之间的链接（便捷方法）
func (c *Client) Unlink(ctx context.Context, from, relation, to string) error {
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
