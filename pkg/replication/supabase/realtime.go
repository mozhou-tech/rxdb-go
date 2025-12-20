package supabase

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/mozhou-tech/rxdb-go/pkg/rxdb"
)

// RealtimeEvent Supabase Realtime 事件类型。
type RealtimeEvent string

const (
	RealtimeInsert RealtimeEvent = "INSERT"
	RealtimeUpdate RealtimeEvent = "UPDATE"
	RealtimeDelete RealtimeEvent = "DELETE"
)

// RealtimeMessage Supabase Realtime 消息结构。
type RealtimeMessage struct {
	Event   string          `json:"event"`
	Topic   string          `json:"topic"`
	Payload json.RawMessage `json:"payload"`
	Ref     string          `json:"ref"`
}

// RealtimePayload Realtime 变更负载。
type RealtimePayload struct {
	Schema     string         `json:"schema"`
	Table      string         `json:"table"`
	CommitTime string         `json:"commit_timestamp"`
	EventType  RealtimeEvent  `json:"eventType"`
	New        map[string]any `json:"new"`
	Old        map[string]any `json:"old"`
	Errors     []string       `json:"errors"`
}

// RealtimeOptions Realtime 连接选项。
type RealtimeOptions struct {
	// SupabaseURL Supabase 项目 URL
	SupabaseURL string
	// SupabaseKey Supabase API Key
	SupabaseKey string
	// Table 监听的表名
	Table string
	// Schema 数据库 schema，默认 public
	Schema string
	// PrimaryKey 主键字段名
	PrimaryKey string
	// ReconnectInterval 重连间隔
	ReconnectInterval time.Duration
	// HeartbeatInterval 心跳间隔
	HeartbeatInterval time.Duration
}

// RealtimeSubscription Realtime 订阅。
type RealtimeSubscription struct {
	opts       RealtimeOptions
	collection rxdb.Collection
	conn       *websocket.Conn
	mu         sync.RWMutex
	stopChan   chan struct{}
	errChan    chan error
	connected  bool
	ref        int
}

// NewRealtimeSubscription 创建 Realtime 订阅。
func NewRealtimeSubscription(collection rxdb.Collection, opts RealtimeOptions) (*RealtimeSubscription, error) {
	if opts.SupabaseURL == "" {
		return nil, fmt.Errorf("supabase URL is required")
	}
	if opts.SupabaseKey == "" {
		return nil, fmt.Errorf("supabase API key is required")
	}
	if opts.Table == "" {
		return nil, fmt.Errorf("table name is required")
	}
	if opts.Schema == "" {
		opts.Schema = "public"
	}
	if opts.PrimaryKey == "" {
		opts.PrimaryKey = "id"
	}
	if opts.ReconnectInterval == 0 {
		opts.ReconnectInterval = 5 * time.Second
	}
	if opts.HeartbeatInterval == 0 {
		opts.HeartbeatInterval = 30 * time.Second
	}

	return &RealtimeSubscription{
		opts:       opts,
		collection: collection,
		stopChan:   make(chan struct{}),
		errChan:    make(chan error, 10),
	}, nil
}

// Start 启动 Realtime 订阅。
func (rs *RealtimeSubscription) Start(ctx context.Context) error {
	go rs.connectLoop(ctx)
	return nil
}

// Stop 停止订阅。
func (rs *RealtimeSubscription) Stop() {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	select {
	case <-rs.stopChan:
		return
	default:
		close(rs.stopChan)
	}

	if rs.conn != nil {
		rs.conn.Close()
		rs.conn = nil
	}
	rs.connected = false
}

// Connected 返回连接状态。
func (rs *RealtimeSubscription) Connected() bool {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.connected
}

// Errors 返回错误通道。
func (rs *RealtimeSubscription) Errors() <-chan error {
	return rs.errChan
}

// connectLoop 连接循环。
func (rs *RealtimeSubscription) connectLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-rs.stopChan:
			return
		default:
		}

		if err := rs.connect(ctx); err != nil {
			rs.sendError(err)
		}

		// 等待重连
		select {
		case <-ctx.Done():
			return
		case <-rs.stopChan:
			return
		case <-time.After(rs.opts.ReconnectInterval):
		}
	}
}

// connect 建立 WebSocket 连接。
func (rs *RealtimeSubscription) connect(ctx context.Context) error {
	// 构建 WebSocket URL
	wsURL := strings.Replace(rs.opts.SupabaseURL, "https://", "wss://", 1)
	wsURL = strings.Replace(wsURL, "http://", "ws://", 1)
	wsURL = fmt.Sprintf("%s/realtime/v1/websocket?apikey=%s&vsn=1.0.0", wsURL, rs.opts.SupabaseKey)

	// 建立连接
	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}

	header := http.Header{}
	header.Set("apikey", rs.opts.SupabaseKey)

	conn, _, err := dialer.DialContext(ctx, wsURL, header)
	if err != nil {
		return fmt.Errorf("failed to connect to realtime: %w", err)
	}

	rs.mu.Lock()
	rs.conn = conn
	rs.connected = true
	rs.mu.Unlock()

	// 订阅表变更
	if err := rs.subscribe(); err != nil {
		conn.Close()
		return err
	}

	// 启动心跳
	go rs.heartbeatLoop(ctx)

	// 读取消息
	return rs.readLoop(ctx)
}

// subscribe 订阅表变更。
func (rs *RealtimeSubscription) subscribe() error {
	rs.ref++
	topic := fmt.Sprintf("realtime:%s:%s", rs.opts.Schema, rs.opts.Table)

	msg := map[string]any{
		"topic":   topic,
		"event":   "phx_join",
		"payload": map[string]any{},
		"ref":     fmt.Sprintf("%d", rs.ref),
	}

	return rs.conn.WriteJSON(msg)
}

// heartbeatLoop 心跳循环。
func (rs *RealtimeSubscription) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(rs.opts.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-rs.stopChan:
			return
		case <-ticker.C:
			rs.mu.RLock()
			conn := rs.conn
			connected := rs.connected
			rs.mu.RUnlock()

			if !connected || conn == nil {
				return
			}

			rs.ref++
			msg := map[string]any{
				"topic":   "phoenix",
				"event":   "heartbeat",
				"payload": map[string]any{},
				"ref":     fmt.Sprintf("%d", rs.ref),
			}

			if err := conn.WriteJSON(msg); err != nil {
				rs.sendError(fmt.Errorf("heartbeat failed: %w", err))
				return
			}
		}
	}
}

// readLoop 读取消息循环。
func (rs *RealtimeSubscription) readLoop(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-rs.stopChan:
			return nil
		default:
		}

		rs.mu.RLock()
		conn := rs.conn
		rs.mu.RUnlock()

		if conn == nil {
			return fmt.Errorf("connection closed")
		}

		var msg RealtimeMessage
		if err := conn.ReadJSON(&msg); err != nil {
			rs.mu.Lock()
			rs.connected = false
			rs.mu.Unlock()
			return fmt.Errorf("read error: %w", err)
		}

		// 处理消息
		if err := rs.handleMessage(ctx, msg); err != nil {
			rs.sendError(err)
		}
	}
}

// handleMessage 处理接收到的消息。
func (rs *RealtimeSubscription) handleMessage(ctx context.Context, msg RealtimeMessage) error {
	switch msg.Event {
	case "phx_reply":
		// 连接确认
		return nil
	case "postgres_changes":
		// 数据变更
		return rs.handleChange(ctx, msg.Payload)
	case "phx_error":
		return fmt.Errorf("realtime error: %s", string(msg.Payload))
	}
	return nil
}

// handleChange 处理数据变更。
func (rs *RealtimeSubscription) handleChange(ctx context.Context, payload json.RawMessage) error {
	var change RealtimePayload
	if err := json.Unmarshal(payload, &change); err != nil {
		return fmt.Errorf("failed to parse change payload: %w", err)
	}

	if len(change.Errors) > 0 {
		return fmt.Errorf("realtime change errors: %v", change.Errors)
	}

	switch change.EventType {
	case RealtimeInsert:
		_, err := rs.collection.Insert(ctx, change.New)
		if err != nil {
			// 如果已存在，尝试 upsert
			_, err = rs.collection.Upsert(ctx, change.New)
		}
		return err
	case RealtimeUpdate:
		_, err := rs.collection.Upsert(ctx, change.New)
		return err
	case RealtimeDelete:
		id, ok := change.Old[rs.opts.PrimaryKey]
		if !ok {
			return fmt.Errorf("delete event missing primary key")
		}
		return rs.collection.Remove(ctx, fmt.Sprintf("%v", id))
	}

	return nil
}

// sendError 发送错误。
func (rs *RealtimeSubscription) sendError(err error) {
	select {
	case rs.errChan <- err:
	default:
	}
}
