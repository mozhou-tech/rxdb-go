package rxdb

import (
	"encoding/json"
	"sync"
	"unsafe"
)

// unsafeB2S 将 byte slice 转换为 string，不涉及内存分配。
// 注意：转换后的 string 与原始 []byte 共享内存，因此原始 []byte 不应再被修改。
func unsafeB2S(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(b), len(b))
}

// unsafeS2B 将 string 转换为 byte slice，不涉及内存分配。
// 注意：返回的 []byte 不应被修改，因为 string 在 Go 中是不可变的。
func unsafeS2B(s string) []byte {
	if s == "" {
		return nil
	}
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

var (
	// documentPool 用于复用 document 对象，压榨内存申请
	documentPool = sync.Pool{
		New: func() interface{} {
			return &document{}
		},
	}

	// changeEventPool 用于复用 ChangeEvent 指针
	changeEventPool = sync.Pool{
		New: func() interface{} {
			return &ChangeEvent{}
		},
	}
)

// acquireDocument 从池中获取一个 document 对象
func acquireDocument(id string, data map[string]any, col *collection) *document {
	d := documentPool.Get().(*document)
	d.id = id
	d.data = data
	d.collection = col
	if col != nil {
		d.revField = col.schema.RevField
	}
	return d
}

// releaseDocument 将 document 对象放回池中
// 注意：只有在确定用户不再使用该对象时才能释放，目前主要用于内部临时对象
func releaseDocument(d *document) {
	d.id = ""
	d.data = nil
	d.collection = nil
	d.revField = ""
	d.changes = nil
	documentPool.Put(d)
}

// encodeIndexKey 编码索引键，将值和文档 ID 组合在一起，避免序列化数组的开销。
func encodeIndexKey(values []any, docID string) []byte {
	// 简单实现：使用 JSON 序列化值作为前缀（保持原有兼容性的最快方式，但支持范围查询需要更复杂的编码）
	// 为了真正支持有序范围查询，后续应改用二进制编码（如 byte-order-mark 或 fixed-width）
	valBytes, _ := json.Marshal(values)
	if docID == "" {
		return valBytes
	}
	// 在值和 ID 之间添加分隔符，确保前缀匹配准确
	res := make([]byte, 0, len(valBytes)+1+len(docID))
	res = append(res, valBytes...)
	res = append(res, 0x00) // 使用空字节作为分隔符
	res = append(res, []byte(docID)...)
	return res
}

// decodeIndexKey 从索引键中提取文档 ID。
func decodeIndexKey(key []byte) string {
	for i := len(key) - 1; i >= 0; i-- {
		if key[i] == 0x00 {
			return unsafeB2S(key[i+1:])
		}
	}
	return ""
}

// DeepCloneMap 对 map[string]any 进行深拷贝。
func DeepCloneMap(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	newMap := make(map[string]any, len(m))
	for k, v := range m {
		newMap[k] = deepCloneValue(v)
	}
	return newMap
}

func deepCloneValue(v any) any {
	switch val := v.(type) {
	case map[string]any:
		return DeepCloneMap(val)
	case []any:
		newSlice := make([]any, len(val))
		for i, item := range val {
			newSlice[i] = deepCloneValue(item)
		}
		return newSlice
	case []map[string]any:
		newSlice := make([]map[string]any, len(val))
		for i, item := range val {
			newSlice[i] = DeepCloneMap(item)
		}
		return newSlice
	case int:
		return float64(val)
	case int8:
		return float64(val)
	case int16:
		return float64(val)
	case int32:
		return float64(val)
	case int64:
		return float64(val)
	case uint:
		return float64(val)
	case uint8:
		return float64(val)
	case uint16:
		return float64(val)
	case uint32:
		return float64(val)
	case uint64:
		return float64(val)
	case float32:
		return float64(val)
	default:
		return v
	}
}
