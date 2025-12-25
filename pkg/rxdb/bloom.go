package rxdb

import (
	"github.com/bits-and-blooms/bloom/v3"
)

// BloomFilter 封装了布隆过滤器的基本操作。
type BloomFilter struct {
	filter *bloom.BloomFilter
}

// NewBloomFilter 创建一个新的布隆过滤器。
// n 是预期的元素数量，fp 是可接受的误报率（如 0.01 表示 1%）。
func NewBloomFilter(n uint, fp float64) *BloomFilter {
	if n == 0 {
		n = 1000 // 默认初始容量
	}
	return &BloomFilter{
		filter: bloom.NewWithEstimates(n, fp),
	}
}

// Add 向布隆过滤器中添加一个元素。
func (bf *BloomFilter) Add(item string) {
	if bf == nil || bf.filter == nil {
		return
	}
	bf.filter.AddString(item)
}

// Test 检查元素是否可能存在于布隆过滤器中。
func (bf *BloomFilter) Test(item string) bool {
	if bf == nil || bf.filter == nil {
		return true // 如果过滤器未初始化，默认返回可能存在以防漏掉
	}
	return bf.filter.TestString(item)
}

// Clear 重置布隆过滤器。
func (bf *BloomFilter) Clear() {
	if bf == nil || bf.filter == nil {
		return
	}
	bf.filter.ClearAll()
}

// MarshalBinary 将布隆过滤器序列化为字节数组。
func (bf *BloomFilter) MarshalBinary() ([]byte, error) {
	if bf == nil || bf.filter == nil {
		return nil, nil
	}
	return bf.filter.MarshalBinary()
}

// UnmarshalBinary 从字节数组反序列化布隆过滤器。
func (bf *BloomFilter) UnmarshalBinary(data []byte) error {
	if bf == nil {
		return nil
	}
	if bf.filter == nil {
		bf.filter = &bloom.BloomFilter{}
	}
	return bf.filter.UnmarshalBinary(data)
}
