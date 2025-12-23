package rxdb

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
