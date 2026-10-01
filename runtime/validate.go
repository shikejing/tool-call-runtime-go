package runtime

import (
	"fmt"
	"math"
	"reflect"
	"sort"
)

// Validate 按 schema 校验 args（顶层视为 object）。
// 返回首个可定位到参数路径的拒绝原因；合法返回 nil。
// 校验顺序：类型 -> 必填 -> 未声明字段 -> 取值范围/枚举 -> 递归嵌套。
func Validate(schema *ParamSchema, args map[string]any) *RejectError {
	if schema == nil {
		return nil
	}
	if schema.Type != "" && schema.Type != TypeObject {
		return reject(KindNotObject, "$", "top-level params must be object, got %s", schema.Type)
	}
	return validateObject(schema, args, "$")
}

func validateObject(schema *ParamSchema, obj map[string]any, path string) *RejectError {
	// 1. 必填字段
	names := make([]string, 0, len(schema.Properties))
	for n := range schema.Properties {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		sub := schema.Properties[n]
		if sub.Required {
			if _, ok := obj[n]; !ok {
				return reject(KindMissingRequired, path+"."+n, "required field missing")
			}
		}
	}
	// 2. 未声明字段
	keys := make([]string, 0, len(obj))
	for k := range obj {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if _, ok := schema.Properties[k]; !ok {
			return reject(KindUnknownField, path+"."+k, "field not declared in schema")
		}
	}
	// 3. 逐字段校验
	for _, k := range keys {
		if r := validateValue(schema.Properties[k], obj[k], path+"."+k); r != nil {
			return r
		}
	}
	return nil
}

func validateValue(schema *ParamSchema, v any, path string) *RejectError {
	if v == nil {
		if schema.Required {
			return reject(KindTypeMismatch, path, "null not allowed")
		}
		return nil
	}
	switch schema.Type {
	case TypeString:
		s, ok := v.(string)
		if !ok {
			return reject(KindTypeMismatch, path, "expect string, got %s", typeName(v))
		}
		if schema.MinLen != nil && len(s) < *schema.MinLen {
			return reject(KindOutOfRange, path, "string length %d < min %d", len(s), *schema.MinLen)
		}
		if schema.MaxLen != nil && len(s) > *schema.MaxLen {
			return reject(KindOutOfRange, path, "string length %d > max %d", len(s), *schema.MaxLen)
		}
	case TypeNumber, TypeInteger:
		f, ok := toFloat(v)
		if !ok {
			return reject(KindTypeMismatch, path, "expect number, got %s", typeName(v))
		}
		if schema.Type == TypeInteger && f != math.Trunc(f) {
			return reject(KindTypeMismatch, path, "expect integer, got %v", f)
		}
		if schema.Min != nil && f < *schema.Min {
			return reject(KindOutOfRange, path, "value %v < min %v", f, *schema.Min)
		}
		if schema.Max != nil && f > *schema.Max {
			return reject(KindOutOfRange, path, "value %v > max %v", f, *schema.Max)
		}
	case TypeBoolean:
		if _, ok := v.(bool); !ok {
			return reject(KindTypeMismatch, path, "expect boolean, got %s", typeName(v))
		}
	case TypeObject:
		m, ok := v.(map[string]any)
		if !ok {
			return reject(KindTypeMismatch, path, "expect object, got %s", typeName(v))
		}
		return validateObject(schema, m, path)
	case TypeArray:
		arr, ok := v.([]any)
		if !ok {
			return reject(KindTypeMismatch, path, "expect array, got %s", typeName(v))
		}
		if schema.Items != nil {
			for i, e := range arr {
				if r := validateValue(schema.Items, e, fmt.Sprintf("%s[%d]", path, i)); r != nil {
					return r
				}
			}
		}
	default:
		return reject(KindTypeMismatch, path, "unknown schema type %q", schema.Type)
	}
	// 枚举
	if len(schema.Enum) > 0 {
		found := false
		for _, e := range schema.Enum {
			if reflect.DeepEqual(e, v) {
				found = true
				break
			}
		}
		if !found {
			return reject(KindEnumViolation, path, "value %v not in enum", v)
		}
	}
	return nil
}

func toFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case int32:
		return float64(n), true
	}
	return 0, false
}

func typeName(v any) string {
	switch v.(type) {
	case string:
		return "string"
	case bool:
		return "boolean"
	case map[string]any:
		return "object"
	case []any:
		return "array"
	case nil:
		return "null"
	}
	if _, ok := toFloat(v); ok {
		return "number"
	}
	return fmt.Sprintf("%T", v)
}
