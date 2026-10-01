package toolruntime

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

// ValidationError 描述一个具体的参数校验错误，Path 形如 "$.foo.bar[0]"。
type ValidationError struct {
	Path    string `json:"path"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("%s: %s (%s)", e.Path, e.Message, e.Code)
}

// ValidateRaw 校验一段原始 JSON 参数；JSON 本身非法时返回 invalid_json 错误。
func (s *Schema) ValidateRaw(raw json.RawMessage) []ValidationError {
	var data any
	if len(raw) == 0 {
		return []ValidationError{{Path: "$", Code: "invalid_json", Message: "empty parameter body"}}
	}
	if err := json.Unmarshal(raw, &data); err != nil {
		return []ValidationError{{Path: "$", Code: "invalid_json", Message: err.Error()}}
	}
	return s.Validate(data)
}

// Validate 按 schema 逐层校验已解析的参数，返回所有校验错误（空切片表示通过）。
func (s *Schema) Validate(data any) []ValidationError {
	var errs []ValidationError
	validateValue(s, data, "$", &errs)
	return errs
}

func validateValue(s *Schema, data any, path string, errs *[]ValidationError) {
	if s == nil {
		return
	}
	if data == nil {
		*errs = append(*errs, ValidationError{Path: path, Code: "type_mismatch",
			Message: fmt.Sprintf("expected %s, got null", s.Type)})
		return
	}
	if !checkType(s.Type, data) {
		*errs = append(*errs, ValidationError{Path: path, Code: "type_mismatch",
			Message: fmt.Sprintf("expected %s, got %s", s.Type, jsonTypeName(data))})
		return
	}
	if len(s.Enum) > 0 {
		matched := false
		for _, v := range s.Enum {
			if fmt.Sprint(v) == fmt.Sprint(data) {
				matched = true
				break
			}
		}
		if !matched {
			*errs = append(*errs, ValidationError{Path: path, Code: "enum_mismatch",
				Message: fmt.Sprintf("value %v not in enum %v", data, s.Enum)})
		}
	}
	switch s.Type {
	case TypeString:
		validateString(s, data.(string), path, errs)
	case TypeNumber, TypeInteger:
		validateNumber(s, data, path, errs)
	case TypeObject:
		validateObject(s, data.(map[string]any), path, errs)
	case TypeArray:
		validateArray(s, data.([]any), path, errs)
	case TypeBoolean:
		// 布尔无额外约束
	}
}

func checkType(t SchemaType, data any) bool {
	switch t {
	case TypeString:
		_, ok := data.(string)
		return ok
	case TypeBoolean:
		_, ok := data.(bool)
		return ok
	case TypeObject:
		_, ok := data.(map[string]any)
		return ok
	case TypeArray:
		_, ok := data.([]any)
		return ok
	case TypeNumber:
		_, ok := data.(float64)
		return ok
	case TypeInteger:
		f, ok := data.(float64)
		if !ok {
			return false
		}
		return f == float64(int64(f))
	}
	return false
}

func jsonTypeName(data any) string {
	switch data.(type) {
	case string:
		return "string"
	case bool:
		return "boolean"
	case float64:
		return "number"
	case map[string]any:
		return "object"
	case []any:
		return "array"
	case nil:
		return "null"
	}
	return fmt.Sprintf("%T", data)
}

func validateString(s *Schema, v, path string, errs *[]ValidationError) {
	n := len([]rune(v))
	if s.MinLength != nil && n < *s.MinLength {
		*errs = append(*errs, ValidationError{Path: path, Code: "too_short",
			Message: fmt.Sprintf("string length %d < minLength %d", n, *s.MinLength)})
	}
	if s.MaxLength != nil && n > *s.MaxLength {
		*errs = append(*errs, ValidationError{Path: path, Code: "too_long",
			Message: fmt.Sprintf("string length %d > maxLength %d", n, *s.MaxLength)})
	}
	if s.Pattern != "" {
		re, err := regexp.Compile(s.Pattern)
		if err != nil {
			*errs = append(*errs, ValidationError{Path: path, Code: "bad_pattern",
				Message: fmt.Sprintf("invalid pattern %q: %v", s.Pattern, err)})
		} else if !re.MatchString(v) {
			*errs = append(*errs, ValidationError{Path: path, Code: "pattern_mismatch",
				Message: fmt.Sprintf("string %q does not match pattern %q", v, s.Pattern)})
		}
	}
}

func validateNumber(s *Schema, data any, path string, errs *[]ValidationError) {
	f := data.(float64)
	if s.Min != nil && f < *s.Min {
		*errs = append(*errs, ValidationError{Path: path, Code: "out_of_range",
			Message: fmt.Sprintf("value %v < minimum %v", f, *s.Min)})
	}
	if s.Max != nil && f > *s.Max {
		*errs = append(*errs, ValidationError{Path: path, Code: "out_of_range",
			Message: fmt.Sprintf("value %v > maximum %v", f, *s.Max)})
	}
}

func validateObject(s *Schema, obj map[string]any, path string, errs *[]ValidationError) {
	for _, req := range s.RequiredFields {
		if _, ok := obj[req]; !ok {
			*errs = append(*errs, ValidationError{Path: pathKey(path, req), Code: "missing_required",
				Message: fmt.Sprintf("missing required field %q", req)})
		}
	}
	for k, v := range obj {
		childPath := pathKey(path, k)
		prop, ok := s.Properties[k]
		if !ok {
			if s.AdditionalProperties {
				continue
			}
			*errs = append(*errs, ValidationError{Path: childPath, Code: "unknown_field",
				Message: fmt.Sprintf("field %q is not declared in schema", k)})
			continue
		}
		validateValue(prop, v, childPath, errs)
	}
}

func validateArray(s *Schema, arr []any, path string, errs *[]ValidationError) {
	n := len(arr)
	if s.MinItems != nil && n < *s.MinItems {
		*errs = append(*errs, ValidationError{Path: path, Code: "too_few_items",
			Message: fmt.Sprintf("array length %d < minItems %d", n, *s.MinItems)})
	}
	if s.MaxItems != nil && n > *s.MaxItems {
		*errs = append(*errs, ValidationError{Path: path, Code: "too_many_items",
			Message: fmt.Sprintf("array length %d > maxItems %d", n, *s.MaxItems)})
	}
	if s.Items != nil {
		for i, v := range arr {
			validateValue(s.Items, v, fmt.Sprintf("%s[%d]", path, i), errs)
		}
	}
}

func pathKey(path, key string) string {
	if path == "$" {
		return "$." + key
	}
	return path + "." + key
}

// JoinErrors 把多个校验错误拼接为单条消息。
func JoinErrors(errs []ValidationError) string {
	parts := make([]string, len(errs))
	for i, e := range errs {
		parts[i] = e.Error()
	}
	return strings.Join(parts, "; ")
}
