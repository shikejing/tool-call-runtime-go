// Package runtime 提供模型工具调用的本地运行时：
// 工具定义登记、参数校验、流式拼接、版本绑定、并发隔离、
// 重试、容量边界与审计，全部在本进程内完成。
package runtime

// ParamType 参数类型枚举。
type ParamType string

const (
	TypeString  ParamType = "string"
	TypeNumber  ParamType = "number"
	TypeInteger ParamType = "integer"
	TypeBoolean ParamType = "boolean"
	TypeObject  ParamType = "object"
	TypeArray   ParamType = "array"
)

// ParamSchema 描述单个参数的约束，可嵌套。
type ParamSchema struct {
	Type       ParamType               `json:"type"`
	Required   bool                    `json:"required,omitempty"`
	Enum       []any                   `json:"enum,omitempty"`
	Min        *float64                `json:"min,omitempty"`
	Max        *float64                `json:"max,omitempty"`
	MinLen     *int                    `json:"minLen,omitempty"`
	MaxLen     *int                    `json:"maxLen,omitempty"`
	Properties map[string]*ParamSchema `json:"properties,omitempty"` // object 子字段
	Items      *ParamSchema            `json:"items,omitempty"`      // array 元素
}

// ToolDef 工具定义，Version 单调递增。
type ToolDef struct {
	Name        string       `json:"name"`
	Version     int          `json:"version"`
	Description string       `json:"description"`
	Params      *ParamSchema `json:"params"` // 顶层固定为 object
}

// ToolFunc 本地工具桩的签名。args 已通过校验。
type ToolFunc func(ctx *CallContext, args map[string]any) (any, error)
