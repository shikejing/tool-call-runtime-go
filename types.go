package toolruntime

import (
	"context"
	"encoding/json"
	"time"
)

// Status 表示一次调用在其生命周期中的状态。
type Status string

const (
	StatusPending   Status = "pending"   // 已受理，排队中
	StatusRunning   Status = "running"   // 执行中
	StatusSucceeded Status = "succeeded" // 执行成功，已完成
	StatusFailed    Status = "failed"    // 执行失败且重试耗尽
	StatusRejected  Status = "rejected"  // 受理前/执行前被拒绝
	StatusCancelled Status = "cancelled" // 超时或取消，未产生结果
)

// RejectCode 是可区分的拒绝原因码。
type RejectCode string

const (
	RejectNone            RejectCode = ""
	RejectUnknownTool     RejectCode = "unknown_tool"
	RejectVersionNotFound RejectCode = "version_not_found"
	RejectSchemaInvalid   RejectCode = "schema_invalid"
	RejectParamTooLarge   RejectCode = "param_too_large"
	RejectQueueFull       RejectCode = "queue_full"
	RejectTimeout         RejectCode = "timeout"
	RejectCancelled       RejectCode = "cancelled"
	RejectStreamGap       RejectCode = "stream_gap"
	RejectStreamDuplicate RejectCode = "stream_duplicate"
	RejectStreamMissing   RejectCode = "stream_missing"
	RejectStreamTimeout   RejectCode = "stream_timeout"
	RejectResultTooLarge  RejectCode = "result_too_large"
	RejectExecutionFailed RejectCode = "execution_failed"
)

// RejectReason 描述一次拒绝的原因，Path 定位到具体参数路径。
type RejectReason struct {
	Code    RejectCode `json:"code"`
	Path    string     `json:"path,omitempty"`
	Message string     `json:"message"`
}

func (r *RejectReason) Error() string {
	if r == nil {
		return ""
	}
	if r.Path != "" {
		return string(r.Code) + " at " + r.Path + ": " + r.Message
	}
	return string(r.Code) + ": " + r.Message
}

// SchemaType 是参数 schema 的类型。
type SchemaType string

const (
	TypeString  SchemaType = "string"
	TypeNumber  SchemaType = "number"
	TypeInteger SchemaType = "integer"
	TypeBoolean SchemaType = "boolean"
	TypeObject  SchemaType = "object"
	TypeArray   SchemaType = "array"
)

// Schema 是 JSON-Schema 风格的参数类型描述，支持嵌套结构与取值范围约束。
type Schema struct {
	Type                 SchemaType         `json:"type"`
	Enum                 []any              `json:"enum,omitempty"`
	Properties           map[string]*Schema `json:"properties,omitempty"`
	RequiredFields       []string           `json:"required,omitempty"`
	AdditionalProperties bool               `json:"additionalProperties,omitempty"`
	Items                *Schema            `json:"items,omitempty"`
	MinItems             *int               `json:"minItems,omitempty"`
	MaxItems             *int               `json:"maxItems,omitempty"`
	Min                  *float64           `json:"minimum,omitempty"`
	Max                  *float64           `json:"maximum,omitempty"`
	MinLength            *int               `json:"minLength,omitempty"`
	MaxLength            *int               `json:"maxLength,omitempty"`
	Pattern              string             `json:"pattern,omitempty"`
}

// RetryPolicy 描述执行失败时的重试策略。
type RetryPolicy struct {
	MaxAttempts    int           `json:"max_attempts"`
	InitialBackoff time.Duration `json:"initial_backoff"`
	BackoffFactor  float64       `json:"backoff_factor"`
	MaxBackoff     time.Duration `json:"max_backoff"`
}

// ResultStrategy 描述返回结果超限时的处理策略。
type ResultStrategy string

const (
	ResultReject   ResultStrategy = "reject"
	ResultTruncate ResultStrategy = "truncate"
)

// ToolDefinition 是工具定义，包含参数 schema 与执行/限流/重试配置。
type ToolDefinition struct {
	Name           string         `json:"name"`
	Version        string         `json:"version"`
	Params         *Schema        `json:"params"`
	Concurrency    int            `json:"concurrency"`
	QueueSize      int            `json:"queue_size"`
	Timeout        time.Duration  `json:"timeout"`
	MaxParamBytes  int            `json:"max_param_bytes"`
	MaxResultBytes int            `json:"max_result_bytes"`
	RetryPolicy    RetryPolicy    `json:"retry_policy"`
	ResultStrategy ResultStrategy `json:"result_strategy"`
	Executor       Executor       `json:"-"`
}

// AttemptRecord 记录一次执行尝试的历史，用于审计与不确定结果的追溯。
type AttemptRecord struct {
	Attempt    int       `json:"attempt"`
	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at"`
	Err        string    `json:"error,omitempty"`
	SideEffect bool      `json:"side_effect"`
	ResultSize int       `json:"result_size,omitempty"`
}

// Call 是一次工具调用的完整记录。
type Call struct {
	ID              string           `json:"id"`
	ToolName        string           `json:"tool_name"`
	ToolVersion     string           `json:"tool_version"` // 受理时绑定的定义版本
	Params          json.RawMessage  `json:"params,omitempty"`
	Status          Status           `json:"status"`
	RejectReason    *RejectReason    `json:"reject_reason,omitempty"`
	Result          json.RawMessage  `json:"result,omitempty"`
	ResultTruncated bool             `json:"result_truncated,omitempty"`
	Attempts        []*AttemptRecord `json:"attempts,omitempty"`
	IdempotencyKey  string           `json:"idempotency_key,omitempty"`
	CreatedAt       time.Time        `json:"created_at"`
	AdmittedAt      *time.Time       `json:"admitted_at,omitempty"`
	StartedAt       *time.Time       `json:"started_at,omitempty"`
	FinishedAt      *time.Time       `json:"finished_at,omitempty"`
}

// Executor 是本地执行器接口，由运行时驱动，不得产生进程外副作用。
type Executor interface {
	Execute(ctx context.Context, call *Call) (json.RawMessage, error)
}

// Chunk 是流式分片的基本单元。
type Chunk struct {
	Seq   uint64 `json:"seq"`
	Data  []byte `json:"data"`
	Final bool   `json:"final"`
}

// Config 是运行时全局默认配置。
type Config struct {
	DefaultConcurrency    int
	DefaultQueueSize      int
	DefaultTimeout        time.Duration
	DefaultMaxParamBytes  int
	DefaultMaxResultBytes int
	DefaultRetryPolicy    RetryPolicy
	StreamAssemblyTimeout time.Duration
	MaxCallRecords        int // 调用记录与审计保留上限，防止高负载下无界内存增长
}
