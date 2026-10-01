package runtime

import "fmt"

// RejectKind 可区分的拒绝/失败类别。
type RejectKind string

const (
	KindTypeMismatch    RejectKind = "type_mismatch"
	KindMissingRequired RejectKind = "missing_required"
	KindUnknownField    RejectKind = "unknown_field"
	KindOutOfRange      RejectKind = "out_of_range"
	KindEnumViolation   RejectKind = "enum_violation"
	KindNotObject       RejectKind = "not_object"
	KindStreamCorrupt   RejectKind = "stream_corrupt"
	KindStreamDuplicate RejectKind = "stream_duplicate"
	KindStreamMissing   RejectKind = "stream_missing"
	KindArgsTooLarge    RejectKind = "args_too_large"
	KindResultTooLarge  RejectKind = "result_too_large"
	KindConcurrencyFull RejectKind = "concurrency_full"
	KindQueueFull       RejectKind = "queue_full"
	KindUnknownTool     RejectKind = "unknown_tool"
	KindTimeout         RejectKind = "timeout"
	KindCanceled        RejectKind = "canceled"
	KindExecFailed      RejectKind = "exec_failed"
)

// RejectError 携带类别与参数路径的拒绝原因。
type RejectError struct {
	Kind RejectKind `json:"kind"`
	Path string     `json:"path"` // 如 $.user.age，流式/容量类为 ""
	Msg  string     `json:"msg"`
}

func (e *RejectError) Error() string {
	if e.Path == "" {
		return fmt.Sprintf("%s: %s", e.Kind, e.Msg)
	}
	return fmt.Sprintf("%s at %s: %s", e.Kind, e.Path, e.Msg)
}

func reject(kind RejectKind, path, format string, args ...any) *RejectError {
	return &RejectError{Kind: kind, Path: path, Msg: fmt.Sprintf(format, args...)}
}
