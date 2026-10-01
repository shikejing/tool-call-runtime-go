package runtime

import (
	"sync"
	"time"
)

// CallStatus 调用状态机：pending -> running -> completed/failed/canceled，
// 或 pending -> rejected。结果不确定时可据此区分。
type CallStatus string

const (
	StatusPending   CallStatus = "pending"  // 已受理未执行
	StatusRejected  CallStatus = "rejected" // 校验/容量/并发拒绝
	StatusRunning   CallStatus = "running"
	StatusCompleted CallStatus = "completed"
	StatusFailed    CallStatus = "failed"
	StatusCanceled  CallStatus = "canceled"
)

// CallRecord 一次调用的审计记录，含校验依据（绑定版本）与执行历史。
type CallRecord struct {
	CallID     string       `json:"callId"`
	Tool       string       `json:"tool"`
	Version    int          `json:"version"` // 受理时绑定的定义版本
	Status     CallStatus   `json:"status"`
	Reject     *RejectError `json:"reject,omitempty"`
	Attempts   int          `json:"attempts"`
	Result     any          `json:"result,omitempty"`
	AcceptedAt time.Time    `json:"acceptedAt"`
	FinishedAt time.Time    `json:"finishedAt,omitempty"`
	History    []string     `json:"history"`
}

// AuditLog 线程安全的调用记录表。
type AuditLog struct {
	mu      sync.RWMutex
	records map[string]*CallRecord
}

func NewAuditLog() *AuditLog {
	return &AuditLog{records: make(map[string]*CallRecord)}
}

// Open 创建一条 pending 记录。
func (l *AuditLog) Open(callID, tool string, version int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.records[callID] = &CallRecord{
		CallID:     callID,
		Tool:       tool,
		Version:    version,
		Status:     StatusPending,
		AcceptedAt: time.Now(),
		History:    []string{"accepted, bound to version " + itoa(version)},
	}
}

// Transition 更新状态并追加历史。
func (l *AuditLog) Transition(callID string, status CallStatus, note string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	r, ok := l.records[callID]
	if !ok {
		return
	}
	r.Status = status
	r.History = append(r.History, note)
	if status == StatusCompleted || status == StatusFailed ||
		status == StatusRejected || status == StatusCanceled {
		r.FinishedAt = time.Now()
	}
}

// SetReject 记录拒绝原因并置为 rejected。
func (l *AuditLog) SetReject(callID string, rej *RejectError) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if r, ok := l.records[callID]; ok {
		r.Reject = rej
		r.Status = StatusRejected
		r.History = append(r.History, "rejected: "+rej.Error())
		r.FinishedAt = time.Now()
	}
}

// SetResult 记录成功结果（只写一次，保证重试前后对外一致）。
func (l *AuditLog) SetResult(callID string, result any, attempts int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if r, ok := l.records[callID]; ok {
		if r.Status == StatusCompleted {
			return // 幂等：不覆盖
		}
		r.Result = result
		r.Attempts = attempts
		r.Status = StatusCompleted
		r.History = append(r.History, "completed")
		r.FinishedAt = time.Now()
	}
}

// SetAttempts 更新重试计数。
func (l *AuditLog) SetAttempts(callID string, n int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if r, ok := l.records[callID]; ok {
		r.Attempts = n
	}
}

// Get 返回记录快照（拷贝，避免外部修改内部状态）。
func (l *AuditLog) Get(callID string) (CallRecord, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	r, ok := l.records[callID]
	if !ok {
		return CallRecord{}, false
	}
	cp := *r
	cp.History = append([]string(nil), r.History...)
	return cp, true
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}
