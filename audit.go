package toolruntime

import (
	"sync"
	"time"
)

// AuditEntry 是一条不可变的审计记录。
type AuditEntry struct {
	Time    time.Time `json:"time"`
	CallID  string    `json:"call_id"`
	Tool    string    `json:"tool"`
	Version string    `json:"version"`
	Event   string    `json:"event"`
	Detail  string    `json:"detail,omitempty"`
}

// AuditLog 保存全部调用的状态变迁历史，便于复现当时的结论。
type AuditLog struct {
	mu      sync.Mutex
	entries []*AuditEntry
	max     int
}

func newAuditLog(maxEntries int) *AuditLog {
	return &AuditLog{max: maxEntries}
}

func (l *AuditLog) append(call *Call, event, detail string) {
	l.record(call.ID, call.ToolName, call.ToolVersion, event, detail)
}

func (l *AuditLog) record(callID, tool, version, event, detail string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, &AuditEntry{
		Time:    time.Now(),
		CallID:  callID,
		Tool:    tool,
		Version: version,
		Event:   event,
		Detail:  detail,
	})
	if l.max > 0 && len(l.entries) > l.max {
		l.entries = append([]*AuditEntry(nil), l.entries[len(l.entries)-l.max:]...)
	}
}

// Entries 返回指定调用的审计记录（按时间顺序）。
func (l *AuditLog) Entries(callID string) []*AuditEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]*AuditEntry, 0)
	for _, e := range l.entries {
		if e.CallID == callID {
			cp := *e
			out = append(out, &cp)
		}
	}
	return out
}

// All 返回全部审计记录。
func (l *AuditLog) All() []*AuditEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]*AuditEntry, len(l.entries))
	for i, e := range l.entries {
		cp := *e
		out[i] = &cp
	}
	return out
}
