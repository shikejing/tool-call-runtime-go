package toolruntime

import (
	"encoding/json"
	"errors"
	"sort"
	"sync"
	"time"
)

// 流式拼装相关的可区分错误。
var (
	ErrStreamDuplicate = errors.New("duplicate chunk sequence")
	ErrStreamGap       = errors.New("chunk sequence gap")
	ErrStreamMissing   = errors.New("missing chunks at finalize")
	ErrStreamTooLarge  = errors.New("stream exceeds size limit")
	ErrStreamComplete  = errors.New("stream already complete")
)

// StreamAssembler 负责流式分片的乱序缓存、重复识别与缺失检测。
// 只有收到 final 分片且 0..finalSeq 全部到齐时才产出完整参数，
// 中途任何合法前缀都不会被当作最终参数。
type StreamAssembler struct {
	mu        sync.Mutex
	chunks    map[uint64][]byte
	nextSeq   uint64
	finalSeq  *uint64
	startedAt time.Time
	maxBytes  int
	total     int
	complete  bool
	hadGap    bool // 是否出现过乱序缺口（用于审计）
}

// NewStreamAssembler 创建一个分片拼装器，maxBytes 为参数大小上限（0 表示不限）。
func NewStreamAssembler(maxBytes int) *StreamAssembler {
	return &StreamAssembler{
		chunks:    make(map[uint64][]byte),
		startedAt: time.Now(),
		maxBytes:  maxBytes,
	}
}

// StartedAt 返回拼装开始时间。
func (a *StreamAssembler) StartedAt() time.Time {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.startedAt
}

// Feed 喂入一个分片。
//   - complete=true 时 raw 为完整参数；
//   - err 为 ErrStreamDuplicate/ErrStreamMissing/ErrStreamTooLarge/ErrStreamComplete 时调用方应拒绝；
//   - err 为 ErrStreamGap 表示出现乱序缺口，分片已缓存，可继续等待缺失分片。
func (a *StreamAssembler) Feed(chunk Chunk) (complete bool, raw json.RawMessage, err error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.complete {
		return false, nil, ErrStreamComplete
	}
	if a.finalSeq != nil && chunk.Seq > *a.finalSeq {
		return false, nil, ErrStreamComplete // final 之后的分片视为多余
	}
	if _, dup := a.chunks[chunk.Seq]; dup {
		return false, nil, ErrStreamDuplicate
	}
	if chunk.Seq < a.nextSeq {
		return false, nil, ErrStreamDuplicate
	}
	if a.maxBytes > 0 && a.total+len(chunk.Data) > a.maxBytes {
		return false, nil, ErrStreamTooLarge
	}

	a.chunks[chunk.Seq] = append([]byte(nil), chunk.Data...)
	a.total += len(chunk.Data)

	gap := chunk.Seq > a.nextSeq
	if gap {
		a.hadGap = true
	}

	if chunk.Final {
		seq := chunk.Seq
		a.finalSeq = &seq
	}

	// 尝试推进连续前缀
	for {
		data, ok := a.chunks[a.nextSeq]
		if !ok {
			break
		}
		a.nextSeq++
		_ = data
	}

	if a.finalSeq != nil {
		if a.nextSeq <= *a.finalSeq {
			return false, nil, ErrStreamMissing
		}
		// 全部到齐，按序拼装
		out := make([]byte, 0, a.total)
		for i := uint64(0); i <= *a.finalSeq; i++ {
			out = append(out, a.chunks[i]...)
		}
		a.complete = true
		return true, json.RawMessage(out), nil
	}

	if gap {
		return false, nil, ErrStreamGap
	}
	return false, nil, nil
}

// Missing 返回当前缺失的分片序号（用于审计与等待）。
func (a *StreamAssembler) Missing() []uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.finalSeq == nil {
		// 未声明 final 时，缺失区间为 [nextSeq, maxSeen]
		maxSeen := uint64(0)
		have := false
		for k := range a.chunks {
			have = true
			if k > maxSeen {
				maxSeen = k
			}
		}
		if !have || a.nextSeq > maxSeen {
			return nil
		}
		out := make([]uint64, 0, maxSeen-a.nextSeq+1)
		for i := a.nextSeq; i <= maxSeen; i++ {
			if _, ok := a.chunks[i]; !ok {
				out = append(out, i)
			}
		}
		return out
	}
	out := make([]uint64, 0, *a.finalSeq-a.nextSeq+1)
	for i := a.nextSeq; i <= *a.finalSeq; i++ {
		if _, ok := a.chunks[i]; !ok {
			out = append(out, i)
		}
	}
	return out
}

// BufferedBytes 返回当前已缓存的字节数。
func (a *StreamAssembler) BufferedBytes() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.total
}

// SnapshotSeqs 返回已缓存分片序号（排序，用于审计）。
func (a *StreamAssembler) SnapshotSeqs() []uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]uint64, 0, len(a.chunks))
	for k := range a.chunks {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
