package runtime

import "bytes"

// Chunk 参数分片。Seq 从 0 连续编号，Last 标记最终分片。
type Chunk struct {
	Seq  int
	Data []byte
	Last bool
}

// StreamAssembler 按序拼接分片。
// 策略：乱序分片先缓存，重复分片拒绝（KindStreamDuplicate），
// 缺口在 Finish 时识别为缺失（KindStreamMissing）。
// 只有完整序列（0..N 且末片 Last=true）才返回完成。
type StreamAssembler struct {
	next    int           // 期望的下一个序号
	buf     bytes.Buffer  // 已按序拼接的数据
	pending map[int]Chunk // 乱序缓存
	seen    map[int]bool  // 所有已见序号（含已拼接）
	done    bool
	lastSeq int // 末片序号，-1 未知
}

func NewStreamAssembler() *StreamAssembler {
	return &StreamAssembler{
		pending: make(map[int]Chunk),
		seen:    make(map[int]bool),
		lastSeq: -1,
	}
}

// Feed 接收一个分片。返回 (完整字节, 完成标志, 错误)。
// 完成前返回的完整字节为 nil；只有 done=true 时数据才可作为最终参数。
func (a *StreamAssembler) Feed(c Chunk) ([]byte, bool, *RejectError) {
	if a.done {
		return nil, false, reject(KindStreamCorrupt, "", "stream already finished")
	}
	if c.Seq < 0 {
		return nil, false, reject(KindStreamCorrupt, "", "negative seq %d", c.Seq)
	}
	if a.seen[c.Seq] {
		return nil, false, reject(KindStreamDuplicate, "", "duplicate chunk seq %d", c.Seq)
	}
	a.seen[c.Seq] = true
	if c.Last {
		if a.lastSeq >= 0 && a.lastSeq != c.Seq {
			return nil, false, reject(KindStreamCorrupt, "", "conflicting last chunk: seq %d and %d", a.lastSeq, c.Seq)
		}
		a.lastSeq = c.Seq
	}
	if c.Seq == a.next {
		a.appendChunk(c)
		// 冲刷缓存中连续的分片
		for {
			p, ok := a.pending[a.next]
			if !ok {
				break
			}
			delete(a.pending, a.next)
			a.appendChunk(p)
		}
	} else {
		a.pending[c.Seq] = c
	}
	// 完成判定：末片已知且已拼接到末片
	if a.lastSeq >= 0 && a.next > a.lastSeq {
		a.done = true
		return a.buf.Bytes(), true, nil
	}
	return nil, false, nil
}

func (a *StreamAssembler) appendChunk(c Chunk) {
	a.buf.Write(c.Data)
	a.next = c.Seq + 1
}

// Missing 返回当前缺口序号（用于诊断与 Finish 检查）。
func (a *StreamAssembler) Missing() []int {
	var out []int
	for i := a.next; ; i++ {
		if a.lastSeq >= 0 && i > a.lastSeq {
			break
		}
		if !a.seen[i] {
			out = append(out, i)
		}
		if a.lastSeq < 0 && i > a.next+len(a.pending) {
			break
		}
	}
	return out
}

// Finish 在发送方结束时调用：若序列不完整则报缺失。
func (a *StreamAssembler) Finish() *RejectError {
	if a.done {
		return nil
	}
	if a.lastSeq < 0 {
		return reject(KindStreamMissing, "", "stream ended without last chunk; next expected seq %d", a.next)
	}
	m := a.Missing()
	if len(m) > 0 {
		return reject(KindStreamMissing, "", "missing chunks %v", m)
	}
	return reject(KindStreamCorrupt, "", "stream incomplete")
}
