package runtime

import "fmt"

// StreamSession 一次流式调用的会话。
// 分片经 Feed 到达，只有 Finish 时序列完整且参数合法才进入执行；
// 中途任何恰好合法的前缀都不会被执行。
type StreamSession struct {
	rt   *Runtime
	id   string
	tool string
	asm  *StreamAssembler
}

// BeginStream 开启流式调用。受理发生在 Finish，版本绑定也在 Finish 时确定。
func (r *Runtime) BeginStream(callID, tool string) (*StreamSession, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.streams[callID]; ok {
		return nil, fmt.Errorf("stream call %q already exists", callID)
	}
	s := &StreamSession{rt: r, id: callID, tool: tool, asm: NewStreamAssembler()}
	r.streams[callID] = s
	return s, nil
}

// Feed 接收一个分片。乱序会缓存，重复/冲突立即报错。
func (s *StreamSession) Feed(c Chunk) *RejectError {
	_, _, rej := s.asm.Feed(c)
	return rej
}

// Finish 结束流：检查缺失分片，完整才受理执行。
func (s *StreamSession) Finish() (any, *RejectError) {
	s.rt.mu.Lock()
	delete(s.rt.streams, s.id)
	s.rt.mu.Unlock()

	if rej := s.asm.Finish(); rej != nil {
		s.rt.audit.Open(s.id, s.tool, 0)
		s.rt.audit.SetReject(s.id, rej)
		return nil, rej
	}
	return s.rt.accept(s.id, s.tool, s.asm.buf.Bytes())
}
