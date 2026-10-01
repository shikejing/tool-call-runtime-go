package toolruntime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Runtime 是工具调用运行时：登记定义、校验参数、驱动本地执行。
type Runtime struct {
	cfg      Config
	registry *Registry
	audit    *AuditLog

	mu      sync.Mutex
	calls   map[string]*Call
	streams map[string]*streamState
	tools   map[string]*toolRuntime
	ctxs    map[string]context.Context
	seq     uint64

	defaultExecutor Executor
	stopCh          chan struct{}
	wg              sync.WaitGroup
	closeOnce       sync.Once
}

type streamState struct {
	assembler     *StreamAssembler
	def           *ToolDefinition
	toolName      string
	runtimeCallID string // 拼装完成后关联的运行时调用 ID
	finalized     bool
	timedOut      bool // 已被超时清扫标记，迟到分片直接拒绝
}

type toolRuntime struct {
	slots chan struct{} // 容量 = 并发上限 + 队列容量：占不到位即拒绝
	exec  chan struct{} // 容量 = 并发上限：实际执行位
	def   *ToolDefinition
}

// NewRuntime 创建运行时。
func NewRuntime(cfg Config) *Runtime {
	if cfg.DefaultConcurrency <= 0 {
		cfg.DefaultConcurrency = 4
	}
	if cfg.DefaultQueueSize <= 0 {
		cfg.DefaultQueueSize = 16
	}
	if cfg.DefaultTimeout <= 0 {
		cfg.DefaultTimeout = 30 * time.Second
	}
	if cfg.DefaultMaxParamBytes <= 0 {
		cfg.DefaultMaxParamBytes = 1 << 20
	}
	if cfg.DefaultMaxResultBytes <= 0 {
		cfg.DefaultMaxResultBytes = 1 << 20
	}
	if cfg.DefaultRetryPolicy.MaxAttempts <= 0 {
		cfg.DefaultRetryPolicy = RetryPolicy{MaxAttempts: 3, InitialBackoff: 100 * time.Millisecond, BackoffFactor: 2, MaxBackoff: 2 * time.Second}
	}
	if cfg.DefaultRetryPolicy.InitialBackoff <= 0 {
		cfg.DefaultRetryPolicy.InitialBackoff = 100 * time.Millisecond
	}
	if cfg.DefaultRetryPolicy.BackoffFactor <= 0 {
		cfg.DefaultRetryPolicy.BackoffFactor = 2
	}
	if cfg.DefaultRetryPolicy.MaxBackoff <= 0 {
		cfg.DefaultRetryPolicy.MaxBackoff = 2 * time.Second
	}
	if cfg.StreamAssemblyTimeout <= 0 {
		cfg.StreamAssemblyTimeout = 30 * time.Second
	}
	if cfg.MaxCallRecords <= 0 {
		cfg.MaxCallRecords = 10000
	}
	rt := &Runtime{
		cfg:             cfg,
		registry:        NewRegistry(),
		audit:           newAuditLog(cfg.MaxCallRecords),
		calls:           make(map[string]*Call),
		streams:         make(map[string]*streamState),
		tools:           make(map[string]*toolRuntime),
		ctxs:            make(map[string]context.Context),
		defaultExecutor: NewStubExecutor(),
		stopCh:          make(chan struct{}),
	}
	rt.wg.Add(1)
	go rt.streamSweeper()
	return rt
}

// Registry 返回运行时的工具定义登记处。
func (rt *Runtime) Registry() *Registry { return rt.registry }

// Audit 返回审计日志，供查询调用状态变迁历史。
func (rt *Runtime) Audit() *AuditLog { return rt.audit }

// DefaultExecutor 返回运行时默认的本地桩执行器（可注入失败）。
func (rt *Runtime) DefaultExecutor() *StubExecutor {
	return rt.defaultExecutor.(*StubExecutor)
}

// applyDefaults 用全局默认值补全定义中未配置的项。
func (rt *Runtime) applyDefaults(def *ToolDefinition) *ToolDefinition {
	cp := *def
	if cp.Concurrency <= 0 {
		cp.Concurrency = rt.cfg.DefaultConcurrency
	}
	if cp.QueueSize <= 0 {
		cp.QueueSize = rt.cfg.DefaultQueueSize
	}
	if cp.Timeout <= 0 {
		cp.Timeout = rt.cfg.DefaultTimeout
	}
	if cp.MaxParamBytes <= 0 {
		cp.MaxParamBytes = rt.cfg.DefaultMaxParamBytes
	}
	if cp.MaxResultBytes <= 0 {
		cp.MaxResultBytes = rt.cfg.DefaultMaxResultBytes
	}
	if cp.RetryPolicy.MaxAttempts <= 0 {
		cp.RetryPolicy = rt.cfg.DefaultRetryPolicy
	}
	if cp.RetryPolicy.InitialBackoff <= 0 {
		cp.RetryPolicy.InitialBackoff = rt.cfg.DefaultRetryPolicy.InitialBackoff
	}
	if cp.RetryPolicy.BackoffFactor <= 0 {
		cp.RetryPolicy.BackoffFactor = rt.cfg.DefaultRetryPolicy.BackoffFactor
	}
	if cp.RetryPolicy.MaxBackoff <= 0 {
		cp.RetryPolicy.MaxBackoff = rt.cfg.DefaultRetryPolicy.MaxBackoff
	}
	if cp.ResultStrategy == "" {
		cp.ResultStrategy = ResultReject
	}
	if cp.Executor == nil {
		cp.Executor = rt.defaultExecutor
	}
	return &cp
}

// ---------- 提交 ----------

// Submit 一次性提交完整参数。
func (rt *Runtime) Submit(ctx context.Context, toolName string, params json.RawMessage) (*Call, error) {
	def, ok := rt.registry.Current(toolName)
	if !ok {
		call := rt.newCall(toolName, "", nil)
		rt.reject(call, &RejectReason{Code: RejectUnknownTool,
			Message: fmt.Sprintf("tool %q is not registered", toolName)})
		return call, nil
	}
	bound := rt.applyDefaults(def)
	call := rt.newCall(toolName, bound.Version, params)
	if !rt.admit(ctx, call, bound, params) {
		return call, nil
	}
	rt.enqueue(call, bound)
	return call, nil
}

// SubmitChunk 以流式分片方式提交参数。
func (rt *Runtime) SubmitChunk(ctx context.Context, toolName, callID string, chunk Chunk) (*Call, error) {
	rt.mu.Lock()
	state, exists := rt.streams[callID]
	if !exists {
		def, ok := rt.registry.Current(toolName)
		if !ok {
			rt.mu.Unlock()
			call := rt.newCall(toolName, "", nil)
			rt.reject(call, &RejectReason{Code: RejectUnknownTool,
				Message: fmt.Sprintf("tool %q is not registered", toolName)})
			return call, nil
		}
		bound := rt.applyDefaults(def)
		state = &streamState{
			assembler: NewStreamAssembler(bound.MaxParamBytes),
			def:       bound,
			toolName:  toolName,
		}
		rt.streams[callID] = state
	}
	// 流式拼装超时检测
	if state.timedOut || (rt.cfg.StreamAssemblyTimeout > 0 &&
		time.Since(state.assembler.StartedAt()) > rt.cfg.StreamAssemblyTimeout) {
		bound := state.def
		rt.mu.Unlock()
		call := rt.newCall(toolName, bound.Version, nil)
		rt.reject(call, &RejectReason{Code: RejectStreamTimeout,
			Message: fmt.Sprintf("stream %q assembly timed out", callID)})
		return call, nil
	}
	rt.mu.Unlock()

	complete, raw, err := state.assembler.Feed(chunk)
	switch {
	case err == nil && !complete:
		return nil, nil // 分片已缓存，参数尚未完整
	case errors.Is(err, ErrStreamGap):
		rt.audit.record(callID, state.toolName, state.def.Version, "stream_gap",
			fmt.Sprintf("missing seqs: %v", state.assembler.Missing()))
		return nil, nil // 乱序缺口已记录，继续等待缺失分片
	case errors.Is(err, ErrStreamDuplicate):
		call := rt.newCall(toolName, state.def.Version, nil)
		rt.reject(call, &RejectReason{Code: RejectStreamDuplicate,
			Message: fmt.Sprintf("stream %q duplicate chunk seq %d", callID, chunk.Seq)})
		return call, nil
	case errors.Is(err, ErrStreamMissing):
		call := rt.newCall(toolName, state.def.Version, nil)
		rt.reject(call, &RejectReason{Code: RejectStreamMissing,
			Message: fmt.Sprintf("stream %q missing chunks: %v", callID, state.assembler.Missing())})
		return call, nil
	case errors.Is(err, ErrStreamTooLarge):
		call := rt.newCall(toolName, state.def.Version, nil)
		rt.reject(call, &RejectReason{Code: RejectParamTooLarge,
			Message: fmt.Sprintf("stream %q params exceed %d bytes", callID, state.def.MaxParamBytes)})
		return call, nil
	case errors.Is(err, ErrStreamComplete):
		rt.mu.Lock()
		rc := state.runtimeCallID
		rt.mu.Unlock()
		if rc == "" {
			return nil, nil
		}
		return rt.GetCall(rc), nil
	case complete:
		// 只有完整且合法的参数才能进入执行
		bound := state.def
		call := rt.newCall(toolName, bound.Version, raw)
		if !rt.admit(ctx, call, bound, raw) {
			return call, nil
		}
		rt.mu.Lock()
		state.runtimeCallID = call.ID
		state.finalized = true
		rt.mu.Unlock()
		if missing := state.assembler.Missing(); len(missing) == 0 && state.assembler.hadGap {
			rt.audit.append(call, "stream_gap_observed", "out-of-order chunks were buffered before finalize")
		}
		rt.enqueue(call, bound)
		return call, nil
	}
	return nil, nil
}

// admit 校验参数并把调用置为已受理排队状态；返回 false 表示已拒绝。
func (rt *Runtime) admit(ctx context.Context, call *Call, def *ToolDefinition, params json.RawMessage) bool {
	if def.MaxParamBytes > 0 && len(params) > def.MaxParamBytes {
		rt.reject(call, &RejectReason{Code: RejectParamTooLarge,
			Message: fmt.Sprintf("params %d bytes exceed limit %d", len(params), def.MaxParamBytes)})
		return false
	}
	if verrs := def.Params.ValidateRaw(params); len(verrs) > 0 {
		rt.reject(call, &RejectReason{
			Code:    RejectSchemaInvalid,
			Path:    verrs[0].Path,
			Message: JoinErrors(verrs),
		})
		return false
	}
	now := time.Now()
	call.Status = StatusPending
	call.AdmittedAt = &now
	call.IdempotencyKey = fmt.Sprintf("%s|%s|%s", call.ToolName, call.ToolVersion, call.ID)
	rt.mu.Lock()
	rt.ctxs[call.ID] = ctx
	rt.mu.Unlock()
	rt.audit.append(call, "admitted", fmt.Sprintf("bound version %s", call.ToolVersion))
	rt.evictOldRecords()
	return true
}

// evictOldRecords 在调用记录超上限时，淘汰最旧的已终结调用，限制内存增长。
func (rt *Runtime) evictOldRecords() {
	max := rt.cfg.MaxCallRecords
	if max <= 0 {
		return
	}
	rt.mu.Lock()
	if len(rt.calls) <= max {
		rt.mu.Unlock()
		return
	}
	type candidate struct {
		id       string
		finished time.Time
	}
	var cands []candidate
	for id, c := range rt.calls {
		if c.Status == StatusPending || c.Status == StatusRunning {
			continue
		}
		ft := time.Now()
		if c.FinishedAt != nil {
			ft = *c.FinishedAt
		}
		cands = append(cands, candidate{id, ft})
	}
	sort.Slice(cands, func(i, j int) bool { return cands[i].finished.Before(cands[j].finished) })
	for len(rt.calls) > max && len(cands) > 0 {
		victim := cands[0]
		cands = cands[1:]
		delete(rt.calls, victim.id)
		delete(rt.ctxs, victim.id)
	}
	rt.mu.Unlock()
}

// enqueue 把调用放入对应工具版本的限流等待路径；
// 等待位（并发上限 + 队列容量）占满时返回可区分的拒绝。
func (rt *Runtime) enqueue(call *Call, def *ToolDefinition) {
	key := toolKey(def)
	rt.mu.Lock()
	tr, ok := rt.tools[key]
	if !ok {
		tr = &toolRuntime{
			slots: make(chan struct{}, def.Concurrency+def.QueueSize),
			exec:  make(chan struct{}, def.Concurrency),
			def:   def,
		}
		rt.tools[key] = tr
	}
	rt.mu.Unlock()

	rt.wg.Add(1)
	go func() {
		defer rt.wg.Done()
		rt.runCall(call, tr)
	}()
}

func toolKey(def *ToolDefinition) string { return def.Name + "\x00" + def.Version }

func (rt *Runtime) runCall(call *Call, tr *toolRuntime) {
	// 已被关闭流程标记取消的调用不再执行
	rt.mu.Lock()
	if call.Status == StatusCancelled {
		rt.mu.Unlock()
		return
	}
	rt.mu.Unlock()

	def := tr.def
	ctx := rt.ctxFor(call.ID)

	// 先响应取消，再抢占等待位
	select {
	case <-ctx.Done():
		rt.finishCancelled(call, ctx)
		return
	default:
	}
	select {
	case tr.slots <- struct{}{}:
		rt.audit.append(call, "queued", "")
	case <-ctx.Done():
		rt.finishCancelled(call, ctx)
		return
	default:
		rt.reject(call, &RejectReason{Code: RejectQueueFull,
			Message: fmt.Sprintf("tool %s at capacity (concurrency %d, queue %d)",
				def.Name, def.Concurrency, def.QueueSize)})
		return
	}

	// 等待执行位期间持续响应取消，避免半执行
	select {
	case tr.exec <- struct{}{}:
	case <-ctx.Done():
		<-tr.slots
		rt.finishCancelled(call, ctx)
		return
	}
	defer func() {
		<-tr.exec
		<-tr.slots
	}()

	rt.mu.Lock()
	call.Status = StatusRunning
	now := time.Now()
	call.StartedAt = &now
	rt.mu.Unlock()
	rt.audit.append(call, "running", "")

	policy := def.RetryPolicy
	var lastErr error
	for attempt := 1; attempt <= policy.MaxAttempts; attempt++ {
		rec := &AttemptRecord{Attempt: attempt, StartedAt: time.Now()}
		tctx, cancel := context.WithTimeout(ctx, def.Timeout)
		res, err := def.Executor.Execute(tctx, call)
		cancel()
		rec.FinishedAt = time.Now()
		rec.ResultSize = len(res)
		if err != nil {
			rec.Err = err.Error()
			if se, ok := def.Executor.(*StubExecutor); ok && se.HasSideEffect(call.ID) {
				rec.SideEffect = true
			}
			rt.mu.Lock()
			call.Attempts = append(call.Attempts, rec)
			rt.mu.Unlock()
			rt.audit.append(call, "attempt_failed", fmt.Sprintf("attempt %d: %s", attempt, err.Error()))
			lastErr = err
			if attempt < policy.MaxAttempts {
				backoff := backoffDuration(policy, attempt)
				select {
				case <-ctx.Done():
					rt.finishCancelled(call, ctx)
					return
				case <-time.After(backoff):
				}
			}
			continue
		}
		rt.mu.Lock()
		call.Attempts = append(call.Attempts, rec)
		rt.mu.Unlock()
		rt.handleResult(call, def, res)
		return
	}

	// 重试耗尽
	code := RejectExecutionFailed
	if errors.Is(lastErr, context.DeadlineExceeded) {
		code = RejectTimeout
	} else if errors.Is(lastErr, context.Canceled) {
		code = RejectCancelled
	}
	rt.mu.Lock()
	call.Status = StatusFailed
	now = time.Now()
	call.FinishedAt = &now
	call.RejectReason = &RejectReason{Code: code, Message: lastErr.Error()}
	rt.mu.Unlock()
	rt.audit.append(call, "failed", lastErr.Error())
}

func (rt *Runtime) handleResult(call *Call, def *ToolDefinition, res json.RawMessage) {
	if def.MaxResultBytes > 0 && len(res) > def.MaxResultBytes {
		switch def.ResultStrategy {
		case ResultTruncate:
			rt.mu.Lock()
			call.Result = append(json.RawMessage(nil), res[:def.MaxResultBytes]...)
			call.ResultTruncated = true
			call.Status = StatusSucceeded
			now := time.Now()
			call.FinishedAt = &now
			rt.mu.Unlock()
			rt.audit.append(call, "succeeded", fmt.Sprintf("result truncated to %d bytes", def.MaxResultBytes))
			return
		default:
			rt.reject(call, &RejectReason{Code: RejectResultTooLarge,
				Message: fmt.Sprintf("result %d bytes exceed limit %d", len(res), def.MaxResultBytes)})
			return
		}
	}
	rt.mu.Lock()
	call.Result = append(json.RawMessage(nil), res...)
	call.Status = StatusSucceeded
	now := time.Now()
	call.FinishedAt = &now
	rt.mu.Unlock()
	rt.audit.append(call, "succeeded", "")
}

func (rt *Runtime) finishCancelled(call *Call, ctx context.Context) {
	code := RejectCancelled
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		code = RejectTimeout
	}
	rt.mu.Lock()
	call.Status = StatusCancelled
	call.RejectReason = &RejectReason{Code: code,
		Message: fmt.Sprintf("call ended before completion: %v", ctx.Err())}
	now := time.Now()
	call.FinishedAt = &now
	rt.mu.Unlock()
	rt.audit.append(call, "cancelled", call.RejectReason.Error())
}

func backoffDuration(policy RetryPolicy, attempt int) time.Duration {
	d := policy.InitialBackoff
	for i := 1; i < attempt; i++ {
		d = time.Duration(float64(d) * policy.BackoffFactor)
		if d > policy.MaxBackoff {
			return policy.MaxBackoff
		}
	}
	if d > policy.MaxBackoff {
		return policy.MaxBackoff
	}
	return d
}

// ---------- 状态查询 ----------

func (rt *Runtime) ctxFor(callID string) context.Context {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if ctx, ok := rt.ctxs[callID]; ok {
		return ctx
	}
	return context.Background()
}

func (rt *Runtime) newCall(toolName, version string, params json.RawMessage) *Call {
	id := fmt.Sprintf("call-%d-%d", time.Now().UnixNano(), atomic.AddUint64(&rt.seq, 1))
	call := &Call{
		ID:          id,
		ToolName:    toolName,
		ToolVersion: version,
		Params:      append(json.RawMessage(nil), params...),
		Status:      StatusPending,
		CreatedAt:   time.Now(),
	}
	rt.mu.Lock()
	rt.calls[id] = call
	rt.mu.Unlock()
	return call
}

func (rt *Runtime) reject(call *Call, reason *RejectReason) {
	rt.mu.Lock()
	call.Status = StatusRejected
	call.RejectReason = reason
	now := time.Now()
	call.FinishedAt = &now
	rt.mu.Unlock()
	rt.audit.append(call, "rejected", reason.Error())
}

// GetCall 按 ID 查询调用记录（返回副本，避免外部修改）。
func (rt *Runtime) GetCall(callID string) *Call {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	call, ok := rt.calls[callID]
	if !ok {
		return nil
	}
	cp := *call
	cp.Params = append(json.RawMessage(nil), call.Params...)
	cp.Result = append(json.RawMessage(nil), call.Result...)
	cp.Attempts = append([]*AttemptRecord(nil), call.Attempts...)
	return &cp
}

// CallFilter 是调用列表的过滤条件。
type CallFilter struct {
	ToolName string
	Status   Status
}

// ListCalls 返回符合过滤条件的调用记录副本。
func (rt *Runtime) ListCalls(filter CallFilter) []*Call {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	out := make([]*Call, 0)
	for _, call := range rt.calls {
		if filter.ToolName != "" && call.ToolName != filter.ToolName {
			continue
		}
		if filter.Status != "" && call.Status != filter.Status {
			continue
		}
		cp := *call
		cp.Params = append(json.RawMessage(nil), call.Params...)
		cp.Result = append(json.RawMessage(nil), call.Result...)
		cp.Attempts = append([]*AttemptRecord(nil), call.Attempts...)
		out = append(out, &cp)
	}
	return out
}

// BoundDefinition 返回某调用受理时绑定的工具定义（校验依据可追溯）。
func (rt *Runtime) BoundDefinition(callID string) (*ToolDefinition, bool) {
	rt.mu.Lock()
	call, ok := rt.calls[callID]
	rt.mu.Unlock()
	if !ok || call.ToolVersion == "" {
		return nil, false
	}
	def, ok := rt.registry.Version(call.ToolName, call.ToolVersion)
	if !ok {
		return nil, false
	}
	cp := *def
	cp.Params = cloneSchema(def.Params)
	return &cp, true
}

// ---------- 流式超时清扫 ----------

func (rt *Runtime) streamSweeper() {
	defer rt.wg.Done()
	interval := rt.cfg.StreamAssemblyTimeout / 3
	if interval < 200*time.Millisecond {
		interval = 200 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-rt.stopCh:
			return
		case <-ticker.C:
			rt.sweepStreams()
		}
	}
}

func (rt *Runtime) sweepStreams() {
	rt.mu.Lock()
	type timedOut struct {
		callID   string
		toolName string
		version  string
	}
	var outs []timedOut
	now := time.Now()
	for id, state := range rt.streams {
		if state.finalized {
			continue
		}
		if now.Sub(state.assembler.StartedAt()) > rt.cfg.StreamAssemblyTimeout {
			outs = append(outs, timedOut{id, state.toolName, state.def.Version})
		}
	}
	for _, o := range outs {
		state := rt.streams[o.callID]
		if state != nil {
			state.timedOut = true
		}
	}
	rt.mu.Unlock()
	for _, o := range outs {
		call := rt.newCall(o.toolName, o.version, nil)
		rt.reject(call, &RejectReason{Code: RejectStreamTimeout,
			Message: fmt.Sprintf("stream %q assembly timed out", o.callID)})
	}
}

// Close 停止运行时：等待执行中的调用结束，
// 仍在排队的调用标记为取消，不留下中间状态。
func (rt *Runtime) Close() {
	rt.closeOnce.Do(func() {
		close(rt.stopCh)
		rt.mu.Lock()
		// 仍在排队、未抢占到等待位的调用：标记为取消
		now := time.Now()
		for _, call := range rt.calls {
			if call.Status == StatusPending {
				call.Status = StatusCancelled
				call.RejectReason = &RejectReason{Code: RejectCancelled,
					Message: "runtime closed before execution started"}
				call.FinishedAt = &now
			}
		}
		rt.mu.Unlock()
	})
	rt.wg.Wait()
}
