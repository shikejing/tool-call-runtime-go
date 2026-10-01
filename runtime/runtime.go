package runtime

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// Config 运行时总配置。
type Config struct {
	Executor       ExecutorConfig
	MaxArgBytes    int // 单次参数字节上限，超限拒绝（KindArgsTooLarge）
	MaxResultBytes int // 返回结果字节上限，超限失败（KindResultTooLarge）
}

func DefaultConfig() Config {
	return Config{
		Executor: ExecutorConfig{
			MaxConcurrent: 4,
			QueueSize:     16,
			CallTimeout:   5 * time.Second,
			MaxRetries:    1,
		},
		MaxArgBytes:    1 << 20,
		MaxResultBytes: 1 << 20,
	}
}

// Runtime 门面：登记、受理、执行、查询。全部在本进程内完成。
type Runtime struct {
	cfg   Config
	reg   *Registry
	exec  *executor
	audit *AuditLog

	mu      sync.Mutex
	cancels map[string]chan struct{}
	streams map[string]*StreamSession
}

func New(cfg Config) *Runtime {
	if cfg.MaxArgBytes <= 0 {
		cfg.MaxArgBytes = 1 << 20
	}
	if cfg.MaxResultBytes <= 0 {
		cfg.MaxResultBytes = 1 << 20
	}
	return &Runtime{
		cfg:     cfg,
		reg:     NewRegistry(),
		exec:    newExecutor(cfg.Executor),
		audit:   NewAuditLog(),
		cancels: make(map[string]chan struct{}),
		streams: make(map[string]*StreamSession),
	}
}

// Registry 暴露登记接口。
func (r *Runtime) Registry() *Registry { return r.reg }

// Audit 暴露审计查询接口。
func (r *Runtime) Audit() *AuditLog { return r.audit }

// Status 查询调用当前状态与校验依据。
func (r *Runtime) Status(callID string) (CallRecord, bool) {
	return r.audit.Get(callID)
}

// Cancel 取消进行中的调用；已结束的调用调用此方法为空操作。
func (r *Runtime) Cancel(callID string) {
	r.mu.Lock()
	ch, ok := r.cancels[callID]
	r.mu.Unlock()
	if ok {
		select {
		case <-ch: // 已关闭
		default:
			close(ch)
		}
	}
}

// Call 一次性调用：args 为完整参数。
// 受理时绑定当前生效的定义版本，之后定义更新不影响本次调用。
func (r *Runtime) Call(callID, tool string, args map[string]any) (any, *RejectError) {
	raw, err := json.Marshal(args)
	if err != nil {
		return nil, reject(KindTypeMismatch, "$", "args not JSON-serializable: %v", err)
	}
	return r.accept(callID, tool, raw)
}

// accept 公共受理管线：版本绑定 -> 容量 -> 解析 -> 校验 -> 执行。
func (r *Runtime) accept(callID, tool string, raw []byte) (any, *RejectError) {
	def, ok := r.reg.Current(tool)
	version := 0
	if ok {
		version = def.Version
	}
	r.audit.Open(callID, tool, version)
	cancel := make(chan struct{})
	r.mu.Lock()
	r.cancels[callID] = cancel
	r.mu.Unlock()

	fail := func(rej *RejectError) (any, *RejectError) {
		r.audit.SetReject(callID, rej)
		r.mu.Lock()
		delete(r.cancels, callID)
		r.mu.Unlock()
		return nil, rej
	}

	if !ok {
		return fail(reject(KindUnknownTool, "", "tool %q not registered", tool))
	}
	// 容量边界：参数规模
	if len(raw) > r.cfg.MaxArgBytes {
		return fail(reject(KindArgsTooLarge, "", "args %d bytes exceed limit %d", len(raw), r.cfg.MaxArgBytes))
	}
	// 解析并归一化（数字统一为 float64，与流式路径一致）
	var norm map[string]any
	if err := json.Unmarshal(raw, &norm); err != nil {
		return fail(reject(KindNotObject, "$", "args must be a JSON object: %v", err))
	}
	if norm == nil {
		norm = map[string]any{}
	}
	// 按受理时绑定的版本校验
	if rej := Validate(def.Params, norm); rej != nil {
		return fail(rej)
	}
	// 执行
	res, rej := r.execute(callID, def, norm, cancel)
	r.mu.Lock()
	delete(r.cancels, callID)
	r.mu.Unlock()
	if rej != nil {
		return nil, rej
	}
	return res, nil
}

// execute 并发隔离 + 超时/取消 + 重试。
// 重试使用同一 CallID，工具桩须按 CallID 幂等；
// 结果只写一次，重试前后对外可见结果一致。
func (r *Runtime) execute(callID string, def ToolDef, args map[string]any, cancel <-chan struct{}) (any, *RejectError) {
	release, rej := r.exec.acquire(def.Name, cancel)
	if rej != nil {
		r.audit.SetReject(callID, rej)
		return nil, rej
	}
	defer release()

	_, fn, _ := r.reg.Version(def.Name, def.Version) // 绑定版本的桩
	r.audit.Transition(callID, StatusRunning, "running")

	maxAttempts := r.cfg.Executor.MaxRetries + 1
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		r.audit.SetAttempts(callID, attempt)
		select {
		case <-cancel:
			r.audit.Transition(callID, StatusCanceled, "canceled before attempt "+itoa(attempt))
			return nil, reject(KindCanceled, "", "call %s canceled", callID)
		default:
		}
		done := make(chan struct{})
		ctx := &CallContext{
			CallID: callID, Tool: def.Name, Version: def.Version,
			Attempt: attempt, Done: done,
		}
		type outcome struct {
			res any
			err error
		}
		outCh := make(chan outcome, 1)
		go func() {
			res, err := fn(ctx, args)
			outCh <- outcome{res, err}
		}()
		timer := time.NewTimer(r.cfg.Executor.CallTimeout)
		select {
		case out := <-outCh:
			timer.Stop()
			close(done)
			if out.err != nil {
				lastErr = out.err
				r.audit.Transition(callID, StatusRunning,
					fmt.Sprintf("attempt %d failed: %v", attempt, out.err))
				continue // 按策略重试
			}
			// 容量边界：结果大小
			raw, err := json.Marshal(out.res)
			if err != nil {
				rej := reject(KindExecFailed, "", "result not JSON-serializable: %v", err)
				r.audit.Transition(callID, StatusFailed, rej.Error())
				return nil, rej
			}
			if len(raw) > r.cfg.MaxResultBytes {
				rej := reject(KindResultTooLarge, "", "result %d bytes exceed limit %d", len(raw), r.cfg.MaxResultBytes)
				r.audit.Transition(callID, StatusFailed, rej.Error())
				return nil, rej
			}
			r.audit.SetResult(callID, out.res, attempt)
			return out.res, nil
		case <-timer.C:
			close(done) // 通知桩停止
			r.audit.Transition(callID, StatusFailed, "timeout after "+r.cfg.Executor.CallTimeout.String())
			return nil, reject(KindTimeout, "", "call %s timed out after %s", callID, r.cfg.Executor.CallTimeout)
		case <-cancel:
			close(done)
			r.audit.Transition(callID, StatusCanceled, "canceled during attempt "+itoa(attempt))
			return nil, reject(KindCanceled, "", "call %s canceled", callID)
		}
	}
	r.audit.Transition(callID, StatusFailed, fmt.Sprintf("all %d attempts failed", maxAttempts))
	return nil, reject(KindExecFailed, "", "call %s failed after %d attempts: %v", callID, maxAttempts, lastErr)
}
