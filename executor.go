package toolruntime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sync"
)

// StubExecutor 是本地桩执行器：
//   - 结果对同一调用确定性可复现；
//   - 副作用按 callID 幂等记录，重试不会重复产生副作用；
//   - 可通过 FailFunc 注入失败（可指定失败发生在副作用之前还是之后）。
type StubExecutor struct {
	mu              sync.Mutex
	sideEffects     map[string]json.RawMessage // callID -> 已落定的结果
	attempts        map[string]int
	sideEffectCount map[string]int
	FailFunc        func(call *Call, attempt int) (fail bool, afterSideEffect bool)
}

func NewStubExecutor() *StubExecutor {
	return &StubExecutor{
		sideEffects:     make(map[string]json.RawMessage),
		attempts:        make(map[string]int),
		sideEffectCount: make(map[string]int),
	}
}

// Execute 执行本地桩逻辑。
func (e *StubExecutor) Execute(ctx context.Context, call *Call) (json.RawMessage, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	// 幂等重放：副作用已落定时直接返回同一结果，不重复产生副作用
	if res, ok := e.sideEffects[call.ID]; ok {
		return res, nil
	}

	attempt := e.attempts[call.ID] + 1
	e.attempts[call.ID] = attempt

	// 副作用开始前响应取消/超时，避免半执行
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if e.FailFunc != nil {
		if fail, afterSideEffect := e.FailFunc(call, attempt); fail {
			if afterSideEffect {
				// 模拟“副作用已落定但结果未返回”的崩溃：记录副作用后报错，
				// 重试时必须重放同一结果，不得再次产生副作用。
				res := e.buildResult(call)
				e.sideEffects[call.ID] = res
				e.sideEffectCount[call.ID]++
				return nil, &sideEffectError{msg: "injected failure after side effect"}
			}
			return nil, &sideEffectError{msg: "injected failure before side effect"}
		}
	}

	res := e.buildResult(call)
	e.sideEffects[call.ID] = res
	e.sideEffectCount[call.ID]++
	return res, nil
}

// Attempts 返回某调用的执行尝试次数。
func (e *StubExecutor) Attempts(callID string) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.attempts[callID]
}

// SideEffectCount 返回某调用实际产生副作用的次数。
func (e *StubExecutor) SideEffectCount(callID string) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.sideEffectCount[callID]
}

// HasSideEffect 返回某调用是否已落定副作用。
func (e *StubExecutor) HasSideEffect(callID string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	_, ok := e.sideEffects[callID]
	return ok
}

// Reset 清空桩执行器的全部状态（测试隔离用）。
func (e *StubExecutor) Reset() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.sideEffects = make(map[string]json.RawMessage)
	e.attempts = make(map[string]int)
	e.sideEffectCount = make(map[string]int)
}

func (e *StubExecutor) buildResult(call *Call) json.RawMessage {
	sum := sha256.Sum256(append(append([]byte(call.ToolName+"|"+call.ToolVersion+"|"), call.Params...), []byte("|"+call.ID)...))
	fp := hex.EncodeToString(sum[:])
	out, _ := json.Marshal(map[string]any{
		"tool":        call.ToolName,
		"version":     call.ToolVersion,
		"call_id":     call.ID,
		"fingerprint": fp,
		"echo":        json.RawMessage(call.Params),
	})
	return out
}

type sideEffectError struct{ msg string }

func (e *sideEffectError) Error() string { return e.msg }
