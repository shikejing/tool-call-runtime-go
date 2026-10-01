package runtime

import (
	"errors"
	"strings"
	"sync/atomic"
	"testing"
)

// 重试：前 N 次失败后成功；桩按 CallID 幂等，副作用只生效一次。
func TestRetryIdempotent(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Executor.MaxRetries = 2
	rt := New(cfg)

	var sideEffects atomic.Int32
	var attempts atomic.Int32
	rt.Registry().Register(ToolDef{
		Name: "flaky", Version: 1,
		Params: &ParamSchema{Type: TypeObject, Properties: map[string]*ParamSchema{}},
	}, func(ctx *CallContext, args map[string]any) (any, error) {
		attempts.Add(1)
		// 幂等：同一 CallID 副作用只记一次（真实场景用 CallID 去重）
		if ctx.Attempt == 1 {
			sideEffects.Add(1)
		}
		if ctx.Attempt < 3 {
			return nil, errors.New("transient error")
		}
		return "ok", nil
	})

	res, rej := rt.Call("r1", "flaky", map[string]any{})
	if rej != nil || res != "ok" {
		t.Fatalf("expect success after retries, res=%v rej=%v", res, rej)
	}
	if attempts.Load() != 3 {
		t.Fatalf("expect 3 attempts, got %d", attempts.Load())
	}
	rec, _ := rt.Status("r1")
	if rec.Status != StatusCompleted || rec.Attempts != 3 {
		t.Fatalf("bad record: %+v", rec)
	}
	// 结果只写一次，对外一致
	if rec.Result != "ok" {
		t.Fatalf("result inconsistent: %v", rec.Result)
	}
}

// 重试耗尽：返回可定位的失败原因，状态 failed。
func TestRetryExhausted(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Executor.MaxRetries = 1
	rt := New(cfg)
	rt.Registry().Register(ToolDef{
		Name: "bad", Version: 1,
		Params: &ParamSchema{Type: TypeObject, Properties: map[string]*ParamSchema{}},
	}, func(ctx *CallContext, args map[string]any) (any, error) {
		return nil, errors.New("always fails")
	})
	_, rej := rt.Call("r2", "bad", map[string]any{})
	if rej == nil || rej.Kind != KindExecFailed {
		t.Fatalf("expect exec_failed, got %v", rej)
	}
	if !strings.Contains(rej.Msg, "r2") {
		t.Fatalf("failure must locate the call: %v", rej)
	}
	rec, _ := rt.Status("r2")
	if rec.Status != StatusFailed || rec.Attempts != 2 {
		t.Fatalf("bad record: %+v", rec)
	}
}

// 参数超限拒绝；结果超限失败；原因可区分。
func TestCapacityLimits(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxArgBytes = 64
	cfg.MaxResultBytes = 32
	rt := New(cfg)
	rt.Registry().Register(ToolDef{
		Name: "t", Version: 1,
		Params: &ParamSchema{Type: TypeObject, Properties: map[string]*ParamSchema{
			"s": {Type: TypeString},
		}},
	}, func(ctx *CallContext, args map[string]any) (any, error) {
		if args["s"] == "big" {
			return strings.Repeat("x", 100), nil // 超限结果
		}
		return "ok", nil
	})

	// 参数超限
	_, rej := rt.Call("big-args", "t", map[string]any{"s": strings.Repeat("a", 100)})
	if rej == nil || rej.Kind != KindArgsTooLarge {
		t.Fatalf("expect args_too_large, got %v", rej)
	}
	rec, _ := rt.Status("big-args")
	if rec.Status != StatusRejected {
		t.Fatalf("expect rejected, got %s", rec.Status)
	}

	// 结果超限
	_, rej = rt.Call("big-result", "t", map[string]any{"s": "big"})
	if rej == nil || rej.Kind != KindResultTooLarge {
		t.Fatalf("expect result_too_large, got %v", rej)
	}
	rec, _ = rt.Status("big-result")
	if rec.Status != StatusFailed {
		t.Fatalf("expect failed, got %s", rec.Status)
	}
}

// 未知工具拒绝。
func TestUnknownTool(t *testing.T) {
	rt := New(DefaultConfig())
	_, rej := rt.Call("u1", "nope", map[string]any{})
	if rej == nil || rej.Kind != KindUnknownTool {
		t.Fatalf("expect unknown_tool, got %v", rej)
	}
	rec, ok := rt.Status("u1")
	if !ok || rec.Status != StatusRejected {
		t.Fatalf("rejection must be auditable: %+v", rec)
	}
}

// 非法参数不得进入执行阶段。
func TestInvalidArgsNeverExecute(t *testing.T) {
	rt := New(DefaultConfig())
	executed := false
	rt.Registry().Register(ToolDef{
		Name: "t", Version: 1,
		Params: &ParamSchema{Type: TypeObject, Properties: map[string]*ParamSchema{
			"n": {Type: TypeInteger, Required: true},
		}},
	}, func(ctx *CallContext, args map[string]any) (any, error) {
		executed = true
		return nil, nil
	})
	_, rej := rt.Call("v1", "t", map[string]any{"n": "not-a-number"})
	if rej == nil || rej.Kind != KindTypeMismatch || rej.Path != "$.n" {
		t.Fatalf("expect type_mismatch at $.n, got %v", rej)
	}
	if executed {
		t.Fatal("invalid args must not reach execution")
	}
}
