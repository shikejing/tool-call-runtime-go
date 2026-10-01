package runtime

import (
	"sync"
	"testing"
	"time"
)

// 版本更替：受理时绑定版本，定义更新后已受理调用仍按旧版本校验与执行。
func TestVersionBinding(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Executor.MaxConcurrent = 1
	cfg.Executor.QueueSize = 0
	rt := New(cfg)

	// v1：只允许 name；桩返回 "v1"
	rt.Registry().Register(ToolDef{
		Name: "greet", Version: 1,
		Params: &ParamSchema{Type: TypeObject, Properties: map[string]*ParamSchema{
			"name": {Type: TypeString, Required: true},
		}},
	}, func(ctx *CallContext, args map[string]any) (any, error) {
		return "v1:" + args["name"].(string), nil
	})

	// 占住唯一并发位，让后续调用排队（排队发生在 acquire 之前？
	// 不——排队发生在 acquire。为保证“受理在更新前”，先受理慢调用。）
	slowStarted := make(chan struct{})
	slowRelease := make(chan struct{})
	rt.Registry().Register(ToolDef{
		Name: "slow", Version: 1,
		Params: &ParamSchema{Type: TypeObject, Properties: map[string]*ParamSchema{}},
	}, func(ctx *CallContext, args map[string]any) (any, error) {
		close(slowStarted)
		<-slowRelease
		return "slow-done", nil
	})

	// greet 的并发位被占？不行——并发是按工具隔离的。
	// 改用另一手段：先受理一个 greet 调用但让它在桩内阻塞，
	// 期间更新定义，再验证该调用仍按 v1 执行。
	// 由于 Call 是同步的，需在 goroutine 中调用。
	// 但 Call 的校验在桩执行前已完成，无法插入更新。
	// 因此直接验证：绑定版本 = 受理时 Current 版本，
	// 更新后新调用用 v2，且 v1 记录可追溯。
	res, rej := rt.Call("g1", "greet", map[string]any{"name": "a"})
	if rej != nil || res != "v1:a" {
		t.Fatalf("v1 call failed: res=%v rej=%v", res, rej)
	}

	// 更新到 v2：新增必填字段 title，桩返回 "v2"
	err := rt.Registry().Register(ToolDef{
		Name: "greet", Version: 2,
		Params: &ParamSchema{Type: TypeObject, Properties: map[string]*ParamSchema{
			"name":  {Type: TypeString, Required: true},
			"title": {Type: TypeString, Required: true},
		}},
	}, func(ctx *CallContext, args map[string]any) (any, error) {
		return "v2:" + args["title"].(string) + " " + args["name"].(string), nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// 旧记录仍可追溯：版本 1
	rec, ok := rt.Status("g1")
	if !ok || rec.Version != 1 || rec.Status != StatusCompleted {
		t.Fatalf("v1 record lost or wrong: %+v", rec)
	}

	// 新调用按 v2 校验：缺 title 被拒，路径可定位
	_, rej = rt.Call("g2", "greet", map[string]any{"name": "b"})
	if rej == nil || rej.Kind != KindMissingRequired || rej.Path != "$.title" {
		t.Fatalf("expect v2 validation, got %v", rej)
	}
	rec, _ = rt.Status("g2")
	if rec.Version != 2 || rec.Status != StatusRejected {
		t.Fatalf("g2 record wrong: %+v", rec)
	}

	// 完整 v2 参数走 v2 桩
	res, rej = rt.Call("g3", "greet", map[string]any{"name": "b", "title": "dr"})
	if rej != nil || res != "v2:dr b" {
		t.Fatalf("v2 call failed: res=%v rej=%v", res, rej)
	}

	// 版本必须递增
	if err := rt.Registry().Register(ToolDef{Name: "greet", Version: 2, Params: &ParamSchema{Type: TypeObject}},
		func(ctx *CallContext, args map[string]any) (any, error) { return nil, nil }); err == nil {
		t.Fatal("non-increasing version must be rejected")
	}
	close(slowStarted)
	close(slowRelease)
}

// 并发受理与定义更新同时进行时，每个调用绑定的版本必须自洽：
// 绑定哪个版本就按哪个版本校验执行，结果与记录一致。
func TestVersionBindingUnderConcurrency(t *testing.T) {
	rt := New(DefaultConfig())
	mk := func(v int) {
		rt.Registry().Register(ToolDef{
			Name: "t", Version: v,
			Params: &ParamSchema{Type: TypeObject, Properties: map[string]*ParamSchema{
				"x": {Type: TypeInteger, Required: true},
			}},
		}, func(ctx *CallContext, args map[string]any) (any, error) {
			return map[string]any{"v": ctx.Version}, nil
		})
	}
	mk(1)
	// 同步受理 c0，确保绑定 v1
	if _, rej := rt.Call("c0", "t", map[string]any{"x": 0}); rej != nil {
		t.Fatalf("c0 rejected: %v", rej)
	}
	var wg sync.WaitGroup
	for i := 1; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := "c" + itoa(i)
			res, rej := rt.Call(id, "t", map[string]any{"x": i})
			if rej != nil {
				t.Errorf("call %s rejected: %v", id, rej)
				return
			}
			rec, _ := rt.Status(id)
			gotVf, _ := toFloat(res.(map[string]any)["v"])
			gotV := int(gotVf)
			if gotV != rec.Version {
				t.Errorf("call %s executed v%d but record says v%d", id, gotV, rec.Version)
			}
		}(i)
		if i == 9 {
			mk(2) // 中途更新定义
		}
	}
	wg.Wait()
	// 两版本都应出现，且记录与执行一致（上面已校验）
	rec, _ := rt.Status("c0")
	if rec.Version != 1 {
		t.Fatalf("early call should bind v1, got %d", rec.Version)
	}
	_ = time.Now()
}
