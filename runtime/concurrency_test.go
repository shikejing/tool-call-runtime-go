package runtime

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func fastCfg(maxConc, queue int, timeout time.Duration) Config {
	cfg := DefaultConfig()
	cfg.Executor.MaxConcurrent = maxConc
	cfg.Executor.QueueSize = queue
	cfg.Executor.CallTimeout = timeout
	cfg.Executor.MaxRetries = 0
	return cfg
}

// 并发上限与队列容量：超出返回可区分拒绝；慢工具不阻塞其他工具。
func TestConcurrencyLimit(t *testing.T) {
	rt := New(fastCfg(2, 1, 5*time.Second))
	block := make(chan struct{})
	rt.Registry().Register(ToolDef{
		Name: "slow", Version: 1,
		Params: &ParamSchema{Type: TypeObject, Properties: map[string]*ParamSchema{}},
	}, func(ctx *CallContext, args map[string]any) (any, error) {
		<-block
		return "ok", nil
	})
	rt.Registry().Register(ToolDef{
		Name: "fast", Version: 1,
		Params: &ParamSchema{Type: TypeObject, Properties: map[string]*ParamSchema{}},
	}, func(ctx *CallContext, args map[string]any) (any, error) {
		return "fast", nil
	})

	var wg sync.WaitGroup
	started := make(chan struct{}, 8)
	// 占满 2 个执行位 + 1 个队列位
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			started <- struct{}{}
			rt.Call("s"+itoa(i), "slow", map[string]any{})
		}(i)
	}
	// 等执行位与队列位都被占
	time.Sleep(100 * time.Millisecond)

	// 第 4 个 slow 调用：队列满 -> KindQueueFull
	_, rej := rt.Call("s-overflow", "slow", map[string]any{})
	if rej == nil || rej.Kind != KindQueueFull {
		t.Fatalf("expect queue_full, got %v", rej)
	}
	rec, _ := rt.Status("s-overflow")
	if rec.Status != StatusRejected {
		t.Fatalf("expect rejected record, got %s", rec.Status)
	}

	// 其他工具不受影响
	res, rej := rt.Call("f1", "fast", map[string]any{})
	if rej != nil || res != "fast" {
		t.Fatalf("fast tool blocked: res=%v rej=%v", res, rej)
	}

	close(block)
	wg.Wait()
	<-started
	// 三个 slow 调用最终都应完成，不留中间状态
	for i := 0; i < 3; i++ {
		rec, _ := rt.Status("s" + itoa(i))
		if rec.Status != StatusCompleted {
			t.Fatalf("slow call s%d stuck in %s", i, rec.Status)
		}
	}
}

// 无队列时并发满返回 KindConcurrencyFull，与队列满可区分。
func TestConcurrencyFullNoQueue(t *testing.T) {
	rt := New(fastCfg(1, 0, 5*time.Second))
	block := make(chan struct{})
	rt.Registry().Register(ToolDef{
		Name: "t", Version: 1,
		Params: &ParamSchema{Type: TypeObject, Properties: map[string]*ParamSchema{}},
	}, func(ctx *CallContext, args map[string]any) (any, error) {
		<-block
		return nil, nil
	})
	done := make(chan struct{})
	go func() { rt.Call("a", "t", map[string]any{}); close(done) }()
	time.Sleep(50 * time.Millisecond)
	_, rej := rt.Call("b", "t", map[string]any{})
	if rej == nil || rej.Kind != KindConcurrencyFull {
		t.Fatalf("expect concurrency_full, got %v", rej)
	}
	close(block)
	<-done
}

// 超时：慢调用超时后状态为 failed，原因为 KindTimeout。
func TestTimeout(t *testing.T) {
	rt := New(fastCfg(1, 0, 50*time.Millisecond))
	rt.Registry().Register(ToolDef{
		Name: "t", Version: 1,
		Params: &ParamSchema{Type: TypeObject, Properties: map[string]*ParamSchema{}},
	}, func(ctx *CallContext, args map[string]any) (any, error) {
		select {
		case <-ctx.Done:
			return nil, errors.New("aborted")
		case <-time.After(5 * time.Second):
			return "late", nil
		}
	})
	_, rej := rt.Call("t1", "t", map[string]any{})
	if rej == nil || rej.Kind != KindTimeout {
		t.Fatalf("expect timeout, got %v", rej)
	}
	rec, _ := rt.Status("t1")
	if rec.Status != StatusFailed {
		t.Fatalf("expect failed, got %s", rec.Status)
	}
}

// 取消：执行中取消，状态为 canceled，不停在中间态。
func TestCancel(t *testing.T) {
	rt := New(fastCfg(1, 0, 5*time.Second))
	rt.Registry().Register(ToolDef{
		Name: "t", Version: 1,
		Params: &ParamSchema{Type: TypeObject, Properties: map[string]*ParamSchema{}},
	}, func(ctx *CallContext, args map[string]any) (any, error) {
		<-ctx.Done // 响应取消
		return nil, errors.New("aborted")
	})
	resCh := make(chan *RejectError, 1)
	go func() {
		_, rej := rt.Call("c1", "t", map[string]any{})
		resCh <- rej
	}()
	time.Sleep(50 * time.Millisecond)
	rt.Cancel("c1")
	rej := <-resCh
	if rej == nil || rej.Kind != KindCanceled {
		t.Fatalf("expect canceled, got %v", rej)
	}
	rec, _ := rt.Status("c1")
	if rec.Status != StatusCanceled {
		t.Fatalf("expect canceled status, got %s", rec.Status)
	}
}

// 排队中取消。
func TestCancelWhileQueued(t *testing.T) {
	rt := New(fastCfg(1, 4, 5*time.Second))
	block := make(chan struct{})
	rt.Registry().Register(ToolDef{
		Name: "t", Version: 1,
		Params: &ParamSchema{Type: TypeObject, Properties: map[string]*ParamSchema{}},
	}, func(ctx *CallContext, args map[string]any) (any, error) {
		<-block
		return "done", nil
	})
	go rt.Call("head", "t", map[string]any{})
	time.Sleep(50 * time.Millisecond) // head 占住执行位
	resCh := make(chan *RejectError, 1)
	go func() {
		_, rej := rt.Call("queued", "t", map[string]any{})
		resCh <- rej
	}()
	time.Sleep(50 * time.Millisecond) // queued 在队列中
	rt.Cancel("queued")
	rej := <-resCh
	if rej == nil || rej.Kind != KindCanceled {
		t.Fatalf("expect canceled while queued, got %v", rej)
	}
	close(block)
}
