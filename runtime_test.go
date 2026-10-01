package toolruntime

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"
)

// ---------- 测试辅助 ----------

func ptr[T any](v T) *T { return &v }

func echoSchema() *Schema {
	return &Schema{
		Type:           TypeObject,
		RequiredFields: []string{"a"},
		Properties: map[string]*Schema{
			"a": {Type: TypeInteger, Min: ptr(0.0), Max: ptr(100.0)},
			"b": {Type: TypeString, MinLength: ptr(1), MaxLength: ptr(10)},
			"nested": {
				Type: TypeObject,
				Properties: map[string]*Schema{
					"x": {Type: TypeArray, Items: &Schema{Type: TypeString}},
				},
			},
		},
	}
}

func registerTool(t *testing.T, rt *Runtime, name, version string, mods ...func(*ToolDefinition)) *ToolDefinition {
	t.Helper()
	def := &ToolDefinition{
		Name:        name,
		Version:     version,
		Params:      echoSchema(),
		Concurrency: 2,
		QueueSize:   2,
		Timeout:     2 * time.Second,
	}
	for _, m := range mods {
		m(def)
	}
	if err := rt.Registry().Register(def); err != nil {
		t.Fatalf("register %s: %v", name, err)
	}
	return def
}

func waitTerminal(t *testing.T, rt *Runtime, callID string, timeout time.Duration) *Call {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		call := rt.GetCall(callID)
		if call == nil {
			t.Fatalf("call %s not found", callID)
		}
		switch call.Status {
		case StatusSucceeded, StatusFailed, StatusRejected, StatusCancelled:
			return call
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("call %s did not finish within %s", callID, timeout)
	return nil
}

func submitAndWait(t *testing.T, rt *Runtime, name string, params string) *Call {
	t.Helper()
	call, err := rt.Submit(context.Background(), name, json.RawMessage(params))
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	return waitTerminal(t, rt, call.ID, 5*time.Second)
}

// ---------- 1. 参数校验 ----------

func TestSchemaValidation(t *testing.T) {
	rt := NewRuntime(Config{})
	defer rt.Close()
	registerTool(t, rt, "echo", "v1")

	cases := []struct {
		name   string
		params string
		code   RejectCode
		path   string
	}{
		{"type mismatch", `{"a":"x"}`, RejectSchemaInvalid, "$.a"},
		{"missing required", `{}`, RejectSchemaInvalid, "$.a"},
		{"unknown field", `{"a":1,"z":2}`, RejectSchemaInvalid, "$.z"},
		{"out of range", `{"a":101}`, RejectSchemaInvalid, "$.a"},
		{"nested type mismatch", `{"a":1,"nested":{"x":[1]}}`, RejectSchemaInvalid, "$.nested.x[0]"},
		{"string too long", `{"a":1,"b":"12345678901"}`, RejectSchemaInvalid, "$.b"},
		{"integer expected", `{"a":1.5}`, RejectSchemaInvalid, "$.a"},
		{"bad json", `{not json`, RejectSchemaInvalid, "$"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			call := submitAndWait(t, rt, "echo", tc.params)
			if call.Status != StatusRejected {
				t.Fatalf("expected rejected, got %s", call.Status)
			}
			if call.RejectReason == nil || call.RejectReason.Code != tc.code {
				t.Fatalf("expected code %s, got %v", tc.code, call.RejectReason)
			}
			if call.RejectReason.Path != tc.path {
				t.Fatalf("expected path %s, got %s", tc.path, call.RejectReason.Path)
			}
		})
	}

	t.Run("valid params succeed", func(t *testing.T) {
		call := submitAndWait(t, rt, "echo", `{"a":5,"b":"hi","nested":{"x":["y"]}}`)
		if call.Status != StatusSucceeded {
			t.Fatalf("expected succeeded, got %s: %v", call.Status, call.RejectReason)
		}
		if len(call.Result) == 0 {
			t.Fatal("expected result")
		}
	})
}

func TestUnknownTool(t *testing.T) {
	rt := NewRuntime(Config{})
	defer rt.Close()
	call := submitAndWait(t, rt, "nope", `{"a":1}`)
	if call.Status != StatusRejected || call.RejectReason.Code != RejectUnknownTool {
		t.Fatalf("expected unknown_tool reject, got %v", call.RejectReason)
	}
}

// ---------- 2. 流式分片 ----------

func feedChunks(t *testing.T, rt *Runtime, tool, streamID string, chunks []Chunk) *Call {
	t.Helper()
	var last *Call
	for _, ch := range chunks {
		call, err := rt.SubmitChunk(context.Background(), tool, streamID, ch)
		if err != nil {
			t.Fatalf("submit chunk: %v", err)
		}
		if call != nil {
			last = call
		}
	}
	return last
}

func TestStreamAssembly(t *testing.T) {
	rt := NewRuntime(Config{})
	defer rt.Close()
	registerTool(t, rt, "echo", "v1")
	raw := `{"a":5,"b":"hi"}`

	t.Run("in order", func(t *testing.T) {
		chunks := []Chunk{
			{Seq: 0, Data: []byte(raw[:8])},
			{Seq: 1, Data: []byte(raw[8:])},
			{Seq: 2, Final: true},
		}
		call := feedChunks(t, rt, "echo", "s-inorder", chunks)
		if call == nil {
			t.Fatal("expected call after final chunk")
		}
		call = waitTerminal(t, rt, call.ID, 5*time.Second)
		if call.Status != StatusSucceeded {
			t.Fatalf("expected succeeded, got %s: %v", call.Status, call.RejectReason)
		}
	})

	t.Run("out of order buffered then completed", func(t *testing.T) {
		chunks := []Chunk{
			{Seq: 1, Data: []byte(raw[8:])},
			{Seq: 0, Data: []byte(raw[:8])},
			{Seq: 2, Final: true},
		}
		call := feedChunks(t, rt, "echo", "s-ooo", chunks)
		if call == nil {
			t.Fatal("expected call after final chunk")
		}
		call = waitTerminal(t, rt, call.ID, 5*time.Second)
		if call.Status != StatusSucceeded {
			t.Fatalf("expected succeeded, got %s: %v", call.Status, call.RejectReason)
		}
	})

	t.Run("duplicate rejected", func(t *testing.T) {
		chunks := []Chunk{
			{Seq: 0, Data: []byte(raw[:4])},
			{Seq: 0, Data: []byte(raw[:4])},
		}
		call := feedChunks(t, rt, "echo", "s-dup", chunks)
		if call == nil || call.Status != StatusRejected {
			t.Fatalf("expected rejected, got %v", call)
		}
		if call.RejectReason.Code != RejectStreamDuplicate {
			t.Fatalf("expected stream_duplicate, got %s", call.RejectReason.Code)
		}
	})

	t.Run("missing rejected", func(t *testing.T) {
		chunks := []Chunk{
			{Seq: 0, Data: []byte(raw[:4])},
			{Seq: 2, Data: []byte(raw[8:]), Final: true},
		}
		call := feedChunks(t, rt, "echo", "s-missing", chunks)
		if call == nil || call.Status != StatusRejected {
			t.Fatalf("expected rejected, got %v", call)
		}
		if call.RejectReason.Code != RejectStreamMissing {
			t.Fatalf("expected stream_missing, got %s", call.RejectReason.Code)
		}
	})

	t.Run("valid prefix without final is not executed", func(t *testing.T) {
		// chunk0 本身是一段“合法 JSON 前缀”但未声明 final，不得执行
		chunks := []Chunk{
			{Seq: 0, Data: []byte(`{"a":5}`)},
			{Seq: 1, Data: nil, Final: true},
		}
		call := feedChunks(t, rt, "echo", "s-prefix", chunks)
		if call == nil {
			t.Fatal("expected call after final")
		}
		call = waitTerminal(t, rt, call.ID, 5*time.Second)
		if call.Status != StatusSucceeded {
			t.Fatalf("expected succeeded, got %s: %v", call.Status, call.RejectReason)
		}
	})
}

func TestStreamTimeout(t *testing.T) {
	rt := NewRuntime(Config{StreamAssemblyTimeout: 300 * time.Millisecond})
	defer rt.Close()
	registerTool(t, rt, "echo", "v1")
	chunks := []Chunk{
		{Seq: 0, Data: []byte(`{"a":`)},
	}
	feedChunks(t, rt, "echo", "s-timeout", chunks)
	time.Sleep(400 * time.Millisecond)
	call, err := rt.SubmitChunk(context.Background(), "echo", "s-timeout", Chunk{Seq: 1, Final: true})
	if err != nil {
		t.Fatal(err)
	}
	if call == nil || call.Status != StatusRejected || call.RejectReason.Code != RejectStreamTimeout {
		t.Fatalf("expected stream_timeout reject, got %v", call)
	}
}

// ---------- 3. 版本更替 ----------

func TestVersionBinding(t *testing.T) {
	rt := NewRuntime(Config{})
	defer rt.Close()
	slow := &blockingExecutor{started: make(chan struct{}), release: make(chan struct{})}
	registerTool(t, rt, "echo", "v1", func(d *ToolDefinition) {
		d.Concurrency = 1
		d.Executor = slow
	})

	// v1 调用在 v1 定义下受理并入队，执行被阻塞
	callV1, _ := rt.Submit(context.Background(), "echo", json.RawMessage(`{"a":1}`))
	<-slow.started

	// 注册 v2（新增必填字段 b）；定义更新不影响已受理调用
	schemaV2 := echoSchema()
	schemaV2.RequiredFields = []string{"a", "b"}
	if err := rt.Registry().Register(&ToolDefinition{
		Name: "echo", Version: "v2", Params: schemaV2,
		Concurrency: 2, QueueSize: 2, Timeout: 2 * time.Second,
	}); err != nil {
		t.Fatalf("register v2: %v", err)
	}

	// 释放后 v1 调用才执行完，仍绑定 v1 语义
	close(slow.release)
	callV1 = waitTerminal(t, rt, callV1.ID, 5*time.Second)
	if callV1.Status != StatusSucceeded {
		t.Fatalf("v1 call should succeed: %v", callV1.RejectReason)
	}
	if callV1.ToolVersion != "v1" {
		t.Fatalf("admitted call must keep bound version v1, got %s", callV1.ToolVersion)
	}

	// v2 调用按 v2 校验：缺 b 被拒
	callV2Bad := submitAndWait(t, rt, "echo", `{"a":3}`)
	if callV2Bad.Status != StatusRejected || callV2Bad.RejectReason.Code != RejectSchemaInvalid {
		t.Fatalf("v2 should reject missing b, got %v", callV2Bad.RejectReason)
	}
	callV2 := submitAndWait(t, rt, "echo", `{"a":3,"b":"x"}`)
	if callV2.Status != StatusSucceeded || callV2.ToolVersion != "v2" {
		t.Fatalf("v2 call should succeed with bound v2, got %s/%s", callV2.Status, callV2.ToolVersion)
	}

	// 校验依据可追溯
	bound, ok := rt.BoundDefinition(callV1.ID)
	if !ok || bound.Version != "v1" {
		t.Fatalf("bound definition traceability failed: %v", bound)
	}
}

// ---------- 4. 并发与隔离 ----------

func TestConcurrencyBound(t *testing.T) {
	rt := NewRuntime(Config{})
	defer rt.Close()
	slow := &blockingExecutor{started: make(chan struct{}), release: make(chan struct{})}
	registerTool(t, rt, "slow", "v1", func(d *ToolDefinition) {
		d.Concurrency = 1
		d.QueueSize = 4
		d.Executor = slow
	})

	c1, _ := rt.Submit(context.Background(), "slow", json.RawMessage(`{"a":1}`))
	<-slow.started // c1 正在执行

	// c2 已受理但拿不到执行位，状态必须停在 pending（排队中）
	c2, _ := rt.Submit(context.Background(), "slow", json.RawMessage(`{"a":2}`))
	time.Sleep(50 * time.Millisecond)
	c2Snap := rt.GetCall(c2.ID)
	if c2Snap.Status != StatusPending {
		t.Fatalf("queued call should be pending, got %s", c2Snap.Status)
	}

	// 释放后 c2 才能执行
	close(slow.release)
	c1 = waitTerminal(t, rt, c1.ID, 2*time.Second)
	c2 = waitTerminal(t, rt, c2.ID, 2*time.Second)
	if c1.Status != StatusSucceeded || c2.Status != StatusSucceeded {
		t.Fatalf("expected both succeeded, got %s/%s", c1.Status, c2.Status)
	}
}

type blockingExecutor struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (e *blockingExecutor) Execute(ctx context.Context, call *Call) (json.RawMessage, error) {
	e.once.Do(func() { close(e.started) })
	select {
	case <-e.release:
		return json.RawMessage(`{"ok":true}`), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestConcurrencyAndQueue(t *testing.T) {
	rt := NewRuntime(Config{})
	defer rt.Close()
	slow := &blockingExecutor{started: make(chan struct{}), release: make(chan struct{})}
	registerTool(t, rt, "slow", "v1", func(d *ToolDefinition) {
		d.Concurrency = 1
		d.QueueSize = 1
		d.Executor = slow
	})
	registerTool(t, rt, "fast", "v1", func(d *ToolDefinition) {
		d.Concurrency = 1
		d.QueueSize = 1
	})

	c1, err := rt.Submit(context.Background(), "slow", json.RawMessage(`{"a":1}`))
	if err != nil {
		t.Fatal(err)
	}
	<-slow.started // 确保 c1 已开始执行

	c2, err := rt.Submit(context.Background(), "slow", json.RawMessage(`{"a":2}`))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond) // c2 排队中

	c3, err := rt.Submit(context.Background(), "slow", json.RawMessage(`{"a":3}`))
	if err != nil {
		t.Fatal(err)
	}
	c3 = waitTerminal(t, rt, c3.ID, 2*time.Second)
	if c3.Status != StatusRejected || c3.RejectReason.Code != RejectQueueFull {
		t.Fatalf("expected queue_full reject, got %v", c3.RejectReason)
	}

	// 慢调用不阻塞其他工具
	fastDone := make(chan *Call, 1)
	go func() {
		call, _ := rt.Submit(context.Background(), "fast", json.RawMessage(`{"a":1}`))
		fastDone <- waitTerminal(t, rt, call.ID, 2*time.Second)
	}()
	select {
	case call := <-fastDone:
		if call.Status != StatusSucceeded {
			t.Fatalf("fast tool should not be blocked by slow tool: %v", call.RejectReason)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("fast tool was blocked by slow tool")
	}

	close(slow.release)
	c1 = waitTerminal(t, rt, c1.ID, 2*time.Second)
	c2 = waitTerminal(t, rt, c2.ID, 2*time.Second)
	if c1.Status != StatusSucceeded || c2.Status != StatusSucceeded {
		t.Fatalf("queued calls should succeed after release: %s/%s", c1.Status, c2.Status)
	}
}

func TestCancelWhileQueued(t *testing.T) {
	rt := NewRuntime(Config{})
	defer rt.Close()
	slow := &blockingExecutor{started: make(chan struct{}), release: make(chan struct{})}
	registerTool(t, rt, "slow", "v1", func(d *ToolDefinition) {
		d.Concurrency = 1
		d.QueueSize = 4
		d.Executor = slow
	})
	// 占住唯一执行位
	rt.Submit(context.Background(), "slow", json.RawMessage(`{"a":0}`))
	<-slow.started

	ctx, cancel := context.WithCancel(context.Background())
	call, _ := rt.Submit(ctx, "slow", json.RawMessage(`{"a":1}`))
	cancel()
	call = waitTerminal(t, rt, call.ID, 2*time.Second)
	if call.Status != StatusCancelled {
		t.Fatalf("expected cancelled, got %s: %v", call.Status, call.RejectReason)
	}
	close(slow.release)
}

func TestTimeout(t *testing.T) {
	rt := NewRuntime(Config{})
	defer rt.Close()
	slow := &blockingExecutor{started: make(chan struct{}), release: make(chan struct{})}
	registerTool(t, rt, "slow", "v1", func(d *ToolDefinition) {
		d.Concurrency = 1
		d.QueueSize = 1
		d.Timeout = 100 * time.Millisecond
		d.RetryPolicy = RetryPolicy{MaxAttempts: 1}
		d.Executor = slow
	})
	call, _ := rt.Submit(context.Background(), "slow", json.RawMessage(`{"a":1}`))
	<-slow.started
	call = waitTerminal(t, rt, call.ID, 2*time.Second)
	if call.Status != StatusFailed || call.RejectReason.Code != RejectTimeout {
		t.Fatalf("expected timeout failure, got %s: %v", call.Status, call.RejectReason)
	}
	if len(call.Result) != 0 {
		t.Fatal("timed-out call must not produce a result")
	}
	close(slow.release)
}

// ---------- 5. 重试与失败语义 ----------

func TestRetryBeforeSideEffect(t *testing.T) {
	rt := NewRuntime(Config{})
	defer rt.Close()
	stub := rt.DefaultExecutor()
	stub.Reset()
	registerTool(t, rt, "echo", "v1", func(d *ToolDefinition) {
		d.RetryPolicy = RetryPolicy{MaxAttempts: 3, InitialBackoff: 10 * time.Millisecond, BackoffFactor: 1, MaxBackoff: 10 * time.Millisecond}
	})
	stub.FailFunc = func(call *Call, attempt int) (bool, bool) {
		return attempt < 2, false // 第一次在副作用前失败
	}
	call := submitAndWait(t, rt, "echo", `{"a":1}`)
	if call.Status != StatusSucceeded {
		t.Fatalf("expected success after retry, got %s: %v", call.Status, call.RejectReason)
	}
	if len(call.Attempts) != 2 {
		t.Fatalf("expected 2 attempts, got %d", len(call.Attempts))
	}
	if stub.SideEffectCount(call.ID) != 1 {
		t.Fatalf("side effects must happen exactly once, got %d", stub.SideEffectCount(call.ID))
	}
}

func TestRetryAfterSideEffectIsIdempotent(t *testing.T) {
	rt := NewRuntime(Config{})
	defer rt.Close()
	stub := rt.DefaultExecutor()
	stub.Reset()
	registerTool(t, rt, "echo", "v1", func(d *ToolDefinition) {
		d.RetryPolicy = RetryPolicy{MaxAttempts: 3, InitialBackoff: 10 * time.Millisecond, BackoffFactor: 1, MaxBackoff: 10 * time.Millisecond}
	})
	stub.FailFunc = func(call *Call, attempt int) (bool, bool) {
		return attempt < 2, true // 第一次：副作用已落定但返回失败
	}
	call := submitAndWait(t, rt, "echo", `{"a":1}`)
	if call.Status != StatusSucceeded {
		t.Fatalf("expected success, got %s: %v", call.Status, call.RejectReason)
	}
	if len(call.Attempts) != 2 {
		t.Fatalf("expected 2 attempts, got %d", len(call.Attempts))
	}
	if stub.SideEffectCount(call.ID) != 1 {
		t.Fatalf("side effect must not be duplicated on retry, got %d", stub.SideEffectCount(call.ID))
	}
	if len(call.Result) == 0 {
		t.Fatal("retried call must expose the same result")
	}
}

func TestRetryExhausted(t *testing.T) {
	rt := NewRuntime(Config{})
	defer rt.Close()
	stub := rt.DefaultExecutor()
	stub.Reset()
	registerTool(t, rt, "echo", "v1", func(d *ToolDefinition) {
		d.RetryPolicy = RetryPolicy{MaxAttempts: 2, InitialBackoff: 10 * time.Millisecond, BackoffFactor: 1, MaxBackoff: 10 * time.Millisecond}
	})
	stub.FailFunc = func(call *Call, attempt int) (bool, bool) { return true, false }
	call := submitAndWait(t, rt, "echo", `{"a":1}`)
	if call.Status != StatusFailed || call.RejectReason.Code != RejectExecutionFailed {
		t.Fatalf("expected execution_failed, got %s: %v", call.Status, call.RejectReason)
	}
	if len(call.Attempts) != 2 {
		t.Fatalf("expected 2 attempts, got %d", len(call.Attempts))
	}
	if stub.SideEffectCount(call.ID) != 0 {
		t.Fatal("no side effect should remain when all attempts fail before side effect")
	}
}

// ---------- 6. 容量边界 ----------

func TestParamTooLarge(t *testing.T) {
	rt := NewRuntime(Config{})
	defer rt.Close()
	registerTool(t, rt, "echo", "v1", func(d *ToolDefinition) { d.MaxParamBytes = 8 })
	call := submitAndWait(t, rt, "echo", `{"a":100}`)
	if call.Status != StatusRejected || call.RejectReason.Code != RejectParamTooLarge {
		t.Fatalf("expected param_too_large, got %v", call.RejectReason)
	}
}

func TestResultTooLargeReject(t *testing.T) {
	rt := NewRuntime(Config{})
	defer rt.Close()
	registerTool(t, rt, "echo", "v1", func(d *ToolDefinition) {
		d.MaxResultBytes = 10
		d.ResultStrategy = ResultReject
	})
	call := submitAndWait(t, rt, "echo", `{"a":1}`)
	if call.Status != StatusRejected || call.RejectReason.Code != RejectResultTooLarge {
		t.Fatalf("expected result_too_large, got %v", call.RejectReason)
	}
}

func TestResultTooLargeTruncate(t *testing.T) {
	rt := NewRuntime(Config{})
	defer rt.Close()
	registerTool(t, rt, "echo", "v1", func(d *ToolDefinition) {
		d.MaxResultBytes = 10
		d.ResultStrategy = ResultTruncate
	})
	call := submitAndWait(t, rt, "echo", `{"a":1}`)
	if call.Status != StatusSucceeded {
		t.Fatalf("truncate strategy should still succeed, got %s: %v", call.Status, call.RejectReason)
	}
	if !call.ResultTruncated || len(call.Result) != 10 {
		t.Fatalf("expected truncated result of 10 bytes, got %d bytes, truncated=%v", len(call.Result), call.ResultTruncated)
	}
}

// ---------- 7. 可观测与审计 ----------

func TestRecordEviction(t *testing.T) {
	rt := NewRuntime(Config{MaxCallRecords: 5})
	defer rt.Close()
	registerTool(t, rt, "echo", "v1")

	ids := make([]string, 10)
	for i := 0; i < 10; i++ {
		call := submitAndWait(t, rt, "echo", `{"a":1}`)
		ids[i] = call.ID
	}
	// 旧记录应被淘汰，只保留最近的少量记录
	if rt.GetCall(ids[0]) != nil {
		t.Fatal("old call record should have been evicted")
	}
	if rt.GetCall(ids[9]) == nil {
		t.Fatal("newest call record should be retained")
	}
	if len(rt.Audit().All()) > 5 {
		t.Fatalf("audit entries should be capped, got %d", len(rt.Audit().All()))
	}
}

func TestObservabilityAndAudit(t *testing.T) {
	rt := NewRuntime(Config{})
	defer rt.Close()
	registerTool(t, rt, "echo", "v1")

	ok := submitAndWait(t, rt, "echo", `{"a":1}`)
	bad := submitAndWait(t, rt, "echo", `{"a":"bad"}`)

	if ok.Status != StatusSucceeded || bad.Status != StatusRejected {
		t.Fatal("unexpected terminal states")
	}

	entries := rt.Audit().Entries(ok.ID)
	events := make([]string, len(entries))
	for i, e := range entries {
		events[i] = e.Event
		if e.Tool != "echo" || e.Version != "v1" {
			t.Fatalf("audit entry missing tool/version binding: %+v", e)
		}
	}
	want := []string{"admitted", "queued", "running", "succeeded"}
	if fmt.Sprint(events) != fmt.Sprint(want) {
		t.Fatalf("audit events = %v, want %v", events, want)
	}

	badEntries := rt.Audit().Entries(bad.ID)
	if len(badEntries) == 0 || badEntries[len(badEntries)-1].Event != "rejected" {
		t.Fatalf("bad call audit should end with rejected, got %v", badEntries)
	}

	// 列表过滤
	succeeded := rt.ListCalls(CallFilter{ToolName: "echo", Status: StatusSucceeded})
	if len(succeeded) != 1 {
		t.Fatalf("expected 1 succeeded call, got %d", len(succeeded))
	}
}
