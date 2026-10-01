package runtime

import (
	"strings"
	"testing"
)

func TestStreamInOrder(t *testing.T) {
	a := NewStreamAssembler()
	if _, done, _ := a.Feed(Chunk{Seq: 0, Data: []byte(`{"na`)}); done {
		t.Fatal("should not complete at chunk 0")
	}
	if _, done, _ := a.Feed(Chunk{Seq: 1, Data: []byte(`me":"x"}`)}); done {
		t.Fatal("should not complete without Last flag")
	}
	data, done, rej := a.Feed(Chunk{Seq: 2, Data: nil, Last: true})
	if rej != nil || !done {
		t.Fatalf("expect complete, done=%v rej=%v", done, rej)
	}
	if string(data) != `{"name":"x"}` {
		t.Fatalf("bad assembly: %q", data)
	}
}

func TestStreamOutOfOrder(t *testing.T) {
	a := NewStreamAssembler()
	// 乱序：1 先到，0 后到
	if rej := func() *RejectError { _, _, r := a.Feed(Chunk{Seq: 1, Data: []byte(`me":"x"}`)}); return r }(); rej != nil {
		t.Fatalf("out-of-order chunk should be buffered, got %v", rej)
	}
	data, done, rej := a.Feed(Chunk{Seq: 0, Data: []byte(`{"na`)})
	_ = data
	if rej != nil || done {
		t.Fatalf("not done yet: done=%v rej=%v", done, rej)
	}
	_, done, rej = a.Feed(Chunk{Seq: 2, Last: true})
	if !done || rej != nil {
		t.Fatalf("expect done, rej=%v", rej)
	}
	if string(a.buf.Bytes()) != `{"name":"x"}` {
		t.Fatalf("bad assembly: %q", a.buf.String())
	}
}

func TestStreamDuplicate(t *testing.T) {
	a := NewStreamAssembler()
	a.Feed(Chunk{Seq: 0, Data: []byte("x")})
	if _, _, rej := a.Feed(Chunk{Seq: 0, Data: []byte("x")}); rej == nil || rej.Kind != KindStreamDuplicate {
		t.Fatalf("expect duplicate rejection, got %v", rej)
	}
}

func TestStreamMissing(t *testing.T) {
	a := NewStreamAssembler()
	a.Feed(Chunk{Seq: 0, Data: []byte("a")})
	a.Feed(Chunk{Seq: 2, Data: []byte("c"), Last: true}) // 缺 seq 1
	rej := a.Finish()
	if rej == nil || rej.Kind != KindStreamMissing {
		t.Fatalf("expect missing rejection, got %v", rej)
	}
	if !strings.Contains(rej.Msg, "[1]") {
		t.Fatalf("missing seq should be reported: %v", rej)
	}
}

func TestStreamPrefixNotExecuted(t *testing.T) {
	// 前缀 `{"name":"ok"}` 恰好是合法 JSON，但流未结束不得执行
	rt := New(DefaultConfig())
	executed := 0
	rt.Registry().Register(ToolDef{
		Name: "echo", Version: 1,
		Params: &ParamSchema{Type: TypeObject, Properties: map[string]*ParamSchema{
			"name": {Type: TypeString, Required: true},
		}},
	}, func(ctx *CallContext, args map[string]any) (any, error) {
		executed++
		return args, nil
	})

	s, err := rt.BeginStream("c1", "echo")
	if err != nil {
		t.Fatal(err)
	}
	// 第一片恰好是完整合法 JSON
	if rej := s.Feed(Chunk{Seq: 0, Data: []byte(`{"name":"ok"}`)}); rej != nil {
		t.Fatal(rej)
	}
	if executed != 0 {
		t.Fatal("prefix must not be executed")
	}
	// 继续分片使整体非法
	if rej := s.Feed(Chunk{Seq: 1, Data: []byte(`garbage`), Last: true}); rej != nil {
		t.Fatal(rej)
	}
	if _, rej := s.Finish(); rej == nil {
		t.Fatal("corrupt stream must be rejected at finish")
	}
	if executed != 0 {
		t.Fatal("invalid stream must not execute")
	}
	rec, _ := rt.Status("c1")
	if rec.Status != StatusRejected {
		t.Fatalf("expect rejected, got %s", rec.Status)
	}
}

func TestStreamEndToEnd(t *testing.T) {
	rt := New(DefaultConfig())
	rt.Registry().Register(ToolDef{
		Name: "echo", Version: 1,
		Params: &ParamSchema{Type: TypeObject, Properties: map[string]*ParamSchema{
			"name": {Type: TypeString, Required: true},
		}},
	}, func(ctx *CallContext, args map[string]any) (any, error) {
		return map[string]any{"got": args["name"]}, nil
	})
	s, _ := rt.BeginStream("c2", "echo")
	s.Feed(Chunk{Seq: 0, Data: []byte(`{"name":`)})
	s.Feed(Chunk{Seq: 1, Data: []byte(`"stream"}`), Last: true})
	res, rej := s.Finish()
	if rej != nil {
		t.Fatal(rej)
	}
	if res.(map[string]any)["got"] != "stream" {
		t.Fatalf("bad result: %v", res)
	}
	rec, _ := rt.Status("c2")
	if rec.Status != StatusCompleted || rec.Version != 1 {
		t.Fatalf("bad record: %+v", rec)
	}
}
