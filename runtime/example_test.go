package runtime_test

import (
	"fmt"

	runtime "github.com/shikejing/tool-call-runtime-go/runtime"
)

// 端到端示例：登记 -> 流式调用 -> 审计查询，全部在本进程内可复现。
func Example() {
	rt := runtime.New(runtime.DefaultConfig())

	rt.Registry().Register(runtime.ToolDef{
		Name:    "add",
		Version: 1,
		Params: &runtime.ParamSchema{
			Type: runtime.TypeObject,
			Properties: map[string]*runtime.ParamSchema{
				"a": {Type: runtime.TypeInteger, Required: true},
				"b": {Type: runtime.TypeInteger, Required: true},
			},
		},
	}, func(ctx *runtime.CallContext, args map[string]any) (any, error) {
		a := args["a"].(float64)
		b := args["b"].(float64)
		return map[string]any{"sum": a + b}, nil
	})

	// 流式分片调用
	s, _ := rt.BeginStream("call-1", "add")
	s.Feed(runtime.Chunk{Seq: 0, Data: []byte(`{"a": 1,`)})
	s.Feed(runtime.Chunk{Seq: 1, Data: []byte(` "b": 2}`), Last: true})
	res, rej := s.Finish()
	fmt.Println("result:", res, "reject:", rej)

	rec, _ := rt.Status("call-1")
	fmt.Println("status:", rec.Status, "version:", rec.Version, "attempts:", rec.Attempts)

	// 非法参数被拒绝且可定位
	_, rej = rt.Call("call-2", "add", map[string]any{"a": 1})
	fmt.Println("reject:", rej)
	rec2, _ := rt.Status("call-2")
	fmt.Println("status:", rec2.Status)

	// Output:
	// result: map[sum:3] reject: <nil>
	// status: completed version: 1 attempts: 1
	// reject: missing_required at $.b: required field missing
	// status: rejected
}
