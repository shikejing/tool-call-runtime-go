# tool-call-runtime-go

模型工具调用的本地运行时：登记工具定义、校验调用参数、驱动本地桩执行。
整条链路只在本进程内运行，不依赖任何外部服务，调用与执行结果可在本地复现（`go test ./...`）。

## 快速开始

```go
rt := runtime.New(runtime.DefaultConfig())
rt.Registry().Register(runtime.ToolDef{
    Name: "add", Version: 1,
    Params: &runtime.ParamSchema{
        Type: runtime.TypeObject,
        Properties: map[string]*runtime.ParamSchema{
            "a": {Type: runtime.TypeInteger, Required: true},
            "b": {Type: runtime.TypeInteger, Required: true},
        },
    },
}, func(ctx *runtime.CallContext, args map[string]any) (any, error) {
    return map[string]any{"sum": args["a"].(float64) + args["b"].(float64)}, nil
})

res, rej := rt.Call("call-1", "add", map[string]any{"a": 1, "b": 2})
```

## 校验语义

`Validate` 按工具定义逐层核对参数，校验顺序：**类型 → 必填 → 未声明字段 → 取值范围/枚举 → 递归嵌套**。
任何非法参数都在执行前被拒绝，不会进入执行阶段。拒绝原因 `RejectError` 携带：

- `Kind`：可区分的类别（见下表）；
- `Path`：定位到具体参数的路径，如 `$.addr.city`、`$.tags[1]`。

| Kind | 含义 |
|---|---|
| `type_mismatch` | 类型不符（含 integer 出现小数） |
| `missing_required` | 缺失必填字段 |
| `unknown_field` | 出现未声明字段 |
| `out_of_range` | 数值越界（min/max）或字符串长度越界（minLen/maxLen） |
| `enum_violation` | 取值不在枚举内 |
| `not_object` | 顶层参数不是 JSON object |
| `args_too_large` / `result_too_large` | 参数/结果超过容量上限 |
| `stream_corrupt` / `stream_duplicate` / `stream_missing` | 流式分片冲突/重复/缺失 |
| `concurrency_full` / `queue_full` | 并发上限已满（无队列）/ 等待队列已满 |
| `unknown_tool` | 工具未登记 |
| `timeout` / `canceled` / `exec_failed` | 执行超时 / 被取消 / 重试耗尽后失败 |

## 流式拼接

参数可分片到达（`BeginStream` → `Feed` → `Finish`）：

- 分片按 `Seq`（从 0 连续编号）拼接，末片须置 `Last: true`；
- **乱序**：先缓存，到齐后按序拼接；
- **重复**：立即以 `stream_duplicate` 拒绝；
- **缺失**：`Finish` 时以 `stream_missing` 拒绝并列出缺口序号；
- 只有完整序列才解析与执行——**中途某个恰好合法的前缀不会被执行**。

## 版本绑定规则

- 同名工具的 `Version` 必须严格递增（≥1），历史版本全部保留；
- 一次调用在**受理时**（`Call` 入口 / 流式的 `Finish`）绑定当时生效的定义版本；
- 绑定后定义更新不影响该调用：校验与执行都使用绑定版本的定义与桩；
- 每次调用的绑定版本记录在审计中（`CallRecord.Version`），可随时查询追溯。

## 并发、取消与超时

- 每个工具有独立的并发信号量与有界等待队列，**工具间互不影响**；
- 执行位与队列都满时按 `concurrency_full`（无队列）或 `queue_full` 拒绝；
- `Cancel(callID)` 可取消执行中或排队中的调用；超时由 `CallTimeout` 控制；
- 取消/超时后调用进入终态（`canceled`/`failed`），不会停在中间状态。

## 重试语义

- 工具桩返回错误时按 `MaxRetries` 重试；**超时不重试**，直接失败；
- 重试使用同一 `CallID`，桩通过 `CallContext.Attempt` 感知次数——**桩应按 CallID 幂等**，保证重试不重复产生副作用；
- 结果只写一次（`AuditLog.SetResult` 幂等），同一调用重试前后对外可见结果一致；
- 重试耗尽返回 `exec_failed`，消息含调用 ID，可定位到具体调用。

## 容量边界

- `MaxArgBytes`：单次参数（JSON 序列化后）上限，超限以 `args_too_large` 拒绝；
- `MaxResultBytes`：返回结果上限，超限以 `result_too_large` 置为失败；
- 所有队列、缓存均有界，长时间高负载下无无界内存增长。

## 可观测与审计

```go
rec, ok := rt.Status("call-1")
```

`CallRecord` 包含：状态（`pending`/`rejected`/`running`/`completed`/`failed`/`canceled`）、
绑定版本、拒绝原因、重试次数、结果与逐步历史。结果不确定时可据此区分
**未受理**（查无记录）、**已拒绝**、**执行中**、**已完成**。

## 配置项

| 字段 | 默认 | 说明 |
|---|---|---|
| `Executor.MaxConcurrent` | 4 | 每工具并发上限 |
| `Executor.QueueSize` | 16 | 每工具等待队列容量 |
| `Executor.CallTimeout` | 5s | 单次执行超时 |
| `Executor.MaxRetries` | 1 | 失败重试次数 |
| `MaxArgBytes` | 1MiB | 参数字节上限 |
| `MaxResultBytes` | 1MiB | 结果字节上限 |

## 测试

```sh
go test ./...          # 全部测试
go test ./... -race    # 竞态检测
```

覆盖：非法参数（类型/必填/未声明/范围/枚举/嵌套路径）、流式分片（乱序/重复/缺失/前缀不执行）、
版本更替（绑定与追溯）、并发上限与队列、取消与超时、重试幂等、容量超限。
