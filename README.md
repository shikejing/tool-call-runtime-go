# tool-call-runtime-go

一套进程内运行的模型工具调用（tool call）运行时：登记工具定义，对每次调用给出的参数做逐层校验，再驱动本地桩执行器。整条链路不依赖任何外部服务，调用与执行的可复现性可在本地通过测试验证。

## 快速开始

```go
rt := toolruntime.NewRuntime(toolruntime.Config{})
defer rt.Close()

err := rt.Registry().Register(&toolruntime.ToolDefinition{
    Name:    "echo",
    Version: "v1",
    Params: &toolruntime.Schema{
        Type: toolruntime.TypeObject,
        RequiredFields: []string{"a"},
        Properties: map[string]*toolruntime.Schema{
            "a": {Type: toolruntime.TypeInteger, Min: ptr(0.0), Max: ptr(100.0)},
        },
    },
})

call, _ := rt.Submit(context.Background(), "echo", json.RawMessage(`{"a":42}`))
// 轮询或等待 call.Status == toolruntime.StatusSucceeded
```

流式提交：

```go
rt.SubmitChunk(ctx, "echo", "stream-id", toolruntime.Chunk{Seq: 0, Data: []byte(`{"a":`)})
rt.SubmitChunk(ctx, "echo", "stream-id", toolruntime.Chunk{Seq: 1, Data: []byte(`42}`), Final: true})
```

## 1. 参数校验语义

参数为 JSON，按工具定义的 schema 逐层核对，**任何非法参数都不会进入执行阶段**。

- **类型**：`string` / `number` / `integer`（无小数部分）/ `boolean` / `object` / `array`，类型不符返回 `type_mismatch`。
- **必填字段**：object 的 `required` 中任一字段缺失返回 `missing_required`。
- **取值范围**：数值 `minimum`/`maximum` 越界返回 `out_of_range`；字符串 `minLength`/`maxLength`（按 rune 计）越界返回 `too_short`/`too_long`；数组 `minItems`/`maxItems` 越界返回 `too_few_items`/`too_many_items`；另有 `pattern` 正则与 `enum` 枚举校验。
- **嵌套结构**：object 的 properties 与 array 的 items 递归校验，错误路径逐层定位，形如 `$.nested.x[0]`。
- **未声明字段**：object 中出现 schema 未声明的字段返回 `unknown_field`（`additionalProperties: true` 可放行）。
- **非法 JSON**：返回 `invalid_json`。

所有校验错误都带可定位到具体参数的 `Path`（如 `$.a`、`$.items[2].id`），拒绝原因码统一为 `schema_invalid`，`RejectReason.Path` 指向首个错误路径，`Message` 包含全部错误。

## 2. 流式分片规则

参数以分片（chunk）到达，每个分片带 `Seq` 序号与 `Final` 结束标记。

- **只有完整且合法的参数才能进入执行**：分片全部到齐（`0..finalSeq` 连续）并显式声明 `Final` 后才拼装；中途任何“恰好是合法 JSON 的前缀”都不会被当作最终参数（即使该前缀本身是一段合法 JSON）。
- **乱序**：分片允许乱序到达，缺失序号的分片会被缓存等待，并记录 `stream_gap` 审计事件；到齐后正常拼装执行。
- **重复**：同一 `Seq` 重复到达立即拒绝（`stream_duplicate`）。
- **缺失**：声明 `Final` 后仍有分片缺失，拒绝（`stream_missing`），原因中列出缺失序号。
- **超时**：拼装超过 `StreamAssemblyTimeout` 未完成，由清扫任务或后续分片触发拒绝（`stream_timeout`）；超时后迟到的分片同样拒绝。
- **容量**：拼装期间累计字节超过 `MaxParamBytes` 拒绝（`param_too_large`）。

## 3. 版本绑定规则

- 工具定义按 `(name, version)` 登记，同名工具可同时存在多个版本；`Register` 对定义深拷贝，登记后调用方再修改不影响已登记版本。
- 一次调用在**受理时**绑定当时生效的定义版本（一次性提交取当前版本；流式提交在首个分片到达时绑定），调用记录中记录 `ToolVersion`。
- 定义更新（登记新版本）后，**已受理的调用仍按绑定版本校验与执行**，中途不改变语义。
- 每次调用的校验依据可通过 `rt.BoundDefinition(callID)` 追溯查询；审计事件中也记录 tool 与 version。

## 4. 并发与资源隔离

- 每个工具版本独立配置并发上限 `Concurrency` 与队列容量 `QueueSize`：等待位容量 = `Concurrency + QueueSize`。
- 慢调用只占用所属工具版本的执行位，**不阻塞其他工具**的调用。
- 超出等待位容量时立即返回可区分的拒绝（`queue_full`，消息中含并发与队列配置），不排队等待。
- **取消与超时**：调用方 ctx 取消、或执行超过单次尝试超时 `Timeout` 时，调用不会停在中间状态：
  - 排队中取消 → `cancelled`；
  - 执行中取消/超时 → 桩执行器在副作用开始前响应 ctx，不产生半执行结果，调用以 `cancelled`/`timeout` 终态结束，无结果输出。

## 5. 执行失败与重试

- 执行失败时按 `RetryPolicy` 重试（`MaxAttempts`、`InitialBackoff`、`BackoffFactor`、`MaxBackoff`），退避期间响应取消。
- **重试不重复产生副作用**：本地桩执行器按 callID 幂等记录副作用；若失败发生在副作用落定之后（模拟“崩溃”），重试直接重放同一结果，副作用次数始终为 1，同一调用重试前后对外可见的结果一致。
- 重试耗尽后调用终态为 `failed`，拒绝原因码为 `execution_failed`（最后一次错误为超时时为 `timeout`，为取消时为 `cancelled`）。
- 每次尝试的开始/结束时间、错误、是否产生副作用都记录在 `Attempts` 中，可定位到具体调用与参数。

## 6. 容量边界

- **参数规模**：一次性提交与流式拼装都受 `MaxParamBytes` 限制，超限拒绝（`param_too_large`）。
- **返回结果**：执行结果超过 `MaxResultBytes` 时按 `ResultStrategy` 处理：
  - `reject`：拒绝（`result_too_large`），结果不输出；
  - `truncate`：截断到上限并标记 `ResultTruncated = true`，调用仍成功。
- **内存有界**：调用记录与审计条目受 `MaxCallRecords` 上限约束，超限时淘汰最旧的已终结调用，长时间高负载下不出现无界内存增长。

## 7. 可观测与审计

- 调用状态机：`pending`（已受理，排队中）→ `running`（执行中）→ `succeeded` / `failed` / `rejected` / `cancelled`。
  - **未受理**：从未通过受理校验的调用（如未知工具），`AdmittedAt` 为 nil，审计中无 `admitted` 事件；
  - **已拒绝**：终态 `rejected`，`RejectReason` 含可区分原因码与参数路径；
  - **执行中**：`running`；
  - **已完成**：`succeeded` / `failed` / `cancelled`。
- 查询接口：
  - `rt.GetCall(callID)`：返回调用记录副本（状态、拒绝原因、绑定版本、参数、结果、尝试历史）；
  - `rt.ListCalls(CallFilter{ToolName, Status})`：按工具/状态过滤；
  - `rt.BoundDefinition(callID)`：查询受理时绑定的定义（校验依据）；
  - `rt.Audit().Entries(callID)`：查询该调用的状态变迁历史（`admitted` / `queued` / `running` / `attempt_failed` / `succeeded` / `rejected` / `cancelled` / `stream_gap` 等）。

## 8. 配置项一览

| 配置 | 默认值 | 说明 |
| --- | --- | --- |
| `DefaultConcurrency` | 4 | 单工具默认并发上限 |
| `DefaultQueueSize` | 16 | 单工具默认队列容量 |
| `DefaultTimeout` | 30s | 单次执行尝试超时 |
| `DefaultMaxParamBytes` | 1 MiB | 参数大小上限 |
| `DefaultMaxResultBytes` | 1 MiB | 返回结果大小上限 |
| `DefaultRetryPolicy.MaxAttempts` | 3 | 最大尝试次数（含首次） |
| `DefaultRetryPolicy.InitialBackoff` | 100ms | 初始退避 |
| `DefaultRetryPolicy.BackoffFactor` | 2 | 退避指数因子 |
| `DefaultRetryPolicy.MaxBackoff` | 2s | 退避上限 |
| `StreamAssemblyTimeout` | 30s | 流式拼装超时 |
| `MaxCallRecords` | 10000 | 调用记录与审计保留上限 |

工具定义可逐项覆盖全局默认：`Concurrency`、`QueueSize`、`Timeout`、`MaxParamBytes`、`MaxResultBytes`、`RetryPolicy`、`ResultStrategy`，并可通过 `Executor` 字段接入自定义本地执行器（默认为确定性结果、幂等副作用的桩执行器，可注入失败）。

## 测试

```bash
go test ./... -race -count=1
```

覆盖场景：非法参数（类型/必填/未声明字段/范围/嵌套路径）、流式分片（乱序/重复/缺失/合法前缀不执行/超时）、版本更替、并发上限与队列拒绝、慢调用隔离、取消与超时、重试（副作用前/后失败、耗尽）、容量超限（拒绝/截断）、记录淘汰与审计。
