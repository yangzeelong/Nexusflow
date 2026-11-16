# NexusFlow 开发路线图与待办事项

本文档记录 NexusFlow 框架当前状态与下一步计划，供跨设备/跨 agent 协作时同步。

**同步基线**：`master` @ `9bf04e2`（PR #18 已合并），另有 PR #19 待合并。
**测试基线**：`134 tests from 23 test suites，PASSED 134`（约 4.1 s，MSVC 19.40）。
**构建**：`cmake --build build --config Release --parallel 16` → 退出码 0，无 warning。

> 改框架后必须跑：上面两条命令 + `git diff --stat` 与 `git diff --stat --ignore-cr-at-eol` 比对。
> 细则见 `AGENTS.md`，尤其 §5（计数器与快照的命名/并发规则）。

---

## 已完成（历史条目，保留供追溯）

### ~~优雅停机 (Graceful Shutdown / Draining)~~ ✅ PR #15

- 拓扑逆序停机、`drainTimeout`/`stopTimeout`、`RequestDrain`/`RequestStop` 分离。
- **关键坑（已记入代码注释）**：源模块可能正卡在 `push` 里等一个已经没人消费的满队列，
  所以 `Stop()` 必须**先 `ShutdownOutputs()` 再 join 源模块**，否则整个 stop 窗口都耗在等这把锁上
  （实测 `Stop()` 2003 ms → 31 ms）。

### ~~抽象监控与状态报告机制~~ ✅ PR #16 / #17

- `include/nexusflow/Stats.hpp`：`EdgeSnapshot` / `ModuleSnapshot` / `PipelineSnapshot`，
  `Pipeline::GetSnapshot()` / `ResetMetrics()`。
- `MetricsCollector`（原 `TimerRegistry`）：`Intern` 句柄 + `Record`，`MetricSamples`（含 p95/p99 + 窗口）。
- 计数器单写者化：普通 `uint64_t` + `PublishCounters()`/`ObserveCounters()` 栅栏配对，
  热路径 0.3 ns/次 vs 原子 3.4 ns/次。

### ~~导出日志模块 API~~ ✅ PR #12

- `include/nexusflow/Logging.hpp` 公开门面（Level/LoggerParam/LogRecord/LogSink/SetSink），
  不 include spdlog；后端在 `src/common/Logging.cpp`。

### 其他已完成

- 无锁 SPSC 边队列（PR #14）、每 pipeline 上下文（PR #13）、过期 tag/分支收敛。
- 去框架级默认边容量 `kDefaultMessageQueueCapacity`；`Build(int)` / `graph.messageQueueCapacity` 必填（PR #18）。
- 源模块背压改为 park 在满输出边上（`SpscQueue::waitForRoom`），不再定时 sleep（PR #18）。
- `Pipeline::Start()` 幂等；删除全部 `@deprecated`（PR #18）。

---

## P0 — 当前遗留（先做，都是小改动）

### 1. `build/perf-probe` 的 `perf_probe` 目标编译失败

```
error LNK2019: 无法解析的外部符号 "nexusflow::profiling::TimerRegistry::Intern(...)"
```

该探针源码（`build/perf-probe/`，不入库）仍在用已删除的 `TimerRegistry`。
**只构建 `pipeline_benchmark` 目标时不受影响**：

```powershell
cmake --build build\perf-probe\build --config Release --target pipeline_benchmark --parallel 16
```

- [ ] 决定：删掉 `perf_probe` 目标，或把它改到 `MetricsCollector`

### 2. 未合并的远程分支需要处置

`aifootball`、`codex/architecture-roadmap-milestones`、`dev`、`fix/core-hardening`、`refactor/docs-and-format`

- [ ] 确认各自是否还有用；无用的删除，有用的合并或重建

### 3. `docs/telemetry.md` 补 p99/窗口说明

代码和 `README.md` 已有 p95/p99 + `windowItems`/`windowSpanMs`，`docs/telemetry.md` §3 还没跟上。

- [ ] 更新 §3（**该目录约定不入库**：`git ls-files docs` 必须为 0）

---

## P1 — 下个大版本的核心优化

### 4. 融合批处理 (Fusion batching)

位置：`src/core/Worker.cpp` `Worker::RunFusion()`，里面有现成 TODO：
`// TODO: 如何优化呢，现在只有单Batch, 并且需要测试下内存占用.`

现状问题：

- `messageCache` 是 `std::unordered_map<uint64_t, std::unordered_map<std::string, Message>>`，
  每帧至少两次哈希；`messageCache[messageId][sourceModuleName] = message` 会无条件构造表项。
- 一组凑齐时**单独**调用一次 `ProcessBatch`（每次只 `push_back` 1 个消息），
  所以融合阶段实际 batch 恒为 1。

要做：

- [ ] 一次迭代把**所有**凑齐的组攒进一个 batch 再调 `ProcessBatch`
- [ ] 用 `vector` + 小表（组员数通常个位数）替代双层哈希
- [ ] 测内存占用（TODO 里明确要求）

### 5. ~~线程池 / 拓扑并行~~（影响面最大，放最后）

位置：`src/module/ModuleActor.cpp:63` `m_workThread = std::thread([this]() { m_worker->WorkLoop(); });`

- 每模块一个 `std::thread`；100 个模块 = 100 线程。
- 方向：worker 池 + 模块绑定，或按拓扑层并行。
- **约束**：必须保住 `AGENTS.md` §5 的"每计数器单写者"前提，否则计数器要退回原子或加锁。

- [ ] 先做可行性调研（线程数 vs 吞吐曲线），再决定是否动

### 6. `ProcessTimed` / `DispatchOutput` 热路径开销

位置：`src/module/Module.cpp:65-118`

- 每次处理 `m_pendingOutputs.clear()` → `push_back` → `std::move` 出来 → 再 `clear()`。
- `DispatchOutput` 每个输出都 `Message routed = output.message;`（拷贝），
  再逐条拷贝 `m_activeInputTimingMs`，最后 `timings[m_moduleName] = elapsedMs`。
- `LOG_DEBUG("ModuleTiming: module='{}' elapsed_ms={:.3f}", ...)` **无条件**构造。
  （已知：2 ms sleep 的模块实测 ~15 ms/次，因为这条 DEBUG 日志本身比 sleep 还贵。）

- [ ] 日志走 early-return 检查，或只在开启 profile 时才构造
- [ ] timings 按需收集（未开启时跳过整条 `m_activeInputTimingMs` 链路）

---

## P2 — 质量与工程化

### 7. CI

仓库目前没有 CI（PR 显示无 checks）。

- [ ] 加 Linux + Windows 的 build + `nexusflow_tests`

### 8. 用例数自动校验

`AGENTS.md` §3 写着"用例数随手改：`--gtest_list_tests` 数一遍，别照抄本节"——这已经是已知痛点。

- [ ] 加脚本在 CI 里校验 `AGENTS.md` 里的 134 与实际数量一致

### 9. 测试稳定性

`PipelineMetricsTest.StopAccountsForMessagesTheDispatcherCouldNotPush` 历史上是 flaky 的
（依赖生产者在 `push` 里被 block）。已用 50 µs 的 sink 修过，但这类"时序靠 sleep 保证"的测试值得统一处理。

- [ ] 改成显式同步点（信号量/条件变量），而不是靠 sleep 窗口

---

## 关键经验（改代码前先看）

| 坑 | 表现 | 处理 |
| --- | --- | --- |
| 陈旧 MSBuild 节点 | 构建显示 exit 0，但改动没进二进制 | `Get-Process MSBuild \| Stop-Process -Force` |
| 陈旧 `.obj` | 改了测试，exe 还是旧断言 | `Remove-Item build\tests\nexusflow_tests.dir\Release\<File>.obj -Force` 后重建 |
| `--parallel` 参数写错 | 构建 exit 1 且**什么都没编** | 永远 `--parallel 16` |
| PowerShell 改写文件 | UTF-8 被转 GBK，出现 U+FFFD / U+00BE | 用 `[System.IO.File]::ReadAllBytes` + `UTF8Encoding($false,$true)`；改完扫 `[char]0x00BE` |
| `read` 越界 offset | 反复报同一错误 | 按报错里的总行数重新给 offset |
| 弹窗 | 测试运行弹控制台 | `Start-Process -PassThru -WindowStyle Hidden -RedirectStandardOutput/Error` |

不要提交 `docs/`（用户自己的设计文档目录，`git ls-files docs` 必须为 0）。
