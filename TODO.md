# NexusFlow 开发路线图与待办事项

本文档记录 NexusFlow 框架的**长期路线图**：已完成的历史、待办事项，以及踩过的坑。

- 下一版怎么落地（工作项、要先拍板的决策、验收口径）→ [`NEXT-VERSION.md`](NEXT-VERSION.md)
- 基准数字、测量协议与测量机器 → [`benchmarks/README.md`](benchmarks/README.md)
- 构建/测试命令、用例基线、硬规则 → [`AGENTS.md`](AGENTS.md)

**构建**：`cmake --build build --config Release --parallel 16` → 退出码 0。注意会刷出
大量 `C4819`（源文件含 UTF-8 中文注释，MSVC 按 936 代码页读取）。想要干净构建，
给目标加 `/utf-8`；在加上之前不要在本文件里声称"无 warning"。

> 改框架后必须跑：构建 + `nexusflow_tests` + `git diff --stat` 与 `git diff --stat --ignore-cr-at-eol` 比对。
> 用例数以 `AGENTS.md` §3 为准（这里不再复述，免得两处数字打架）。
> 细则见 `AGENTS.md`，尤其 §5（计数器与快照的命名/并发规则）。

---

## 已完成（历史条目，保留供追溯）

### ~~优雅停机 (Graceful Shutdown / Draining)~~ ✅ PR #15

- 拓扑逆序停机、`RequestDrain`/`RequestStop` 分离（当时的 `drainTimeout`/`stopTimeout`
  两个预算后来在「停止语义」一节里整个删掉了：它们从来没能让 `Stop()` 更早返回）。
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
- 停机语义 `Stop()`（排空）/ `Cancel()`（丢弃）＋每阶段 `drainInputsMs`/`exitLoopMs`（PR #23）。
- 目录归位：全部用例收进 `tests/test/`、全部基准源码收进 `benchmarks/`（PR #24）。

---

## P0 — 当前遗留（先做，都是小改动）

### 1. `build/perf-probe` 的 `perf_probe` 目标编译失败

```
error LNK2019: 无法解析的外部符号 "nexusflow::profiling::TimerRegistry::Intern(...)"
```

该探针源码（`build/perf-probe/`，不入库）仍在用已删除的 `TimerRegistry`。
**仓库内已有 `pipeline_benchmark` 目标，不需要那个探针副本**：

```powershell
cmake --build build --config Release --target pipeline_benchmark --parallel 16
.\build\benchmarks\Release\pipeline_benchmark.exe --capacity 8 --benchmark_min_time=3s
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

### 4. 融合批处理 (Join batching)

位置：`src/core/Worker.cpp` `Worker::RunJoin()`，里面有现成 TODO：
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

## 1.0.0 定版修复（已完成，随 v1.0.0 发布）

- **构建导出**：`cmake/3rdparty/yaml-cpp.cmake` 的 pin 由 0.7.0 → 0.8.0。0.7.0 用
  `cmake_dependent_option(YAML_CPP_INSTALL ... "CMAKE_SOURCE_DIR STREQUAL PROJECT_SOURCE_DIR" OFF)`，
  嵌套 FetchContent 下条件恒假，`set(... CACHE FORCE)` 被 dependent option 覆盖，于是没有
  export set，`cmake -S . -B build` 在 generate 阶段直接失败（干净 cache 可复现）。
  0.8.0 用普通 `option()` 且 `install(EXPORT ... NAMESPACE yaml-cpp::)`，正好对上
  `NexusflowConfig.cmake.in` 要找的 `yaml-cpp::yaml-cpp`。
- **`SpscQueue::restart()`**：先把 `[head, tail)` 的残留元素析构掉再归零索引（原先直接归零，
  残留 `Message` 泄漏，且下一轮 push 会 placement new 覆盖活对象），并把上一轮的索引折进
  `m_pushedBase`/`m_poppedBase`，让 `*Total` 不再回退。
- **`GetSnapshot()`**：改为先填 modules 再填 edges，与 `Stats.hpp`/§5 的契约一致
  （原实现顺序相反，注释却写着正确方向）。
- **计数器并发**：`Worker`/`Dispatcher` 的计数器改为 `std::atomic<uint64_t>`（relaxed
  add/load）；`RecordDropped`/`RecordFailed` 的多写者检测改成 compare-exchange
  （原先只有慢路径清 flag，慢路径因此永不可达，检测形同不存在）。
- **`Start()`**：在 `restart()` 之前检查是否还有上一轮被放弃的线程（`IsStarted()`），
  有则拒绝启动 —— 否则会在活线程脚下重置队列索引。
- **`RunJoin()`**：分组 key 由 `MessageMeta::sourceName` 改为**输入边名**。框架从不写
  `sourceName`，同一 messageId 的多路输入会挤在同一个 key 上、永远凑不齐，最后按 60 s
  超时静默丢弃。同时给完整分组的异常路径补上 failed 计数。
- **`PullBatchMessage()`**：所有输入边都 shutdown 时提前返回，不再空转整个 batchTimeout。
- **`waitForRoom()`**：park 前登记 `m_producerWaiting`，消费者的 `WakeProducer()` 才看得见它。
- **`CollectDownstream()`**：`placed.insert` 移到递归之前，环的防御保护才真正生效。
- **建图校验**：`PipelineBuilder` 与 `CreateGraphFromYaml` 都拒绝"无边模块"（原先静默
  不建 actor 却返回 SUCCESS）。
- **`~ModuleActor`**：上一轮已超预算的线程会先打 ERROR 再照常 join；**不 detach** ——
  detach 会掩盖问题，让调用方无从得知有数据没处理完。

新增回归用例：`SpscQueueTest.RestartDestroysLeftoversAndKeepsTotals`、
`SpscQueueTest.WaitForRoomWakesWhenConsumerFreesSpace`、
`PipelineErrorTest.BuilderRejectsAModuleWithNoEdge`、
`PipelineErrorTest.BuilderRejectsAModuleNothingConnectsTo`。

---

## 下个版本（计划中）：接入 `yangzeelong/Containers`，做家族化

1. **错误传递用 `Result`**
   - 参考 <https://github.com/yangzeelong/Containers> 的 `Result`，让 `Init/Start/Stop/DeInit`
     能携带错误上下文，而不只是一个 `ErrorCode` 枚举值。
   - **这是破坏性变更**：这些方法现在返回 `ErrorCode`，换签名要走 major 版本，并同步改
     `PipelineBuilder`、`examples/`、扩展与全部测试。
   - 1.0.0 **不引入**：放进 `include/` 即使不宣传，也会随
     `install(DIRECTORY include/nexusflow ...)` 一起被装出去，"不对外"不成立；放进 `src/`
     又不改造现有接口，就是死代码。
2. **选型岔路（动手前先定）**
   - (a) **源码内化**：把 Containers 的 `Result`/`Optional`/队列实现拷进
     `include/nexusflow/common/` 并改成 `nexusflow` 命名空间。无外部依赖，但与上游脱钩，
     后续同步靠人。
   - (b) **外部依赖**：Containers 走 FetchContent，`nexusflow::common` 变成转发层/别名。
     单一事实源，但要额外维护它的 install/export —— 本项目刚在 1.0.0 修好导出，
     不建议同一版本再叠一层。
3. **顺带要做**
   - 用 `Result` 重写 `Pipeline::Impl::Init()` 的错误路径，收掉一部分
     `LOG_ERROR` + 返回 `nullptr` 的隐式约定。
   - 用 Containers 的队列替换自研版本时，必须保住 `AGENTS.md` §5 的计数与并发约定。

---

## 停机排水（已实现）

`Stop()` 现在是**生产者优先**的单趟拓扑顺序：停一个阶段时，先 `RequestStop`、关掉它的输出边、
再 join，然后才轮到他下游的阶段。关掉输出边是释放"卡在 push 里等空位"的生产者所必需的——
下游完全可能根本不消费。下游阶段此时的输入边已经关闭，于是它自己把积压消费完，
`WaitForInputsDrained` 这时才真的等到队列清空。

`SpscQueue::waitUntilEmpty()` 不再在 shutdown 时短路返回 true：shutdown 只是拒绝新 push，
消费者仍在 pop，提前返回正是"排水跳过积压"的原因。

**踩过的两只坑**，每个都让实现回退过一次，别再犯：

1. 曾让 worker 在"输入全关且取空"时自行退出循环。看起来更简洁，但有竞态：一次已经通过
   shutdown 检查的 push 可能在该边关闭之后才落地，而消费者已按"空 + 已关"退出，于是这条
   消息没人取，排水只能白等整个预算。现在消费者只在被 `RequestStop` 时退出。
2. 曾把源的 `ShutdownOutputs()` 挪到 join 之后。当下游是"从不消费的 sink"时，源会永久卡在
   push 里，只能等停机超时后把线程丢掉（那个超时机制本身也在「停止语义」一节里删掉了）。
   所以源必须**先关边、再 join**。

回归用例：`PipelineDrainTest.StopDeliversWhateverTheSinkHasNotTakenYet` 与
`PipelineDrainTest.StopDrainsThroughAnIntermediateStage`。前者在旧实现上会看到边里剩一半
消息（pushed 8 / popped 4）。

仍然成立：停机后残留的消息在 `Stop()` 下不存在（边被排空）；`Cancel()` 下则只体现在
`EdgeSnapshot::discardedTotal`，**不要**把它记进任何模块的 `droppedTotal`——生产者已经算过
processed+pushed，消费者从未 pop 过它，记给谁都是重复计账。

---

## 停止语义：`Stop()` / `Cancel()`（已实现）

两个函数，都没有参数，也没有超时：

| API | 语义 |
| --- | --- |
| `ErrorCode Stop()` | **排空**：生产者优先，逐阶段"关输出边 → 等输入空 → join"，返回时每条边 `size == 0`，积压全部送达。 |
| `ErrorCode Cancel()` | **立刻停**：所有阶段同时 `RequestStop`，所有边先 `shutdown()`（卡在 push / pull 的线程立刻返回），join 之后把边里剩下的消息 `clear()` 掉。 |

两者都是**同步**的：返回时所有 worker 线程都已退出，因此之后可以直接 `Start()` 再跑一遍。
已经停过再调，返回 `FAILED_ALREADY_STOP`（`Cancel()` 之后再 `Stop()` 同理）。

### 为什么没有超时参数

原先的 `drainTimeout{5000}` / `stopTimeout{2000}` 是 PR #15 里凭感觉加的，没有任何实测依据，
却引出了一连串问题：两个预算怎么合并、"留 200 ms 宽限"到底等多久、`0` 和负数分别算什么、
按阶段还是全局……这些全是自找的复杂度。

更关键的是：`stopTimeout` **从来没有让 `Stop()` 更早返回**。老实现超时后只是记一条 `ERROR`
然后不 join，而 `~ModuleActor()` 照样会 join——它改变的只是"你什么时候知道出问题"。

去掉超时之后的正确性来自 PR #22 的停机顺序：

1. 生产者先关输出边、再 join。源卡在 `push` 里等空位时，边一关 `push` 立刻返回（不会丢数据：
   `shutdown()` 只是拒绝新 push，仍照常交出缓冲区里已有的消息）；
2. 于是 `Stop()` 不会因为框架原因卡住：源会自己退出循环，消费者把输入取空后也退出；
3. 唯一还可能卡住的是用户 `Process()` 里自己写的死循环/死等——那是用户代码的责任，
   用超时把它盖住只会让问题更晚暴露。

### 容器：三件套，各管一件事

| 方法 | 丢弃残留？ | 改变关闭状态？ |
| --- | --- | --- |
| `shutdown()` | 否 | 置为已关闭 |
| `clear()` | **是** | 否（关着还是关着，开着还是开着） |
| `restart()` | 是 | 置为已打开（`Start()` 用） |

`clear()` 同时加在 `SpscQueue` 与 `ConcurrentQueue`（fallback 路径），否则不定长队列的行为
会和定长队列不一致。实现是先逐槽 `~T()` 再归零索引，并把丢弃条数累加进 `discardedTotal`。

### 计数与不变式

`EdgeSnapshot::discardedTotal`（累计）记的是"被 `Cancel()` 销毁、消费者从未 pop 过"的条数。
**不变式**（已有用例）：

```
pushedTotal == poppedTotal + size + discardedTotal
```

丢弃数由队列自己记（`QueueCounters::discardedTotal`），`EdgeSnapshot` 直接透传，所以不变式
天然成立，不需要上层另算。`clear()` 会先把索引折进 baseline（`m_pushedBase` / `m_poppedBase`）
再归零，因此 `*Total` 在任意次 `clear()` 之后都不会回退。

**不要把 discarded 记进模块的 `droppedTotal`**：生产者已经把它算进 `processedTotal` +
`pushedTotal`，消费者从未 pop 过它，记给谁都是重复计账。

### 停机计时（可观测性）

`Stop()` / `Cancel()` 自己计时，结果落在快照里，不需要在外部用 `steady_clock` 把调用包一层：

| 字段 | 含义 |
| --- | --- |
| `PipelineSnapshot::stopPipelineMs` | 最近一次 `Stop()`/`Cancel()` 的墙钟时间 |
| `ModuleSnapshot::drainInputsMs` | 这次停机里该阶段等输入边变空用了多久；源的天然是 0，`Cancel()` 之后全是 0 |
| `ModuleSnapshot::exitLoopMs` | 该阶段从"被要求停"到线程退出用了多久（含它当时手里那一批） |

三者都**由 `Start()` 清零**（`ModuleActor::ResetStopTiming()` / `PipelineImpl::lastStopPipelineMs`），所以
运行中的快照不会报上一轮是怎么结束的。它们不是 `*Total`，不受"`*Total` 永不清零"那条规则约束
（`AGENTS.md` §5 已单独写明）。写侧是 relaxed atomic（`ModuleActor::RecordStopTiming()`），
读侧 `Pipeline::GetSnapshot()` 直接 load。

`Stop()` 结束时还会打一条 DEBUG，省得调用方自己拼：

```
Pipeline drained in 125 ms of waiting, slowest stage 'timing-sink' at addad ms; whole stop took 249 ms.
```

（实测那次的数字：drain 125 ms、整体 249 ms——差额来自 sink 手里那一批还没处理完，记在它的
`exitLoopMs` 里。）

### 用例

| 用例 | 断言 |
| --- | --- |
| `PipelineCancelTest.CancelDestroysTheBacklogInsteadOfDeliveringIt` | 慢 sink 仍有积压时 `Cancel()`：sink 收到 < N，边 `size == 0`，`discardedTotal > 0`，`received + discarded == pushed` |
| `PipelineCancelTest.DiscardedMessagesAreChargedToTheEdgeNotToAModule` | 同上一条链路，另外所有模块 `droppedTotal == 0`、`failedTotal == 0` |
| `PipelineCancelTest.CancelDiscardsThroughAnIntermediateStage` | 两级链路：每条边都满足不变式；relay 已投递但没进边的消息记在 relay 的 `droppedTotal` |
| `PipelineCancelTest.CancelThenStartRunsAgain` | `Cancel()` → `Start()` 能再跑满一批；`Stop()` 不改 `discardedTotal`，`pushedTotal` 累计到 2N |
| `PipelineCancelTest.CancelBeforeStartIsRejected` / `CancelTwiceIsRejected` / `StopThenCancelIsRejected` | 三个 guard |
| `PipelineDrainTest.*` | `Stop()` 全量送达、每条边 `poppedTotal == pushedTotal` |
| `SpscQueueTest.ClearDiscardsBufferedMessagesAndKeepsTheIdentity` / `ClearKeepsTheQueueClosed` / `RepeatedClearsAccumulateInsteadOfRewindingTotals` | `clear()` 析构残留、保持关闭、`*Total` 单调 |
| `ConcurrentQueueTest.ClearDiscardsAndKeepsTheIdentity` / `ClearKeepsTheQueueClosed` | fallback 路径与上面一致 |
| `PipelineStopTimingTest.StopRecordsDrainTimeOfTheStageThatHeldItUp` | 有积压时 `Stop()`：sink 的 `drainInputsMs >= 20`、源的 `drainInputsMs == 0`、`stopPipelineMs >= drainInputsMs`，边空且 `discardedTotal == 0` |
| `PipelineStopTimingTest.CancelTimesTheExitInsteadOfADrain` | `Cancel()`：所有模块 `drainInputsMs == 0`、`exitLoopMs <= stopPipelineMs`，卡在 `Process()` 里的 sink `exitLoopMs >= 20` |
| `PipelineStopTimingTest.StartForgetsThePreviousStopTiming` | `Stop()` 后 sink 的 `drainInputsMs > 0`，`Start()` 后 `stopPipelineMs`/`drainInputsMs`/`exitLoopMs` 全为 0 |
| `PipelineStopTimingTest.SnapshotReportsZeroTimingBeforeAnyStop` | 一次都没停过时三者全为 0 |

### 内部改动清单

| 文件 | 改什么 |
| --- | --- |
| `include/nexusflow/Pipeline.hpp` | `Stop()` 文档改写；新增 `Cancel()` |
| `include/nexusflow/Stats.hpp` | `EdgeSnapshot::discardedTotal`；`ModuleSnapshot::drainInputsMs`/`exitLoopMs`；`PipelineSnapshot::stopPipelineMs` |
| `include/nexusflow/common/SpscQueue.hpp` | `clear()`；`QueueCounters::discardedTotal`；`DiscardBufferedLocked()`；`waitUntilEmpty()` 去掉超时参数 |
| `include/nexusflow/common/ConcurrentQueue.hpp` | 同上（fallback 路径） |
| `src/pipeline/Pipeline.cpp` | `Stop()` 去掉预算；新增 `Cancel()`；`GetSnapshot()` 透传 `discardedTotal` 与停机计时；`Stop()`/`Cancel()` 逐阶段计时并发布 `lastStopPipelineMs` |
| `src/pipeline/impl/PipelineImpl.hpp` | 删除 `drainTimeout` / `stopTimeout`；新增 `lastStopPipelineMs` |
| `src/module/ModuleActor.{hpp,cpp}` | 删除 `StopAndJoinWithin` / `IsStopOverdue` / `BlockingTime`；`WaitForInputsDrained()` 去掉参数；新增 `RecordStopTiming()` / `LastDrainInputsMs()` / `LastExitLoopMs()` / `ResetStopTiming()` |
| `src/core/Worker.{hpp,cpp}` | 删除 `WaitForStopRequest` / `m_loopCond` / 若干只服务旧预算的 accessor；`WaitForInputsDrained()` 改为无参数 |

### 别重犯的坑

- **不要让 worker 自行退出**（"输入全关且取空就 break"）：与一次已通过 shutdown 检查的
  在途 push 竞态，那条消息落地时消费者已退出。
- **源必须「先关输出边、再 join」**：下游若是从不消费的 sink，源会永久卡在 push 里。
- **`Cancel()` 里 `clear()` 必须在 join 之后**：`clear()` 只在没有线程使用队列时才安全。
- **恢复备份文件后要 touch**：`Copy-Item` 保留旧时间戳会让 MSBuild 认为头文件比 obj 还旧，
  从而不重新编译，跑的还是旧二进制。
- **不要为了让测试通过而改测试语义**（`AGENTS.md` §6.4）。

---

## 已知小问题（不影响 1.0.0）

- `docs/telemetry.md` 不入库；`AGENTS.md` §5 现在只把它当本地笔记提及，不要再当权威引用。

---

## 关键经验（改代码前先看）

| 坑 | 表现 | 处理 |
| --- | --- | --- |
| 陈旧 MSBuild 节点 | 构建显示 exit 0，但改动没进二进制 | `Get-Process MSBuild \| Stop-Process -Force` |
| 陈旧 `.obj` | 改了测试，exe 还是旧断言 | `Remove-Item build\tests\nexusflow_tests.dir\Release\<File>.obj -Force` 后重建 |
| `Copy-Item` 还原源码 | 文件 mtime 跟着备份一起回退，MSBuild 认为源码比 `.obj` 旧 → **静默不重编**，测出来的还是上一版 | 还原后 `(Get-Item <file>).LastWriteTime = Get-Date`，并删掉对应 `.obj` 再 build |
| `--parallel` 参数写错 | 构建 exit 1 且**什么都没编** | 永远 `--parallel 16` |
| PowerShell 改写文件 | UTF-8 被转 GBK，出现 U+FFFD / U+00BE | 用 `[System.IO.File]::ReadAllBytes` + `UTF8Encoding($false,$true)`；改完扫 `[char]0x00BE` |
| `read` 越界 offset | 反复报同一错误 | 按报错里的总行数重新给 offset |
| 弹窗 | 测试运行弹控制台 | `Start-Process -PassThru -WindowStyle Hidden -RedirectStandardOutput/Error` |

不要提交 `docs/`（用户自己的设计文档目录，`git ls-files docs` 必须为 0）。
