# NexusFlow 开发路线图与待办事项

本文档记录 NexusFlow 框架当前状态与下一步计划，供跨设备/跨 agent 协作时同步。

**同步基线**：`master` @ `1eae366`（PR #20 已合并）。本轮定版修复的工作区改动**未提交**。
**测试基线**：`138 tests from 23 test suites，PASSED 138`（MSVC 19.40）。
**构建**：`cmake --build build --config Release --parallel 16` → 退出码 0。注意会刷出
大量 `C4819`（源文件含 UTF-8 中文注释，MSVC 按 936 代码页读取）。想要干净构建，
给目标加 `/utf-8`；在加上之前不要在本文件里声称"无 warning"。

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

## 1.0.0 定版修复（本轮已完成，改动未提交）

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
- **`RunFusion()`**：分组 key 由 `MessageMeta::sourceName` 改为**输入边名**。框架从不写
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
   push 里，只能等 `stopTimeout` 被放弃线程。所以源必须**先关边、再 join**。

回归用例：`PipelineDrainTest.StopDeliversWhateverTheSinkHasNotTakenYet` 与
`PipelineDrainTest.StopDrainsThroughAnIntermediateStage`。前者在旧实现上会看到边里剩一半
消息（pushed 8 / popped 4）。

仍然成立：排水预算用尽时残留的消息只体现在 `EdgeSnapshot::size`（= pushedTotal - poppedTotal），
**不要**把它记进任何模块的 `droppedTotal`——生产者已经算过 processed+pushed，消费者从未 pop
过它，记给谁都是重复计账。

---

## 待实现：两种停止语义（`StopMode`）

目标是把「怎么关流水线」变成两个明确的选择，而不是一个永远排空的 `Stop()`。

| 语义 | 行为 |
| --- | --- |
| **Graceful**（正常关闭，现有行为） | 不再接收新数据；按拓扑顺序把积压处理完再退出；可给每个阶段一个超时上限 |
| **Immediate**（强制关闭） | 不再接收新数据；积压**直接丢弃**、不处理；边被清空，丢弃数记在边上 |

现状：Graceful 已由 PR #22 实现（生产者优先、逐阶段排空），但**超时值硬编码**在
`PipelineImpl.hpp`（`drainTimeout{5000}`、`stopTimeout{2000}`），调用方改不了；
Immediate **完全没有**——`Stop()` 永远排空，队列也没有「丢弃但保持关闭」的方法。

### 公开 API（只加这几个）

```cpp
// include/nexusflow/Pipeline.hpp
enum class StopMode { Graceful, Immediate };
ErrorCode Stop(StopMode mode = StopMode::Graceful);   // 默认值保持现有调用不变

// include/nexusflow/PipelineBuilder.hpp —— 只有一个参数，名字就叫 timeout
PipelineBuilder& WithStopTimeout(std::chrono::milliseconds timeout);
```

`timeout` 是**每个阶段的总预算**：先拿它去排空，剩下的时间用来让线程退出；排空吃光预算时
留一个固定小宽限（200 ms）再 join，避免一到点就放弃线程。

`timeout` 的取值约定：

- 正数：每阶段最多等这么久；
- `0`：不等，直接走强制路径；
- 负数：**拒绝**（`Stop()` 返回失败），不要悄悄当成「无限」——无限等待与
  `StopAndJoinWithin` 注释里那条设计原则直接冲突（一个卡住的模块会把整条流水线拖住）。

YAML 侧对应可选键（与 `graph.messageQueueCapacity` 同级）：`graph.stopTimeoutMs`。

### 容器：三件套，各管一件事

| 方法 | 丢弃残留？ | 改变关闭状态？ |
| --- | --- | --- |
| `shutdown()` | 否 | 置为已关闭 |
| `clear()` | **是** | 否（关着还是关着，开着还是开着） |
| `restart()` | 是 | 置为已打开（`Start` 用） |

`clear()` 要同时加到 `SpscQueue` 与 `ConcurrentQueue`，否则走 fallback 分支时行为不一致。
实现上直接复用 PR #21 修好的残留析构逻辑（先逐个 `~T()` 再归零索引）。

**不要叫 `discardAll()`**：丢弃是 pipeline 停机时的决定，不是容器的职责；容器只说
"clear"。

### 计数与不变式

`EdgeSnapshot` 增加一个累计字段：

```cpp
std::uint64_t discardedTotal = 0;   // 被强制停止丢弃的消息数；只在 Immediate 下增长
```

放边上而不是模块上：丢的是边里的数据，不属于任何模块。

**不变式**（必须写成用例）：

```
pushedTotal == poppedTotal + size + discardedTotal
```

实现建议：让队列自己记——`QueueCounters` 加 `discardedTotal`，`clear()` 把丢弃条数累加进去，
`GetStats()` 一并返回，`EdgeSnapshot` 直接透传。这样丢弃数与 `pushed`/`popped` 同源，
不变式天然成立，不需要上层另行推算。

**不要**把 discarded 记进任何模块的 `droppedTotal`：生产者已经把它算进 `processedTotal` +
`pushedTotal`，消费者从未 pop 过它，记给谁都是重复计账（这一点在 PR #21 已经踩过一次，
见 `ProcessedDroppedAndFailedAccountForEveryMessage` 与
`StopAccountsForMessagesTheDispatcherCouldNotPush`）。

### 内部改动清单

| 文件 | 改什么 |
| --- | --- |
| `include/nexusflow/Pipeline.hpp` | 加 `StopMode` 枚举；`Stop(StopMode = Graceful)` |
| `include/nexusflow/PipelineBuilder.hpp` / `src/builder/PipelineBuilder.cpp` | 加 `WithStopTimeout(timeout)` |
| `include/nexusflow/Stats.hpp` | `EdgeSnapshot::discardedTotal` + 注释 |
| `include/nexusflow/common/SpscQueue.hpp` | `clear()`；`QueueCounters` 加 `discardedTotal`；`GetStats()` 透传 |
| `include/nexusflow/common/ConcurrentQueue.hpp` | 同上（fallback 路径） |
| `src/pipeline/impl/PipelineImpl.hpp` | 删除 `drainTimeout`，只留一个 `stopTimeout`（由 builder 设置，默认 5000 ms） |
| `src/pipeline/Pipeline.cpp` | `Stop(mode)` 分两支：Graceful = 现有 PR #22 逻辑（预算改用 `stopTimeout`）；Immediate = 全阶段 `RequestStop` → 所有边 `shutdown()` → 有预算 join → 每条边 `clear()` |
| `src/base/GraphUtils.cpp` | yaml 可选键 `graph.stopTimeoutMs` |
| `AGENTS.md` §3 / §5 | 用例数与不变式同步 |

### 测试清单

1. **Immediate 丢弃**：源发 N 条到慢 sink，`Stop(Immediate)` 后：sink 收到 < N；
   每条边 `size == 0`；`pushedTotal == poppedTotal + size + discardedTotal`；
   且没有任何模块的 `processed`/`dropped`/`failed` 因这批消息而变化。
2. **Graceful 全量送达**：已由 `PipelineDrainTest` 覆盖，无需重复。
3. **超时上限生效**：sink 极慢 + 很小的 `timeout`，断言 `Stop(Graceful)` 在
   `timeout` + 宽限附近返回，残留仍可见（`size > 0` 或 `discardedTotal` 为 0 但 size 非 0）。
4. **`timeout == 0`**：等价于不排空，直接走强制路径。
5. **负数 `timeout`**：被拒绝，返回失败。
6. **`clear()` 单测**：清空后 `size == 0`、`isShutdown()` 仍为真、再 push 失败；
   随后 `restart()` 能正常 push。
7. **Discard 后再 Start**：`Stop(Immediate)` → `Start()` → 新消息照常流动（边被 `clear()`
   过但仍可 `restart()`）。

### 验证命令

```
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release ...   -> exit 0
cmake --build build --config Release --parallel 16   -> exit 0
build\tests\Release\nexusflow_tests.exe              -> 全绿
git diff --stat 与 --git diff --stat --ignore-cr-at-eol -> 一致
```

### 别重犯的坑

- **不要让 worker 自行退出**（"输入全关且取空就 break"）：与一次已通过 shutdown 检查的
  在途 push 竞态，那条消息落地时消费者已退出。实测把 55 ms 的用例拖成 5140 ms。
- **源必须「先关输出边、再 join」**：下游若是从不消费的 sink，源会永久卡在 push 里。
- **恢复备份文件后要 touch**：`Copy-Item` 保留旧时间戳会让 MSBuild 认为头文件比 obj 还旧，
  从而不重新编译，跑的还是旧二进制。
- **不要为了让测试通过而改测试语义**（`AGENTS.md` §6.4）。

### 不在本次范围

- `Result` / Containers 家族化（见上一节）。
- 线程池、join 热路径优化（见 §P1）。

---

## 已知小问题（不影响 1.0.0）

- `src/common/MetricsCollector.cpp:157` 注释里有一个损坏字符（`p99 over ...m20 items`），
  是历史编码事故留下的，读起来费解。
- `docs/telemetry.md` 不入库；`AGENTS.md` §5 现在只把它当本地笔记提及，不要再当权威引用。

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
