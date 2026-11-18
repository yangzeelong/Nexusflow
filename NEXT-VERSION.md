# 下个版本开工单

> **定位**：下一版"这一刀怎么落地"。
> 长期路线图见 [`TODO.md`](TODO.md)；基准数字、测量协议与环境见 [`benchmarks/README.md`](benchmarks/README.md)；
> 硬规则、构建命令、用例基线见 [`AGENTS.md`](AGENTS.md)。本文件不重复它们。
>
> **起点**：1.0.0 收尾已完成（每消息日志降为 TRACE、默认日志级别 INFO、逐跳计时链改为按需开启、CI 立起来）。
> 下面的工作项都从"收尾之后"起算。
>
> **有效期**：只到本版发布为止。发版后**重写本文件**（清空重填），不要在旧内容上叠加——
> "做完了什么"进 `TODO.md` 的已完成历史，基准数字进 `benchmarks/README.md`。
> 上一版就是这么废掉的：它开头那三条 P0 全是失效条目（`build/perf-probe` 目标不存在、
> 五个远程分支早已合并删除、`docs/telemetry.md` 那台机器上根本没有这个目录）。

---

## 1. 版本目标：性能 + 资源占用

三条抓手，每条都要有前后数字（口径见 §5）：

| # | 目标 | 现状 |
| --- | --- | --- |
| 1 | 线程数不再等于模块数 | 每模块一个 worker 线程：3 个模块 = 3 个线程，100 个模块 = 100 个线程 |
| 2 | 降低每跳固定开销 | 约 **366 ns/跳**（3 段线性流水线、模块内无计算） |
| 3 | 让"批处理"真的是批处理 | 融合 `ProcessBatch` 收到的 batch **恒为 1** |

---

## 2. 开工前要拍板的三个决策

**不定就别动 §3 的实现**（它们会改公开 API 或大面积重命名）。

1. **Containers / `Result` 怎么接**
   - (a) 源码内化：拷进 `include/nexusflow/`，零外部依赖，符合「核心不依赖扩展」。
   - (b) FetchContent 引入：版本好升级，但离线构建要多传一个 `FETCHCONTENT_SOURCE_DIR_*`。
   - **注意**：`Init/Start/Stop/DeInit` 改返回 `Result` 是破坏性变更 → 走 major 版本，并同步改 `PipelineBuilder`、examples、扩展、全部用例。
   - 现状不是"要不要返回错误"，而是"返回什么错误"：[`ErrorCode.hpp`](include/nexusflow/ErrorCode.hpp) 是 8 值裸 `enum`，[`Pipeline.hpp`](include/nexusflow/Pipeline.hpp) 的 `Init/Start/Stop/Cancel/DeInit` 已经返回它。
2. **边计数改名** `pushedTotal/poppedTotal` → `enqueuedTotal/dequeuedTotal`。实测面：**12 个文件 45 处**（含 4 个用例文件 26 处）。单独 PR，纯重命名，改完 `git grep` 必须为 0。
3. **模块输出命名** `moduleNameA_OUTPUT_moduleNameB`（让边的身份自解释）。会影响 `queueNames` 拼装、`EdgeSnapshot::from/to`、若干测试断言。

---

## 3. 工作项

### 3.1 线程模型（资源占用最大的单点）—— [`src/module/ModuleActor.cpp:67`](src/module/ModuleActor.cpp#L67)

现状：`m_workThread = std::thread([this]() { m_worker->WorkLoop(); });` —— 每模块一线程，各自 spin/park 拉队列。

三档方案，建议**先做 L3 再决定要不要 L2**：

| 档 | 做法 | 线程数 | 风险 |
| --- | --- | --- | --- |
| **L3（先做）** | 保留 1 线程/模块，只削减空转与最内层 `Step` 的开销 | 不降 | 低 |
| **L2（推荐目标）** | 池化 + **模块亲和**：模块 i 固定绑定池中第 `i % poolSize` 个线程 → 同一模块永远同一宿主线程 | `min(modules, poolSize)` | 中 |
| L1 | 同层并行、模块可漂移 | 降最多 | 高：破坏 §5 单写者前提 |

- **硬约束**：`AGENTS.md` §5 的前提是"每个计数器只有一个写线程"。L2 靠"模块固定宿主线程"保住这一点，plain `++` + `PublishCounters()` 不变。代价是 `WorkLoop()`（阻塞式 while）必须拆成非阻塞的 `Step()`，由宿主线程轮询自己那组模块——**延迟特征会变，必须用曲线说话**。
- **第二个硬约束**：停机顺序是"生产者先关边，再 `actor->Stop()` → `join()`"。共享线程后 join 无法表达"单个模块排水完成" → 完成信号要改成 per-module 的完成标志（C++14 用 `atomic` + `condition_variable` 自写），`join` 只留给池析构。
- **前置条件**：`pipeline_benchmark` 现在固定 3 个模块，测不出线程模型收益。先加 `--modules N` 拓扑参数化 + 扫 `--capacity` × 模块数拿曲线，再决定动不动。

验收：同等吞吐下线程数下降；`WorkerBehaviorTest` / `PipelineLifecycleTest` 全绿；`AGENTS.md` §5 同步更新。

### 3.2 融合批处理 —— [`src/core/Worker.cpp:316`](src/core/Worker.cpp#L316) `Worker::RunFusion()`

现状（已读代码确认）：`messageCache` 是 `unordered_map<uint64_t, unordered_map<std::string, Message>>`，凑齐一组**立刻**处理且一组只装一条 —— `fusedMessageVec.clear(); push_back(MakeMessage(...)); ProcessBatch(...)`。**两处**要改：正常路径 [`377-380`](src/core/Worker.cpp#L377-L380) 与排水路径 [`415-420`](src/core/Worker.cpp#L415-L420)。函数开头第 317 行就是这条 TODO。

要做：

- 一次迭代把所有凑齐的组攒进同一个 `fusedMessageVec`，最后只调一次 `ProcessBatch`；
- 内层 `unordered_map<std::string, Message>` 换成小 `vector`（输入数已知 = `expectedInputCount`），输入边 → 下标在启动时预建，省掉每消息的哈希与字符串比较；
- **错误语义要钉死**：现在每组各自 try/catch、按组计 `AddProcessed/AddFailed`；改成批调用后一个组抛异常会中断整批 → 取"整批一个 try/catch，异常时剩余条数计 `AddFailed(n)`"，并用用例写清；
- **必须测内存**：半凑齐状态下 `messageCache` 的峰值（`GetSnapshot()` 看不到，用进程 RSS 或临时统计，不要入库）。

验收：用例断言融合模块收到的 `ProcessBatch` 大小 > 1；吞吐不低于基线。

### 3.3 每多一个模块的框架开销（原「每跳固定开销」）

**本版已做**：每消息级日志降为 TRACE（`Module.cpp` 7 处 + `Worker.cpp:363`）；默认日志级别 DEBUG → INFO；逐段计时链改为按需开启（`PipelineContext::EnableModuleTimings()`，可链式，默认关；单模块可用 `Module::SetModuleTimingsEnabled()`）。实测代价：开启时 capacity=8 中位约 1.33M、关闭后约 2.97M msg/s。

剩下的（**已用裸基线量过，不再靠估**；完整表见 [`benchmarks/PERFORMANCE-laptop.md`](benchmarks/PERFORMANCE-laptop.md) §2）:

1. **调度链是主体**：Module/Worker/Dispatcher 这一圈（取消息、虚调用、`ProcessTimed` 的两次读时钟、`pendingOutputs` + 两次 Message 拷贝、推队列、`PublishCounters`）每多一个模块约 **0.12–0.38 µs**（默认 batch 4）；Message 自身每多一个模块只有 **0.04–0.12 µs**，两者差 3–5 倍。
2. **批处理已经吃掉约一半**：同拓扑 batch 1 → batch 4，每多一个模块的调度成本从 0.23–0.52 降到 0.12–0.38。所以以后比固定开销，必须先钉住 batch 口径。
3. **每消息分配**约 0.06–0.18 µs，排第二，且不随模块数增长；要做就做小对象池/复用，别指望它救大局。
4. 原列的四条——计时读数按需、名字解析成下标、计数发布降频、`std::map` 换扁平容器——依然成立；前三条正是上面第 1 项的组成部分，**现在有基线可以逐条验证它们各值多少**。

判断顺序：先动第 1 项（收益在 µs 级），再看第 3 项（收益在几十 ns 级）；Message 那 0.04–0.12 µs 的"重做容器"优先级最低。

---

## 4. 性能现状（数字见 [`benchmarks/PERFORMANCE-laptop.md`](benchmarks/PERFORMANCE-laptop.md)，规则见 [`benchmarks/HARNESS.md`](benchmarks/HARNESS.md)）

笔记本（i7-11800H，8 物理核）：3 模块线性、模块内无计算、capacity 8、batch 4（默认）——约 **2.47 M msg/s**、**1.20 µs CPU/消息**、**约 2.9 核**。同构的裸线程 + 裸 SpscQueue 基线是 **0.19 µs/消息**（零分配口径）、**0.32 µs**（每消息分配口径）。

判断：

- **饱和吞吐下框架开销是大头**：2 模块时框架 0.96 µs vs 裸实现 0.19 µs（约 5 倍）。但按真实帧率算无关紧要：60 fps × 4 分支约 960 消息/秒，1.2 µs/消息只有约 0.1% 的一个核。
- **线程数才是资源占用的主要单点**（= 模块数），对应 §3.1。
- **容量旋钮有上限**：capacity 8 之后吞吐不再涨，只买尾部延迟（p99 从 9 µs 涨到 161 µs）。
- **定位不变**：适合"每帧几十 µs ~ 几 ms"的视觉/算法流水线，**不适合当高频小消息的事件总线**；对外不要按低延迟消息框架的口径描述性能。

早期那份"每多一个模块 366 ns、构成靠读代码估"的结论已作废：现在有裸基线，框架开销能拆成「分配 / Message / 调度」三层，每层都在同机、同 commit、同批参数下量过（报告 §2）。要更细的归因，才轮到 profiler（ETW / VS 采样 / `perf`）。

---

## 5. 验收口径

同拓扑、同机器、同 batch、同观测模式；每档 3 次，报 `median [min .. max]`，从 `stat,` 行取数，覆盖率断言（`cov,`）必须 ok。数字连同 **commit + 机器名** 写进 `benchmarks/PERFORMANCE-<machine>.md`，**不许跨机器比绝对值**。没有前后数字的"性能优化"不算完成——完整规则见 [`benchmarks/HARNESS.md`](benchmarks/HARNESS.md)。

---

## 6. 下版注意事项

### 6.1 三个决策我的倾向（可覆盖）

- **`Result`：选 (a) 源码内化**，并分两步：先加包装层（不破坏调用点），再换公开签名（major）。
- **改名：做，但单独一个 commit**，不要和别的改动混在一个 diff 里。
- **输出命名：可以推后**，它是可读性问题不是正确性，等吞吐改完再动。

### 6.2 风险

- **只有 §3.1 可能把框架改坏**：§5 的单写者计数器前提、停机里"生产者先关边再 join"的顺序，都绑在"每模块一个线程"上。动它要单独分支 + 测量数据 + 文档同步。
- **§3.2 动的是没有专门用例的代码**：`RunFusion()` 现在没有"融合 batch 大小 > 1"的用例，先补一个再改实现。

### 6.3 顺手可做（按价值排序）

- ~~**benchmark 拓扑参数化**（`--modules N`）~~：已做——`--modules`/`--branches`/`--capacity`/`--batch`/`--observe` 阶梯加上三条裸基线（`--baseline raw|raw-alloc|raw-message`），规则见 `benchmarks/HARNESS.md`，§3.1 / §3.2 的收益可以直接量。
- **CI 基准记录**：先只记录数字贴到 PR，不做门禁（托管 runner 噪声大，70% 阈值会假红）。
- **`ToString()` 补 `AppendTo(std::string&)`**：现在 `GetSnapshot()` 与 `ToString()` 各拼一次字符串。
- **`MetricSamples` 窗口容量可配**：现在 1024 是编译期常量，p99 可信度直接取决于它。
- **文档重组（单独一个 PR）**：`TODO.md` → `ROADMAP.md`；把里面的设计长文（停机排水、停止语义）抽成 `DESIGN.md`，已完成的版本记录抽成 `CHANGELOG.md`，并与 README 的 "Stopping a Pipeline" 去重。全仓目前只有 1 处引用 `TODO.md`，改名成本很低。
