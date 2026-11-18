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

五条抓手（前三条是原计划，后两条来自本轮台式机基准），每条都要有前后数字（口径见 §5）：

| # | 目标 | 现状 |
| --- | --- | --- |
| 1 | 线程数不再等于模块数 | 每模块一个 worker 线程：3 个模块 = 3 个线程，100 个模块 = 100 个线程 |
| 2 | 降低每跳固定开销 | 约 **366 ns/跳**（3 段线性流水线、模块内无计算） |
| 3 | 让"批处理"真的是批处理 | 融合 `ProcessBatch` 收到的 batch **恒为 1** |
| 4 | 跨过 P 核线程数后不再断崖 | 台式机 16→18 模块 batch1 吞吐 −41%（8.755→12.622 µs/消息），18–28 段不可稳定测量（§3.4） |
| 5 | 低速率/空闲路径不被 OS 定时器放大 | 框架自身 idle 等待仍是 `sleep_for(1ms)`/条件变量超时；只有 delay 负载改用高精度定时器绕过，16 模块 `stopMs` 15.6 ms（§3.4） |

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
- **前置条件已满足**：`pipeline_benchmark` 已参数化（`--modules/--branches/--capacity/--batch/--observe`，规则见 `benchmarks/HARNESS.md`），台式机 2–40 模块阶梯已经扫出 16→18 的拐点（§3.4）；线程模型改动直接用这条阶梯量收益。

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

### 3.4 本轮基准暴露的框架问题（下版优化清单）

> 数据：[`benchmarks/PERFORMANCE-desktop.md`](benchmarks/PERFORMANCE-desktop.md)（台式机 i7-14700KF，20 物理 / 28 逻辑核；114 条 `stat`、342 条 `cov` 全 ok；duration 5 s × 3，observe full；规则见 [`benchmarks/HARNESS.md`](benchmarks/HARNESS.md)）。这里列的是**框架问题**，不是测量误差；数字只用于同机前后对比。

**P0：线程模型的断崖（§3.1 的直接动机，现在有曲线了）**

| 证据 | 数字 |
| --- | --- |
| 拐点 | 16→18 模块 batch1：8.755→12.622 µs/消息（每模块 +1.93 µs，是拐点前 0.56 的 3.4 倍），吞吐 1.76M→1.04M（−41%） |
| 拐点区不可测 | 18–28 模块多数 arms 的 spread 15–126%（P/E 线程落点每次不同） |
| 远端点 | 32/40 模块：46.649/77.149 µs；0.33/0.23 M msg/s |
| 归因 | 40 模块裸 `raw` 68.379 µs、框架 77.149 µs（框架增量仍约 0.23 µs/模块）；裸线程自身 16→40 模块涨 10.6× → 机器满了，但框架无法把一个慢 stage 摊开 |
| 机器模型 | 8 个 P 核 × SMT = 16 个 P 逻辑线程；第 17 个模块线程起落到 12 个 E 核 |

要做：§3.1 的 L2（池化 + 模块亲和）优先，再加 **stage→核摆放**（先填满 P 线程，超过才用 E 核，避免整条链被一个 E 核 stage 决定）；拓扑阶梯要能标出"第几个模块开始落到 E 核"（Windows 用 `GetLogicalProcessorInformationEx`）。

验收（同机、同 commit、同 batch/observe）：24 模块吞吐 ≥ 16 模块的 70%（现在 16.447 µs vs 8.755，约 53%）；18–28 模块 spread ≤ 10%（先靠摆放稳定，再谈优化）；32/40 模块仍可重复测量，报告给出拐点后的斜率。

**P0：低速率/空闲路径的 15.6 ms 定时器地板**

- 已证实：Windows 11 在进程运行数秒后不再兑现 `timeBeginPeriod` 对 `sleep_for`/短超时等待的精度。独立探针里 `Sleep(2ms)` 从 2.35 ms 变成 14.75–15.49 ms，而高精度 waitable timer 稳定 2.33–2.38 ms；`NtQueryTimerResolution` 一直报 current=1.0 ms，说明是调度合并，不是请求丢失。
- 现状：只有 delay 工作负载改用高精度定时器绕开了；框架自身仍在用普通等待：`src/core/Worker.cpp:98` 的 `IdleAfterEmptyOutput`（source 空闲 `sleep_for(1ms)` / `waitForRoom(1ms)`）、`include/nexusflow/common/SpscQueue.hpp` 的 `kParkTimeoutMs = 5`、`Worker.hpp` 的 batch timeout 100 ms。
- 另一个未解释的跳变：16 模块 `stopMs` 15.6 ms，浅链只要 0.2–0.27 ms。
- 要做：把所有 idle/超时等待换成高精度可等待定时器（Windows）或 `eventfd`（Linux），超时只做兜底、正常唤醒走事件；给 HARNESS 加**低速率阶梯**（10/100/1000 msg/s × 30 s），把 p99 与 15.6 ms 量化写进报告。
- 验收：100 msg/s 持续 30 s，p99 < 2 ms；16 模块 `stopMs` ≤ 2 ms。

**P1：每消息固定开销**

- 2 模块 batch1：框架 0.876 µs，裸 `raw` 0.234 / `raw-alloc` 0.471 / `raw-message` 0.564 µs（约 1.6–3.7×）；batch4 压到 0.739 µs。
- 3 模块 batch4、cap8：**0.931 µs/消息、3.156 M msg/s、约 2.9 核**。
- 目标：3 模块 batch4 ≤ 0.75 µs（−20%），2 模块 batch1 ≤ 0.70 µs；先动 §3.3 第 1 项（调度链），再动分配。注意这是 CPU 时间不是延迟：真实 60 fps × 4 分支下 0.93 µs/消息约等于 0.02% 一个核。

**P1：fan-out / fusion 的每帧成本**

- 8 分支：4.950 M msg/s = 618 K 帧/s，每帧 14.877 µs；1 分支 1.14 µs/帧。
- 8 分支融合：361 K 帧/s、26.749 µs/帧（spread 13.9%，标 unstable）。
- 要做：广播路径去掉可以合并的 per-branch 检查；融合按 §3.2 批量。验收：fan8 ≤ 10 µs/帧，fusion8 ≥ 500 K 帧/s。

**P1：批处理语义**

- batch1 2.572M / batch4 3.156M（+23%）/ batch8 2.768M / batch16 2.026M（−36%）。
- 融合 `ProcessBatch` 恒为 1（§3.2 已列）。
- 验收：默认 batch 4 保持；加"融合 batch 大小 > 1"用例；batch16 相对 batch4 的跌幅压到 ≤15%。

**P2：外部输入边界**

- 本分支已加 `Pipeline::Push/TryPush`（passive source、单调用线程、fan-out 可能部分投递）：注入 block 2.803M、try 2.927M（39% 重试）。
- 缺：多生产者输入、优先级/QoS、Push 的批量接口、按模块反压查询。先把"单生产者/单调用线程"写进文档；如做多生产者，先补 MPSC 或外部队列 + 单 pusher，再给前后数字。

**P2：可观测性**

- coverage/stat 已够用（342 条 cov ok、`pushed==popped+discarded+residual`）；RSS/stop 只在 harness 侧。可加进程级 RSS 到 snapshot 不是必须，优先级低。

**测量配套**：HARNESS 的深度维度在混合核机器上要延长到 40（本机 16 之后还有 12 个 E 核）；增加低速率阶梯；拐点区复测 5 次；所有优化前后数字必须同机同 commit。

---

## 4. 性能现状（数字见 [`benchmarks/PERFORMANCE-laptop.md`](benchmarks/PERFORMANCE-laptop.md)，规则见 [`benchmarks/HARNESS.md`](benchmarks/HARNESS.md)）

笔记本（i7-11800H，8 物理核）：3 模块线性、模块内无计算、capacity 8、batch 4（默认）——约 **2.47 M msg/s**、**1.20 µs CPU/消息**、**约 2.9 核**。同构的裸线程 + 裸 SpscQueue 基线是 **0.19 µs/消息**（零分配口径）、**0.32 µs**（每消息分配口径）。

台式机（i7-14700KF，20 物理 / 28 逻辑核，Windows 11）：同构 3 模块、capacity 8、batch 4 —— **3.156 M msg/s**、**0.931 µs CPU/消息**、约 2.9 核；16 模块 batch1 1.76 M / 8.755 µs，batch4 2.24 M / 6.791 µs；2 模块裸基线 0.234–0.564 µs/消息。深度阶梯在 **16→18 模块**出现拐点（§3.4），两台机器分报告、不跨机比绝对值。

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
