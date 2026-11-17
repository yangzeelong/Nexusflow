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
> 上一版就是因为没重写而过期：它还在指 `fix/pipeline-drain-on-stop` 分支和"未提交的改动"。

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

### 3.3 每跳固定开销

**本版已做**：每消息级日志降为 TRACE（`Module.cpp` 7 处 + `Worker.cpp:363`）；默认日志级别 DEBUG → INFO；逐跳计时链改为按需开启（`PipelineContext::EnableModuleTimings()`，可链式，默认关；单模块可用 `Module::SetModuleTimingsEnabled()`）。实测代价：开启时 capacity=8 中位约 1.33M、关闭后约 2.97M msg/s。

剩下的大头（按预期收益排序，**先用 profiler 量**，别猜）：

1. **计时读数按需**：`Module::ProcessTimed` 里的两次 `steady_clock::now()` 在计时链关着时无人消费，可以整个跳过。
2. **`Dispatcher` 名字解析**：现在是按输出名字符串查下游；应在启动时解析成基底下标，热路径只做数组索引。
3. **计数器发布降频**：`Worker` 每个循环迭代一次 `PublishCounters()`（release fence），可改成每 N 条或按时间发布。
4. `std::map` 换扁平容器（`MessageMeta::moduleTimingMs`）：只在"必须默认开启计时链"时才值得做。

---

## 4. 性能现状（数字与协议见 [`benchmarks/README.md`](benchmarks/README.md)）

3 段线性流水线、模块内没有计算：capacity=8 中位约 **3.50M msg/s / 5.3 µs**，10 s 内耗 **38.86 s CPU ≈ 3.88 个核**，每消息约 **1.097 µs**、每跳约 **366 ns**，工作集 **5.1 MB**。

判断：

- **每跳 366 ns 基本全是框架开销**——这三个模块里没有计算，所以 366 ns/hop 就是排队 + 取时间 + 名字分发 + 计数 + COW 引用计数的总价。这个价码决定适用区间。
- **线程数是资源占用的主要单点**（= 模块数），对应 §3.1。
- **内存是加分项**：5.1 MB 定长、无按消息增长，`--capacity` 就是内存旋钮。
- **定位**：适合"每帧几十 µs ~ 几 ms"的视觉/算法流水线（一帧计算完全摊得掉这 366 ns），**不适合当高频小消息的事件总线**；对外不要按低延迟消息框架的口径描述性能。

每跳 366 ns 的构成是**读代码估的量级，不是采样结果**：时钟 ×2 占 50–80 ns、按字符串名查下游 50–100 ns、SPSC push/pop + fence 40–60 ns、shared_ptr 原子引用计数 20–40 ns、pending 输出向量 20–40 ns、计数 fence 10–20 ns。**下版本第一件事应该是接真 profiler（ETW / VS 采样 / `perf`），而不是继续猜。**

---

## 5. 验收口径

同拓扑、同机器、热身 1 次 + 3 次取中位数、前后对比，并把 **commit-id + 平台** 一起记进 `benchmarks/README.md`。没有前后数字的"性能优化"不算完成。

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

- **benchmark 拓扑参数化**（`--modules N`）：§3.1 和 §3.2 收益的前置。
- **CI 基准记录**：先只记录数字贴到 PR，不做门禁（托管 runner 噪声大，70% 阈值会假红）。
- **`ToString()` 补 `AppendTo(std::string&)`**：现在 `GetSnapshot()` 与 `ToString()` 各拼一次字符串。
- **`MetricSamples` 窗口容量可配**：现在 1024 是编译期常量，p99 可信度直接取决于它。
- **文档重组（单独一个 PR）**：`TODO.md` → `ROADMAP.md`；把里面的设计长文（停机排水、停止语义）抽成 `DESIGN.md`，已完成的版本记录抽成 `CHANGELOG.md`，并与 README 的 "Stopping a Pipeline" 去重。全仓目前只有 1 处引用 `TODO.md`，改名成本很低。
