## 性能评测 (Performance Benchmark)

CPU 横评的正式结果（测试机配置、编译选项、用例细节、各仓库 commit）：
[中文](PERFORMANCE.zh-CN.md) / [English](PERFORMANCE.md)；
完整叙述与成本归因见 [`COMPARISON.md`](COMPARISON.md)。

### 目录内容

`benchmarks/` 里放两个基准目标，**源码都在这一层**（不再有子目录）：

| 文件 | 目标 | 说明 |
| --- | --- | --- |
| `BenchmarkMessage.cpp` + `benchmark_main.cpp` | `nexusflow_benchmarks` | Google Benchmark 微基准：消息创建/广播（拷贝）/数据访问两种实现对比 |
| `pipeline_benchmark.cpp` | `pipeline_benchmark` | 端到端吞吐与延迟扫描：`--topology diamond|linear|inject`、`--baseline`、`--batch`、`--observe`、`--modules`、`--branches`、`--capacity`、`--payload`、`--payload-fresh`、`--sync-inputs`、`--workload`、`--cpus`、`--push-mode`、`--straggler-us`、`--pool-size`、`--queue-full`、`--timings`、`--duration`、`--repeat`；自己带 `main()`。框架自身的参数扫描与跨框架横评见 [`PERFORMANCE.md`](PERFORMANCE.md)（中文 [`PERFORMANCE.zh-CN.md`](PERFORMANCE.zh-CN.md)） |
| `LatencyHistogram.hpp` / `ProcessCpuTime.hpp` / `ProcessMemory.hpp` | — | 对数桶直方图（P50/P99）、CPU 时间与常驻内存采集 |
| `BenchmarkNodes.hpp` / `BenchmarkTopology.hpp` | — | 基准用的模块与拓扑构造器（线性 / 扇出扇入 / 外部注入）；`WorkloadModule` 提供 delay / compute / memory 三种负载 |
| `RawPipeline.hpp` | — | 裸线程 + 裸 `SpscQueue` 的同构基线，用于把框架成本拆成三层 |

```powershell
# 微基准
cmake --build build --config Release --target nexusflow_benchmarks --parallel 16
.\build\benchmarks\Release\nexusflow_benchmarks.exe

# 端到端扫描（默认 diamond 2 分支，与旧基线同构）
cmake --build build --config Release --target pipeline_benchmark --parallel 16
.\build\benchmarks\Release\pipeline_benchmark.exe --duration 5 --repeat 3
.\build\benchmarks\Release\pipeline_benchmark.exe --topology linear --modules 3 --capacity 8
.\build\benchmarks\Release\pipeline_benchmark.exe --topology linear --modules 2 --capacity 8   # 对照档：source→sink
.\build\benchmarks\Release\pipeline_benchmark.exe --topology diamond --branches 2 --sync-inputs
.\build\benchmarks\Release\pipeline_benchmark.exe --payload 1080p-rgb          # 复用一块帧缓冲
.\build\benchmarks\Release\pipeline_benchmark.exe --payload 1080p-rgb --payload-fresh   # 每帧新建

# 外部注入：主线程用 Pipeline::Push/TryPush 喂 passive source
.\build\benchmarks\Release\pipeline_benchmark.exe --topology inject --modules 3 --capacity 8 --push-mode block
.\build\benchmarks\Release\pipeline_benchmark.exe --topology inject --modules 3 --capacity 8 --push-mode try

# 负载节点：把瓶颈从框架开销换成真实计算
.\build\benchmarks\Release\pipeline_benchmark.exe --topology linear --modules 3 --capacity 4 --workload compute --compute-iters 500
.\build\benchmarks\Release\pipeline_benchmark.exe --topology linear --modules 3 --capacity 4 --workload memory --memory-bytes 65536
```

每次运行会打印一行
`csv,<label>,<platform>,<baseline>,<batch>,<observe>,<topology>,<modules>,<branches>,<capacity>,<payload>,<fresh>,<sync>,<duration>,<rep>,<delivered>,<throughput>,<cpu_s>,<cores>,<avg>,<p50>,<p95>,<p99>,<min>,<max>,<cpus>,<timings>,<workload>,<work_param>,<push_mode>,<attempts>,<refused>,<rss_start>,<rss_peak>,<rss_end>,<edge_depth_max>,<stop_ms>,<cycles_per_msg>,<pool>`，
批量跑时 `grep '^csv,'` 收集成表。`min`/`max` 是单次样本的极值（`max` 通常是调度离群点，别当稳态读）。
`modules` / `branches` 是**实际建出来的拓扑**，不是命令行回显：`diamond --branches 2` 记 `modules=4, branches=2`，`linear --modules 2` 记 `modules=2, branches=0`。
后 12 列是本分支扩展：`cpus`（绑核数，0=不绑）、`timings`（逐跳计时 0/1）、`workload`/`work_param`（负载类型与参数）、`push_mode`（`block`/`try`）、`attempts`/`refused`（注入尝试/队列满拒绝次数）、`rss_start`/`rss_peak`/`rss_end`（MiB）、`edge_depth_max`（最深积压）、`stop_ms`（Stop 排水+join 耗时）。
倒数第 2 列 `cycles_per_msg` 是补充指标：`cpu/msg × TSC`，TSC 频率由 `tsc` 行给出（三窗校准）；ARM 上恒为 0，不伪造。口径见下文「测量协议」§4。
最后一列 `pool` 是池化线程数（0 = 每模块一线程，`--pool-size N` 开启）。

`--baseline raw|raw-alloc|raw-message` 跑**裸线程 + 裸 SpscQueue** 的同构基线（[`RawPipeline.hpp`](RawPipeline.hpp)），用来把"框架成本"和"机器成本"分开：同拓扑、同容量、同阻塞语义，一个 stage 一个线程。三档口径，用于三层拆分：

- `raw`：消息是普通结构体，**不分配**——任何框架成本的下界；
- `raw-alloc`：每条消息 `make_shared` 一个 payload 包装（同框架 `MakeMessage()` 的分配量）；
- `raw-message`：消息直接用框架的 `nexusflow::Message`（`MakeMessage` 建、`BorrowPtr` 读时间戳），**队列与消息层和框架完全同构**。

于是：`raw→raw-alloc` = 每消息分配成本；`raw-alloc→raw-message` = Message 自身（元数据/类型擦除/COW）；`raw-message→框架` = Module/Worker/Dispatcher 调度成本。
三者都不分批，所以和它们对比时框架侧用 `--batch 1`。

`--batch N` 设每个非 source 模块的 `maxBatchSize`（默认 4，保持历史口径）；深度阶梯的对照口径用 `--batch 1`。

`--observe none` 关掉 harness 自己的观测（source 的时钟读、sink 的时钟读 + 直方图采样），用来量观测开销；`--micro-observer N` 单独跑 N 次"两次时钟读 + 一次直方图采样"并退出，直接给 ns/消息。

`--topology inject` 把线性链的 source 换成 passive source（`IsPassiveSource()`），主线程用 `Pipeline::Push`（`--push-mode block`）或 `TryPush`（`--push-mode try`）从外部喂帧——就是解码器/网络收包的形状。`attempts` 是送入尝试次数，`refused` 是 `TryPush` 遇到边队列满的次数；Stop 前 harness 会校验 pushed/popped 与 sink 计数（见 `cov,` 行）。inject 必须配 `--baseline framework`。

`--pool-size N` 走 v1.1 的线程池模型（`PipelineContext::WithThreadPoolSize`）：非融合模块固定绑定到 N 个共享线程，融合模块仍独占线程。池模式用精确配额背压（`OutputRoom()`），不丢帧，`cov` 应为 `ok`；代价是浅队列深链上吞吐下降，实测见 [`PERFORMANCE.zh-CN.md`](PERFORMANCE.zh-CN.md) 第二部分 §14。

`--queue-full block|drop` 选择输出队列满时的行为：`block`（默认）阻塞等待，慢下游给上游施加背压；`drop` 丢掉这条消息并计数（`QueueFullPolicy::kDrop`），只在允许丢帧的流水线上有意义。丢弃发生在模块的发送路径（`Dispatcher::Broadcast`），不是队列内部：队列里的旧消息一条都不会被赶走，**被拒收的是正要入队的新消息**。策略是**全有或全无**：一条消息要么进所有输出队列、要么一条都不进，否则 join 组会永远凑不齐。实测见 [`PERFORMANCE.zh-CN.md`](PERFORMANCE.zh-CN.md) 第二部分 §15：它买到的是活性，不是吞吐。

`--workload none|delay|compute|memory` 给每个中间 stage 加负载：`--delay-us`（等待；Windows 上用高精度 waitable timer，因为系统的 `Sleep` 在进程跑几秒后会被合并到 15.6 ms tick，`sleep_for` 不再可信；这个维度用 ≥2 ms 档）、`--compute-iters`（sin/cos 串行链）、`--memory-bytes`（每消息两次半区 memcpy）。`work_param` 记录实际参数。

`--cpus N` 用 `SetProcessAffinityMask`（Windows）/ `sched_setaffinity`（Linux）把进程绑到前 N 个逻辑核，0 表示不绑；`affinity:` 横幅会打印实际绑定。HARNESS 的阶梯矩阵默认不绑核。

`--timings` 打开框架的逐跳计时链（`PipelineContext::EnableModuleTimings()`），默认关闭；打开后吞吐会明显下降，只用于确认瓶颈位置，不要和关闭档混在同一张表里。`--log-level trace|debug|info|warn|err|off` 控制日志，基准默认 `warn`。

除了每次运行一行的 `csv,`，还有两种行：

- 每个配置一行 **`stat,`**（阶梯表就是用它拼的）：
  `stat,<label>,<platform>,<baseline>,<batch>,<observe>,<topology>,<modules>,<branches>,<edges>,<capacity>,<payload>,<fresh>,<sync>,<reps>,<tp_med>,<tp_min>,<tp_max>,<tp_spread_pct>,<cores_med>,<cores_min>,<cores_max>,<p50_med>,<p95_med>,<p99_med>,<cpu_per_msg_med>,<unstable>`。
  中位数 + 三次重复的范围 + 波动百分比；`unstable=1` 表示吞吐波动超过 `--max-spread`（默认 10%），这一档别直接读，先重跑。
- 每次运行一行 **`cov,`**（覆盖率断言）：
  `cov,<label>,<baseline>,<modules>,<branches>,<capacity>,<rep>,<pushed>,<popped>,<discarded>,<residual>,<sink_popped>,<delivered>,<processed>,<dropped>,<failed>,<join_expired>,<dropped_mid_run>,<join_expired_mid_run>,<stop_ms>,<ok>`。
  判据：**跑动中** `dropped`/`failed`/`join_expired` 必须为 0；`pushed == popped + discarded + residual`；sink 收到的条数与 `delivered` 对得上（融合路径按 `branches` 折算）。
  不满足就打印 `COVERAGE MISMATCH` 并让退出码为 1。停机尾部为关队列而丢掉的那几条只报不判错（`dropped`/`failure` 列照实给）。

`--payload` 档位：`none` / `1080p-rgb`（5.937 MiB）/ `1080p-yuv420`（2.969 MiB）/ `2k-rgb`（QHD 2560×1440，10.547 MiB）/ `2k-yuv420`（5.273 MiB）。
`--capacity` 在载荷模式下默认 4，因为 `capacity × payload` 就是每条边占的内存（1080p RGB × 64 ≈ 380 MiB/边）。

**参数下限与报错**（不再静默纠正，猜错就退出 1）：
`--topology linear` 的 `--modules` 最小 **2**（source→sink，1 条边，是深度阶梯的对照档）；`--topology diamond` 的 `--branches` 最小 **1**；
`--sync-inputs` 只能配 diamond；`--topology inject` 要求 `--baseline framework`、`--modules >= 2`，且不接受 `--sync-inputs`；`--baseline raw*` 不接受 `--workload`；数字参数不是整数、`--memory-bytes` 小于 1、`--payload` 未知档位、`--label` 含逗号，一律报错退出。

## 测量协议

一句话：**每个维度跑一条阶梯，把表贴出来，看「比上一档多花多少」就能下结论。**

成本只说「每多一个模块多花 X µs」：**不要发明「跳 / 段 / 级」这类同义词**（工具输出、报告、提交信息里都一样），机器里本来就叫模块，直接用它。
「模块」包括 source 与 sink：`--modules 3` = source + 1 个中间模块 + sink。**模块数 = 线程数**（一模块一线程），加一个模块就是同时加一个线程、一条队列、一次拷贝。

---

### 规则（5 条）

1. **每个维度跑一条阶梯**：≥5 档，跨过拐点两侧，含一个远端点；表里必须有「比上一档多花」这一列。
2. **每档跑 3 次**，报最小~最大；相邻差**大于波动**才允许解读（基准机上 5 s × 3 的相邻档波动也能到 10%，别省次数）。工具已经按 `--max-spread`（默认 10%）打 `UNSTABLE` 标记。
   附注：**`unstable` 也不等于可信**。遇到过 `spread` 没超阈值、但某一档明显反常的样本（比如 batch4 反而比 batch1 慢），复测就正常了。所以任何反常点先重跑一次；重跑还反常就在报告里标出来并说明，不许当数据用。
3. **每个数字带机器名 + commit**；两台机器各写各的报告（§5）。
4. **结论只写两句**：从哪一档开始变贵；是框架的事还是机器满了（拿裸基线对照，§3）。
5. **观测开销**：关掉计时/直方图再跑一遍，把差值写一行。

被砍掉的（不许再加回来）：回归、截距、R²、斜率区间、拐点判据公式、观测模式矩阵。

---

### 1. 怎么读表

真数据（基准台式机 i7-14700KF，`--topology linear --payload none --capacity 8 --duration 5 --repeat 3 --batch 1`，2026-11-18、commit `31b4420`，完整表见 [`PERFORMANCE.zh-CN.md`](PERFORMANCE.zh-CN.md) 第二部分 §1）：

| 模块数 | 吞吐（M msg/s） | CPU/消息（µs） | 比上一档多花（µs） |
| --- | --- | --- | --- |
| 3 | 2.05 | 1.449 | — |
| 4 | 1.91 | 2.052 | +0.603 |
| 5 | 1.87 | 2.580 | +0.528 |
| 6 | 1.82 | 3.173 | +0.592 |
| 8 | 1.79 | 4.278 | +0.553/模块 |
| 10 | 1.80 | 5.364 | +0.543/模块 |
| 12 | 1.77 | 6.481 | +0.559/模块 |
| 16 | 1.76 | 8.755 | +0.569/模块 |

结论一句话：3–16 个模块每多一个模块 0.42–0.60 µs；16 已经贴到 P 核线程容量（8 P × SMT = 16），再往上（2026-11-20 复测）24→28 个模块从 13.6 涨到 23.5 µs/消息，28 = 逻辑核数。
所以**深度和线程数在这个框架里是同一件事**，报告里必须写出来；跨过拐点后单值不稳，要按区间报。

每条阶梯报告只要求三样：这张表、一句"从哪档开始变贵"、一句"归因"（§3 的对照）。

### 2. 跑哪些维度

| 维度 | 参数 | 阶梯 | 对照档 |
| --- | --- | --- | --- |
| 深度（linear） | `--modules` | 2,3,4,5,6,8,10,12,16 | **2**（source 直连 sink） |
| 扇出（diamond） | `--branches` | 1,2,3,4,6,8 | **1**（有边不分支） |
| 队列容量 | `--capacity` | 1,2,4,8,16,64,256 | — |
| 批大小 | `--batch` | 1,2,4,8 | **1** |
| 载荷 | `--payload` | none,1080p-yuv,1080p-rgb,2k-yuv,2k-rgb | **none** |
| 观测 | `--observe` | none,full | **none** |

### 3. 两条基线

1. **裸线程 + 裸 SPSC 队列**（`std::thread` + `SpscQueue`，同容量、同拓扑，先只做 linear）：跑同一套阶梯。
   一直不贵 ⇒ 框架没问题；也变贵 ⇒ 是机器/队列模式，不是框架。
   三个口味：`raw`（普通结构体、零分配，下界）、`raw-alloc`（每消息 `make_shared` 一个 payload 包装）、`raw-message`（直接用框架的 `Message`，队列与消息层同构）。三层拆分：`raw→raw-alloc` = 分配，`raw-alloc→raw-message` = Message 自身，`raw-message→框架` = Module/Worker/Dispatcher 调度。三者都不分批，对比时框架侧用 `--batch 1`。
2. **无观测重跑**：同一配置加 `--observe none`，差值就是观测开销。本机实测这个差值落在批次噪声以下（测不出来），所以另有微基准 `--micro-observer N`，直接给「两次时钟读 + 一次直方图采样」的 ns/消息（本机约 40 ns）。

### 4. 四个数字，别混用

同一个 run 给出四个量，都在 `csv,` 行里（`throughput` / `cpu_s` / `cores` / `cycles_per_msg`）。口径先定义清楚，
跨机器、跨框架才谈得上比较：

| 数字 | 公式 | 单位 | 回答什么 | 边界 |
| --- | --- | --- | --- | --- |
| 交付吞吐 | `delivered / wall`，wall = `Start()` 后到 `Stop()` 前的 `steady_clock` | msg/s | 这台机器每秒真正交出多少帧 | 不含启动与停机；跨 CPU 频率不可比 |
| CPU 归一吞吐 | `delivered / cpu_seconds` | msg/s·core | 单核能压出多少条 | **不是交付吞吐**：CPU 含自旋、轮询、丢帧重试，核多时数字好看而墙钟不涨 |
| 每消息 CPU | `cpu_seconds / delivered` | µs/msg | 一条消息摊多少 CPU | 多线程自旋会让它虚高，且不区分交付与丢弃 |
| 每消息周期 | `cpu_seconds / delivered × TSC` | cycles/msg | 同上的"无主频"表述 | 仅在 x86 启用；TSC 由三个 20 ms 窗口校准，取最大 |

三条恒等式用来交叉验算，避免口径串味：

- `cores = cpu_seconds / wall`，因此 `throughput = cores × 1e6 / cpu_per_msg`；
- `cycles_per_msg = cpu_per_msg × tsc_ghz × 1000`；
- 两条同时不成立时，多半是采样窗口错位：吞吐取 Stop 之前，CPU 取 Stop 之后。

一句话规则：**说"有多快"只用墙钟吞吐；跨机器比"cycles/消息"；跨框架比"每交付一条的 CPU"。**

常见坑：Google Benchmark 的 `items_per_second` 是 `items / cpu_time`，属 CPU 归一口径。
直接拿它和墙钟吞吐对比会差出数倍，必须先换算到同一口径（本文件用"每交付一条"）。

另外两个不在上表里的数字：**延迟 p50/p99** 是 source 造消息 → sink 处理（含 sink 攒批等待，
不是纯处理时延）；**观测开销** 是关掉观测的同一配置之差，回答"这些数字里有多少是测量本身"。

### 5. 报告落点与基准机

| 文件 | 内容 |
| --- | --- |
| `PERFORMANCE.md` / `PERFORMANCE.zh-CN.md` | 基准机（i7-14700KF，8 P + 12 E，28 逻辑核）：第一部分跨框架横评（raw / TBB ×2 / Taskflow / NexusFlow / ai-pipe），第二部分 NexusFlow 自身参数扫描 |

基准机固定为这台台式机；换机器就新开一份 `PERFORMANCE-<machine>.md`，跨机器**只比趋势**，不比绝对值。台式机数据的方向（细节见 PERFORMANCE 第二部分）：深度阶梯在模块线程数接近 P 核线程容量（16→18）时出现拐点，之后成本跳升；batch 甜点在 4–8，16 明显回撤；队列容量在个位数饱和，再大只买到尾部延迟。

---

### 6. 提 PR 前

- [ ] 每个维度有阶梯表，且含「比上一档多花」列
- [ ] 每档 3 次、写了噪声范围
- [ ] 每个数字带机器名 + commit
- [ ] 结论只有两句，且有一句是"归因"（§3 对照）
- [ ] 观测开销差值已写
- [ ] 构建 + `nexusflow_tests` 已跑，汇报里说明跑了没有
- [ ] 提交已收敛：PR 里的 commit 按「代码 / 基准文档 / 仓库文档」分组，`git log master..HEAD` 一眼能读完；逐次试跑的碎片提交在开 PR 前合并掉
- [ ] PR 由用户提交与合并：agent 只准备本地分支和本地提交；push、开 PR、合并都要用户明确指令（[`HARNESS.md`](../HARNESS.md) §1.1）
- [ ] PR 描述按 `.github/PULL_REQUEST_TEMPLATE.md` 填：有架构调整给「之前 / 之后 / 代价」表，有新增函数逐个写「做什么 / 为什么引入 / 边界」

### 7. 现状：还没做到的地方（改完一条删一条）

已完成（2026-11-18）：`--modules` 最小 2、`--branches` 最小 1；参数非法一律报错退出（不再静默 clamp 或静默回退）；CSV 的 `modules`/`branches` 改为记实际建出来的拓扑；`--topology linear --sync-inputs` 改为报错；每档输出 `median [min .. max]` 与波动百分比（`stat,` 行，超过 `--max-spread` 标 `UNSTABLE`）；读 `Pipeline::GetSnapshot()` 做覆盖率断言（`cov,` 行，跑动中有丢弃就让退出码非 0）；裸线程 + 裸 SpscQueue 基线已实现（`--baseline raw|raw-alloc|raw-message`，linear 已跑出三层拆分）；观测开销已量（微基准约 40 ns/消息，流水线 A/B 落在噪声内）。

- 还没有观测开销那一行（关掉计时/直方图重跑取差值）

### 结果

报告按机器分文件：

- 台式机（i7-14700KF，基准测试机）：[`PERFORMANCE.zh-CN.md`](PERFORMANCE.zh-CN.md) / [`PERFORMANCE.md`](PERFORMANCE.md)（第一部分横评、第二部分参数扫描）

下面两张表是 1.0.0 收尾时的最小基线，**用旧工具测的、不满足现行规则**（无区间、无覆盖率断言、无裸基线），只作历史对照，不许跨机器或跨 commit 比较。

#### 本机 Windows / MSVC 实测（2026-11-17，旧工具：diamond 2 分支、仅 avg 延迟）

| 提交 | 平台 | capacity | 吞吐 (msg/s) | 平均延迟 (µs) |
| --- | --- | --- | --- | --- |
| `1594ccb` | Windows / MSVC 19.40 / Release / x64 | 8 | **3,495,318** | **5.318** |
| `1594ccb` | 同上 | 64 | 3,295,229 | 25.220 |

**测量协议**：先跑一次热身并丢弃，再连跑 3 次取中位数；每次固定跑满 10 s。同机多批测量落在
**2.965M–3.814M msg/s**，波动约 ±15%，所以前后对比必须同批测量；`capacity=64` 的延迟由排队深度决定，
本批 3 次的延迟是 14.5–30.8 µs。
`capacity=1` 是病态档位（每边只能排 1 条消息，吞吐在 2k–15k msg/s 之间随调度抖动），不作为指标记录。

#### 资源占用（同一拓扑，capacity=8，跑满 10 s）

| 指标 | 值 |
| --- | --- |
| 拓扑 | 线性 3 模块（source → passthrough → sink），**模块内无计算** |
| CPU | 10.0101 s 墙钟内 **38.86 s** CPU ≈ **3.883 个核**（16 逻辑核机器） |
| 每消息 CPU | 38.86 s ÷ 35,435,513 条 ≈ **1.097 µs** |
| 每多一个模块的 CPU | ≈ **366 ns**（3 个模块） |
| 常驻内存 | 工作集 **5.1 MB**，10 s 内不增长 |
| 进程线程数 | **8**（其中 3 个是模块 worker） |

**改动说明**：本次收尾改了三处——每消息级日志 DEBUG→TRACE、默认日志级别 DEBUG→INFO、逐模块计时链改为按需开启
（`PipelineContext::EnableModuleTimings()`，默认关）。前两项不影响本基准（基准运行在 INFO，且日志不在热路径）；
第三项是本轮吞吐变化的主因：**开启时 capacity=8 中位约 1.33M msg/s，关闭后约 2.97M msg/s（约 2.2×）**。
（另一次 A/B 的关闭组测到 3.58M，本机同条件波动约 ±15%，比较时请同批测量。）

> 表里的 sha 是实测时的代码提交（`perf(module): make per-hop timings opt-in and default logs to INFO`）。重测时请新增一行，不要覆盖旧行。

### 历史结果（WSL2 / GCC 9.4，2025-07-19 及更早，仅作参考）

下面记录 `pipeline_benchmark` 的测试拓扑与历史数字（在 WSL2 上测得）。

为了评估 NexusFlow 框架的性能开销，项目内提供了一个基准测试脚本。该脚本用于测量在特定拓扑结构下，框架的消息**吞吐量**和端到端**平均延迟**。

### 测试拓扑

评测采用了一个经典的 **扇出/扇入 (Fan-out/Fan-in)** 拓扑结构，以测试框架的并行分发和数据汇总能力：



### 评测环境 (Benchmark Environment)

以下性能数据是在特定的硬件和软件环境下测得。在不同配置的机器上，您的结果可能会有所差异。

*   **CPU**: `Intel(R) Core(TM) i7-14700KF`
*   **核心数 (Cores)**: 28 (14 Cores x 2 Threads)
*   **L3 缓存 (L3 Cache)**: 33 MB
*   **内存 (Memory)**: 16 GB
*   **操作系统 (OS)**: Ubuntu 20.04.5 LTS WSL2
*   **内核版本 (Kernel)**: `6.6.87.2-microsoft-standard-WSL2`
*   **编译器 (Compiler)**: `g++ (Ubuntu 9.4.0-1ubuntu1~20.04.2) 9.4.0`

```bash 
# Release
--- Benchmark Results ---
Total Duration:   10.0005 s
Messages Processed: 1614764
-------------------------
Throughput:       161468 msg/s
Avg. Latency:     100.415 us
-------------------------

# Latest Release Result (2025.07.19)
--- Benchmark Results ---
Total Duration:   10.0004 s
Messages Processed: 2804190
-------------------------
Throughput:       280406 msg/s
Avg. Latency:     35.4208 us
-------------------------

```

### 说明

- **Total Duration**: 总测试时间。
- **Messages Processed**: 总处理消息数。
- **Throughput**: 吞吐量，即每秒处理的消息数。
- **Avg. Latency**: 平均延迟，即处理每条消息的平均时间。

### 注意

基准测试脚本仅用于评估 NexusFlow 框架的性能，实际使用时需要根据具体应用场景和需求进行调整。
