## 性能评测 (Performance Benchmark)

### 目录内容

`benchmarks/` 里放两个基准目标，**源码都在这一层**（不再有子目录）：

> **测量规则**（阶梯、拐点、归因、观测开销、两机分报告）见 [`HARNESS.md`](HARNESS.md)。本文件只讲怎么跑。

| 文件 | 目标 | 说明 |
| --- | --- | --- |
| `BenchmarkMessage.cpp` + `benchmark_main.cpp` | `nexusflow_benchmarks` | Google Benchmark 微基准：消息创建/广播（拷贝）/数据访问两种实现对比 |
| `pipeline_benchmark.cpp` | `pipeline_benchmark` | 端到端吞吐与延迟扫描：`--topology diamond|linear|inject`、`--baseline`、`--batch`、`--observe`、`--modules`、`--branches`、`--capacity`、`--payload`、`--payload-fresh`、`--sync-inputs`、`--workload`、`--cpus`、`--push-mode`、`--straggler-us`、`--pool-size`、`--timings`、`--duration`、`--repeat`；自己带 `main()`。结果见 [`PERFORMANCE.md`](PERFORMANCE.md) |
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
`csv,<label>,<platform>,<baseline>,<batch>,<observe>,<topology>,<modules>,<branches>,<capacity>,<payload>,<fresh>,<sync>,<duration>,<rep>,<delivered>,<throughput>,<cpu_s>,<cores>,<avg>,<p50>,<p95>,<p99>,<min>,<max>,<cpus>,<timings>,<workload>,<work_param>,<push_mode>,<attempts>,<refused>,<rss_start>,<rss_peak>,<rss_end>,<edge_depth_max>,<stop_ms>`，
批量跑时 `grep '^csv,'` 收集成表。`min`/`max` 是单次样本的极值（`max` 通常是调度离群点，别当稳态读）。
`modules` / `branches` 是**实际建出来的拓扑**，不是命令行回显：`diamond --branches 2` 记 `modules=4, branches=2`，`linear --modules 2` 记 `modules=2, branches=0`。
后 12 列是本分支扩展：`cpus`（绑核数，0=不绑）、`timings`（逐跳计时 0/1）、`workload`/`work_param`（负载类型与参数）、`push_mode`（`block`/`try`）、`attempts`/`refused`（注入尝试/队列满拒绝次数）、`rss_start`/`rss_peak`/`rss_end`（MiB）、`edge_depth_max`（最深积压）、`stop_ms`（Stop 排水+join 耗时）、`pool`（池化线程数，0 = 每模块一线程）。

`--baseline raw|raw-alloc|raw-message` 跑**裸线程 + 裸 SpscQueue** 的同构基线（[`RawPipeline.hpp`](RawPipeline.hpp)），用来把"框架成本"和"机器成本"分开：同拓扑、同容量、同阻塞语义，一个 stage 一个线程。三档口径，用于三层拆分：

- `raw`：消息是普通结构体，**不分配**——任何框架成本的下界；
- `raw-alloc`：每条消息 `make_shared` 一个 payload 包装（同框架 `MakeMessage()` 的分配量）；
- `raw-message`：消息直接用框架的 `nexusflow::Message`（`MakeMessage` 建、`BorrowPtr` 读时间戳），**队列与消息层和框架完全同构**。

于是：`raw→raw-alloc` = 每消息分配成本；`raw-alloc→raw-message` = Message 自身（元数据/类型擦除/COW）；`raw-message→框架` = Module/Worker/Dispatcher 调度成本。
三者都不分批，所以和它们对比时框架侧用 `--batch 1`。

`--batch N` 设每个非 source 模块的 `maxBatchSize`（默认 4，保持历史口径）；深度阶梯的对照口径用 `--batch 1`。

`--observe none` 关掉 harness 自己的观测（source 的时钟读、sink 的时钟读 + 直方图采样），用来量观测开销；`--micro-observer N` 单独跑 N 次"两次时钟读 + 一次直方图采样"并退出，直接给 ns/消息。

`--topology inject` 把线性链的 source 换成 passive source（`IsPassiveSource()`），主线程用 `Pipeline::Push`（`--push-mode block`）或 `TryPush`（`--push-mode try`）从外部喂帧——就是解码器/网络收包的形状。`attempts` 是送入尝试次数，`refused` 是 `TryPush` 遇到边队列满的次数；Stop 前 harness 会校验 pushed/popped 与 sink 计数（见 `cov,` 行）。inject 必须配 `--baseline framework`。

`--pool-size N` 走 v1.1 的线程池模型（`PipelineContext::WithThreadPoolSize`）：非融合模块固定绑定到 N 个共享线程，融合模块仍独占线程。池模式用精确配额背压（`OutputRoom()`），不丢帧，`cov` 应为 `ok`；代价是浅队列深链上吞吐下降，实测见 [`PERFORMANCE-desktop.md`](PERFORMANCE-desktop.md) §14。

`--workload none|delay|compute|memory` 给每个中间 stage 加负载：`--delay-us`（等待；Windows 上用高精度 waitable timer，因为系统的 `Sleep` 在进程跑几秒后会被合并到 15.6 ms tick，`sleep_for` 不再可信；这个维度用 ≥2 ms 档）、`--compute-iters`（sin/cos 串行链）、`--memory-bytes`（每消息两次半区 memcpy）。`work_param` 记录实际参数。

`--cpus N` 用 `SetProcessAffinityMask`（Windows）/ `sched_setaffinity`（Linux）把进程绑到前 N 个逻辑核，0 表示不绑；`affinity:` 横幅会打印实际绑定。HARNESS 的阶梯矩阵默认不绑核。

`--timings` 打开框架的逐跳计时链（`PipelineContext::EnableModuleTimings()`），默认关闭；打开后吞吐会明显下降，只用于确认瓶颈位置，不要和关闭档混在同一张表里。`--log-level trace|debug|info|warn|err|off` 控制日志，基准默认 `warn`。

除了每次运行一行的 `csv,`，还有两种行：

- 每个配置一行 **`stat,`**（阶梯表就是用它拼的）：
  `stat,<label>,<platform>,<baseline>,<batch>,<observe>,<topology>,<modules>,<branches>,<edges>,<capacity>,<payload>,<fresh>,<sync>,<reps>,<tp_med>,<tp_min>,<tp_max>,<tp_spread_pct>,<cores_med>,<cores_min>,<cores_max>,<p50_med>,<p95_med>,<p99_med>,<cpu_per_msg_med>,<unstable>`。
  中位数 + 三次重复的范围 + 波动百分比；`unstable=1` 表示吞吐波动超过 `--max-spread`（默认 10%），这一档别直接读，先重跑。
- 每次运行一行 **`cov,`**（覆盖率断言）：
  `cov,<label>,<baseline>,<modules>,<branches>,<capacity>,<rep>,<pushed>,<popped>,<discarded>,<residual>,<sink_popped>,<delivered>,<processed>,<dropped>,<failed>,<fusion_expired>,<dropped_mid_run>,<fusion_expired_mid_run>,<stop_ms>,<ok>`。
  判据：**跑动中** `dropped`/`failed`/`fusion_expired` 必须为 0；`pushed == popped + discarded + residual`；sink 收到的条数与 `delivered` 对得上（融合路径按 `branches` 折算）。
  不满足就打印 `COVERAGE MISMATCH` 并让退出码为 1。停机尾部为关队列而丢掉的那几条只报不判错（`dropped`/`failure` 列照实给）。

`--payload` 档位：`none` / `1080p-rgb`（5.937 MiB）/ `1080p-yuv420`（2.969 MiB）/ `2k-rgb`（QHD 2560×1440，10.547 MiB）/ `2k-yuv420`（5.273 MiB）。
`--capacity` 在载荷模式下默认 4，因为 `capacity × payload` 就是每条边占的内存（1080p RGB × 64 ≈ 380 MiB/边）。

**参数下限与报错**（不再静默纠正，猜错就退出 1）：
`--topology linear` 的 `--modules` 最小 **2**（source→sink，1 条边，是深度阶梯的对照档）；`--topology diamond` 的 `--branches` 最小 **1**；
`--sync-inputs` 只能配 diamond；`--topology inject` 要求 `--baseline framework`、`--modules >= 2`，且不接受 `--sync-inputs`；`--baseline raw*` 不接受 `--workload`；数字参数不是整数、`--memory-bytes` 小于 1、`--payload` 未知档位、`--label` 含逗号，一律报错退出。

### 结果

报告按机器分文件，索引见 [`PERFORMANCE.md`](PERFORMANCE.md)：

- 笔记本（i7-11800H，8C16T）：[`PERFORMANCE-laptop.md`](PERFORMANCE-laptop.md)
- 台式机（i7-14700KF）：`PERFORMANCE-desktop.md`（待测）

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
