## 性能评测 (Performance Benchmark)

### 目录内容

`benchmarks/` 里放两个基准目标，**源码都在这一层**（不再有子目录）：

| 文件 | 目标 | 说明 |
| --- | --- | --- |
| `BenchmarkMessage.cpp` + `benchmark_main.cpp` | `nexusflow_benchmarks` | Google Benchmark 微基准：消息创建/广播（拷贝）/数据访问两种实现对比 |
| `pipeline_benchmark.cpp` | `pipeline_benchmark` | 端到端吞吐与平均延迟，带 `--capacity N`（边队列深度），自己带 `main()` |

```powershell
# 微基准
cmake --build build --config Release --target nexusflow_benchmarks --parallel 16
.\build\benchmarks\Release\nexusflow_benchmarks.exe

# 端到端吞吐（时长固定 10 s，`--benchmark_min_time` 是无效参数）
cmake --build build --config Release --target pipeline_benchmark --parallel 16
.\build\benchmarks\Release\pipeline_benchmark.exe --capacity 8
```

### 结果

#### 本机 Windows / MSVC 实测（2026-11-17）

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
| 拓扑 | 3 段线性（source → passthrough → sink），**模块内无计算** |
| CPU | 10.0101 s 墙钟内 **38.86 s** CPU ≈ **3.883 个核**（16 逻辑核机器） |
| 每消息 CPU | 38.86 s ÷ 35,435,513 条 ≈ **1.097 µs** |
| 每跳 CPU | ≈ **366 ns**（3 跳） |
| 常驻内存 | 工作集 **5.1 MB**，10 s 内不增长 |
| 进程线程数 | **8**（其中 3 个是模块 worker） |

**改动说明**：本次收尾改了三处——每消息级日志 DEBUG→TRACE、默认日志级别 DEBUG→INFO、逐跳 timing 链改为按需开启
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
