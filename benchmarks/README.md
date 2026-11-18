## 性能评测 (Performance Benchmark)

### 目录内容

`benchmarks/` 里放两个基准目标，**源码都在这一层**（不再有子目录）：

| 文件 | 目标 | 说明 |
| --- | --- | --- |
| `BenchmarkMessage.cpp` + `benchmark_main.cpp` | `nexusflow_benchmarks` | Google Benchmark 微基准：消息创建/广播（拷贝）/数据访问两种实现对比 |
| `pipeline_benchmark.cpp` | `pipeline_benchmark` | 端到端吞吐与延迟扫描，自己带 `main()`；结果见 [`PERFORMANCE.md`](PERFORMANCE.md) |
| `LatencyHistogram.hpp` / `ProcessCpuTime.hpp` / `ProcessMemory.hpp` | — | 对数桶直方图（P50/P99）、CPU 时间与常驻内存（RSS）采集 |
| `BenchmarkNodes.hpp` / `BenchmarkTopology.hpp` | — | 基准用的模块（source/sink/负载节点）与拓扑构造器（线性 / 扇出扇入） |

### 构建与运行

```bash
# 微基准
cmake --build build --config Release --target nexusflow_benchmarks --parallel 16
./build/benchmarks/Release/nexusflow_benchmarks.exe

# 端到端扫描（默认 diamond 2 分支）
cmake --build build --config Release --target pipeline_benchmark --parallel 16
./build/benchmarks/Release/pipeline_benchmark.exe --duration 10 --repeat 3
./build/benchmarks/Release/pipeline_benchmark.exe --topology linear --modules 3 --capacity 8
./build/benchmarks/Release/pipeline_benchmark.exe --topology diamond --branches 2 --sync-inputs
./build/benchmarks/Release/pipeline_benchmark.exe --payload 1080p-rgb                 # 复用一块帧缓冲
./build/benchmarks/Release/pipeline_benchmark.exe --payload 1080p-rgb --payload-fresh # 每帧新建
./build/benchmarks/Release/pipeline_benchmark.exe --topology linear --modules 3 \
    --workload compute --compute-iters 2000

# 外部注入、观测开关与融合掉队分支
./build/benchmarks/Release/pipeline_benchmark.exe --topology inject --modules 3 --capacity 8
./build/benchmarks/Release/pipeline_benchmark.exe --topology inject --modules 3 --capacity 1 --push-mode try
./build/benchmarks/Release/pipeline_benchmark.exe --topology linear --modules 3 --capacity 8 --timings
./build/benchmarks/Release/pipeline_benchmark.exe --topology linear --modules 3 --capacity 8 --log-level trace
./build/benchmarks/Release/pipeline_benchmark.exe --topology diamond --branches 2 --sync-inputs --straggler-us 2000
```

### 选项

| 选项 | 默认 | 含义 |
| --- | --- | --- |
| `--topology diamond\|linear\|inject` | `diamond` | `diamond` = source → N 分支 → sink；`linear` = source → 若干 stage → sink；`inject` = passive source → stage → sink，主线程 `Push` 喂 |
| `--modules N` | `3` | 仅 linear/inject：总段数（含 source/sink），最小 2 |
| `--branches N` | `2` | 仅 diamond：并行分支数 |
| `--capacity N` | `4` | 每条边的队列深度；`4` 是吞吐平台的下沿 |
| `--payload none\|1080p-rgb\|1080p-yuv420\|2k-rgb\|2k-yuv420` | `none` | 每条消息携带的帧大小 |
| `--payload-fresh` | off | 每条消息新建 frame，而不是复用同一块缓冲 |
| `--workload none\|delay\|compute\|memory` | `none` | 每个中间 stage 每条消息额外做的负载 |
| `--delay-us N` | `100` | workload=delay：每条消息 `sleep_for(N µs)` |
| `--compute-iters N` | `5000` | workload=compute：每条消息做 N 次 `sin`/`cos` |
| `--memory-bytes N` | `65536` | workload=memory：每条消息 memcpy N 字节 |
| `--sync-inputs` | off | diamond 的 sink 走融合路径（`RunFusion`） |
| `--straggler-us N` | `0` | 仅 diamond：branch_0 改成 `sleep N µs`，其余分支保持 `--workload`，把融合组拖成半凑齐 |
| `--push-mode block\|try` | `block` | 仅 inject：`block` = 等空位，`try` = `TryPush` 记一次拒绝后重试 |
| `--timings` | off | 打开 `EnableModuleTimings()`：每跳深拷贝一份计时 map |
| `--log-level trace\|debug\|info\|warn\|err\|off` | `warn` | 全局日志级别；TRACE 在热路径逐条打 |
| `--duration S` | `10` | 每次运行秒数 |
| `--repeat R` | `3` | 预热 1 次丢弃后再测 R 次，取中位数 |
| `--cpus N` | `0` | 把进程绑到前 N 个逻辑核；`0` = 不绑 |
| `--label TEXT` | `local` | 写进每条 CSV 的标签 |

非法取值、未知选项会以退出码 2 报错；`--help` 打印全部默认值。

### 外部注入（`--topology inject`）

`inject` 用被动 source（`Module::IsPassiveSource()`）当边界：主线程就是生产者，
`Push`/`TryPush` 直接写 source 的输出边，下游 stage/sink 照常调度。它量的是
"外部解码器/网卡把数据塞进图"的路径，而不是框架自己造消息。

- `--push-mode block`：边满时 `Push` 阻塞，生产速率被消费速率约束，吞吐是整条流水线的。
- `--push-mode try`：`TryPush` 立即返回 `kQueueFull`，harness 记一次 `refused` 后 `yield` 重试；
  `attempts` 是投递次数，`refused` 是等空位的总次数，所以 `refused/attempts` 是
  "平均每次投递被拒几次"，可以大于 1。
- 单生产者语义（边是 SPSC）；多个线程同时 `Push` 同一个 source 不在本版保证内。

### 输出

每次运行打印一行

`csv,label,platform,topology,modules,branches,capacity,payload,workload,work_param,fresh,sync,cpus,duration,rep,delivered,throughput,cpu_s,cores,avg,p50,p95,p99,min,max,timings,log_level,straggler_us,push_mode,rssStartMiB,rssPeakMiB,rssEndMiB,stop_ms,edge_depth_max,fusion_expired,attempts,refused`

批量跑时 `grep '^csv,'` 收集成表。`work_param` 是该负载的参数（delay 的 µs、compute 的迭代数、memory 的字节数）；`min`/`max` 是单次样本的极值（`max` 常是调度离群点，别当稳态读）。

后 12 列（本版新增）：

| 列 | 含义 |
| --- | --- |
| `timings` | 0/1，是否打开逐跳计时链 |
| `log_level` | 本次运行的全局日志级别 |
| `straggler_us` | diamond 的掉队分支睡眠微秒数，0 = 没有掉队分支 |
| `push_mode` | inject 的投递模式（其它拓扑恒为 `block`） |
| `rssStartMiB` / `rssPeakMiB` / `rssEndMiB` | 进程常驻内存：`Start()` 前 / 运行中每 25 ms 采样峰值 / `Stop()` 后 |
| `stop_ms` | `Stop()` 的墙钟（drain + join） |
| `edge_depth_max` | 所有边 `maxSize` 的最大值，这次跑到的最大积压 |
| `fusion_expired` | 因融合超时被丢弃的组数（各模块 `fusionExpiredTotal` 之和） |
| `attempts` / `refused` | 仅 inject：投递次数 / 因队满重试次数 |

`rssStart` 含上一轮释放后 allocator 未归还的页，同一进程里越往后越高；
比较内存时看 `rssPeak - rssStart`，不要跨轮比绝对值。

`--payload` 档位：`none` / `1080p-rgb`（5.93 MiB）/ `1080p-yuv420`（2.97 MiB）/ `2k-rgb`（QHD 2560×1440，10.547 MiB）/ `2k-yuv420`（5.27 MiB）。
`--capacity` 与帧大小决定每条边常驻内存：默认下 1080p RGB 的 linear 3 段约 47 MiB、diamond 2 分支约 95 MiB；扫容量时显式传 `--capacity`。
`--cpus N` 把进程绑到前 N 个逻辑核（`0` = 不绑，默认），用于压掉跨进程调度抖动；报告里的矩阵**未绑核**，绑定会改变数字，只作对照。

### 负载节点（`--workload`）

三个负载都作用在**每个中间 stage**（linear 的 `modules-2` 个、diamond 的每个分支），source/sink 仍是空的框架节点，因此：

- `delay` 测"模块在等 IO/推理结果"时框架的表现；
- `compute` 测"模块在烧 CPU"时的表现；
- `memory` 测"模块在拷内存"时的表现。

`--workload none` 走原来的 `PassThroughModule`，所以无负载数字和旧基线可比。

> Windows 上 `std::this_thread::sleep_for` 的粒度约 1–2 ms（MSVC 会把小于 1 ms 的请求
> 舍入到下一次可调度点），因此 `--delay-us` 在 2 ms 以下实际是"约 2 ms"的地板；
> 做 delay 维度时请用 `--delay-us 2000` 及以上。POSIX 上没有这个地板。
`--straggler-us` 走同一个 `sleep_for`，Windows 上小于 2 ms 同样会被抬到约 2 ms。

### 测量协议

每个配置预热 1 次丢弃，再跑 `--repeat` 次取**中位数**；整批串行执行、不绑核。
同机不同时间批次的同一配置可差 **3–5%**（本机 28 逻辑核上实测），所以**表内差异在
10% 以内不解读**，跨表比较只对同批数据成立。

`--timings` 与 `--log-level` 默认都关，打开后 CSV 的 `timings`/`log_level` 列写实值；
比较时只改一个开关，其它参数保持一致。`--log-level trace` 每消息数十行、几 MB/s 起，
只在低速率配置下可测——报告里用 `delay` 负载把速率压到千级 msg/s 再比。
`rssPeak` 是 25 ms 采样，短于 25 ms 的尖峰可能漏掉；它读整个进程，包含 allocator
保留页与 sink 的 `messageCache`，因此比 `capacity × 帧 × 边` 的算术估算更接近真实。
