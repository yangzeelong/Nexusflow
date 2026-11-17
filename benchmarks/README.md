## 性能评测 (Performance Benchmark)

### 目录内容

`benchmarks/` 里放两个基准目标，**源码都在这一层**（不再有子目录）：

| 文件 | 目标 | 说明 |
| --- | --- | --- |
| `BenchmarkMessage.cpp` + `benchmark_main.cpp` | `nexusflow_benchmarks` | Google Benchmark 微基准：消息创建/广播（拷贝）/数据访问两种实现对比 |
| `pipeline_benchmark.cpp` | `pipeline_benchmark` | 端到端吞吐与延迟扫描，自己带 `main()`；结果见 [`PERFORMANCE.md`](PERFORMANCE.md) |
| `LatencyHistogram.hpp` / `ProcessCpuTime.hpp` | — | 对数桶直方图（P50/P99）与 CPU 时间采集 |
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
```

### 选项

| 选项 | 默认 | 含义 |
| --- | --- | --- |
| `--topology diamond\|linear` | `diamond` | `diamond` = source → N 分支 → sink；`linear` = source → 若干 stage → sink |
| `--modules N` | `3` | 仅 linear：总段数（含 source/sink），最小 2 |
| `--branches N` | `2` | 仅 diamond：并行分支数 |
| `--capacity N` | `4` | 每条边的队列深度；`4` 是吞吐平台的下沿 |
| `--payload none\|1080p-rgb\|1080p-yuv420\|2k-rgb\|2k-yuv420` | `none` | 每条消息携带的帧大小 |
| `--payload-fresh` | off | 每条消息新建 frame，而不是复用同一块缓冲 |
| `--workload none\|delay\|compute\|memory` | `none` | 每个中间 stage 每条消息额外做的负载 |
| `--delay-us N` | `100` | workload=delay：每条消息 `sleep_for(N µs)` |
| `--compute-iters N` | `5000` | workload=compute：每条消息做 N 次 `sin`/`cos` |
| `--memory-bytes N` | `65536` | workload=memory：每条消息 memcpy N 字节 |
| `--sync-inputs` | off | diamond 的 sink 走融合路径（`RunFusion`） |
| `--duration S` | `10` | 每次运行秒数 |
| `--repeat R` | `3` | 预热 1 次丢弃后再测 R 次，取中位数 |
| `--cpus N` | `0` | 把进程绑到前 N 个逻辑核；`0` = 不绑 |
| `--label TEXT` | `local` | 写进每条 CSV 的标签 |

非法取值、未知选项会以退出码 2 报错；`--help` 打印全部默认值。

### 输出

每次运行打印一行

`csv,label,platform,topology,modules,branches,capacity,payload,workload,work_param,fresh,sync,cpus,duration,rep,delivered,throughput,cpu_s,cores,avg,p50,p95,p99,min,max`

批量跑时 `grep '^csv,'` 收集成表。`work_param` 是该负载的参数（delay 的 µs、compute 的迭代数、memory 的字节数）；`min`/`max` 是单次样本的极值（`max` 常是调度离群点，别当稳态读）。

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

### 测量协议

每个配置预热 1 次丢弃，再跑 `--repeat` 次取**中位数**；整批串行执行、不绑核。
同机不同时间批次的同一配置可差 **3–5%**（本机 28 逻辑核上实测），所以**表内差异在
10% 以内不解读**，跨表比较只对同批数据成立。
