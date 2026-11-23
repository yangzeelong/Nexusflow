# NexusFlow 运行期指标速查（telemetry.md）

本文件与实现同源，随代码一起入库（见 [`../README.md`](../README.md)），
所以可以随便改。它记录的是**现在已经落地的代码**，不是设计草案；设计讨论见
[`telemetry-design.md`](telemetry-design.md)。

---

## 0. 三套命名空间

| 命名空间 | 谁写 | 谁读 | 回答什么问题 |
| --- | --- | --- | --- |
| 边（edge） | 队列自己（`SpscQueue::push/pop` 顺手更新 head/tail） | `Pipeline::GetSnapshot()` | 这条边现在积压多少？累计过了多少条？ |
| 模块（module） | 该模块的 worker 线程 | `Pipeline::GetSnapshot()` | 这个模块处理了多少、丢了多少、抛了多少？ |
| 指标（metric） | 模块自己（`PipelineContext::Record`） | `PipelineContext::ForEachMetric()` 等 | 这个模块自报的耗时/计数（例如推理 p95）分布如何？ |

三者的连接点只有名字：边的 `from`/`to` 是模块名，模块自报的指标名必须以
`<模块名>.` 开头。这样日志里一行 `sink processed=...`、一行 `detect.infer p95Ms=...`
和一行 `source -> sink max=...` 能对得上同一个模块。

---

## 1. 后缀规则（写新字段前先读这一节）

| 后缀/形式 | 语义 | 例子 | Prometheus 侧 |
| --- | --- | --- | --- |
| `*Total` | 累计计数，只增不减；**两次快照相减**才有意义 | `pushedTotal`、`processedTotal`、`droppedTotal` | counter → `_total` |
| `*Sum` | 值的累加和（不是"发生了几次"） | `sumMs` | counter / `_sum` |
| 无后缀的名词 | 快照里的裸名就是"此刻"（gauge） | `size`、`capacity` | gauge |
| `max*` | 自上次 reset 以来的极值（water mark） | `maxSize` | gauge |
| `*Ms` | 单样本/分位数的服务时间（毫秒），不是累计 | `minMs`、`maxMs`、`p95Ms` | histogram 派生 |

三条硬规则：

1. **累计量必须带后缀**，其余一律不加（不发明 `*Current`、`*Peak`、
   `highWaterMark`）。
2. **不再用 `Stats` / `Metrics` 做结构体名**：快照族 `*Snapshot`、队列计数
   `QueueCounters`、采样统计 `MetricSamples`；只有写入端保留 `Metrics*`
   （`MetricsCollector`）。
3. **模块自报指标名必须带模块名前缀**（`"detect.frames"`），context 是整条流水线
   共享的，裸名会撞。

---

## 2. 快照（`include/nexusflow/Stats.hpp`）

取法：`Pipeline::GetSnapshot()`（旧名 `GetMetrics()` 仍可用，但已
`[[deprecated]]`）。它是**纯值**，每秒拿一次不心疼。

```cpp
nexusflow::PipelineSnapshot snap = pipeline->GetSnapshot();
std::cout << snap.ToString() << std::endl;
pipeline->ResetMetrics();   // 只清 maxSize，*Total 不回退
```

### `EdgeSnapshot`（每条边一份）

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `from` | `std::string` | 上游模块名 |
| `to` | `std::string` | 下游模块名 |
| `capacity` | `std::size_t` | 队列容量，`0` 表示无界 |
| `size` | `std::size_t` | 此刻积压的消息数 |
| `maxSize` | `std::size_t` | 自上次 `ResetMetrics()` 以来最深的积压 |
| `pushedTotal` | `std::uint64_t` | 累计 push 成功次数 |
| `poppedTotal` | `std::uint64_t` | 累计 pop 成功次数 |

`Name()` 返回 `"from -> to"`，只给展示用；**反查请用 `from`/`to` 字段**。

### `ModuleSnapshot`（每个模块一份）

| 字段 | 含义 |
| --- | --- |
| `name` | 模块名 |
| `processedTotal` | 处理完成的消息数（合成批按合成后的条数算） |
| `droppedTotal` | 下游推不进去（队列满/已关闭）而丢掉的消息 + 融合超时丢掉的 |
| `fusionExpiredTotal` | `droppedTotal` 的子集：融合分组超时 |
| `failedTotal` | `Process` 抛异常的次数（按调用次数算，不按消息数） |

恒等式（`PipelineMetricsTest.EdgeCountersReconcileWithBothEnds` 守着）：

```text
source.processedTotal + source.droppedTotal == edge.pushedTotal
edge.pushedTotal == edge.poppedTotal + edge.size
edge.poppedTotal >= sink.processedTotal + sink.failedTotal        // 见下
```

最后一条是**不等式而不是等式**，这是故意的：消费者从边里成批取走消息（`poppedTotal`
先 +N），再一条条处理并记账。`Stop()` 只等排空上游、不打断已经进批的消费者，
所以一次运行**允许**在 "已取走但还没记账" 这个缝隙里结束。等式形式
（`+ edge.size`）只在消费者真的空闲时才成立，测试里会偶发失败。

### `PipelineSnapshot`

`name`（图名）+ `edges` + `modules` + `ToString()`（一行，给日志用，**格式不稳定，别解析它**）。

### 读出来的数可能"不真实"

`GetSnapshot()` 先读模块、后读它写出去的那条边。所以能得到的最坏情况是
"pushed 已经 +1、popped 还没跟上"——一个**从未同时存在**的组合。反过来读就会
看到凭空出现的消息。要精确对账，先 `Stop()` 再取。

`ResetMetrics()` **只清 water mark**（`maxSize`）；`*Total` 在流水线生命周期内
永不回退，`Start()` 重新启动也不清零。

---

## 3. 采样（`include/nexusflow/MetricsCollector.hpp`）

写入端是 `profiling::MetricsCollector`，通过 `PipelineContext::Profiler()` 拿到。

```cpp
// 热路径：名字 Intern 一次，之后按句柄计时，不再哈希字符串
const auto infer = GetContext()->Intern(GetModuleName() + ".infer");
{
    auto scope = GetContext()->Profiler().Scope(infer, profiling::TimerPrintPolicy::EveryMs(5000));
    DoInference();
}

// 或者直接用宏（宏帮你把 __FILE__/__LINE__ 带上）
TIMER_START_AVERAGE_MS("detect.infer", 5000);
DoInference();
TIMER_STOP_AVERAGE("detect.infer");
```

`MetricSamples` 字段：

| 字段 | 含义 |
| --- | --- |
| `name` | 指标名 |
| `items` | 已计数的样本数（一次调用声明 N 个 item 就 +N） |
| `sumMs` | 所有样本耗时之和（`sum`，不是 `total`：它是值的累加） |
| `minMs` / `maxMs` | 最慢/最快的一次，**单条样本**的耗时 |
| `p95Ms` | 最近窗口内（1024 个样本）的 p95，不是全程 |
| `file` / `line` | 第一次测量的调用点 |

`AvgItemMs()` = `sumMs / items`；`Where()` = `"file.cpp:123"`。

### 没有 QPS 字段，自己算

`MetricSamples` 故意不带吞吐：QPS 需要一段墙钟时间跨度，采样统计里没有。

```cpp
nexusflow::profiling::MetricSamples a, b;
GetContext()->Profiler().GetStats("detect.infer", a);
std::this_thread::sleep_for(std::chrono::seconds(1));
GetContext()->Profiler().GetStats("detect.infer", b);
const double itemsPerSecond = static_cast<double>(b.items - a.items);   // Δt = 1 s
```

边缘同理：`(snap2.edges[i].pushedTotal - snap1.edges[i].pushedTotal) / Δt`。
若只想在日志里看当前窗口的吞吐，用 `TimerPrintPolicy::Interval(n)`
（`TIMER_START_INTERVAL`）——那个策略自己知道窗口。

---

## 4. 模块自报指标（`PipelineContext`）

```cpp
const auto frames = GetContext()->Intern(GetModuleName() + ".frames");  // 初始化里一次
GetContext()->Record(frames, 1.0);                                      // 热路径：一次 vector 下标

// 读
GetContext()->GetReportedMetricCount();                 // 只要个数：一次锁
GetContext()->ForEachMetric([](const PipelineContext::Metric& m) { ... });   // 一次锁遍历，不拷贝
const PipelineContext::Metric* m = GetContext()->FindMetric(frames);         // 指针只到下次 Intern() 前有效
std::vector<PipelineContext::Metric> all = GetContext()->GetReportedMetrics(); // 拷贝一份（方便但最慢）
```

`Metric` 现在是 `{name, samples, total, last, Average()}`；`total` 是值的累加和，
因此按上面的后缀规则它将来若要出现在快照里应叫 `sum*`。

实测（`build/t_bench_metrics.cpp`，1000 帧 × 3 指标，/O2，MSVC）：

| 形状 | 每样本 |
| --- | --- |
| 旧 `Report`（`std::map` + mutex） | 19.4–34.6 ns |
| 新 `Record(handle)`（vector 下标 + 同一把锁） | 11.1–18.5 ns（约 0.54–0.58×） |
| 读 `GetReportedMetrics()`（全量拷贝） | 39.5–73.2 ns/次 |
| 读 `GetReportedMetricCount()` | 10.6–17.6 ns/次 |

绝对值随机器噪声摆动，比值稳定在约 0.55×：句柄化省掉的是每次写入的字符串哈希
与红黑树查找，锁还是同一把。

---

## 5. 并发模型（为什么热路径上 `++` 不加锁）

| 计数器 | 写者 | 读者 |
| --- | --- | --- |
| `Worker::m_processedCount` / `m_droppedCount` / `m_fusionExpiredCount` / `m_failedCount` | 该模块的 worker 线程 | `ModuleActor::*Count()` → `GetSnapshot()` |
| `Dispatcher::m_droppedCount` | 该模块的 worker 线程（`Broadcast`/`SendTo` 都在这里跑） | `Dispatcher::DroppedCount()` |
| `SpscQueue` 的 `pushed`/`popped` | 生产者的 `m_tail`、消费者的 `m_head` | `GetStats()` |
| `SpscQueue::m_highWaterMark` | 生产者 | `GetStats()` |

- 只有一个写者 ⇒ 普通 `uint64_t` + `++`。实测（`build/t_counter_bench.exe`，
  200,000,000 次）：`atomic fetch_add(relaxed)` 3.34–3.38 ns/次，普通 `++`（带
  volatile 观察点）0.18–0.33 ns/次 —— 差一个数量级。注意 `/O2` 会把"没人读的普通
  计数循环"整个删掉（`atomic_signal_fence` 拦不住，必须写进 `volatile` 变量），
  所以裸基准只有在有观察点时才可信。
- 写者每个循环迭代结束 `PublishCounters()`（release fence）发布一次；读者先
  `ObserveCounters()`（acquire fence）再读普通变量。
- `Worker::RecordDropped()` 这类"对外可写"的入口会被第二个线程调用时，
  自动切到 `m_writerMutex` + 原子加的慢路径：写者数量是**检测**出来的，不是约定。
- 读到的组合可能从未同时成立（见 §2），所以顺序固定为模块 → 边。

---

## 6. 字段 → Prometheus 映射（**未实现**，仅备查）

导出层（`MetricsExporter` / `ToLine` / `ToJson` / `ToPrometheus`）按用户决定**不做**。
若将来要接，映射应该集中在一处，规则是 `*Total → _total`、`*Sum → _sum`、
`*Ms → _seconds`、裸 gauge 不加后缀：

| 结构体字段 | Prometheus 名 | `# TYPE` |
| --- | --- | --- |
| `EdgeSnapshot::pushedTotal` | `nxf_edge_pushed_total{from,to}` | counter |
| `EdgeSnapshot::poppedTotal` | `nxf_edge_popped_total{from,to}` | counter |
| `EdgeSnapshot::size` | `nxf_edge_size{from,to}` | gauge |
| `EdgeSnapshot::maxSize` | `nxf_edge_max_size{from,to}` | gauge |
| `ModuleSnapshot::processedTotal` | `nxf_module_processed_total{module}` | counter |
| `ModuleSnapshot::droppedTotal` | `nxf_module_dropped_total{module}` | counter |
| `MetricSamples::sumMs` | `nxf_metric_sum_seconds{name}` | counter |
| `MetricSamples::p95Ms` | `nxf_metric_p95_seconds{name}` | gauge |

---

## 7. 归属速查

| 想问的问题 | 找谁 |
| --- | --- |
| 这个模块处理了多少条？ | `ModuleSnapshot::processedTotal` |
| 累计丢了多少条？多少次抛异常？ | `ModuleSnapshot::droppedTotal` / `failedTotal` |
| 这条边现在积压多少？最深积压过多少？ | `EdgeSnapshot::size` / `maxSize` |
| 现在每秒多少条？（瞬时） | 两次快照相减除以 Δt，或用 `TimerPrintPolicy::Interval` 打印的窗口值 |
| 这帧推理花了多久？p95 多少？ | `MetricSamples::AvgItemMs()` / `p95Ms` |
| 模块自己报的指标？ | `PipelineContext::FindMetric()` / `ForEachMetric()` |
| 为什么 `Stop()` 之后还有残余？ | 先 `Stop()` 再 `GetSnapshot()`，看 `edge.size` 与 `droppedTotal` |

---

## 8. 改了指标之后要跑什么

```pwsh
cmake --build build --config Release --parallel 16
Start-Process .\build\tests\Release\nexusflow_tests.exe -PassThru -WindowStyle Hidden `
  -RedirectStandardOutput build\snap.log -RedirectStandardError build\snap.err
```

- 动了计数器/快照：必须跑 `PipelineMetricsTest.*`（里面有跨层对账的
  `EdgeCountersReconcileWithBothEnds`）与 `PipelineContextTest.*`。
- 动了热路径（`Worker.cpp` 的迭代体、`SpscQueue`、`Dispatcher`）：再跑
  `build\perf-probe\build\Release\pipeline_benchmark.exe --benchmark_min_time=3s`
  对比吞吐/延迟（当前基线 ~1.837–1.859M msg/s、9.18–9.94 µs）。
- 改了源码里被变异测试改过的文件，先删对应的 `.obj` 再编译，否则 exe 里还是旧的断言。
