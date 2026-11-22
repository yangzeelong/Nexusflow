# 可观测性命名与设计重构（Observability Naming & Design Refactor）

> 状态：**设计稿，未动代码**。本文回答两个问题：
> ① 现在这批与"统计/观测"相关的名字为什么乱、按什么规则改，正文 §2、§3；
> ② 其他成熟框架怎么做同类统计、有哪些可以照搬，正文 §1。
> 文末 §6 是任务清单（可直接当 PR 清单用），§7 是验收标准，§8 列出需要拍板的开放问题。
>
> 依据全部来自本仓库当前代码与公开资料，出处见文末「参考」。

---

## 0. 结论摘要

1. **"乱"不是命名风格问题，是职责重叠问题。** 同一个概念（一条 pipeline 的一次计量）在仓库里有 **4 套互不相干的实现**，名字互不呼应：
   `TimerRegistry`（采样 + 分位 + 打印）、`PipelineContext::Metric`（自定义指标，均值）、`QueueMetrics`（边队列计数）、`Worker::m_processedTotal`（模块计数）。四套的字段名、更新时机、采样方式都不同，且**有一处真的算错了**（见 §2.2 `ItemQps()`）。
2. **成熟框架的划分是稳定的、只有两种视图**：计数器/水位快照（Prometheus/QML 的 Counter+Gauge，由队列与节点持有），和时间序列采样（由 profiler 子系统持有）。二者靠 **命名前缀/后缀 + 标签** 隔离，不靠"放在同一个 struct 里"。
3. **建议的新划分**：`PipelineSnapshot`（管线快照 = 边水位 + 模块计数）、`MetricsCollector`（时序样本 + 分位）、`MetricsExporter`（把快照变成 Prometheus 文本 / 日志行）。原有 4 套实现里 **3 套合并进快照，时序那套保留并改名**。
4. **当前最具备"必须大改"证据的三点**：`ItemQps()` 量纲错误、`PipelineContext::Report` 在热点路径上持锁做 `std::map` 查找且无分位、`.png` 级别的命名混用（`Metrics` 同时指快照和时序）。

---

## 1. 其他框架怎么做（证据）

### MediaPipe（Google，DAG + Calculator，与本项目结构最接近）

- **节点/端口级计数在框架里是"按需拉取"的函数，不是持久结构体**：`CalculatorNode` 对外暴露 `NumberOfProcessedTimestamp() / NumberOfProcessedBatch() / NumberOfDroppedTimestamp() / NumberOfDroppedBatch()`，`OutputStreamManager` 有 `NumberOfPacketsAdded()`。三类数**直接以 `Dropped` / `Processed` 为字段语义**，没有"Metric"这个泛称。
- **"丢"是显式传出的参数**：`InputStreamManager::PopPacketAtTimestamp(Timestamp, int* num_packets_dropped, bool* stream_is_done)` —— 丢弃数由调用方接收，而不是在内部默默累加，也没有一个统一 `dropped` 字段把多种原因混在一起。
- **时间统计走独立的 profiling 宏**：`MEDIAPIPE_PROFILING(PROCESS, calculator_context)`，与计数器完全分离。
- 命名规律：**"谁观测谁命名"**——节点自己的叫 `CalculatorNode::NumberOfXxx`，端口的叫 `InputStreamManager::NumberOfXxx` / `OutputStreamManager::NumberOfXxx`，profiler 归 `MEDIAPIPE_PROFILING`。
- 出处：[calculator_node.cc](https://chromium.googlesource.com/chromium/src/+/4c43a7157b3c1cf8b430d1c6e6288b16fcf3d834/third_party/mediapipe/src/mediapipe/framework/calculator_node.cc)、[input_stream_manager.cc](https://illixr.github.io/hand_tracking/api/input__stream__manager_8cc_source/)、[Framework concepts](https://developers.google.com/edge/mediapipe/framework/framework_concepts/overview)

### Prometheus / OpenMetrics（事实标准：Counter / Gauge / Histogram 三分）

- **Counter**：只增不减（"process_cpu_seconds_total"），只能配 `rate()` / `increase()` 使用。
- **Gauge**：可升可降（"node_memory_MemFree_bytes"），配 `delta()` / `deriv()`。
- **Histogram**：对一组样本分桶，给 `p95/p99`（`histogram_quantile()`），本质是"采样序列的聚合"。
- **命名规则**：counter 加 `_total` 后缀（新版 OpenMetrics 已把 `_total` 并入类型，但生态仍以此为约定）；**单位进名字**（`_bytes`、`_seconds`）；标签用 `{name: "x"}` 而不是塞进 metric 名。
- 出处：[OpenMetrics 规范讨论与 Prometheus 指标命名](https://mediatum.ub.tum.de/doc/1784761/https/easyperf.net#13#42#13)、[Instrumentation and Exporters 笔记](https://raw.githubusercontent.com/PatrickWiloak/cloud-data-ai-security-zero-to-hero/refs/heads/main/exams/kubernetes/pca/notes/04-instrumentation-exporters.md)

**对本项目的映射**：`pushed/popped/processed/dropped` 都是 **Counter**（应该带 `_total` 或至少在文档里声明"只增"）；`size` 是 **Gauge**；`highWaterMark` 是 **Gauge（作 Gauge-set 语义，取历史最大值，不是瞬时值）**；耗时是 **Histogram**（当前用 1024 采样窗手算 p95 就是它的近似）。

### Tekton / 云原生 CI 的教训（命名就是 API 约定）

- Tekton 曾专门发起 commit **"Fix the naming for Metrics as per convention"**：一个 metric 同时有 `_count`（Counter，语义为"发生次数"）和 `_running_*_count`（Gauge，语义为"当前在跑几个"）。**同一概念用 `_count` 但类型不同，是真实存在的坑**——它们最终靠 `Counter`/`Gauge` 类型列 + 后缀区分，而不是靠注释。
- 出处：[tektoncd/pipeline@d7c1039](https://github.com/tektoncd/pipeline/commit/d7c10392715157c719e4e0c5f6ece1d907c65fdc)

**对本项目的映射**：现在 `ModuleMetrics::processed`、`QueueMetrics::pushed/popped` 是 counter，`QueueMetrics::size/highWaterMark` 是 gauge，但它们在**同一个 struct 里、同一层命名**，只能靠读实现区分。这正是"名字太乱"的根源之一。

### 不成立的说法（这里不引用）

网上常见的"虚拟主播引擎（如某音）用 `SyncInputQueue`/`NodeStats`"一类说法只存在于用户讨论帖，**找不到可核对的框架源码**，不作为设计依据。此处只保留 MediaPipe / Prometheus / Tekton 三个可复核的来源。

---

## 2. 现状盘点：同一个概念，四套实现

### 2.1 四套实现对照表

| # | 位置 | 形（字段） | 谁更新 | 采样/成本 | 用途 |
|---|---|---|---|---|---|
| A | `profiling::TimerRegistry`<br/>`include/nexusflow/TimerRegistry.hpp` | `TimerStats{name, items, totalMs, minMs, maxMs, p95Ms, file, line}` | 模块线程（`StartAverage/EndAverage`） | **采样**：1024 最近样本滑窗求 p95 | 单次调用的耗时分布 |
| B | `PipelineContext::Metric`<br/>`include/nexusflow/PipelineContext.hpp:59-66` | `Metric{name, samples, total, last}` + `Average()` | 模块线程（`Report(name, value)`） | 持 `m_metricsMutex`，`std::map` 查找 | 模块自定义业务指标 |
| C | `QueueMetrics` / `ModuleMetrics` / `PipelineMetrics`<br/>`include/nexusflow/Pipeline.hpp:37-60` | `{name, capacity, size, highWaterMark, pushed, popped}` / `{name, processed}` | 框架内部：队列与 Worker 原子计数，`GetMetrics()` 拉取 | 读原子，**零采样** | 背压/丢帧/吞吐排查 |
| D | `core::Worker::m_processedTotal`<br/>`src/core/Worker.hpp:124` | 一个 `atomic<uint64_t>` | Worker 自己 | 读原子 | 只被 C 读取 |

**问题**：A 和 B 都在回答"某个量随时间怎么变"，却一个叫 Registry 一个叫 Context::Report；C 和 D 是同一件事的两层皮；A 里的 `name` 是 `"Worker.Process.source"` 这种字符串，C 里的 `name` 是 `"source -> sink"`，B 里的 `name` 是用户随便起——**三套命名空间互相不可见**。

### 2.1.1 全量标识符清单（改名工的输入）

`grep` 结果，仓库里与"统计/观测"相关的公开标识符一共这些，按"谁用"分类。**这是任务①的盘点底稿**：

| 标识符 | 定义处 | 谁在用（写入端 → 读出端） | 名字问题 |
|---|---|---|---|
| `struct TimerStats` | `include/nexusflow/TimerRegistry.hpp` | `TimerRegistry` → 用户 `GetStats/Snapshot/Print*` | 它不是一个 timer 的统计，是一组样本的统计（`samples/items/totalMs/minMs/maxMs/p95Ms`）；`items` vs `samples` 同义 |
| `TimerStats::ItemQps()` | `src/common/TimerRegistry.cpp:71` | 用户 | 名字说 qps，语义是"服务时间倒数"（§2.2） |
| `TimerStats::AvgItemMs()` | 同上 | 用户 | 与 `ItemQps()` 一对，语义 OK |
| `struct TimerPrintPolicy` | `TimerRegistry.hpp` | `StartAverage` 的第二个参数 | 名准；但"打印"混进采集端 |
| `class TimerRegistry` | `TimerRegistry.hpp` | `PipelineContext::Profiler()`、`Worker::Profiler()`、`TimerRegistry::Instance()` | "Timer" 限制成耗时，但它也装自定义指标（`AddItems`） |
| `TimerRegistry::Intern()` / `TimerHandle` | PR#14 新增 | `Worker`、宏 | 名准，但 `TimerHandle` 也是"指标句柄" |
| `TimerRegistry::StartAverage/EndAverage/AddItems/IncrementAverage` | 同上 | 宏 `TIMER_*`、`Worker` | `AddItems` 与 `AddValue` 不统一（§3.2） |
| `TimerRegistry::Snapshot()` | 同上 | 用户 | **与 `Pipeline::GetMetrics()` 同名不同义**：这里是时序数组，那里是快照；§3.2 里二者统一为 `Snapshot` 族 |
| `TimerRegistry::Print/PrintIfDue/PrintSample/PrintWindow` | 同上 | 用户配置 `profileTimers` | 打印属于导出层（§4.4） |
| `PipelineContext::Metric` | `PipelineContext.hpp:59-66` | 模块 `Report` → 用户 `GetReportedMetrics()` | 与 `TimerStats` 是同一概念（样本统计），却完全不同字段 |
| `PipelineContext::Report()` | `PipelineContext.hpp:70` | 模块 | 与 `AddItems` 同义不同名；**热点路径持锁** |
| `PipelineContext::GetReportedMetrics()` | 同上 | 用户 | "Reported" 暗示"已上报过"，实际是"模块自报的" |
| `PipelineContext::Profiler()` | 同上 | `Worker::Profiler()` | 名准（它是采集器） |
| `struct QueueMetrics` | `Pipeline.hpp:37-44` | `GetMetrics()` | 名为 Metrics 实为"一条边的快照"；`name` 是 `"a -> b"` 字符串 |
| `struct ModuleMetrics` | `Pipeline.hpp:49-52` | 同上 | 只有一个 `processed`，撑不起 "Metrics" 这个词 |
| `struct PipelineMetrics` | `Pipeline.hpp:57-60` | 同上 | collective 却叫 Metrics；与 §4.1 的 `PipelineSnapshot` 才是同一个东西 |
| `Pipeline::GetMetrics()` | `Pipeline.hpp:81` | 用户、测试 | 拉取函数，返回快照；名字应体现"取快照" |
| `SpscQueue::Stats` / `ConcurrentQueue::Stats` | `SpscQueue.hpp:80`、`ConcurrentQueue.hpp:36` | `EdgeSnapshot` 的填充 | 泛称 `Stats`，与 §3.2 的 `QueueCounters` 对应；`capacity/size/highWaterMark/pushed/popped` 混装 counter 与 gauge |
| `Worker::ProcessedCount()` | `Worker.hpp:82` | `ModuleActor::ProcessedCount()` → `GetMetrics()` | 名字准 |
| `core::Worker::m_processedTotal` | `Worker.hpp:124` | 同上 | 私有，不必改名 |
| `Pipeline::Stop()` / `PipelineContext::StopRequested()` | `Pipeline.cpp:183`、`PipelineContext.hpp:75` | 模块 | 准；与"排空"语义配套时需要一个 `Drain` 名（TODO 里那一项） |

> 结论：**16 个标识符里有 8 个带 `Metrics` 或 `Stats` 字样的处所不同、语义不同**。改名不是洁癖，是因为 `Metrics` 这个词同时被用在"写入端（TimerRegistry）""快照（PipelineMetrics）""队列计数（QueueMetrics）""模块自定义指标（PipelineContext::Metric）"四个地方。


### 2.2 一个真实的语义错误：`ItemQps()` 量纲不对

```cpp
// include/nexusflow/TimerRegistry.hpp
struct TimerStats {
    uint64_t items = 0;
    double   totalMs = 0.0;   // 累计耗时（只算被计量的那段服务时间）
    ...
    double ItemQps() const;   // 名字说是 items per second
};

// src/common/TimerRegistry.cpp:71-74
double TimerStats::ItemQps() const {
    const double seconds = totalMs / 1000.0;
    return seconds <= 0.0 ? 0.0 : static_cast<double>(items) / seconds;
}
```

`items / (totalMs / 1000)` 得到的是 **1 / 平均单条服务时间**，也就是"如果满负荷连续跑，每秒最多能处理多少条"，而不是"每秒实际发生了多少次"。它成立的前提是 pipeline 一直在处理：模块空闲 3 秒才来一条 item 时，这个数不动（它只看服务时间），同时 `TimerStats` 里**没有任何字段记录时间跨度**，谁也没法把它修正成真实 QPS。

正确做法是显式的两种：
- **累计型**：counter + 时间跨度，QPS = `Δitems / Δwall`；需要 `TimerStats` 暴露 `firstSampleMs` / `lastSampleMs`（或者导出层用两次快照差）。
- **窗口型**：`PrintWindow` 已经这么做了——它用 `sample.windowWallMs` 算 `windowItems * 1000 / wallMs`，并留了注释 "qps comes from wall time while the average comes from the service time"。**同一份数据，两个接口给出两个不同的 "qps" 定义**，这就是要统一的地方。

> 这一条是"设计不好"的最硬证据：不是风格问题，是**派生指标的定义不唯一且默认那个是错的**，而它恰好是用户最常看的那个数。

### 2.3 其他具体问题

| 问题 | 证据 | 后果 |
|---|---|---|
| `PipelineContext::Report` 是热点路径上的持锁 map | `src/pipeline/PipelineContext.cpp:28-35`：`std::lock_guard` + `m_metrics[name]` | 模块每帧报 3 个指标就是 3 次全锁 + 3 次红黑树查找；多路视频下直接成为瓶颈 |
| `Report` 只有 average，没有分布 | `PipelineContext.hpp:59-66`：`Metric{samples,total,last}` + `Average()` | "p99 掉帧数"这类真正关心的问题回答不了 |
| 快照无法区分"counter 还是 gauge" | `QueueMetrics` 把 `size` 与 `pushed` 放同层 | 导出到 Prometheus 时需要人工判断类型，容易再踩 Tekton 的坑 |
| `highWaterMark` 语义未声明"自 reset 以来" | `SpscQueue::Stats.highWaterMark` | 使用者以为是"当前最大值"，实际是历史峰值 |
| 三套命名空间互相不可见 | 见 2.1 | 无法把"这条边"与"这个模块的耗时"关联起来做关联分析 |
| 没有导出/消费层 | 只有 `TimerRegistry::Print*`（日志） | 用户要接 Prometheus/文件，只能自己遍历 |

---

## 3. 命名规则（回答任务①）

### 3.1 后缀规则：只有累计量加后缀

| 后缀 | 语义 | 例子 | 对应 Prometheus |
|---|---|---|---|
| `*Total` | **累计量**：只增不减，可差分 | `pushedTotal`, `processedTotal`, `droppedTotal` | Counter（导出映射 `_total`） |
| `*Sum` | **加出来的累计和**：只对"值"求和，不表示"发生了几次" | `sumMs`（总耗时） | Counter / Histogram 的 `_sum` |
| 无后缀 | 快照里的裸名就是"此刻" | `depth`, `capacity`, `samples` | Gauge |
| `*Max` / `*Min` | 极值 | `depthMax`；`minMs` / `maxMs` | Gauge（记录型） |
| `*Ms` / `*P95Ms` | 样本 / 分位数，不进快照 | `sumMs`, `p95Ms` | Histogram 派生 |
| `*Snapshot` | **某一时刻的一整组数**（collective） | `PipelineSnapshot`, `EdgeSnapshot` | 一次 `/metrics` 抓取 |
| `*Collector` | **写入端**（采集 + 聚合 + 分位） | `MetricsCollector` | SDK |
| `*Exporter` | **读出端**（序列化） | `MetricsExporter` | exporter |

**规则收敛成三条**：

1. **累计量必须带后缀**：`*Total` 是默认选择（"只增、可差分"）；只有当它是"值的累加和"而非计数时才用 `*Sum`。
2. **其余一律不加后缀** —— 快照自带"此刻"语义，所以 `depth` 不叫 `depthCurrent`；极值用最直白的 `Min`/`Max`，不发明 `Peak`/`HighWaterMark`。
3. **内部字段名与 Prometheus 名解耦** —— 结构体内用 `*Total`/`*Sum`，导出层负责映射成生态惯用的 `_total`/`_sum`（见 §4.4）。两边各用最合适的词，映射集中在一个地方。

> 为什么 `Total` 与 `Sum` 都留着：它们在 Prometheus 里本来就是两种东西（`_total` 是计数，`_sum` 是 Histogram 的累计和）。结构体里 `pushedTotal` 与 `sumMs` 并列时读者不会混淆——前者是"次数"，后者是"总量"；而只用一个词，必然有一边读起来别扭（`totalMs` 让人以为"total 相对谁"，`pushedSum` 又像求和操作）。

`Min` / `Max` 在两处各自成立、语境不同，**不需要为区分再加后缀**：

| | `EdgeSnapshot::depthMax` | `MetricSamples::maxMs` |
|---|---|---|
| 是什么 | 队列深度的历史最大值 | 滑窗内最慢的一次采样 |
| 谁维护 | 队列自己在 push/pop 时更新 | collector 在每条样本写入时更新 |
| 何时归零 | `Reset()` 清零 | 永不清零（滑窗自动滚出） |
| 用在哪 | 容量规划（cap=5 够不够） | 找慢的尾巴 |

> 文档里各写一句注释即可；`highWaterMark` 这个名字要弃用——它是 TCP 流量控制的术语，放在队列深度上会被误读成"阈值"，但它实际是"历史最大值"。

**关于 `Metrics` 这个词**：现在它同时被 4 个对象用（写、快照、队列计数、模块自报），改名后 **只有 `MetricsCollector` / `MetricsExporter` 保留 `Metrics`**，快照族用 `*Snapshot`、样本族用 `MetricSamples`，三套命名空间彻底分开。



### 3.2 `XxxMetrics` → 统一改名对照

| 旧 | 新 | 变化 |
|---|---|---|
| `PipelineMetrics` | `PipelineSnapshot` | 改名（collective 却叫 Metrics）；`GetMetrics()` 保留为弃用转发 |
| `QueueMetrics` | `EdgeSnapshot` | 改名；**加 `from`/`to` 字段**，`name` 只作展示 |
| `ModuleMetrics` | `ModuleSnapshot` | 改名；加 `droppedTotal` 等，见 §4 |
| `Pipeline::GetMetrics()` | `Pipeline::GetSnapshot()` | 加新名（更准）；旧名 `[[deprecated]]` 转发一个版本周期，**不破坏** |
| `PipelineContext::Profiler()` / `Worker::Profiler()` | 保留名字，返回类型改为 `MetricsCollector&` | 调用点一行不改 |
| `profiling::TimerRegistry` | `profiling::MetricsCollector` + `using TimerRegistry = MetricsCollector;` | "Registry" 暗示静态登记表，与运行时高频写入冲突；旧名做别名 |
| `TimerStats` | `MetricSamples` | 它不是"timer 的统计"，是一组样本的统计；`ItemQps()` 删除（见 §2.2） |
| `PipelineContext::Metric` | `MetricSamples`（同一类型） | 与 A 合并，自报指标走同一个采集器 |
| `PipelineContext::Report` | `PipelineContext::Record(name, value)` | 与 `AddItems` 语义一致，**名字对齐** |
| `PipelineContext::GetReportedMetrics()` | `PipelineContext::GetCustomMetrics()` | 说清"这是模块自报的"；旧名转发 |
| `core::Worker::m_processedTotal` | 保留为内部 | 只经 `ModuleSnapshot::processedTotal` 暴露，不再单独命名 |
| `SpscQueue::Stats` / `ConcurrentQueue::Stats` | `QueueCounters` | `Stats` 太泛；结构里再分成 `*Total`（累计计数）、`depth`（此刻）与 `depthMax`（极值） |
| `TimerRegistry::Snapshot()` | 保名 | 返回 `std::vector<MetricSamples>`；与 `PipelineSnapshot` 的区别靠"集合 vs 抓取"，文档里写死 |
| `TimerPrintPolicy` | 保留 | 已是"打印策略"，名字准 |

### 3.3 命名前缀：三套命名空间要能互认

| 命名空间 | 现在的样子 | 建议 |
|---|---|---|
| 边（edge） | `"source -> sink"` | 保留展示名，但**结构里拆成 `from`/`to` 两个字段**，排障时能反查"这条边谁生产谁消费" |
| 模块（module） | 模块名 | 保留 |
| 指标名（profiler） | `"Worker.Process.source"` | 统一前缀：框架内置 `nxf.*`，模块自报必须带模块名前缀（见下） |

**模块自报指标强制前缀**：`Record("Detector.GpuMs", 3.2)` 在写入端自动规范成 `Detector.DroppedTotal` / `Detector.GpuMs` —— 用 `<Module>.` 前缀，**不允许裸名**，否则两套命名空间再次混在一起。

---

## 4. 新结构（可大改）

只保留两个视图，与 §0 结论一致。

### 4.1 快照（collective，零采样，高频可拉）

```cpp
// include/nexusflow/Telemetry.hpp
namespace nexusflow {

/// 一条边（step queue）的水位与流量。裸名 = 此刻；*Total = 累计（只增）；*Max = 自 Reset 以来的极值。
struct EdgeSnapshot {
    std::string from;                       // 上游模块名
    std::string to;                         // 下游模块名
    std::size_t capacity = 0;               // 0 = 无界
    std::size_t depth = 0;                  // gauge：此刻积压多少
    std::size_t depthMax = 0;               // gauge：自 Reset() 以来最深的积压
    std::uint64_t pushedTotal = 0;          // 每 push 一次 +1
    std::uint64_t poppedTotal = 0;          // 每 pop 一次 +1
};

/// 一个模块的处理计数。
struct ModuleSnapshot {
    std::string name;
    std::uint64_t processedTotal = 0;       // 处理成功一次 +1
    std::uint64_t droppedTotal = 0;         // 队列满/已停止时 push 失败 +1
    std::uint64_t fusionExpiredTotal = 0;   // 融合超时丢弃 +1
    std::uint64_t failedTotal = 0;          // Process 抛异常 +1
};

/// 某一时刻的整条 pipeline。
struct PipelineSnapshot {
    std::string name;
    std::vector<EdgeSnapshot> edges;
    std::vector<ModuleSnapshot> modules;
    // 自定义指标（§4.3）不放在这里，避免快照层依赖各模块的采集器
};

} // namespace nexusflow
```

### 4.2 时序（采样，低频，可算分位）

```cpp
// include/nexusflow/MetricsCollector.hpp  (profiling::TimerRegistry 的继任者)
struct MetricSamples {          // 取代 TimerStats
    std::string name;
    std::uint64_t samples = 0;  // 已采样次数
    double sumMs = 0.0;         // 加出来的：所有样本耗时之和（原 totalMs）
    double minMs = 0.0;         // 窗口内最快
    double maxMs = 0.0;         // 窗口内最慢（注意：与 EdgeSnapshot::depthMax 不同，见 §3.1）
    double p95Ms = 0.0;         // 最近 1024 样本
    // 不提供 ItemQps()：QPS 需要时间跨度，交给导出层按两次快照差算
    std::string file; int line = 0;
};
```

### 4.3 自定义指标（模块自报）也走时序，不再用 `std::map`

```cpp
// PipelineContext 内部持有 profiling::MetricsCollector m_custom;
// Record(name, value) -> 与计时器共用同一个写入端：
//  - 名字 Intern 一次，句柄化（复用 §PR#14 已做的 MetricHandle 机制）
//  - 写入端是线程局部累加，flush 到共享时也只锁一次
void PipelineContext::Record(const std::string& name, double value);  // 取代 Report
```

这样 `Report` 从"每帧 3 次全锁 + 红黑树"变成"每帧 3 次无锁累加"，并且**自动获得 p95**（复用 A 的滑窗）。

### 4.4 导出层（新增，把快照变成文本）

```cpp
// include/nexusflow/MetricsExporter.hpp
class MetricsExporter {
public:
    /// Prometheus 文本格式（每个指标带 # TYPE counter/gauge）。
    static std::string ToPrometheus(const PipelineSnapshot&, const std::string& pipelineLabel);

    /// 一行人类可读摘要，给日志/CLI 用。
    static std::string ToLine(const PipelineSnapshot&);
};
```

`ToPrometheus` 里**显式声明类型**，正是为了避免 Tekton 那个坑。

**字段名 → Prometheus 名的映射表**（集中在这一层，内部字段不受生态约定束缚）：

| 结构体字段 | Prometheus 名 | `# TYPE` |
|---|---|---|
| `EdgeSnapshot::pushedTotal` | `nxf_edge_pushed_total{from,to}` | counter |
| `EdgeSnapshot::poppedTotal` | `nxf_edge_popped_total{from,to}` | counter |
| `EdgeSnapshot::depth` | `nxf_edge_depth` | gauge |
| `EdgeSnapshot::depthMax` | `nxf_edge_depth_max` | gauge |
| `ModuleSnapshot::processedTotal` | `nxf_module_processed_total{module}` | counter |
| `ModuleSnapshot::droppedTotal` | `nxf_module_dropped_total{module}` | counter |
| `MetricSamples::sumMs` | `nxf_metric_sum_seconds{samples=...}` | counter（`_sum` 语义） |
| `MetricSamples::p95Ms` | `nxf_metric_p95_seconds` | gauge（Histogram 派生） |

规则：**`*Total` → `_total`，`*Sum` → `_sum`，`*Ms` → `_seconds`，裸 gauge 不加后缀**。

### 4.5 三件套的边界声明

三个组件的职责**互不重叠**，且只有第一个位于热点路径上。

| 组件 | 一句话职责 | 回答什么问题 | **不**回答什么 |
|---|---|---|---|
| `MetricsCollector` | 接收写入 + 滑窗聚合 + 算分位 | "这帧处理花多久的 p95？" | "现在队列积压多少？" |
| `*Snapshot` | 某一时刻把原子读数收成一个结构体 | "现在积压多少？累计丢了多少？" | "这一帧耗时的 p95 是多少？" |
| `MetricsExporter` | 把快照序列化成外部格式 | "怎么让 Prometheus/Grafana 读懂？" | 任何采集、存储、定时 |

调用链与代价：

| 链路 | 频率 | 谁调谁 | 代价 |
|---|---|---|---|
| ① 写入 | 每帧多次 | 模块 → `MetricsCollector` | 常数级、无锁 |
| ② 快照 | 每秒若干次 | 用户 → `Pipeline::GetSnapshot()` | 读原子 + 少量拷贝 |
| ③ 导出 | 每秒 1 次 | 用户 → `MetricsExporter` | 纯序列化 |

**归属速查**：

| 想问的问题 | 找谁 |
|---|---|
| 这帧处理花多久的 p95？ | `MetricsCollector::Snapshot()` → `MetricSamples::p95Ms` |
| 现在队列积压多少？ | `Pipeline::GetSnapshot()` → `EdgeSnapshot::depth` |
| 历史最深积压多少？ | `EdgeSnapshot::depthMax` |
| 累计丢了多少包？ | `ModuleSnapshot::droppedTotal` |
| 当前 QPS 是多少？ | **自己算**：两次 `Snapshot` 相减再除以 Δt（不存派生值） |
| 怎么显示到 Grafana？ | `MetricsExporter::ToPrometheus()` |
| 日志里打一行摘要？ | `MetricsExporter::ToLine()` |

**不该混进彼此的东西**：

- `MetricsCollector` 里不放计数器 / 水位（它们没有分布，进滑窗纯属浪费）。
- 快照里不放分位数、不存派生值（`qps`、填充率现场算）。
- 导出层里不放采集、存储、定时器、网络（谁调它、何时调它是使用方的事）。

---

## 5. 迁移路径（不破坏现有使用者）

| 阶段 | 动作 | 破坏性 |
|---|---|---|
| 1 | `PipelineSnapshot` 落盘 + `Pipeline::GetSnapshot()`，旧 `GetMetrics()` 用 `[[deprecated]]` 转发 | 否 |
| 2 | `EdgeSnapshot` 加 `from`/`to`，`QueueMetrics::name` 继续填 | 否 |
| 3 | `ModuleSnapshot` 加 `droppedTotal`/`fusionExpiredTotal`/`failedTotal`，**所有 push 失败路径必须计数**（见 §6.2） | 否 |
| 4 | `Report` → `Record`，旧名转发；`Metric` → 别名 `= MetricSamples` | 否 |
| 5 | `ItemQps()` 标 `[[deprecated]]` 并在文档里给"两次快照差"的替代示例 | 半（编译警告） |
| 6 | `highWaterMark` → `depthMax`；`TimerStats::totalMs` → `sumMs`（结构体内改名，一次做） | 是（结构体字段） |
| 7 | `TimerRegistry` → `MetricsCollector`，保留 `using TimerRegistry = MetricsCollector;` | 否 |
| 8 | `MetricsExporter` + 单元测试 + benchmark（`Record` 前后对比） | 否 |

---

## 6. 任务清单

### 6.1 命名与类型（对应任务①）

- [ ] 新增 `include/nexusflow/Telemetry.hpp`：`EdgeSnapshot` / `ModuleSnapshot` / `PipelineSnapshot`，**字段后缀遵循 §3.1**（累计量带 `*Total` / `*Sum`，其余不加）
- [ ] `Pipeline::GetSnapshot()`；`GetMetrics()` 标记弃用并转发
- [ ] `EdgeSnapshot` 拆 `from` / `to`
- [ ] `QueueMetrics` → `EdgeSnapshot`；`SpscQueue::Stats` → `QueueCounters`
- [ ] `TimerStats` → `MetricSamples`（`totalMs` → `sumMs`）；**删除 `ItemQps()`**，导出示例放文档
- [ ] `PipelineContext::Report` → `Record`；`Metric` → `MetricSamples`；`GetReportedMetrics` → `GetCustomMetrics`
- [ ] `TimerRegistry` → `MetricsCollector`，保留 `using TimerRegistry = MetricsCollector;`

### 6.2 计数器补全（对应任务②里"设计不好"的部分）

- [ ] `Dispatcher::Broadcast` / `Broadcast(&&)` / `SendTo` 的每一次 push 失败 → `droppedTotal++`（今天只有 `LOG_WARN`，**没有任何计数**）
- [ ] `Worker::RunFusion` 的融合超时 → `fusionExpiredTotal++`
- [ ] `Worker::WorkLoop` 的 `catch` 分支 → `failedTotal++`（PR#14 已加 catch，但没有计数）
- [ ] 增加不变量测试：`processedTotal + droppedTotal == pushedTotal`（单 sink、单源场景）

### 6.3 时序与导出

- [ ] `Record` 改为句柄化 + 线程局部累加（复用 `MetricHandle` 机制），不再持锁进 `std::map`
- [ ] `MetricsExporter::ToPrometheus` / `ToLine` + 单测（检查 `# TYPE` 行、`*Total` → `_total` 的映射）
- [ ] 文档：`docs/telemetry.md` 写明三套命名空间（edge/module/metric）、后缀规则、字段→Prometheus 映射表

### 6.4 文档

- [ ] `AGENTS.md` 补一节"新增统计字段：累计量用 `*Total`（求和的用 `*Sum`），其余不加后缀"
- [ ] `TODO.md` 里"MetricsCollector"条目改成本文的阶段表

---

## 7. 验收标准

1. `cmake --build build --config Release --parallel 16` 退出码 0；`./build/tests/Release/nexusflow_tests.exe` 全绿（当前 119）。
2. 新增测试：快照不变量 `processedTotal + droppedTotal == pushedTotal`；`ToPrometheus` 输出含 `# TYPE nxf_edge_pushed_total counter`。
3. `PipelineContext::Record` 的微基准：每帧 3 指标、10 万帧，**相对当前 `Report` 有可测量下降**（测量方法沿用 `build/perf-probe`，不引入新依赖）。
4. 端到端回归：`build\perf-probe\build\Release\pipeline_benchmark.exe` 的吞吐/延迟不低于现基线（1.837–1.859M msg/s，9.18–9.94 µs）。

---

## 8. 需要你拍板的问题

1. **`GetMetrics()` 要不要直接改名**（破坏性），还是先 `[[deprecated]]` 转发一个版本？（我建议后者）
2. **`Report` → `Record` 的改名**：`Report` 现在有测试在用，是否接受同批改测试？
3. **自定义指标是否必须带模块名前缀**（强制隔离两套命名空间）？
4. **`highWaterMark` 是否接受"自 reset 以来"的语义**，即 `Reset()` 时清零？
5. **是否需要 Prometheus 导出**：只出 `ToLine`（日志一行）够不够，还是要完整文本格式？

---

## 参考

- [MediaPipe `calculator_node.cc`](https://chromium.googlesource.com/chromium/src/+/4c43a7157b3c1cf8b430d1c6e6288b16fcf3d834/third_party/mediapipe/src/mediapipe/framework/calculator_node.cc) — `NumberOfProcessedTimestamp() / NumberOfDroppedTimestamp()` 等按节点拉取的计数
- [MediaPipe `input_stream_manager.cc`](https://illixr.github.io/hand_tracking/api/input__stream__manager_8cc_source/) — `PopPacketAtTimestamp(..., int* num_packets_dropped, ...)`，"丢"由调用方接收
- [MediaPipe Framework concepts](https://developers.google.com/edge/mediapipe/framework/framework_concepts/overview) — Packet / 端口 / 每消费者独立队列
- [Prometheus 指标类型与命名约定](https://mediatum.ub.tum.de/doc/1784761/https/easyperf.net#13#42#13)、[Counter/Gauge/Histogram 用法笔记](https://raw.githubusercontent.com/PatrickWiloak/cloud-data-ai-security-zero-to-hero/refs/heads/main/exams/kubernetes/pca/notes/04-instrumentation-exporters.md) — `_total` 后缀、`rate()` 只对 counter 有意义
- [tektoncd/pipeline @ d7c1039 "Fix the naming for Metrics as per convention"](https://github.com/tektoncd/pipeline/commit/d7c10392715157c719e4e0c5f6ece1d907c65fdc) — Counter 与 Gauge 同名同概念的教训
- 本仓库：`include/nexusflow/TimerRegistry.hpp`、`include/nexusflow/Pipeline.hpp`、`include/nexusflow/PipelineContext.hpp`、`src/pipeline/PipelineContext.cpp`、`src/common/TimerRegistry.cpp`、`src/dispatcher/Dispatcher.cpp`、`src/core/Worker.cpp`
