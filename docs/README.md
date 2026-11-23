# docs

本仓的工程文档，入版本控制。只放两类：

- **`platform/`**：平台事实——会直接约束代码决策、并随平台版本变化的那些事。
- **`design/`**：设计文档——与实现同源，改设计时和代码一起改。

不放这里：复盘、长文、跨项目对比。那些放
[engineering-notes](https://github.com/yangzeelong/engineering-notes)，
本文档的“外部阅读”一节维护链接。

## 索引

| 文档 | 内容 |
|---|---|
| [`platform/windows.md`](platform/windows.md) | Windows 定时器粒度、`Sleep` 语义、绑核、P 核/E 核 |
| [`platform/timer-differences.md`](platform/timer-differences.md) | 定时器与睡眠：Windows 分辨率 vs Linux hrtimer，含两平台实测 |
| [`platform/linux.md`](platform/linux.md) | Linux hrtimer、`perf` 可用性、WSL2 差异 |
| [`design/thread-pool.md`](design/thread-pool.md) | 线程池（`--pool-size`）：映射、`StepOnce`、退避阶梯、计数 |
| [`design/metrics.md`](design/metrics.md) | 四种指标口径与恒等式 |
| [`adr/`](adr/README.md) | 架构决策记录（ADR）：处境、决定、代价、翻案条件 |
| [`design/telemetry.md`](design/telemetry.md) | 运行期指标速查（边/模块/自定义指标） |
| [`design/telemetry-design.md`](design/telemetry-design.md) | 指标系统的设计讨论 |
| [`../benchmarks/README.md` 的「测量协议」节](../benchmarks/README.md#测量协议) | 测量协议与口径 |
| [`../HARNESS.md`](../HARNESS.md) | 工程约束总表（角色/流程、代码、测量、文档） |
| [`../benchmarks/README.md` 的「测量协议」节](../benchmarks/README.md#测量协议) | 测量协议与口径 |

## 外部阅读

- [为什么 Windows 上“睡 10 微秒”其实睡 2 毫秒](https://github.com/yangzeelong/engineering-notes/blob/master/posts/2026-11-21-windows-timer-granularity.md)
  —— 线程池空闲退避那个 bug 的完整复盘（故事与教训，事实部分见 `platform/windows.md`）。
- [NexusFlow 与 ai-pipe 的 WSL2 对比](https://github.com/yangzeelong/engineering-notes/blob/master/posts/2026-11-20-nexusflow-vs-ai-pipe.md)
  —— 两个 C++ DAG 框架的同形对比与复现。

## 维护约定

1. 平台事实变了就改 `platform/`，并在 PR 里说明是哪个平台版本。
2. 手册与代码同 commit：改 CLI/CSV 时同步 `benchmarks/README.md`。
3. 不在两处写同一段事实：要么写进本文档并让外部链接指向它，要么反过来。
4. 会活过 PR 的决定写成 ADR（`adr/`），PR 描述只留摘要。
