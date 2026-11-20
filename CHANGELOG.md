# Changelog

本文件记录 NexusFlow 的对外变更。格式参考
[Keep a Changelog](https://keepachangelog.com/)，版本号遵循
[Semantic Versioning](https://semver.org/)。v1.0.0 起，公开头文件
(`include/nexusflow/`) 中的 API 按 semver 承诺。

## [Unreleased]

## [1.0.1] - 2026-11-20

补测量口径与 cycles/消息指标，不改库代码。

### Added

- 基准新增 `cycles_per_msg` 列（`benchmarks/TscClock.hpp`，TSC 三窗校准）：
  `cpu/msg × TSC 频率`，去掉主频差异；非 x86 上该列为 0，不伪造。
- `benchmarks/HARNESS.md §4` 重写指标口径：交付吞吐 / CPU 归一吞吐 /
  每消息 CPU / 每消息周期 四条定义、三条恒等式，以及"Google Benchmark 的
  `items_per_second` 是 CPU 口径"这一常见坑。
- `benchmarks/PERFORMANCE-desktop.md §1.2` 记录 3/4/16 模块的 cycles/消息
  （2,954 / 4,152 / 23,257），给出浅链每跳约 1,477–1,550 cycles 的读法。

## [1.0.0] - 2026-11-19

首个稳定版本。

### Added

- 外部输入 API：`Pipeline::Push()` / `Pipeline::TryPush()`、`PushResult`，以及
  `Module::IsPassiveSource()`。passive source 的边队列由外部生产者填充，`Push`
  在满时阻塞、`TryPush` 立即返回 `kQueueFull`；`Stop()` 会排水并在停机后拒绝新输入。
  配套 6 个单测（`tests/test/PipelinePushTest.cpp`）与 README
  "Feeding a Running Pipeline" 一节。
- 基准工具链：`benchmarks/HARNESS.md`（测量规则与维度）、`benchmarks/RawPipeline.hpp`
  （裸线程基线）、按机器分开的 `benchmarks/PERFORMANCE-laptop.md` 与
  `benchmarks/PERFORMANCE-desktop.md`。`pipeline_benchmark` 新增 workload
  （delay / compute / memory）、inject 拓扑、`--cpus`、`--timings`、RSS 与停机采样、
  以及每次运行的覆盖率断言；CSV 扩到 37 列。
- 可选扩展：`extensions/aifootball`（AI-Football SDK、离线 demo、Python
  touch-evaluation 工具链），通过 `-DNEXUSFLOW_EXTENSIONS=aifootball` 构建。
- CI：GitHub Actions，Linux/GCC Release 与 Windows/MSVC Release 双平台，
  构建并运行 `nexusflow_tests`（167 个用例 / 27 个 suite）。
- 安装/导出：`cmake --install` 产出 `NexusflowConfig.cmake`，导出
  `Nexusflow::nexusflow` 与 `Nexusflow::nexusflow_common`，下游可用
  `find_package(Nexusflow CONFIG)` 消费。

### Changed

- 每消息日志降为 TRACE，默认日志级别 INFO；逐跳计时链默认关闭，用
  `PipelineContext::EnableModuleTimings()` 按需打开。
- `pipeline_benchmark` 默认 `--capacity 4`；wall/CPU 计时包含 `Stop()` 的排水区间，
  吞吐的分母与计数覆盖同一段时间。

### Fixed

- yaml-cpp pin 由 0.7.0 升到 0.8.0，修复嵌套 FetchContent 下导出集缺失导致的
  CMake 配置失败。
- `SpscQueue::restart()` 先析构 `[head, tail)` 的残留元素再归零索引，并把上一轮索引
  折进 base，`*Total` 不再回退。
- `GetSnapshot()` 改为先填 modules 再填 edges，与 `Stats.hpp` / AGENTS §5 的契约一致。
- `Worker` / `Dispatcher` 计数器改为 `std::atomic<uint64_t>`（relaxed add/load）；
  `RecordDropped` / `RecordFailed` 的多写者检测改用 compare-exchange。
- `Start()` 拒绝在上一轮被放弃的活线程上重启；`~ModuleActor` 只等待并报错，不 detach。
- 台式机基准报告的拐点结论更正：2026-11-20 同机同参复测显示 18 模块只比 16 模块低
  2.6%（1.697 vs 1.742 M msg/s），真正的塌陷在 28 模块（0.856 M，−51%）；原报告的
  「16→18 −41%（拐点）」与 P/E 归因不再作为结论引用（详见
  `benchmarks/PERFORMANCE-desktop.md` §1.1）。
- `RunFusion()` 的分组 key 由 `MessageMeta::sourceName` 改为输入边名，修复多路输入
  永远凑不齐、最终按 60 s 超时静默丢弃的问题。
- `PullBatchMessage()` 在所有输入边 shutdown 时提前返回；`waitForRoom()` 在 park 前
  登记生产者等待；`CollectDownstream()` 先插入再递归。
- `PipelineBuilder` 与 YAML 建图拒绝无边模块（原先返回 SUCCESS 但静默不建 actor）。
- `Pipeline::Impl::started` 改为 `std::atomic<bool>`，避免 `Push()` 与并发 `Stop()`
  读写普通 bool 的 data race。

### Known limitations

- 模块线程数跨过 P 核线程容量后吞吐断崖：台式机 16→18 模块 −41%（8.755 → 12.622 µs/消息），
  18–28 模块区间在混合 P/E 核调度下不可稳定测量。见 `NEXT-VERSION.md` §3.4 的 P0。
- 单消息框架成本约为裸线程基线的 3×（按 1.0.0 台式机数据）。
- 低速率/空闲路径仍受 Windows 定时器影响：框架内的 `sleep_for(1ms)` / 条件变量超时在
  长跑中可能被合并到 15.6 ms，16 模块的 `stopMs` 约 15.6 ms（浅链约 0.2 ms）。基准工具
  的 delay workload 已用高精度 waitable timer 绕过，框架本身尚未修改。
- `Push` 目前只支持 passive source 与单生产者线程；fan-out / fusion 的每帧成本较高，
  见 `benchmarks/PERFORMANCE-desktop.md` §7。
- 三维以上（深度、扇出、容量、batch、payload）的完整矩阵与 soak 测试尚未进入 CI。

[Unreleased]: https://github.com/yangzeelong/Nexusflow/compare/v1.0.1...HEAD
[1.0.1]: https://github.com/yangzeelong/Nexusflow/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/yangzeelong/Nexusflow/releases/tag/v1.0.0
