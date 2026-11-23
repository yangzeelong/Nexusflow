# 指标口径

> 四个数字，别混用。基准的 `stat,` 行与 `README.md「测量协议」§4` 一一对应。

## 1. 四条定义

| 指标 | 公式 | 回答的问题 |
|---|---|---|
| 交付吞吐 | `delivered / wallSeconds` | 每秒真正交付多少条（墙钟） |
| CPU 归一吞吐 | `delivered / cpuSeconds` | 每秒每核交付多少条 |
| 每消息 CPU | `cpuSeconds / delivered`（µs/msg） | 一条消息花多少 CPU |
| 每消息周期 | `cpuSeconds × tscHz / delivered` | 去掉主频差异后的同口径 |

其中：

- `delivered` 是 sink 真正收到的条数（不是生产数），`wallSeconds` 用 `steady_clock`
  在 `Start()` 后、`Stop()` 后取差；
- `cpuSeconds` 是**进程** CPU（`GetProcessTimes` / `getrusage`），包含所有线程，
  也包含自旋与空转；
- `tscHz` 由 `benchmarks/TscClock.hpp` 用三个 20 ms 窗口取最大值校准（本机 3.4176 GHz）。

## 2. 三条恒等式（自检用）

```
cores        = cpuSeconds / wallSeconds
throughput   = cores × 1e6 / cpuPerMsgUs
cyclesPerMsg = cpuPerMsgUs × tscGhz × 1000
```

报告里任何一行都该满足；不满足说明采样点或分母不一致。

## 3. 常见坑

- **Google Benchmark 的 `items_per_second` 是 `items / cpu_time`**，属 CPU 口径，
  不是墙钟吞吐。ai-pipe 报告里 922 K frames/s 就是这么来的（同一台机器墙钟 ~64 K）。
- **时延**是 `Clock::now() - payload.creationTime`，`creationTime` 在 source 出帧
  （或 inject `Push` 前）写入；直方图地板 `kMinNs = 1000`（1 µs），低于它的值都记 1 µs。
- 池化让 `cores` 接近线程数，p0 的 `cores` 也接近模块数——**核数不是并行度，是
  实际占用**；两个配置核数相同不代表工作量相同。
- 自旋等 CPU 时间会记进 `cpuSeconds`，所以“等”也会抬高每消息 CPU。这是有意的：
  它确实占了核。

## 4. 各列在哪

- `benchmarks/README.md`：CSV 39 列的 schema；
- `benchmarks/README.md` 的「测量协议」§4：四条定义、三条恒等式、GB 口径的坑；
- `benchmarks/PERFORMANCE-desktop.md §1.2`：cycles/消息的读数示例；
- `benchmarks/ProcessCpuTime.hpp` / `ProcessMemory.hpp` / `TscClock.hpp`：实现。
