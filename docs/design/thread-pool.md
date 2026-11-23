# 线程池设计（`--pool-size N`，v1.1 §3.1 L2）

> 目标：**用更少的线程跑出同样的吞吐**，不是提高吞吐上限。默认关闭（`pool = 0` 时
> 每模块一条线程，行为与 v1.0.x 完全一致）。

## 1. 开关与生命周期

- `PipelineContext::WithThreadPoolSize(n)`（链式）→ `Pipeline::Init()` 前设置。
  `0` = 关闭；`n` = 池线程数。
- `Pipeline::Start()` 建池 → 把每个 `ModuleActor` 挂到池上（`AttachThreadPool`）→
  启动线程；`Stop()`/`Cancel()` 在 actor 收尾之后 `StopAndJoin()` 并清空池。
- 融合（join）模块**不进池**，仍独占线程：`Worker::HasJoinInputs()` 为真时走原有
  `m_workThread`。
- 计数契约不变：每个模块仍固定由**同一条**宿主线程 `StepOnce()`，所以
  `AGENTS.md §5` 的“每计数器单写者”成立（plain 自增 + `PublishCounters()` 的
  release/acquire bookend）。

## 2. 映射：谁和谁共享一条线程

| 模式 | 规则 | 用途 |
|---|---|---|
| `kContiguous`（默认） | `slot = i * n / total`，连续模块打包到同一线程 | 让相邻 stage 同线程，减少跨核交接 |
| `kRoundRobin` | `slot = i % n` | 仅用于对照实验（`--pool-map round-robin`） |

槽内遍历**下游优先**（`rbegin()`）：共享线程里的下游 stage 先把输出发出去，
上游才不会因为边满而空转。每个 worker 每轮最多 `kTurnBudget = 16` 次
`StepOnce()`，防止一个热模块把同线程的其它模块饿死。

实测（MSVC，5 s × 3，L16cap8-p4）：连续映射比轮转高 **25%**；跨核次数
15 → 3 的收益被证实。

## 3. `StepOnce()`：非阻塞单步

```
stopFlag  → MarkExited()，返回 false
join 模块 → 返回 false（不进池）
source    → ProcessTimed(empty) 一次，成功则 AddProcessed(1)
stage     → room = OutputRoom(); if room == 0 → false
            take = min(m_maxBatchSize, room)
            PullBatchNow(take)（只 tryPop，不等待）→ 空则 false
            ProcessBatch(batch) → AddProcessed(size)
每步 PublishCounters()（每轮只发一次已测过无收益，已回退）
```

`OutputRoom()` 返回**最紧**的有界输出边剩余槽位（无界边忽略，全无界返回
`SIZE_MAX`）。因为每条边只有一个生产者，room 只增不减，所以“按 room 取批”
精确且**零溢出、零丢弃**。

## 4. 空闲退避阶梯（2026-11-21 修复）

```cpp
if (spins < 4096)            { ++spins; Pause(); }    // PAUSE，~50-70 us
else if (spins < 8192)       { ++spins; yield(); }
else                         { sleep_for(1ms); }
```

- **为什么不用微秒**：Windows 上 `sleep_for(10µs)` 等于一次 1.9 ms 的 tick 停泊，
  而池线程没有唤醒源（`push` 不唤醒池线程），每次停泊就是这一级流水线停 2 ms。
  修复前后：MSVC L4cap16-p4 2.221 M → 3.019 M（**+36%**），p4/p0 68% → **93.6%**。
- **为什么 8192 轮**：整链 3 M msg/s、每消息过 3 条边 → 单边到达间隔 ≈ 0.33 µs；burst 间隙在亚微秒到几微秒。64 轮（≈0.5 µs）会掉进 sleep，
  8192 轮（≈50–70 µs）比稳态间隙大两个数量级。
- **为什么不用条件变量**：池里一次交接间隔几十纳秒到几微秒，挂起/唤醒的内核
  往返比自旋贵；实测 cv 版本吞吐腰斩（2.03 M → 1.087 M）。

详细机制与平台差异见 [`../platform/windows.md`](../platform/windows.md)、
[`../platform/linux.md`](../platform/linux.md)。

## 5. 观测

`Pipeline::GetThreadPoolStats()` → `ThreadPoolStats{rounds, turns, blockedRounds, idleRounds}`，
基准在 `Stop()` 之前打印 `poolstats,<label>,<rep>,...`。

- `rounds`：一次槽循环；`turns`：一次成功的 `StepOnce`；
- `blocked`：所有 worker 都因输出边满而停下（背压）；
- `idle`：本槽没活干。

读法：`blocked` 高说明背压、`idle` 高说明没喂饱；两者都是**次数**，不是时间。

## 6. 现状与边界

| 场景（5 s × 3） | pool 0 | pool 4 | 比值 |
|---|---|---|---|
| L4cap16（MSVC） | 3.267 M | 3.019 M | 92–94% |
| L16cap8（MSVC） | 2.221 M | 1.411 M | 64% |
| L4/L16（WSL2） | — | — | ≈100% / 反超 |

- 残余 6–8% 是轮询设计本身：每秒约 4 M 次空 `StepOnce`，且每批只拿约 2.4 条
  （p0 等满 4 条）。要补上需要唤醒协议，属于重新设计。
- **非目标**：提高吞吐上限；替代多进程/无锁队列。
