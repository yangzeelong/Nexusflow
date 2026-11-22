# Windows 平台事实

> 只记录会影响代码决策的事实，不写教程。数值都是本机（i7-14700KF：8 P 核 + 12 E 核，
> 8 × 2 + 12 = 28 个逻辑处理器）实测，Windows 11 / MSVC 19.4x。

## 1. 定时器粒度：没有亚毫秒睡眠

| 事实 | 数值 |
|---|---|
| 默认定时器分辨率 | **15.625 ms** |
| `timeBeginPeriod(1)` 之后 | 全局分辨率 1 ms |
| `Sleep(1)` 实际时长（有 period(1)） | 平均 **1.907 ms**（min 1.003 / max 2.508） |
| `sleep_for(10µs)`（无 period(1)） | 平均 **15.374 ms**（min 7.308 / max 16.451） |
| `sleep_for(10µs)`（有 period(1)） | 平均 **1.907 ms** |

机制：

- Win32 的睡眠单位是毫秒：`Sleep(DWORD ms)`，没有亚毫秒参数。
- MSVC STL 的 `std::this_thread::sleep_for` 先 `chrono::ceil<milliseconds>` 再
  `_Thrd_sleep_for(ms)` → `::Sleep(ms)`。**任何正的亚毫秒时长都变成 `Sleep(1)`。**
- `Sleep(1)` 按系统 tick 醒来，不按你给的 1 ms。`Sleep(0)` 只是让出剩余时间片
  （等价 yield），不是睡眠。
- 想更细只能用 `CreateWaitableTimerEx(..., CREATE_WAITABLE_TIMER_HIGH_RESOLUTION, ...)`
  （Win10 1803+，约 **0.5 ms**）或自旋（`_mm_pause`）。**两者都到不了微秒。**

跨平台对照（Windows 分辨率 vs Linux hrtimer）见 [`timer-differences.md`](timer-differences.md)。

## 2. 对框架代码的约束

1. **不要在热路径用 `sleep_for` 表达“等几微秒”**：在 Windows 上是 1–15 ms 的停泊。
2. **库代码不要调 `timeBeginPeriod`**：它会抬高定时器中断频率、增加功耗，而且作用范围随版本不同（Win10 2004 之前全局，之后按进程），
   而且不能假设调用方开过。基准 harness 自己开（`TimerResolutionGuard`），库不能依赖。
3. 需要“短等待”时用自旋或可唤醒等待；两者都拿不到微秒时，说明该换设计，而不是调参。
4. 线程池的空闲退避阶梯必须按**轮数**而不是微秒计数——见
   [`../design/thread-pool.md`](../design/thread-pool.md)。

## 3. 绑核与 SMT

- API：`SetProcessAffinityMask`（进程级）/ `SetThreadAffinityMask`（线程级）。
  基准的 `--cpus N` 把进程绑到“前 N 个逻辑处理器”。
- 本机拓扑：8 个 P 核开 SMT → 前 16 个逻辑处理器是 P 线程；再往后是 12 个 E 核。
- 实测后果：
  - `--cpus 16` 对 16 模块最合适（8 物理 P 核的 16 个线程）；
  - `--cpus 18` 起，第 17/18 个线程落到 E 核，性能断崖；
  - 把 18 个模块的池**钉到 16 个逻辑处理器**上更差（18 模块 → 0.419 M，约 −75%），
    因为 SMT 兄弟争用。
- 结论：**基准默认不绑核（`--cpus 0`）**；只在需要隔离噪声时钉到 `pool × 2` 个 P 线程。

## 4. 观测工具

| 工具 | 状态 |
|---|---|
| `wpr.exe`（WPR） | 能采 CPU 采样，ETL 很大（5 s ≈ 400 MB），命令行无法符号化导出 |
| `xperf.exe` | 内核会话只产出几十 KB 且无采样事件；要看图得开 WPA（GUI） |
| Visual Studio 性能探查器 | 可用，但只能手工看 |
| `perf` | 没有 |

所以 Windows 侧的归因主要靠**内建计数器 + 受控 A/B**，采样剖析在 WSL2 侧做
（见 [`linux.md`](linux.md)）。

## 5. 其它

- `GetProcessMemoryInfo` / `PrivateUsage` 与 `WorkingSetSize` 都能取 RSS；
  基准报的是 working set（`benchmarks/ProcessMemory.hpp`）。
- 高精度计时：`QueryPerformanceCounter`（QPC），频率与 TSC 不同；
  周期口径用 `benchmarks/TscClock.hpp` 校准的 TSC。
- 线程创建比 Linux 贵（~100 µs 量级），所以“每模块一线程”在小规模下更划算。
