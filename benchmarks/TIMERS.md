# 定时器粒度：为什么 Windows 上"睡 10 微秒"其实睡 2 毫秒

> 这份文档记录 v1.1 线程池排查里最贵的一课。它不是"线程池的固有开销"，
> 而是一个实现 bug：用 `sleep_for(微秒)` 表达"等一会儿"，在 Windows 上
> 被放大成整个系统 tick。修复见 `8e628b0`（`src/core/ThreadPool.cpp`）。

## 1. 结论

1. `std::this_thread::sleep_for(10us)` 在 MSVC 上不是 10 µs：STL 先向上取整到
   1 ms，`Sleep(1)` 再等下一个系统 tick——实测 **1.9 ms**（开 `timeBeginPeriod(1)`）
   或 **15.4 ms**（默认）。
2. 线程池的消费者是轮询的，队列 `push` **不会**唤醒睡着的池线程，所以一次
   "歇一下"就是这一级流水线停一个 tick，cap 浅的边后面整队堵住。
3. 池化线程的退避阶梯因此不能用微秒表达：必须"先自旋、再让出、最后才按毫秒停泊"。
   修复后 MSVC 上 pool4/pool0 从 63–68% 回到 **93–94%**。

## 2. 现象

| 平台 | L4cap16 p0 | L4cap16 p4 | 比值 |
|---|---|---|---|
| Windows / MSVC | 3.267 M | 2.221 M | 68.0% |
| WSL2 / GCC | 3.6 M | 3.5 M | ≈97% |

同一份代码、同参数（linear 4 模块、cap 16、batch 4、5 s × 3）。而且 MSVC 上
**越跑越差**：2 s 73.3%、5 s 67.6%、10 s 63.5%——说明有一个随运行时长变化的量在作怪。

## 3. 根因链

### 3.1 API 层：`sleep_for` 把微秒向上取整成毫秒

MSVC STL（`thread:186-215`）的实现是：

```cpp
const auto _Ms = chrono::ceil<chrono::milliseconds>(_Rel_time);
if (_Ms.count() > 0) _Thrd_sleep_for(_Ms.count());   // -> ::Sleep(1)
```

任何正的亚毫秒时长都变成 `Sleep(1)`。也就是说，源码里的"10 微秒耐心"从
API 语义上就不存在。

### 3.2 内核层：`Sleep(1)` 等的是系统 tick

- 默认定时器分辨率 **15.625 ms**：`Sleep(1)` 实际可能是 0–15.6 ms，平均约 15 ms。
- 调用 `timeBeginPeriod(1)` 把全局分辨率提到 1 ms 后，`Sleep(1)` 实测平均
  **1.9 ms**（min 1.0 / max 2.5）——仍按 tick 边界醒来，不是精确的 1 ms。
- `Sleep(0)` 只是让出剩余时间片（等价 yield），**不是**睡眠。

### 3.3 架构层：池线程没有唤醒源

线程池的消费者靠轮询：`ThreadPool::Loop()` 每轮问自己那组 worker 有没有活。
队列 `push()` 只会唤醒阻塞在 `waitAndPopFor`/条件变量上的线程（p0 的路径），
不会碰池线程。所以池线程一旦睡下去，就只能等下一个 tick 自己醒来——这期间
输入早就到了，边（cap 16）填满，上游 `Broadcast` 阻塞，整条链停 2 ms。

诊断计数（诊断分支，p4、5 s）：sink 所在线程 `sleep10=756` 次，
`tscIdle ≈ 1.96 s` → **≈1.96 s / 5 s**（约 40% 的墙钟在等 tick）。

## 4. 实测：一次 `sleep_for` 到底睡多久

Windows（QPC，各 50 样本）：

| 调用 | min | avg | max |
|---|---|---|---|
| `sleep_for(10µs)`，无 `timeBeginPeriod` | 7.308 ms | **15.374 ms** | 16.451 ms |
| `sleep_for(10µs)`，`timeBeginPeriod(1)` | 1.003 ms | **1.907 ms** | 2.508 ms |
| `sleep_for(50µs)`，`timeBeginPeriod(1)` | 1.502 ms | **1.972 ms** | 2.471 ms |

Linux（WSL2，`time.sleep` → `clock_nanosleep`，200 样本，中位 / p10 / p90 / max）：

| 请求 | 中位 | p10 | p90 | max |
|---|---|---|---|---|
| 10 µs | **68.3 µs** | 67.6 | 81.2 | 138.2 |
| 50 µs | 108.1 µs | 107.1 | 119.1 | 179.4 |
| 100 µs | 158.0 µs | 157.2 | 167.3 | 627.3 |
| 1 ms | 1.072 ms | 1.061 | 1.080 | 1.254 |

两点结论：Linux 也有 ~60 µs 的粒度地板（hrtimer + 调度开销，WSL2 尤甚），
但比 Windows 差两个数量级；**两个平台都不能用 sleep 表达"等几微秒"**。

## 5. 为什么 Windows "不能" 睡微秒

| 层面 | Windows | Linux |
|---|---|---|
| 睡眠 API 单位 | `Sleep(DWORD ms)`，整数毫秒 | `nanosleep/clock_nanosleep`，纳秒 |
| 最小粒度 | 默认 15.625 ms；`timeBeginPeriod(1)` 后 ~1.9 ms | ~50–70 µs（hrtimer） |
| 定时器实现 | 时钟中断 + tick 队列；`Sleep` 只能按 tick 醒来 | hrtimer，按到期时间排序 |
| 高精度手段 | `CreateWaitableTimerEx(..., CREATE_WAITABLE_TIMER_HIGH_RESOLUTION, ...)`：Win10 1803+，约 0.5 ms；仍不是微秒 | `timerfd`、`clock_nanosleep` 亚毫秒级 |
| 让出 CPU | `Sleep(0)` / `SwitchToThread()` | `sched_yield()` |
| 结论 | 微秒级"等待"只有自旋（`PAUSE`）一种手段 | 可以用 `nanosleep`，但仍有 ~60 µs 地板 |

补充：`timeBeginPeriod(1)` 是**全局**副作用，会增加定时器中断频率、抬高功耗；
Win10 1803 之前每进程调用都会影响系统，之后的版本对作用域做过调整，但代价仍在。
所以框架代码不应该依赖它，更不能假设"调用方一定开了"。

## 6. 终端里的 `sleep` 表

终端里的 sleep 精度就是它调用的 API 精度，和 shell 无关：

| 环境 | 命令 | 精度 | 说明 |
|---|---|---|---|
| cmd.exe | `timeout /t 1` | 1 s（整数秒） | 无法表达毫秒 |
| cmd.exe | `ping -n 2 127.0.0.1 >nul` | ~1 s | 常用 hack，不可靠 |
| PowerShell | `Start-Sleep -Milliseconds 1` | 受 tick 约束：默认 ~15.6 ms，开 `timeBeginPeriod(1)` 后 ~1.9 ms | 底层是 `Sleep` |
| Windows 上的 WSL | `sleep 0.001` | ~1.07 ms | 走 Linux `nanosleep` |
| Linux bash | `sleep 0.001` | ~1.07 ms | coreutils，`nanosleep` |
| Linux bash | `usleep 10` | ~60–70 µs | 同样有地板 |

这也解释了"为什么在 Windows 终端里 `timeout /t` 只能按秒"：不是命令写得糙，
是 Win32 没有亚毫秒的睡眠原语。

## 7. 正确写法：自旋 → 让出 → 停泊

```cpp
constexpr unsigned kIdlePauseRounds = 4096;   // ~50-70 us of PAUSE
constexpr unsigned kIdleYieldRounds = 4096;
...
if (spins < kIdlePauseRounds)            { ++spins; Pause(); }        // _mm_pause
else if (spins < kIdlePauseRounds + kIdleYieldRounds) { ++spins; std::this_thread::yield(); }
else                                     { std::this_thread::sleep_for(std::chrono::milliseconds(1)); }
```

- **为什么门槛是 8192 轮**：整链 3 M msg/s、每消息过 3 条边 → 单边到达间隔
  ≈ 0.33 µs，burst 间隙在亚微秒到几微秒。64 轮（≈0.5 µs）正好卡在间隙中间，
  所以旧代码经常掉进 sleep；8192 轮（≈50–70 µs）比稳态间隙大两个数量级，
  只有真正长时间没活才停泊。
- **为什么不无条件自旋**：真空闲（启动、停机、无数据源）会白烧核。
- **为什么不用条件变量**：池里一次交接间隔几十纳秒到几微秒，挂起/唤醒的内核
  往返比自旋贵；实测 cv 版本吞吐腰斩（2.03 M → 1.087 M）。
- **副作用**：全空闲线程进入停泊前会多花几十微秒的 pause/yield；实测 p0 无回归。

## 8. 复现

```bat
:: 测量（QPC 包住 sleep_for，50 次取 min/avg/max）
:: 代码见本会话的 sleepmeas.cpp；要点是 QueryPerformanceCounter 前后取差
:: 基准：修复前 vs 修复后（5 s × 3，cpus 8）
pipeline_benchmark.exe --topology linear --modules 4 --capacity 16 --batch 4 --pool-size 0 --cpus 8 --duration 5 --repeat 3
pipeline_benchmark.exe --topology linear --modules 4 --capacity 16 --batch 4 --pool-size 4 --cpus 8 --duration 5 --repeat 3
```

## 9. 教训

1. **`sleep_for` 低于系统 tick 的部分不可移植**：跨平台代码里"等几微秒"只能用
   自旋或可唤醒等待，不能用 sleep。
2. **计数器要分清"次数"和"时间"**：`idle` 只占 5.3% 的轮次，但每次落进去付出
   2 ms——按次数看会得出"无害"的结论，按时间看就是 40% 的墙钟。
3. **off-CPU 火焰图能定位"等在哪"，但解释要靠时钟语义**：当时图里 100% 卡在
   `clock_nanosleep`，我读成了"正常退避"，实际是"退避单位错了"。
4. **API 名字会骗人**：`sleep_for(microseconds)` 的作者意图在 Windows 上根本不成立，
   平台行为必须实测，不能按字面理解。
