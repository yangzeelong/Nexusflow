# 定时器：Windows 和 Linux 到底差在哪（大白话版）

> 这篇先讲“我们怎么发现问题的”，再讲“为什么会这样”，最后才是“以后怎么写”。
> 所有数字都是本机实测（2026-11-21），复现方式在 §7。

## 0. 三句话版本

1. **Windows 的睡眠是按“拍子”来的**：系统默认每 15.6 ms 才“打一拍”，你申请睡 10 µs，
   也要等到下一拍才能醒。所以民间说法“Windows 睡不了微秒”是对的，但原因不是精度不够，
   是**它只在拍子上醒**。
2. **Linux 没有这个拍子**：想 10 µs 后醒，就真在 10 µs 后安排一次中断。
   它的问题变成“醒来后多久能抢到 CPU”，本机大约多付 60–90 µs。
3. **短等待不要用“时间”写，要用“轮数”写**。`sleep_for(10µs)` 在 Windows 上不是 10 µs，
   是 2 ms（开了 1 ms 精度）或 15.6 ms（没开）。这一条让我们吃了一个 32% 的性能亏。

## 1. 故事：池化的那 32% 去哪了

### 1.1 现象

线程池版本（4 个模块、4 条池线程）在 Windows 上只有“每模块一条线程”版本的
**68%** 吞吐。但同一份代码在 WSL2 上能到 89–93%。同一台机器、同一份代码，
只差一个操作系统——所以问题大概率在“等”上，不在计算上。

### 1.2 我先猜错的三个地方

排查的前两天，我猜了三件事，全错：

| 猜测 | 怎么验证的 | 结果 |
|---|---|---|
| 队列里的原子操作太贵 | 把同线程的边改成“本地模式”，去掉原子发布 | 只有 +0.7%，在噪声里 |
| 池的调度循环每轮只干一件事，太碎 | 加“一轮内多跑几次”，再改成“能跑就一直跑” | 有好转，但只到 68% → 74% |
| 线程被操作系统换下去了 | 看 CPU 占用：4 条线程占 3.93 个核（98.4%） | 不可能——它们几乎一直在 CPU 上 |

第三条最打击人：如果线程 98% 的时间都在 CPU 上跑，那“被换下去”最多解释 2%，
解释不了 32%。

### 1.3 换个办法：看线程到底在等什么

用 `perf` 采两种样本：

- **on-CPU**：线程在 CPU 上跑的时候，时间花在哪些函数上；
- **off-CPU**：线程**不在** CPU 上的时候，卡在哪个调用上（`sched_switch` 事件）。

on-CPU 的结论：池自己的循环（`ThreadPool::Loop` + `StepOnce` + `OutputRoom`）占 21%，
剩下的大头是队列交接。这些能解释一部分差距，但不够。

off-CPU 的结论很干净：7 条 worker 线程总共只“离岗”了 0.54 秒，
而且**100% 卡在 `clock_nanosleep`**，一个 futex 都没有。
翻译过来：它们不是在等锁，也不是在等别人叫醒，而是**在睡觉**。

### 1.4 关键一算

“睡觉”听起来很正常——没活干就睡嘛。于是我又数了一遍：

- 5 秒里，光 sink 那条线程就睡了 **756 次**（每次申请 10 µs）；
- 但它累计的“睡着的时长”是 **1.96 秒**。

除一下：1.96 s ÷ 756 ≈ **2.6 ms**。

**申请睡 10 µs，实际睡了 2.6 ms，差 260 倍。** 这就是那 32%。

### 1.5 根因

`std::this_thread::sleep_for(10µs)` 在 MSVC 上先被**向上取整到 1 毫秒**，
再去调 `Sleep(1)`；而 `Sleep(1)` 要等系统“下一拍”，默认一拍 15.6 ms。
两个因素叠在一起，一次“让出”就变成了 2 ms 级的停泊。

更糟的是：池线程没有唤醒源。生产者往队列里放消息**不会**叫醒池线程，
所以这一次停泊期间，整条流水线就停在这一级。

修法很土，但有效：**先自旋 4096 轮，再让出 4096 轮，最后才睡 1 ms**。
结果：4 模块 68% → 93.6%，16 模块 +44%。

> 教训：短等待用“轮数”写，长等待才用“时间”写。

## 2. Windows 的睡眠为什么这么糙

把 Windows 想成**公交车**：车站每 15.6 ms 来一班车，你什么时候能走，取决于你到站
之后下一班车什么时候来。你申请“10 µs 后走”，系统只能说“等下一班”。

- 这个“班次间隔”叫**系统定时器分辨率**，默认 15.625 ms（64 Hz）。
- `timeBeginPeriod(1)` 把它调成 1 ms。注意：这像“让全城公交改成 1 分钟一班”，
  是有代价的（耗电、干扰省电状态）。Win10 2004 之前它对**整台机器**生效，
  之后按进程算。
- 所有等待 API 的单位都是毫秒：`Sleep(ms)`、`WaitForSingleObject(handle, ms)`。
  没有“睡 10 µs”这个选项。
- 想再细，只有一条路：`CreateWaitableTimerExW(..., CREATE_WAITABLE_TIMER_HIGH_RESOLUTION, ...)`
  （Win10 1803+）。它不是“微秒级”，地板是 **0.5 ms**，但有总比没有好。

还有一个容易忽视的点：**Windows 的等待是“至少睡够”**。它保证你不会早醒，
代价就是“只会晚，不会早”。

## 3. Linux 为什么不这样

Linux 像**出租车**：你说“10 µs 后提醒我”，它就真在 10 µs 后安排一次中断
（`clock_nanosleep` 把到期时间塞进 per-CPU 的 hrtimer 红黑树，精度是纳秒）。
不需要等什么“拍子”。

但出租车也要等红绿灯：醒来之后，线程得排队等 CPU。所以 Linux 的代价不是“量化”，
而是**固定的唤醒 + 调度开销**。本机（WSL2）量出来是 **60–90 µs**：

- 申请 1 µs，实际 59 µs；
- 申请 1 ms，实际 1.07 ms；
- 申请 15 ms，实际 15.09 ms。

这个开销和请求时长基本无关，是一条“固定起步价”。

另外，Linux 也不是完全没有毫秒陷阱：`epoll_wait`/`poll` 的超时单位是**毫秒**，
`select` 是微秒，`ppoll`/`timerfd` 才是纳秒。用错 API，一样会被取整。

## 4. 量出来的地板

### 4.1 Windows

| 你申请 | 没开 period | 开了 period(1) | 高分辨率 timer |
|---|---|---|---|
| 1 µs | 15.5 ms | 2.0 ms | 0.5 ms |
| 100 µs | 15.4 ms | 2.0 ms | 0.5 ms |
| 1 ms | 15.4 ms | 2.0 ms | 1.5 ms |
| 2 ms | 15.5 ms | 3.0 ms | — |
| 5 ms | 15.4 ms | 6.0 ms | — |
| 10 ms | 15.4 ms | 11.0 ms | — |
| 15 ms | 15.8 ms（偶尔 30 ms） | 15.5 ms | — |

怎么读这张表：

- 没开 period：**15 ms 以下全是 15.5 ms**，等于“等下一班车”。
- 开了 period：**1 ms 以下全是 2.0 ms**——不是 1 ms，因为要跨过一个拍子边界再等一个。
  之后每加一档就多 1 ms：2→3、5→6、10→11。
- 高分辨率：地板 0.5 ms，请求 1 ms 时给 1.5 ms。

### 4.2 Linux / WSL2

| 你申请 | 实际 | 多付 |
|---|---|---|
| 1 µs | 59 µs | +58 µs |
| 100 µs | 157 µs | +57 µs |
| 500 µs | 562 µs | +62 µs |
| 1 ms | 1.07 ms | +71 µs |
| 5 ms | 5.078 ms | +78 µs |
| 15 ms | 15.094 ms | +94 µs |

没有台阶，只有一条 60–90 µs 的“起步价”。

## 5. 这套排查方法可以复用

下次怀疑“是不是调度/锁的问题”，按这个顺序走，比猜快得多：

1. **先看 CPU 占用**。线程几乎 100% 在 CPU 上 → 调度不是主因，别在这上面浪费时间。
2. **on-CPU 采样**看“跑的时候在干什么”（`perf record -e cpu-clock`）。
3. **off-CPU 采样**看“不跑的时候在等谁”（`perf record -e sched:sched_switch`）。
   如果 off-CPU 全是 `clock_nanosleep`/`futex`，答案已经写在栈上了。
4. **数次数 × 算平均**。别只看“总时长”，要算“每次平均多久”——
   756 次 × 平均 2.6 ms 这个乘积，比任何猜测都响。
5. **最后才改代码**。改完用同样的方法量一遍，确认数字动了。

我们在第 3 步之前绕了两天，就是因为先猜了“锁”和“cache line”，
而这两个猜测都没有数据支撑。

## 6. 以后怎么写

| 你要等多久 | 怎么写 |
|---|---|
| 10 µs 以内（热路径交接） | 自旋（`_mm_pause`/`Pause`），别睡 |
| 10 µs – 1 ms | 先自旋，再让出（按轮数），最后才考虑高分辨率 timer |
| 1 – 15 ms | Windows 必须 `timeBeginPeriod(1)`，否则会变成 15.6 ms；Linux 直接睡 |
| 15 ms 以上 | 两边都直接睡 |
| 周期任务 | 用**绝对时间**（`sleep_until`），别用 `sleep_for` 累加 |
| 硬实时 | Linux 上 `SCHED_FIFO`；Windows 上提优先级 + 高分辨率 timer |

三句口诀：

- **短等待数轮数，长等待看时间。**
- **Windows 上“睡一下”是最贵的操作之一。**
- **没有量过的“优化”，都是猜测。**

## 7. 复现

Windows 探针：`C:\Users\yangy\.dsh\im\timerprobe.cpp`

```bash
cl /O2 /EHsc /std:c++14 timerprobe.cpp /link winmm.lib
timerprobe.exe noperiod    # 默认分辨率
timerprobe.exe period1     # timeBeginPeriod(1)
```

输出：`mode,call,request_us,n,min_us,p50_us,p90_us,max_us`。

WSL2 探针：

```python
t0 = time.perf_counter_ns()
time.sleep(us / 1e6)          # 走 clock_nanosleep
d = (time.perf_counter_ns() - t0) / 1000.0
```

注意：基准 harness 自己会调 `timeBeginPeriod(1)`，所以仓库里的 Windows 基准数字
默认是“开了 period”的那一列。

## 8. 几个容易记错的事实

| 说法 | 对不对 | 怎么回事 |
|---|---|---|
| “Windows 定时器精度是 15.6 ms” | 半对 | 是**默认分辨率**；可调，且高分辨率 timer 能到 0.5 ms |
| “高精度要用 `ExAllocateTimer` + `EX_TIMER_HIGH_RESOLUTION`” | 错 | 那是内核 API；用户态是 `CreateWaitableTimerExW` + `CREATE_WAITABLE_TIMER_HIGH_RESOLUTION` |
| “内核只在 tick 扫描等待队列” | 过时 | 那是 Vista 之前；现代用每 CPU 定时器队列 + 动态 tick，但可观测行为不变 |
| “Linux 上 `nanosleep(100µs)` 就是 100 µs” | 半对 | 不受 tick 量化，但仍有 60–90 µs 的固定开销 |
| “`timeBeginPeriod` 是全局副作用” | 老黄历 | Win10 2004 起按进程生效 |
| “`sleep_for(0)` 等于不睡” | 对 | 相当于让出 CPU 一次（`Sleep(0)`） |

## 9. 参考

- [Microsoft Learn：timeBeginPeriod](https://learn.microsoft.com/windows/win32/api/timeapi/nf-timeapi-timebeginperiod)
- [Microsoft Learn：SleepEx](https://learn.microsoft.com/windows/win32/api/synchapi/nf-synchapi-sleepex)
- microsoft/STL `stl/src/thread.cpp`：`_Clamped_rel_time_ms_count` + `chrono::ceil<milliseconds>`
- [Linux `time(7)`](https://man7.org/linux/man-pages/man7/time.7.html)
