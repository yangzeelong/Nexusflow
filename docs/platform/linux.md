# Linux / WSL2 平台事实

> 本机 WSL2：Ubuntu 20.04，内核 `6.18.58.1-microsoft-standard-WSL2`，28 个逻辑处理器，
> g++ 11.4，cmake 3.16（无 ninja）。数值为实测。

## 1. 睡眠粒度：有地板，但不是 tick

`clock_nanosleep` / `nanosleep` 走 hrtimer，按到期时间排序，不受 tick 对齐限制。
但仍有 ~60 µs 的实测地板（WSL2 的虚拟化开销让它偏高）：

| 请求 | 中位 | p10 | p90 | max |
|---|---|---|---|---|
| 10 µs | **68.3 µs** | 67.6 | 81.2 | 138.2 |
| 50 µs | 108.1 µs | 107.1 | 119.1 | 179.4 |
| 100 µs | 158.0 µs | 157.2 | 167.3 | 627.3 |
| 1 ms | 1.0724 ms | 1.061 | 1.080 | 1.254 |

（200 样本，`time.sleep` → `clock_nanosleep`。）

结论：Linux 上“睡 10 µs”是 68 µs 而不是 1.9 ms，比 Windows 好两个数量级（机制对照见
[`timer-differences.md`](timer-differences.md)）；
但**同样不能用来表达“等几微秒”**——热路径的短等待只能用自旋。

## 2. perf 可用性（WSL2）

| 项 | 状态 |
|---|---|
| 硬件 PMU | **未虚拟化**：`perf stat` 报 `<not supported>` |
| `cpu-clock` 软件事件 | **可用**，`perf record -e cpu-clock -F 1999 -g --call-graph fp` |
| `sched:sched_switch` | 可用（off-CPU 分析） |
| `perf` 二进制 | 发行版包装器会抱怨内核不匹配；用显式路径 `/usr/lib/linux-tools/5.4.0-216-generic/perf` |
| 帧指针 | 必须 `-O2 -g -fno-omit-frame-pointer` 重编，否则栈只有 2–3 层 |

注意点：

- `--filter 'prev_comm == "..."'` 的字符串比较在 WSL2 内核上不生效；只带目标命令时，
  perf 只交“被换下的是目标”的事件，换入事件缺失，无法配对算 off-CPU。
  要看 off-CPU 必须 `-a`（系统级）再按 pid 过滤。
- 采样前 `perf list | grep sched_switch` 可能是空的，但 tracepoint 实际存在。

## 3. 绑核与调度

- API：`sched_setaffinity`（基准 `--cpus N` 用“前 N 个逻辑处理器”）。
- 线程创建便宜（~10–20 µs），所以池化在 Linux 上的收益主要是**省核**，不是省创建。
- 实测 16 模块、pool4 时 WSL2 与 p0 打平甚至反超；同一份代码在 MSVC 上只有 63–68%
  （那是 Windows 定时器 bug，见 [`windows.md`](windows.md)）。

## 4. WSL2 的其它差异

| 项 | 说明 |
|---|---|
| `ip` 命令 | 没有（`iproute2` 未装）；网络排查靠 `/etc/resolv.conf` |
| 外网 | 可用；宿主的 TUN/系统代理会把 WSL 流量带出去（Clash 只监听 `127.0.0.1:7897` 也不影响） |
| RSS | `/proc/self/statm`（页数 × 页大小）；基准用 `ProcessMemory.hpp` |
| CPU 时间 | `getrusage(RUSAGE_SELF)` 的 user+sys |
| 文件系统 | `/mnt/c` 跨文件系统很慢；构建/采样都在 `/root` 下做 |

## 5. 采样剖析的结论口径

- self 占比 = 叶帧（`perf script` 每事件的第一个栈帧）计数 / 总样本；
- 火焰图脚本 `fg.py` 取 `perf script` 文本，root 画在底部；
- off-CPU 图需要把 `sched_switch` 配对成“阻塞时长”，再按 1 ms 折 1 个样本：
  这种图只有一根柱通常就是结论本身（都在某一种睡眠里）。
