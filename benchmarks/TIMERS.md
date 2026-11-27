# 定时器粒度：为什么 Windows 上“睡 10 微秒”其实睡 2 毫秒

一句话：Win32 的睡眠单位是毫秒，`Sleep(1)` 又按系统 tick 醒来；默认分辨率 15.625 ms，
`timeBeginPeriod(1)` 之后是 1 ms。所以任何正的亚毫秒 `sleep_for` 在 Windows 上
都是一次 **1.9 ms** 的停泊——而池线程没有唤醒源，一停就是这一级流水线停一个 tick。

## 结论

| 平台 | `sleep_for(10µs)` 实际 | 可用手段 |
|---|---|---|
| Windows（无 period） | 15.374 ms（min 7.308 / max 16.451） | `_mm_pause` 自旋；或高分辨率 waitable timer（~0.5 ms） |
| Windows（period 1 ms） | 1.907 ms（min 1.003 / max 2.508） | 同上 |
| Linux/WSL2 | 68.3 µs 中位 | 自旋；`clock_nanosleep` 仍不适合微秒级 |

修复前 MSVC 线程池 `pool4 / pool0` 只有 68%，修复后 93.6%（L4cap16）；深链 +44%。
根因与补丁见漏洞修复提交 `8e628b0`（`bugfix/pool-idle-backoff`）。

## 三层根因（缺一层都不成立）

1. **计价单位**：`sleep_for` 先 `ceil<milliseconds>`，亚毫秒时长变成 `Sleep(1)`。
2. **tick 对齐**：`Sleep(1)` 按系统 tick 醒来，不是 1 ms 后。
3. **无唤醒源**：池线程停泊后只有超时能醒（`push` 不唤醒池线程），所以停泊 = 停摆。

## 平台事实与设计文档

- 平台事实（定时器表、绑核、SMT、观测工具）：[`../docs/platform/windows.md`](../docs/platform/windows.md)、
  [`../docs/platform/linux.md`](../docs/platform/linux.md)
- 线程池设计（退避阶梯、为什么不用条件变量）：[`../docs/design/thread-pool.md`](../docs/design/thread-pool.md)
- 复盘长文（故事与教训）：
  [为什么 Windows 上“睡 10 微秒”其实睡 2 毫秒](https://github.com/yangzeelong/engineering-notes/blob/master/posts/2026-11-21-windows-timer-granularity.md)

复现命令见 `benchmarks/PERFORMANCE.zh-CN.md` 第二部分 §14.4。
