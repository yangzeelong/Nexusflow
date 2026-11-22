# HARNESS.md — 工程约束

本文件是仓库的**约束总表**：凡是“必须这样做”的流程、代码、测量、文档规则，都写在这里。
[`AGENTS.md`](AGENTS.md) 是给编码 agent 的操作手册（怎么建、怎么跑、代码结构），它引用本文件；
测量方法论的细节在 [`benchmarks/README.md` 的「测量协议」节](benchmarks/README.md#测量协议)。

一句话：**流程约束看这里，操作手册看 AGENTS，测量细节看 `benchmarks/README.md` 的「测量协议」一节。**

## 0. 目录速查

| 想看什么 | 去哪 |
|---|---|
| 怎么构建、怎么跑测试、代码结构 | [`AGENTS.md`](AGENTS.md) §2 / §4 |
| 测量协议（阶梯、重复次数、噪声、口径） | [`benchmarks/README.md` 的「测量协议」节](benchmarks/README.md#测量协议) |
| 性能数据 | `benchmarks/PERFORMANCE-<machine>.md` |
| 平台事实（定时器、绑核、perf） | `docs/platform/` |
| 设计文档与决策 | `docs/design/`、`docs/adr/` |
| 复盘、长文、跨项目对比 | [engineering-notes](https://github.com/yangzeelong/engineering-notes) |

## 1. 角色与流程

1.1 **PR 只能由用户提交**：不自行 push 新分支、不自行开 PR、不自行合并 PR。只有用户明确说
「可以提交 / 开 PR / 合并」之后才做；「看着办」「你觉得呢」「搞一下」都不算授权。日常只做到
工作区改动——把 diff 与验证结果汇报清楚，等用户拍板。

1.2 **提交收敛**：一个 PR 只留一组逻辑提交（代码 / 基准文档 / 仓库文档三类），逐次试跑的碎片
不带进去。合并用 `git reset --soft <base>` 后按组重新 stage，或 `git rebase -i <base>`；
整理完 `git log master..HEAD` 应该一眼读完。验证完的临时实验分支直接删。

1.3 **PR 必须能自解释**：模板见 [`.github/PULL_REQUEST_TEMPLATE.md`](.github/PULL_REQUEST_TEMPLATE.md)。
有架构调整就必须给「之前 / 之后 / 代价」对比表；有新增函数就逐个写清「做什么 / 为什么引入 /
边界」。缺这两样的 PR 不算 ready。会活过这个 PR 的决策另写 ADR。

1.4 **验证四步**：构建退出码 0 → `nexusflow_tests` 全绿 → 性能类改动再跑基准 smoke → CI 绿
再喊 review。

1.5 **汇报**：数字 + 命令原文；跑了就说跑了，没跑就说没跑，拿不准标“未确认”。

## 2. 代码约束

2.1 **计数单写者**：每个模块的 processed / dropped / join-expired / failed 只能有一个写线程
（依据与实现见 `AGENTS.md` §5）。任何并行化方案都必须让模块固定在一条线程上。

2.2 **新机制默认关闭**：默认行为等于上一个版本——`threadPoolSize = 0`、`queueFullPolicy = kBlock`。

2.3 **不新增逐条日志**：丢弃、超时这类高频事件只计数，不打日志（历史上每次丢弃一行 WRN，
1 秒百万行，直接把吞吐打塌）。

2.4 **C++14**：不用 C++17+ 语法；需要 optional 用 `include/nexusflow/common/Optional.hpp`。

2.5 **构建目录固定 `build`**：不新建 `build-release` 之类的名字。

2.6 **公开接口变更同步 `CHANGELOG.md`**；公开头文件里的枚举带 `k` 前缀（`PushResult`、
`QueueFullPolicy`）。

## 3. 测量约束

3.1 方法细节见 [`benchmarks/README.md` 的「测量协议」节](benchmarks/README.md#测量协议)：一条阶梯、每档重复
不少于 3 次、报 `median [min .. max]` 与噪声范围、数字带机器名与 commit。

3.2 **不跨机器、跨 commit 比绝对值**；只比同一台机器上的前后。

3.3 没有前后数字的“性能优化”不算完成。

## 4. 文档约束

4.1 `docs/` 只放两类：`platform/`（会随平台版本变的**事实**）与 `design/`（与实现同源的
**设计**）。复盘、长文、跨项目对比放 `engineering-notes`，本仓只维护链接。

4.2 会活过 PR 的决策写成 ADR，放 `docs/adr/`，一页纸：状态 / 背景 / 决定 /
后果 / 备选 / 什么时候该重看。

4.3 **同一段事实不写两处**：流程约束在本文件，测量在 `benchmarks/README.md`「测量协议」，设计在 `docs/design/`；
其它地方只留指针。

## 5. 禁止事项（速查）

- 不自行 push / 开 PR / 合并
- 不新增 per-drop、per-timeout 日志
- 不跨机器比绝对值
- 不在两处写同一段事实
- 不用 C++17 语法
