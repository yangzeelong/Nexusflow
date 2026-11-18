# PERFORMANCE-laptop.md — 笔记本实测

> 机器：THUNDERBOT ZERO / i7-11800H。**只对本文件写明的机器、commit、工具参数有效。**
> 规则见 [HARNESS.md](HARNESS.md)，怎么跑见 [README.md](README.md)。台式机（i7-14700KF）写 `PERFORMANCE-desktop.md`，两份**不许混读**。

## 0. 环境与口径

| 项 | 值 |
| --- | --- |
| 机器 | THUNDERBOT ZERO，host `YANGYANGYANG` |
| CPU | Intel Core i7-11800H，**8 物理核 / 16 逻辑核** |
| 内存 | 24 GiB |
| 系统 | Windows 11（build 26200） |
| 工具链 | Visual Studio 17 2022（v143），Release / x64 |
| 代码 | `b364c0d` + 未提交的基准工具改动（dirty） |
| 测量日期 | 2026-11-18 |
| 拓扑 | `--topology linear`，`--capacity 8`，空载荷 |
| 次数 | 每档 1 次预热丢弃 + 3 次，报中位数与 `[最小 .. 最大]` |
| 每轮时长 | **3 s**（比历史基线的 10 s 短，噪声更大） |

三个同构基线（`--baseline`，都不分批，所以框架侧用 `--batch 1` 对照）：

| 口径 | 消息长什么样 | 用途 |
| --- | --- | --- |
| `raw` | 普通结构体，**不分配** | 任何框架成本的下界 |
| `raw-alloc` | 每消息 `make_shared` 一个 payload 包装 | 扣掉「分配」这一项 |
| `raw-message` | 直接用框架的 `nexusflow::Message` | 扣掉「Message 自身」这一项 |

### 0.1 术语速查（先看这个再看表）

| 词 | 意思 |
| --- | --- |
| **模块 / 模块数** | 等于线程数。`--modules 3` = source + 1 个中间模块 + sink（source 和 sink 也各算一个模块）。 |
| **每多一个模块** | 在链上多接一个中间模块（多一个线程 + 一条队列 + 一次拷贝），每条消息多花多少 CPU。2 个模块有 1 次传递，8 个模块有 7 次。 |
| **档** | 阶梯里的一个取值，例如「`--modules 4` 这一档」；「比上一档多花」= 相邻两档的差。 |
| **分配** | 每产生一条消息的那一次堆分配。算 `raw-alloc − raw`，按**每消息**计，与模块数无关。 |
| **Message** | 消息被复制一次的开销（shared_ptr + 元数据）。算 `raw-message − raw-alloc`，按**每多一个模块**计。 |
| **调度** | 框架把消息从上游搬到下游那一圈的额外开销（取消息、虚调用、两次时钟、两次拷贝、推队列、计数发布）。算 `框架 − raw-message`，按**每多一个模块**计。 |
| **batch** | 一个 worker 一次取几条消息再处理（`maxBatchSize`，默认 4）。`batch1` = 一次一条，和裸基线同口径。 |

## 1. 线性深度阶梯（CPU，µs/消息，中位数）

| 模块数 | 框架 batch1 | 框架 batch4 | raw | raw-alloc | raw-message |
| --- | --- | --- | --- | --- | --- |
| 2 | 0.957 | 0.820 | 0.191 | 0.316 | 0.437 |
| 3 | 1.559 | 1.425 | 0.606 | 0.979 | 1.092 |
| 4 | 2.730 ★ | 1.894 | 0.942 | 1.121 | 1.254 |
| 6 | 4.210 | 3.140 | 2.162 | 2.294 | 2.363 |
| 8 | 6.169 | 4.781 | 3.415 | 3.595 | 3.995 |

★ 4 个模块 `--batch 1` 三次实测 2.274 / 2.730 / 2.863，中位数 2.730，**spread 22.7%，`unstable=1`，按规则这格不许读**。

## 2. 三层拆分（每多一个模块多花多少，µs）

| 模块数 | 每消息分配 | Message 每多一个模块 | 调度每多一个模块（batch1） | 调度每多一个模块（batch4） |
| --- | --- | --- | --- | --- |
| 2 | 0.125 | 0.121 | 0.520 | 0.383 |
| 3 | 0.373 | 0.057 | 0.234 | 0.167 |
| 4 | 0.179 | 0.044 | 0.492 | 0.213 |
| 6 | 0.132 | 0.014 | 0.369 | 0.155 |
| 8 | 0.180 | 0.043 | 0.311 | 0.119 |

（3 个模块的 0.373 是噪声：相邻档都在 0.13–0.18。分配按「每消息」算，本来就与模块数无关。）

## 3. 结论

1. **Message 每多一个模块只要 0.04–0.12 µs**（就是一次 `Message` 拷贝：shared_ptr + 元数据）。重做 Message / 换容器的上限大约就是这个量级。
2. **框架的钱基本都花在 Module/Worker/Dispatcher 这条链上**：每多一个模块 0.12–0.38 µs（batch4）、0.23–0.52 µs（batch1），是 Message 的 3–5 倍。
3. **batch4 把调度链砍掉约 60%**（0.52→0.38、0.31→0.12），所以默认口径下框架只比裸实现慢约 1.9 倍，而不是 batch1 的 2.5 倍。
4. **每消息分配 0.06–0.18 µs**，不随模块数增长，排在第二。
5. **「深了变慢」不是框架独有的**：裸实现自己也从 0.19 涨到 3.42 µs/消息（约 18 倍），框架的增量只是叠加在这条曲线上。所以深度退化要先问机器/队列，再问框架。

## 4. 观测开销（harness 自己花了多少）

两种量法都做了：

| 量法 | 结果 |
| --- | --- |
| 真实流水线 A/B：`--observe full` vs `--observe none`，5 个模块数 × batch1/batch4 | 差值 −0.12 ～ +0.18 µs/消息，**落在噪声里，测不出来** |
| 微基准：`--micro-observer 5000000`，只跑那两次时钟读 + 一次直方图采样 | **约 40 ns/消息**（三次分别是 42.3 / 40.0 / 41.1 ns） |

结论：观测开销约 **0.04 µs/消息**，在 1 µs 量级的框架成本上约 **4%**；但它比整条流水线的批次噪声（±0.1–0.2 µs）小，所以 A/B 看不出来。

因此：本文件引用的数字**不需要**做「减去观测」的修正。将来若要把结论压到 0.1 µs 以下，就必须先扣掉这 0.04。

## 5. 已知不稳 / 还没测

- 4 个模块 `--batch 1`：spread 22.7%，`unstable=1`，不许读；要么重跑，要么在表里标着。
- 只跑过 3 s × 3 次；10 s × 3 的长跑没做，所以最小~最大偏乐观。
- 容量 / 载荷 / 扇出 / 融合阶梯**还没按新规则重测**（旧表已从 `PERFORMANCE.md` 移出）。
- 观测开销（关掉计时/直方图重跑取差值）还没做。

## 6. 复现

```powershell
# 框架（对照口径：与裸基线同批处理量）
.\build\benchmarks\Release\pipeline_benchmark.exe --topology linear --modules 3 --capacity 8 --duration 3 --repeat 3 --batch 1

# 三种裸基线
.\build\benchmarks\Release\pipeline_benchmark.exe --topology linear --modules 3 --capacity 8 --duration 3 --repeat 3 --baseline raw
.\build\benchmarks\Release\pipeline_benchmark.exe --topology linear --modules 3 --capacity 8 --duration 3 --repeat 3 --baseline raw-alloc
.\build\benchmarks\Release\pipeline_benchmark.exe --topology linear --modules 3 --capacity 8 --duration 3 --repeat 3 --baseline raw-message

# 取数：grep '^stat,' 拼表；grep '^cov,' 查覆盖率
```
