# 下个版本开工交接单

> 用途：把「下个版本要做什么、从哪开始、怎么验收」写给下一个会话/agent。
> 长期路线图仍在 `TODO.md`；本文件只放交接所需的最短路径，不重复历史条目。
>
> 基线：`master @ e6a8a3b`（PR #23 停机语义、PR #24 目录归位均已合并）。

---

## 0. 开工步骤与硬规则

```bash
git checkout master && git pull
git checkout -b next/<分支名>
```

硬规则见 `AGENTS.md`，必须遵守的几条：

- 不要自动 `git commit`；用户明确说提交才提交。
- C++14，禁用 C++17 语法（结构化绑定、`if constexpr`、`std::optional`、`string_view`、inline 变量）。
- 构建目录固定叫 `build`；并行参数永远写 `--parallel 16`（写错数字会静默不编译）。
- 改完必须：构建退出码 0 + `nexusflow_tests` 全绿；跑了就说跑了，没跑就说没跑。
- 提交前比对 `git diff --stat` 与 `git diff --stat --ignore-cr-at-eol`，两者必须一致。
- **`docs/` 是用户自己的目录，永远不要 `git add docs/`**（`git ls-files docs` 必须为 0）。
- 动并发/计数器前先读 `AGENTS.md` §5：每个计数器只有**一个写线程**，plain `++` + `PublishCounters()`/`ObserveCounters()` 栅栏配对。

---

## 1. 验收基线（动手前先跑一遍，记下数字）

```powershell
Get-Process MSBuild -ErrorAction SilentlyContinue | Stop-Process -Force   # 陈旧节点会导致“exit 0 但没重编”
cmake --build build --config Release --parallel 16                        # 期望 exit 0
.\build\tests\Release\nexusflow_tests.exe                                 # 期望 158 tests / 26 suites，PASSED 158
.\build\benchmarks\Release\pipeline_benchmark.exe --capacity 8 --benchmark_min_time=1s
# 期望 ~1.9M msg/s，Avg. Latency ~11.7 us
```

基准目标需要 `-DWITH_BENCHMARK=ON` 配置过（本机默认是 OFF，验证完记得改回去）。

---

## 2. P0：先清掉的小尾巴（半天量级）

- [ ] **处理 `build/perf-probe` 里的 `perf_probe` 目标**：它还在链接已删除的 `TimerRegistry::Intern`，报 `LNK2019`。仓库内的 `pipeline_benchmark` 已覆盖同等用途，最省事的做法是删掉这个本地脚手架目标（该目录未跟踪，不进仓库）。
- [ ] **处置 5 个未合并远程分支**：`aifootball`、`codex/architecture-roadmap-milestones`、`dev`、`fix/core-hardening`、`refactor/docs-and-format`。逐个看 `git log --oneline master..origin/<branch>`，有未合并的有用提交就留，否则删。
- [ ] **`docs/telemetry.md` §3** 还缺 p95/p99 + `windowItems`/`windowSpanMs` 的说明。该文件不跟踪、属于用户本地文档，改之前先问。

---

## 3. 需要先拍板的三个决策（不定就别动 P1）

1. **Containers / `Result` 怎么接**
   - (a) 源码内化：拷进 `include/nexusflow/`，无外部依赖，符合「核心不依赖扩展」。
   - (b) FetchContent 引入：版本好升级，但增加依赖数量，离线构建要额外的 `FETCHCONTENT_SOURCE_DIR_*`。
   - **注意**：`Init/Start/Stop/DeInit` 改返回 `Result` 是破坏性变更 → 走 major 版本，并同步改 `PipelineBuilder`、examples、扩展、全部用例。
2. **边计数改名** `pushedTotal/poppedTotal` → `enqueuedTotal/dequeuedTotal`（用户明确要求记住，单独 PR，别混进别的改动）。
3. **模块输出命名** `moduleNameA_OUTPUT_moduleNameB`（让边的身份自解释）。会影响 `queueNames` 拼装、`EdgeSnapshot::from/to`、若干测试断言。

---

## 4. P1：下个大版本的核心三件

### 4.1 融合批处理 —— `src/core/Worker.cpp:316` `Worker::RunFusion()`

现状（已读代码确认）：

- `messageCache` 是 `unordered_map<uint64_t, unordered_map<std::string, Message>>`（第 321 行），每个 id 里再套一层哈希表；
- 凑齐一组就立刻处理，而且**一组只装一条**：
  ```cpp
  fusedMessageVec.clear();
  fusedMessageVec.push_back(MakeMessage(std::move(messageMap)));
  m_modulePtr->ProcessBatch(fusedMessageVec);   // 377-381 行
  ```
  → **融合 batch 恒为 1**，与「批处理」的初衷相反；
- 函数开头（第 317 行）就是这条 TODO：`// TODO: 如何优化呢，现在只有单Batch, 并且需要测试下内存占用.`

要做：

- 一次迭代把所有凑齐的组攒进同一个 `fusedMessageVec`，最后只调一次 `ProcessBatch`；
- 内层 `unordered_map<std::string, Message>` 换成小 `vector`（输入数已知 = `expectedInputCount`），按输入边顺序放，省掉每组的哈希表；
- **必须测内存**：半凑齐状态下 `messageCache` 的峰值（`GetSnapshot()` 看不到，用进程 RSS 或加临时统计）。

验收：融合模块 `ProcessBatch` 收到的 batch size > 1（加用例断言）+ 吞吐不低于基线。

### 4.2 线程模型 —— `src/module/ModuleActor.cpp:67`

现状：`m_workThread = std::thread([this]() { m_worker->WorkLoop(); });` —— **每个模块一个线程**，100 个模块 = 100 个线程，每个线程自己 spin/park 拉队列。

方向：worker 池 + 拓扑层并行，或「一个线程跑同层多个模块」。

- **硬约束**：`AGENTS.md` §5 的计数器前提是「每个计数器只有一个写线程」。池化后如果多个线程跑同一个模块的 `Process()`，`m_processedCount` 等就变成多写者 → 要么保证「模块 ↔ 线程」独占，要么退回原子/加锁并同步改 §5 文档。
- **先测量再动手**：现在的 `pipeline_benchmark` 只有 3 个模块，说明不了线程模型的收益；先写一个扫模块数 × `--capacity` 的脚本拿到吞吐/延迟曲线，用数据决定要不要动。

验收：同等吞吐下线程数下降；`WorkerBehaviorTest` / `PipelineLifecycleTest` 全绿；§5 文档同步更新。

### 4.3 热路径 —— `src/module/Module.cpp:65` `ProcessTimed` / `:96` `DispatchOutput`

现状：

- 每帧 `clear()` → `push_back` → 处理 → `clear()`；
- 第 92 行 `LOG_DEBUG("ModuleTiming: module='{}' elapsed_ms={:.3f}", ...)` **无条件构造**（参数在宏外求值）；实测一个 2 ms sleep 的模块在测试里 ~15 ms/次，日志比业务还贵；
- `MessageMeta` 的 module timings 是 `std::map<std::string,double>`，每跳一次模块都写一份。

要做：日志先判 level 或换 `LOG_DEBUG_IF`；timings 链路按需开启（`profileTimers` 配置已存在），容器可换扁平结构。

验收：benchmark 吞吐上升 + `PipelineTimingTest` 仍绿。

---

## 5. P2：工程化

- [ ] **CI**：现在 PR 上没有任何 checks。最小可用：Windows 上 `cmake --build` + 跑 `nexusflow_tests`，再加 Linux（GCC 9+）。
- [ ] **用例数自检**：CI 里比对 `nexusflow_tests --gtest_list_tests` 的实际用例数与 `AGENTS.md` §3 写的数字，不一致就红（现在靠手改，容易忘）。
- [ ] **测试稳定性**：`PipelineMetricsTest.StopAccountsForMessagesTheDispatcherCouldNotPush` 这类依赖 sleep 窗口的，改成显式同步点（已改过一轮，仍需复查）。

---

## 6. 已知坑

改代码前先看 `TODO.md` 末尾那张「关键经验」表。最容易踩的四个：

- 陈旧 MSBuild 节点：构建显示 exit 0 但改动没进二进制 → `Get-Process MSBuild | Stop-Process -Force`。
- `--parallel` 参数写错 → 构建 exit 1，什么都没编。
- PowerShell 改写文件会把 UTF-8 转 GBK（出现 U+FFFD / U+00BE）→ 用 `[System.IO.File]::ReadAllBytes` + `UTF8Encoding($false,$true)` 读写，改完扫 `[char]0x00BE`。
- 跑测试要隐藏窗口：`Start-Process -PassThru -WindowStyle Hidden -RedirectStandardOutput/Error`。

---

## 7. 交接人的建议（我的判断，不是硬性要求，可以覆盖）

### 7.1 推荐开工顺序

1. **P0 那条 `perf_probe`**（十几分钟）：它现在直接编不过，留着会让"全量构建"看起来是红的，掩盖真正的新问题。
2. **CI 先立起来**（P2 第一条，半天）：后面 4.1/4.2/4.3 都是性能改动，没有 CI 就无法证明"没把 158 个用例搞坏"。GitHub Actions 的 runner 成本要先跟用户确认。
3. **4.3 里最便宜的一件**：`LOG_DEBUG`/`LOG_TRACE` 先判 level。`src/core/Worker.cpp` 里还有几处每消息构造的 `LOG_TRACE("Checking message with ID: …")`，和 `Module.cpp:92` 同性质，收益立刻可测（benchmark 只有 3 个模块，日志占比反而高）。
4. **4.1 融合批处理**：它是"batch 恒为 1"，更像正确性问题而不是性能问题，尽早修掉。
5. **4.2 线程模型放最后**：先写测量脚本拿曲线，再决定要不要动；没有数据就不动。

### 7.2 三个决策我倾向于

- **`Result`：选源码内化 (a)**。核心零外部依赖是 `AGENTS.md` 的原则；这仓库已经要传 5 个 `FETCHCONTENT_SOURCE_DIR_*` 才能离线构建，再加依赖只会更难。
- **边计数改名：做，但单独一个 commit**，纯重命名 + 全量 grep（`pushedTotal`/`poppedTotal` 目前分布在 `include/nexusflow/Stats.hpp`、`src/common/Stats.cpp`、`src/pipeline/Pipeline.cpp`、`include/nexusflow/common/SpscQueue.hpp`、5 个用例文件、`README.md`、`AGENTS.md`、`TODO.md`），不要和别的改动混在一个 diff 里。
- **输出命名 `moduleNameA_OUTPUT_moduleNameB`：可以推后**。它是可读性问题，不是正确性；等 P1 的吞吐改完再动，diff 更小、冲突更少。

### 7.3 我想做、但上面没列成条目的事（按价值排序）

- **benchmark 拓扑参数化**：`pipeline_benchmark` 现在固定 3 个模块，测不出线程模型或融合的收益。加 `--modules N` 之类参数，扫 `--capacity` × 模块数，把表格写进 `benchmarks/README.md`。这是 4.2 的前置条件。
- **CI 里的基准守卫**：先用宽松阈值（比如不低于记录的 70%），否则性能 PR 无法自动验证。没有它，4.1/4.3 的收益只能靠人肉对比。
- **`GetSnapshot()` 的拼接成本**：`PipelineSnapshot::ToString()` 和 `GetSnapshot()` 各拼一次字符串，高频打点时值得给 `ToString()` 加个 `AppendTo(std::string&)`。
- **`MetricSamples` 窗口容量**：现在 1024 是编译期常量；p99 的可信度直接取决于窗口大小，值得做成可配（配置项已有 `profileTimers` 的前例）。

### 7.4 风险提示

- **只有 4.2 线程模型可能把框架改坏**：`AGENTS.md` §5 的单写者计数器前提、以及停机里"生产者先关边再 join"的顺序，都是绑在"每模块一个线程"上的。动它需要单独分支 + 测量数据 + 文档同步。
- **4.1 动的是没有专门用例的代码**：`RunFusion()` 目前只被 `MessageMeta`/`PipelineTiming` 间接覆盖，先补一个"融合 batch 大小 > 1"的用例，再改实现。
