# AGENTS.md — NexusFlow 框架开发规则

面向**在本仓库工作的 agent**。人类向总览见 `README.md`。本文件只写框架本体的
规则；扩展专属规程在 `extensions/<name>/AGENTS.md`（AI-Football 见
[`extensions/aifootball/AGENTS.md`](extensions/aifootball/AGENTS.md)）。

---

## 1. 这是什么项目

**NexusFlow**：C++14 的数据流/DAG 流水线框架——`include/nexusflow/` + `src/`。
核心概念：`Message`（Copy-On-Write）、`Module`（每个模块一个工作线程）、
`Pipeline`（YAML 声明式建图 / `PipelineBuilder` 编程式建图）、`Dispatcher`
（每个订阅者各拿一份 Message 拷贝；Message 拷贝只是 shared_ptr 引用计数）、
`MetricsCollector`（命名空间 `nexusflow::profiling`，单轴计时 + p95 滑窗）。

可选功能（AI-Football 算法流水线等）放在 `extensions/<name>/`，通过
`-DNEXUSFLOW_EXTENSIONS=<name>` 启用。**框架核心不得依赖任何扩展**：核心
CMake、`cmake/include.cmake`、`examples/`、`benchmarks/` 里不出现具体扩展名。

| 路径 | 作用 |
| --- | --- |
| `include/nexusflow/` | 公开头文件（Message/Module/Pipeline/PipelineBuilder/ModuleFactory/MetricsCollector/Logging…） |
| `src/` | 框架实现：`base/`（Graph、队列）、`core/`（Worker）、`module/`（ModuleActor）、`dispatcher/`、`pipeline/impl/`、`common/`（MetricsCollector、Logging 实现与测试） |
| `include/nexusflow/common/` | 公开的 header-only 容器/工具：`Any`、`ConcurrentQueue`、`Optional`、`ViewPtr`（`Nexusflow::nexusflow_common` INTERFACE target） |
| `tests/test/`、`src/*/test/` | gtest 用例（`nexusflow_tests`，当前 134 个，见 §3） |
| `examples/` | 3 个纯框架示例：`1-how-to-use`、`2-linear-vision-pipline`、`3-joined-vision-pipline` |
| `benchmarks/` | Google Benchmark 基准（`nexusflow_benchmarks`） |
| `cmake/` | `include.cmake`（第三方依赖 include）、`Summary.cmake`、`3rdparty/*.cmake`（FetchContent 定义） |
| `extensions/` | 可选扩展；机制与新增扩展见 [`extensions/README.md`](extensions/README.md) |
| `tools/` | 仓库级脚本（`clang-format-all.sh`） |

### 代码地图（改哪里找哪里）

| 想改的东西 | 文件 |
| --- | --- |
| 消息容器 / COW | `include/nexusflow/Message.hpp` |
| 模块基类（`Process` / `ProcessBatch` / 广播发送） | `include/nexusflow/Module.hpp` |
| 流水线对外 API | `include/nexusflow/Pipeline.hpp`、`include/nexusflow/PipelineBuilder.hpp` |
| 流水线实现 / 模块线程编排 | `src/pipeline/impl/PipelineImpl.hpp`、`src/module/ModuleActor.hpp`、`src/core/Worker.hpp` |
| 消息分发 | `src/dispatcher/Dispatcher.hpp` |
| YAML → DAG、拓扑排序 | `src/base/Graph.hpp`、`src/base/GraphUtils.*` |
| 模块注册 | `include/nexusflow/ModuleFactory.hpp` |
| 计时器 / profile（`nexusflow::profiling`，p95、单轴） | `include/nexusflow/MetricsCollector.hpp`、`src/common/MetricsCollector.cpp` |
| 运行期快照（边/模块计数、`ToString()`） | `include/nexusflow/Stats.hpp`、`src/common/Stats.cpp` |
| 计数器/快照的命名与并发规则 | 本文件 §5、`docs/telemetry.md`（不入库） |
| 容器/工具（Any、队列、Optional、ViewPtr） | `include/nexusflow/common/*.hpp` |
| 日志 | `include/nexusflow/Logging.hpp`（公开门面：Level/LoggerParam/LogRecord/LogSink/SetSink，不 include spdlog）+ `src/common/Logging.cpp`（spdlog 后端） |
| 每 pipeline 上下文（日志 sink / profiler / 自定义指标 / 停止令牌） | `include/nexusflow/PipelineContext.hpp`、`src/pipeline/PipelineContext.cpp` |
| 单测 | `tests/test/*.cpp`、`src/base/test/`、`src/common/test/` |

---

## 2. 构建

**C++14**（根 `CMakeLists.txt:5` → `set(CMAKE_CXX_STANDARD 14)`）；构建目录统一
叫 `build`（不要新建 `build-release` 之类的名字）。

```bash
# 配置（默认 WITH_TESTING/WITH_BENCHMARK/WITH_EXAMPLES 都是 ON）
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
# 增量构建
cmake --build build --parallel 16
```

启用扩展：`cmake -S . -B build -DNEXUSFLOW_EXTENSIONS=aifootball`（详见
`extensions/README.md`）。

产物（Unix Makefiles）：`build/bin/`（`nexusflow_tests`、示例、扩展 app）；
`build/lib/`（`nexusflow`、扩展库）。多配置生成器（Visual Studio）落在
`build/<subdir>/<Config>/`，例如 `build/src/Release/nexusflow.lib`。

安装/导出：`cmake --install build --prefix <dir>` 会装 `lib/libnexusflow.a`、
`include/nexusflow/**`（含 `include/nexusflow/common/`）与
`lib/cmake/Nexusflow/NexusflowConfig.cmake`（导出 `Nexusflow::nexusflow`、
`Nexusflow::nexusflow_common`，下游 `find_package(Nexusflow CONFIG)`）。
同一次 install 也会把 `spdlog`、`yaml-cpp` 装进同一 prefix（嵌套 FetchContent 默认
不装），因此导出的 target 能自解析依赖。

### 离线构建（无外网）

第三方依赖走 `FetchContent`（`cmake/3rdparty/`：spdlog、yaml-cpp、jsoncpp、
googletest、googlebenchmark）。离线时显式指向本地源码：

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release \
      -DFETCHCONTENT_SOURCE_DIR_SPDLOG=$PWD/build/_deps/spdlog-src \
      -DFETCHCONTENT_SOURCE_DIR_YAML=$PWD/build/_deps/yaml-src \
      -DFETCHCONTENT_SOURCE_DIR_JSONCPP=$PWD/build/_deps/jsoncpp-src \
      -DFETCHCONTENT_SOURCE_DIR_GOOGLETEST=$PWD/build/_deps/googletest-src \
      -DFETCHCONTENT_SOURCE_DIR_GOOGLEBENCHMARK=$PWD/build/_deps/googlebenchmark-src
```

`-DFETCHCONTENT_FULLY_DISCONNECTED=ON` **不行**：它会让 `spdlog::spdlog` /
`GTest::gtest` / `benchmark::benchmark` 这些别名目标不生成。

改名/搬迁构建目录时注意 cache 里的绝对输出目录（`CMAKE_*_OUTPUT_DIRECTORY`）
与生成物里的旧路径（如 `build/CMakeFiles/VerifyGlobs.cmake`）：换目录后先重新
`cmake -S . -B <dir>` 再 build。

### Windows（MSVC，已实测）

```bash
cmake -S . -B build -G "Visual Studio 17 2022" -A x64 -DCMAKE_BUILD_TYPE=Release \
      -DFETCHCONTENT_SOURCE_DIR_SPDLOG=C:/Code/3rdparty/spdlog-1.15.3 \
      -DFETCHCONTENT_SOURCE_DIR_YAML=C:/Code/3rdparty/yaml-cpp-0.8.0 \
      -DFETCHCONTENT_SOURCE_DIR_GOOGLETEST=C:/Code/3rdparty/googletest-1.15.2
cmake --build build --config Release --parallel 16
./build/tests/Release/nexusflow_tests.exe
```

实测：`build/src/Release/nexusflow.lib` 编译通过，`nexusflow_tests` **134 tests
from 23 suites，PASSED 134**（MSVC 19.40）。

---

## 3. 测试

```bash
./build/bin/nexusflow_tests      # Unix；Windows 见上（build/tests/Release/）
```

- 当前 134 个用例（23 个 suite）：消息 7、计时器 8、图 7、Optional 23（TestOptional 18 + OptionalTest 2 + OptionalThrowingTest 3）、Any 5、ConcurrentQueue 7、Dispatcher 4、日志 7、Config 3、MessageMeta 3、SpscQueue 11、SpscMessageStress 1、Pipeline 生命周期 6、Pipeline 错误路径 10、Pipeline 图序 2、Pipeline 计时 1、Pipeline 指标 8、Pipeline 上下文 8、Worker 行为 5、Worker 启动 2、Worker 计时 2。
- 用例数随手改：`./build/tests/Release/nexusflow_tests.exe --gtest_list_tests` 数一遍，别照抄本节。
- 改框架（Message/Module/Pipeline/Dispatcher/Graph/Timer）**必须**跑；不要为了
  让测试通过而改测试语义。

---

## 4. 扩展

- 启用：`-DNEXUSFLOW_EXTENSIONS=aifootball`（分号分隔多个）；未知名字在配置阶段
  `FATAL_ERROR`。
- 框架侧只做 `add_subdirectory("extensions/${ext}" "extensions/${ext}")`；扩展
  自己声明第三方依赖、自己的目标与 install 规则。
- `-DNEXUSFLOW_EXTENSIONS` 为空时，删除整个 `extensions/` 目录不得影响框架构建。
- 新增扩展的写法与验收标准见 [`extensions/README.md`](extensions/README.md)。

---

## 5. 计数器与快照的命名/并发规则

写新的运行期指标时按这一节走，`docs/telemetry.md`（不入库）有完整对照表。

**命名**

| 后缀/形式 | 含义 | 例子 |
| --- | --- | --- |
| `*Total` | **累计计数**："发生了几次"，只增不减；两次快照相减才有意义 | `pushedTotal`、`processedTotal`、`droppedTotal`、`fusionExpiredTotal`、`failedTotal` |
| `*Ms` | 毫秒值；**不要**在毫秒值上用 `*Total`——"total 多少毫秒"在 `items` 旁边读起来像计数 | `sumMs`（样本耗时之和）、`avgMs`、`p95Ms`、`p99Ms`、`minMs`、`maxMs`、`windowSpanMs` |
| 无后缀名词 | 此刻的瞬时值（gauge） | `size`、`capacity` |
| `max*` | 自上次 `ResetMetrics()` 以来的极值（water mark） | `maxSize` |

- **`*Total` 只给计数用**。一个"值的累加和"（耗时、字节、像素）用 `*Ms`/`*Bytes` 之类带单位的
  名字，不要写成 `totalMs`：`items` 与它并列时，`totalMs` 会让读者以为"相对谁的 total"，而
  `sumMs` 读作"这些样本加起来是多少毫秒"。早先的 `TimerStats::totalMs` 就是因此改名为
  `sumMs`（见 `docs/telemetry.md` §1）。
- 不要再造 `Stats` / `Metrics` 这种泛称做结构体名：快照用 `*Snapshot`
  （`EdgeSnapshot`/`ModuleSnapshot`/`PipelineSnapshot`），队列计数用
  `QueueCounters`，采样统计用 `MetricSamples`。
- 模块自报指标名必须带模块名前缀（`"detect.frames"`），因为 context 是整条流水线共享的。

**并发**

- 每个计数器只有一个写者（该模块的 worker 线程 / 该模块的 dispatcher），所以用**普通
  `uint64_t` + `++`**，热路径不付 `lock xadd`（实测 0.3 ns vs 3.4 ns/次）。
- 写者每个循环迭代结束调一次 `PublishCounters()`（release fence）发布；读者先调
  `ObserveCounters()`（acquire fence）再直接读普通变量。读者可能读到"从未同时出现"的组合
  （例如 pushed 已经 `+1` 而 popped 还没跟上），所以 `GetSnapshot()` **先填 modules 再填
  edges**：先读模块、后读它写出去的那条边，就不会看到凭空出现的消息。
- 对外可写的计数器入口（如 `Worker::RecordDropped`）一旦被第二个线程调用，就自动切到
  `m_writerMutex` + 原子加的慢路径，写者数量不再是"约定"而是"检测"。
- `Pipeline::ResetMetrics()` **只清 water mark**（`maxSize`）；`*Total` 在流水线生命周期内
  永不回退，重新 `Start()` 也不清零。写新字段时别打破这条。

---

## 6. 硬规则（务必遵守）

1. **不要自动 `git commit`**。只有用户明确说"提交/commit"时才提交；否则改动一律
   留在工作区，并在汇报里说明未提交。
2. **C++14**，不用 C++17+ 语法（结构化绑定、`if constexpr`、`std::optional`、
   `string_view`、inline 变量、if-init 等）。需要 optional 时用框架内的
   `include/nexusflow/common/Optional.hpp`。
3. **构建目录固定为 `build`**；不要新建或沿用 `build-release` 等其他名字。
4. 改完必须验证：构建退出码 0 + `nexusflow_tests` 全绿；**跑了就说跑了，没跑就
   明确说没跑**，不要含糊。
5. **行尾**：提交前比对 `git diff --stat` 与 `git diff --stat --ignore-cr-at-eol`。
   本仓库工作区已配置仓库局部 `core.autocrlf true`（Windows 下 Git Bash 的
   `git` 默认 `autocrlf=false` 会把整个工作区显示成 CRLF 假 diff）。
6. 汇报带**数字 + 命令原文**；拿不准标"未确认"，不要猜。
