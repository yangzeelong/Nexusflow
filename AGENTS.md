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
`TimerRegistry`（单轴计时 + p95 滑窗）。

可选功能（AI-Football 算法流水线等）放在 `extensions/<name>/`，通过
`-DNEXUSFLOW_EXTENSIONS=<name>` 启用。**框架核心不得依赖任何扩展**：核心
CMake、`cmake/include.cmake`、`examples/`、`benchmarks/` 里不出现具体扩展名。

| 路径 | 作用 |
| --- | --- |
| `include/nexusflow/` | 公开头文件（Message/Module/Pipeline/PipelineBuilder/ModuleFactory/TimerRegistry/Logging…） |
| `src/` | 框架实现：`base/`（Graph、队列）、`core/`（Worker）、`module/`（ModuleActor）、`dispatcher/`、`pipeline/impl/`、`common/`（TimerRegistry 实现与测试） |
| `include/nexusflow/common/` | 公开的 header-only 容器/工具：`Any`、`ConcurrentQueue`、`Optional`、`ViewPtr`（`Nexusflow::nexusflow_common` INTERFACE target） |
| `tests/test/`、`src/*/test/` | gtest 用例（`nexusflow_tests`，当前 79 个，见 §3） |
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
| 计时器（p95、单轴） | `include/nexusflow/TimerRegistry.hpp`、`src/common/TimerRegistry.*` |
| 容器/工具（Any、队列、Optional、ViewPtr） | `include/nexusflow/common/*.hpp` |
| 日志 | `include/nexusflow/Logging.hpp` |
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

实测：`build/src/Release/nexusflow.lib` 编译通过，`nexusflow_tests` **79 tests
from 14 suites，PASSED 79**（MSVC 19.40）。

---

## 3. 测试

```bash
./build/bin/nexusflow_tests      # Unix；Windows 见上（build/tests/Release/）
```

- 当前 79 个用例（14 个 suite）：消息 7、计时器 5、图 7、Optional 23（TestOptional 18 + OptionalTest 2 + OptionalThrowingTest 3）、Any 5、ConcurrentQueue 7、Pipeline 生命周期 5、Pipeline 错误路径 10、Pipeline 图序 1、Pipeline 计时 1、MessageMeta 3、ModuleFactory 1、Worker 行为 4。
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

## 5. 硬规则（务必遵守）

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
