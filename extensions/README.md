# Extensions

NexusFlow 的可选功能放在 `extensions/<name>/` 下。框架本体不依赖任何扩展：
只有在配置时把扩展列进 `NEXUSFLOW_EXTENSIONS`，它才会被 `add_subdirectory`
进来，核心目录里没有一行扩展专属代码。

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DNEXUSFLOW_EXTENSIONS=aifootball
cmake --build build --parallel 16
```

多个扩展用分号分隔：`-DNEXUSFLOW_EXTENSIONS="aifootball;other"`。名字写错时
配置阶段直接 `FATAL_ERROR`，不会静默跳过。

| 扩展 | 说明 | 额外依赖 |
| --- | --- | --- |
| [`aifootball`](aifootball/README.md) | AI-Football 算法 SDK（RF-DETR 检测 + ByteTracker + HRNet 姿态 + 关键点平滑 + 足球跟踪）、离线 demo app、批量/回放工具，以及触球判定与评估的 Python 工具链。 | CUDA、TensorRT、FFmpeg |

## 写一个新扩展

`extensions/<name>/CMakeLists.txt` 的约定：

1. 先断言自己是从根目录进来的，避免被单独 `cmake` 时得到一个半配置的构建：

   ```cmake
   if(NOT TARGET nexusflow)
     message(FATAL_ERROR "<name> is a NexusFlow extension; configure from the "
                         "repository root with -DNEXUSFLOW_EXTENSIONS=<name>")
   endif()
   ```

2. 自己声明额外的第三方依赖，例如
   `include(${PROJECT_SOURCE_DIR}/cmake/3rdparty/jsoncpp.cmake)`。
   核心只保证 `spdlog::spdlog` 与 `yaml-cpp::yaml-cpp` 通过 `nexusflow`
   目标公开传递（`src/CMakeLists.txt`），其余依赖由扩展负责。
3. 目标命名建议 `Nexusflow::<name>`（alias 到实际目标），install 规则自己声明；
   可执行文件/库的落盘位置由扩展自己决定，不要求与框架产物同目录。
4. 每个扩展可以带自己的 `README.md`、`AGENTS.md`（面向 agent 的扩展专属规程）、
   `docs/`、`tools/` 与测试；框架的 CI/构建不感知这些内容。

验收标准：把 `-DNEXUSFLOW_EXTENSIONS` 设为空时，扩展目录整目录删除也不影响
框架的配置、构建与单测。

## 目录

```
extensions/
  README.md          # 本文件
  aifootball/        # AI-Football 扩展
    CMakeLists.txt
    README.md
    AGENTS.md
    include/aifootball/
    src/
    app/             # 离线 demo（FFmpeg 解码 + JSONL + 渲染）
    tools/
    benchmarks/
    touch_eval/      # 触球判定/评估（Python）
    docs/
```
