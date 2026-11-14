# AI-Football extension

AI-Football 是跑在 NexusFlow 上的一条足球视频分析流水线，作为本仓库的可选扩展
提供。扩展包含算法 SDK（`aifootball_sdk`）、离线 demo app、回放/批量工具，以及
触球判定与评估的 Python 工具链。

```
视频 → FFmpeg 解码(app) → RF-DETR 检测 → ByteTracker(人/球/锥) → HRNet 姿态
     → 关键点平滑 → 足球跟踪 → observations.jsonl / 渲染
```

## 目录

| 路径 | 内容 |
| --- | --- |
| `include/aifootball/` | 公共门面 `AIFootball.hpp`（嵌入应用只依赖它） |
| `src/` | 算法模块与流水线：`detector/`、`pose/`、`tracker/`（ByteTracker + FootballTracker）、`smoother/`、`inference/`（TensorRT）、`runtime/`（Source/Sink）、`AIFootballPipeline.cpp` |
| `app/` | 离线 app：FFmpeg 解码、`observations.jsonl` 写出、可选渲染；配置在 `app/config/*.yaml` |
| `tools/` | `render_jsonl.py`（JSONL 回放成视频）、`run_all_configs.sh`（批量跑配置）、`serve_range.py`、`model_conversion/`（ONNX → TensorRT engine） |
| `benchmarks/` | `pose_benchmark`（姿态吞吐基准） |
| `touch_eval/` | 触球判定与评估（Python `py/` uv 工程 + 720p 配置 + `run_touch_eval.sh`） |
| `docs/` | `touch_detection_logic.md`（判定机制）、`touch_summary.md`（数字与产物汇总） |
| `AGENTS.md` | 本扩展的开发规程（agent 用） |

## 依赖

- CUDA Toolkit + TensorRT（`NvInfer.h`、`libnvinfer`、`libnvinfer_plugin`）与 NVIDIA GPU；
  模型 `.engine` 由 `tools/model_conversion/` 生成，`app/config` 里的 `enginePath` 是
  Linux 绝对路径，换机器要改。
- app 与 benchmark 需要 FFmpeg 开发文件（解码与写出）。
- 缺 CUDA/TensorRT 时配置会 WARNING，只构建源码级 SDK 壳（不含推理实现）；缺
  FFmpeg 时跳过 app 与 benchmark。Windows 上已验证框架 + SDK 壳可编译（无推理）。

## 构建

在**仓库根目录**配置，打开扩展：

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release \
      -DNEXUSFLOW_EXTENSIONS=aifootball
cmake --build build --parallel 16
```

扩展选项（都有默认值）：

| 选项 | 默认 | 说明 |
| --- | --- | --- |
| `AIFOOTBALL_BUILD_APP` | `ON` | 构建离线 app（需要 FFmpeg） |
| `AIFOOTBALL_BUILD_BENCHMARKS` | `ON` | 构建 `pose_benchmark`（需要 FFmpeg） |
| `AIFOOTBALL_CUDA_ROOT` | 自动探测 | CUDA Toolkit 根目录 |
| `AIFOOTBALL_TRT_ROOT` | 自动探测 | TensorRT 根目录（含 `include/NvInfer.h` 与 `lib/`） |
| `AIFOOTBALL_FFMPEG_ROOT` | 系统路径 | FFmpeg 安装前缀 |

Linux 产物：`build/bin/aifootball_app`、`build/bin/pose_benchmark`、
`build/lib/libaifootball_sdk.a`；Visual Studio 生成器在
`build/extensions/aifootball/<Config>/` 下。

离线构建的 `FETCHCONTENT_SOURCE_DIR_*` 参数与框架相同，见根 `README.md` / `AGENTS.md`。

## 跑 demo

```bash
./build/bin/aifootball_app extensions/aifootball/app/config/coco_fp16_w48.yaml \
  --video_path data/射门1-1080p60.mov \
  --output_dir output/run --max_frames 300 --render
```

产物：`<output_dir>/observations.jsonl`（带 `--render` 时另加 `rendered.mp4`）。
常用参数：`--video_path/-v`、`--max_frames/-n`、`--stride/-S`、`--output_dir/-o`、
`--render/-r`、`--render_path/-R`、`--device/-d`、`--profile_timers`、`--verbose/-V`、
`--quiet/-q`。

配置档（`app/config/`，放进目录就会被批量脚本跑到）：`finetuned_v1-frozen-step2_fp16_w32_fastest.yaml`
（99.6 FPS）、`coco_fp16_w32_fastest.yaml`（97.1 FPS）、`finetuned_v1_fp16_w48.yaml`、
`coco_tf32_w32.yaml`、`coco_fp16_w48.yaml`、`coco_tf32_w48.yaml`（与 Python 参考
严格对等）。每份文件头部注释写着该档的实测吞吐、测试口径与复测命令。

```bash
# 批量（串行跑目录里所有配置；共享 GPU，不要并发）
PYTHON=/root/miniconda3/envs/ai-football/bin/python \
  ./extensions/aifootball/tools/run_all_configs.sh 300 output/configs
```

## 公共 API

嵌入应用只需要门面头：

```cpp
#include <aifootball/AIFootball.hpp>
#include <chrono>
```

```cpp
aifootball::AIFootballContext context;
context.configPath = "coco_fp16_w48.yaml";
context.deviceId = 0;
context.inputQueuePolicy = aifootball::QueuePolicy::Block;
context.roi.enabled = true;
context.roi.width = 1920;
context.roi.height = 1080;
context.roi.points = {{1579.0f, 1067.0f}, {56.0f, 733.0f},
                     {950.0f, 416.0f}, {1796.0f, 493.0f}};

auto pipeline = aifootball::AIFootballPipeline::Create(context);
if (pipeline->Init() != nexusflow::SUCCESS) return false;

aifootball::DecodedFrameView frame;
frame.frameId = frameIndex;
frame.timestampSec = timestampSec;
frame.width = width;
frame.height = height;
frame.strideBytes = width * 3;
frame.data = rgb24Bytes;
frame.dataBytes = static_cast<std::size_t>(width) * height * 3;

auto resultFuture = pipeline->ProcessAsync(frame);
if (!resultFuture.WaitFor(std::chrono::milliseconds(30))) {
    // 继续提交或稍后再取
}
pipeline->Drain();                       // 仅流结束时调用
const auto output = resultFuture.Get();
if (output.status == nexusflow::SUCCESS) {
    // 消费 output.result
}
pipeline->DeInit();
```

`ProcessAsync()` 入队后返回 move-only 的 `ProcessFuture`（`IsReady()` / `Wait()` /
`WaitFor(timeout)` / `Get()`，`Get()` 只能取一次）。`Drain()` 只用于流末，不是
逐帧同步点。

`DecodedFrameView` 接受 packed RGB24：`dataOwner` 非空时 SDK 在结果交付前一直持有
该 owner、不做输入拷贝；为空时在 `ProcessAsync()` 返回前拷贝，调用方可立即复用
解码缓冲。

输入队列默认无界（`maxPendingFrames == 0`，设正值才启用上限）。`QueuePolicy::Block`
（默认）在源队列满时阻塞 `ProcessAsync()` 做背压；`DropOldest` 丢最旧的排队帧并让
其 future 以失败完成；`DropNew` 拒绝当前帧并返回已就绪的失败 future。结果按输入
顺序完成，同时保留配置的检测 batch 策略。`DeInit()` 会停掉内部 actor 图并把剩余
future 以失败完成，调用方应先 drain 并消费结果。

ROI 由嵌入应用通过 `AIFootballContext` 传入（通常解析 YAML 的 `algorithm.roi`），
坐标使用 `roi.width` × `roi.height` 的源帧坐标系，运行时会按需缩放到解码帧尺寸。

## 触球判定与评估

业务逻辑在 `touch_eval/`（Python；C++ 侧的判定实现已删除，app 只写
`observations.jsonl`）：

```bash
cd extensions/aifootball/touch_eval/py
uv sync
uv run touch-eval --observations ../../../../output/<run>/observations.jsonl \
                  --touches_csv ../../../../data/移动传球.touches.csv \
                  --output_dir ../../../../output/<run>
```

判定方法见 `docs/touch_detection_logic.md`；指标口径、当前数字与产物清单见
`touch_eval/README.md` 与 `docs/touch_summary.md`。

## 与框架的关系

`aifootball_sdk` 只依赖框架公开头（`include/nexusflow/`）与 `nexusflow` 目标；框架
核心不含任何 aifootball 代码。把 `NEXUSFLOW_EXTENSIONS` 置空（或直接删除本目录）
不影响框架的配置、构建与单测。
