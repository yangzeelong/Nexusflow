# GPU pipeline benchmark (plan)

Status: plan only. Nothing here has been measured yet; this directory is the
home for the GPU benchmark, separate from `extensions/aifootball` (which is a
real application, not a benchmark).

Goal: compare the same GPU pipeline shapes on NexusFlow and DeepStream 7.1 —
frames/s per GPU, CPU per frame, latency, and where each saturates.

## Presets

| preset | pipeline | models |
| --- | --- | --- |
| A detect | decode → detect → sink | YOLOv8 or YOLO11, n/s, 640 |
| C1 detect + pose (YOLO) | decode → detect → YOLO-pose(crops) → sink | YOLOv8/11 + YOLO-pose |
| C2 detect + pose (HRNet) | decode → detect → HRNet(persons) → sink | YOLOv8/11 + HRNet-W32 |
| D detect + track | decode → detect → track → sink | YOLOv8/11 + ByteTrack (NvDCF on DeepStream) |

## Fixed choices

* precision: fp16 only — no int8, there is no calibration set
* input: 1080p H.264, 30/60 fps
* streams: 1 / 2 / 4 / 8 / 16 concurrent sources
* measurement: 30 s warm-up, 300 s window, frames counted at the sink, GPU
  util/memory sampled, p50/p95/p99
* output: JSONL observations optional; rendering reference-only, excluded from
  throughput

## Matrix

1. detector throughput — {YOLOv8, YOLO11} × batch {1, 4}, one stream: 4 runs
2. second stage — {YOLOv8, YOLO11} × {YOLO-pose, HRNet-W32}, batch 4, one
   stream: 4 runs
3. stream scaling — the best tier-2 combination at 2 / 4 / 8 / 16 streams:
   4 more runs

Twelve runs × 3 repetitions × 300 s ≈ 3 h per platform. At 16 streams the cell
is expected to hit decode or memory limits before the model does; report where
it saturates, do not read that number as a model limit.

## Engine builds

Every engine is built from an ONNX with a dynamic batch dimension, fp16 weights
and fp32 graph I/O (the runtime contract `IInferenceEngine` uses). TensorRT 11
bakes precision from the graph, so fp16 is applied to the ONNX first
(`convert_fp16_onnx.py --io-precision fp32`), and `trtexec` runs with:

    --builderOptimizationLevel=3 --noTF32 --skipInference
    --minShapes=... --optShapes=... --maxShapes=...

`--noTF32` keeps TF32 out, so the only reduced-precision kernels are the fp16
ones — the pattern `extensions/aifootball/tools/model_conversion/build_tensorrt_engines.sh`
already uses.

Tactic selection is not switchable at runtime and is not "off" by default: the
builder always picks a kernel per layer. Level 3 is the TensorRT default (also
what `trtexec` and DeepStream use when no flag is given); level 5 buys a few
percent at several times the build cost. Keep 3 here, build on the measurement
host, and record the level next to the numbers.

| engine | input | min / opt / max batch |
| --- | --- | --- |
| detector: YOLOv8n/s, YOLO11n/s | 3 × 640 × 640 | 1 / 4 / 16 |
| YOLO-pose | 3 × 640 × 640 | 1 / 8 / 16 (persons) |
| HRNet-W32 | 3 × 256 × 192 | 1 / 4 / 8 |

The maximum covers the 8- and 16-stream tiers (DeepStream batches across
streams, NexusFlow batches inside a stream); the optimum matches the
single-stream batch. NMS stays out of the engine on the NexusFlow side;
DeepStream applies its own NMS in `nvinfer` with the same confidence and IoU.

Engines are machine-specific: builder tactics depend on the GPU, so they are
rebuilt on the measurement host (`--timingCacheFile` is optional and only saves
rebuild time, at the cost of pinning tactics). fp16 also needs a sanity check
before any run: compare per-frame detection count and classes against an fp32
ONNX run on ~100 frames — aifootball's RF-DETR fp16 build dropped a person at
0.86 confidence on 22 of 300 frames, and no threshold recovered it.

## Executor mapping

* NexusFlow: one pipeline per stream, batch 1 or 4 inside the stream, JSONL sink.
* DeepStream: one pipeline, `nvstreammux` batches across streams (batch-size 16),
  `nvinfer` per stage, NvDCF for preset D.
* Both sides use the same engines, confidence/IoU and ROI semantics. D is a
  same-shape comparison, not a same-implementation one (ByteTrack ≠ NvDCF).

## Metrics

Frames/s and ms/frame per stream and in aggregate, GPU util and memory, CPU per
frame, p50/p95/p99 at the sink, dropped frames, and per-stream frame counts (a
batched pipeline can starve one stream while the average looks fine).

## Open items

* pick the clip (1080p60, enough people for pose) and a 4-stream/16-stream replay
  layout
* measurement host needs CUDA Toolkit, TensorRT, FFmpeg, the engines, and a
  running Docker engine for the DeepStream 7.1 container
* decide whether DeepStream runs in the container while NexusFlow runs on the
  host, or both in containers with the same GPU access
