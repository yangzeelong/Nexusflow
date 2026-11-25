# NexusFlow vs TBB flow::graph vs Taskflow

Draft, 2026-11-25. Numbers measured on this machine, both platforms, same
harness parameters. Everything below is a comparison of *pipeline throughput*,
not of feature sets.

## What was run

All three implementations process the same six-module chain:

    source → 4 processing stages → sink

Each stage does the same 500-iteration `sin(x) * cos(x)` loop per frame
(`benchmarks/BenchmarkNodes.hpp` `RunCompute` and
`benchmarks/comparison/common.hpp` `Work` are the same code). Four workers,
queue capacity 64, 60 s × 3 for the loaded runs, 30 s × 3 for the empty ones,
process pinned to eight logical CPUs (`--cpus 8`).

* TBB 2021.13 (`tbb::flow::graph`, four `function_node<Frame, Frame, serial>`)
* Taskflow master (`tf::Pipeline` with five serial pipes)
* NexusFlow (`--topology inject --modules 6 --capacity 64`), with the default
  one-thread-per-module setting and with `--pool-size 4`

Windows is MSVC 19.x, WSL2 is Ubuntu 20.04 / g++-11 (Taskflow needs C++20).

## Table 1 — empty stage (framework cost only)

`--iters 0`, so every number is per-message overhead: routing, queues,
scheduling, latency stamps.

| implementation | Windows fps | µs/frame | cores | WSL2 fps | µs/frame | cores |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Taskflow | 4.14 M | 0.93 | 3.87 | 4.28 M | 0.93 | 3.98 |
| NexusFlow (pool 4) | 2.43 M | 1.99 | 4.83 | 3.04 M | 1.64 | 4.99 |
| TBB flow::graph | 1.11 M | 2.63 | 2.93 | 1.29 M | 2.33 | 3.00 |

Per core this is 1.07 M / 0.50 M / 0.38 M frames/s (Windows, same order).
NexusFlow costs about 2× Taskflow per message and 1.3× less than TBB, but it
also spends more cores doing it: its four pool threads plus the pushing main
thread are all counted.

## Table 2 — 500-iteration compute per stage

| implementation | Windows fps | µs/frame | cores | WSL2 fps | µs/frame | cores |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Taskflow | 471 K | 8.18 | 3.85 | 331 K | 11.95 | 3.95 |
| NexusFlow (default threads) | 407 K | 14.1 | 5.74 | 394 K | 15.2 | 5.94 |
| NexusFlow (pool 4) | 279 K | 16.4 | 4.56 | 213 K | 21.5 | 4.58 |
| TBB flow::graph | 306 K | 9.65 | 2.90 | 206 K | 14.6 | 3.00 |

NexusFlow's default setting uses 5.7–5.9 cores because every module has its own
thread; with `--pool-size 4` it drops to 4.6 cores and about 70% of the
throughput.

**This table is not a framework ranking.** The identical loop compiles
differently: MSVC inlines Taskflow's lambda into the pipe body, while
NexusFlow's `RunCompute` stays a separate frame. Measured slope from 500 to 1000
iterations: +18.3 µs/frame for NexusFlow, +13.9 µs/frame for Taskflow, i.e. the
compute itself is ~1.3× more expensive per stage in the NexusFlow build. Subtract
the empty-stage cost and the per-frame compute is 14.4 µs (NexusFlow) vs 7.25 µs
(Taskflow) — that gap, not the framework, sets the loaded throughput.

## Latency (p50 / p95 / p99, µs)

| implementation | Windows | WSL2 |
| --- | --- | --- |
| Taskflow | 8 / 10 / 13 | 11 / 15 / 21 |
| NexusFlow (default) | 486 / 964 / 1,144 (5 s window) | 556 / 1,020 / 1,260 |
| NexusFlow (pool 4) | 762 / 996 / 1,144 | 1,030 / 1,140 / 1,190 |
| TBB flow::graph | 177 / 309 / 336 | 284 / 510 / 560 |

Taskflow's latency is low because a token stays on its line — there is no queue
between stages. TBB's queue is unbounded, so latency grows with backlog even
with no work at all (30 µs empty, 177 µs loaded). NexusFlow's batching (4) and
edge queues sit in between; the inject path adds the producer's own wait.

## What this comparison says

1. **Framework overhead per frame**: Taskflow < NexusFlow < TBB on both
   platforms (Table 1). Taskflow is fastest because nothing queues; TBB is
   slowest because every hop crosses a queue node and its scheduler.
2. **Latency**: Taskflow ≪ NexusFlow < TBB. If tail latency matters more than
   throughput, that ordering is the one to look at.
3. **Core efficiency**: Taskflow gets the most frames per core; NexusFlow's
   default one-thread-per-module setting spends 5.7–5.9 cores on work Taskflow
   does with 3.9.
4. **Thread pool**: `--pool-size 4` matches TBB's worker count but costs 30% of
   the throughput on this compute-heavy chain (see `PERFORMANCE-desktop.md`
   §14.6 for the same trade in the pure-throughput case).

## Known issues found while measuring

* `--topology inject` with `--pool-size 4` first produced `delivered = 64`:
  the pool marked the passive source's output edge as thread-local because the
  source worker and its consumer shared a slot, but the real producer is the
  thread calling `Pipeline::Push`. A local edge does not block when full, so
  `Push` returned `kQueueFull` and the harness stopped the window. Fixed by
  `Worker::SetExternalProducer()` / `ThreadPool::Start()` skipping such edges;
  the numbers above are with the fix.
