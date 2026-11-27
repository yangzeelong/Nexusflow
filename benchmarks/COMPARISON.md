# NexusFlow vs TBB vs Taskflow vs ai-pipe vs a hand-written chain

Draft, 2026-11-26 (first measured 2026-11-25; re-measured after the fairness
fixes below). Numbers on this machine, both platforms, same harness parameters.
Everything below is a comparison of *pipeline throughput*, not of feature sets.

## What was run

All six implementations process the same six-module chain:

    source → 4 processing stages → sink

Each stage does the same 500-iteration `sin(x) * cos(x)` loop per frame
(`benchmarks/BenchmarkNodes.hpp` `RunCompute` and
`benchmarks/comparison/common.hpp` `Work` are the same code). Four workers,
queue capacity 64 (`--capacity 64`; NexusFlow also supports unbounded edges, but
64 matches TBB's token pool, ai-pipe's buffer and raw's ring), 60 s × 3 for the
loaded runs, 30 s × 3 for the empty ones,
process pinned to eight logical CPUs (`--cpus 8`), 1 s warm-up for the
comparison programs and one discarded repetition for NexusFlow, before every
measured window. Throughput counts frames delivered *inside* the window;
the shutdown drain is not part of it.

* raw (`raw_pipeline.cpp`): one thread per stage, one bounded SPSC ring per
  edge — the lower bound the frameworks are measured against
* TBB, `tbb::flow::graph` (`tbb_graph.cpp`, four
  `function_node<Frame, Frame, serial>`)
* TBB, `tbb::parallel_pipeline` (`tbb_pipeline.cpp`, five
  `serial_in_order` filters, `max_live_tokens` = capacity, arena bounded to
  four threads)
* Taskflow master (`tf_pipeline.cpp`, `tf::Pipeline` with five serial pipes)
* NexusFlow (`--topology linear --modules 6 --capacity 64`), an active source
  module plus four stages and a sink, with the default one-thread-per-module
  setting and with `--pool-size 4`
* ai-pipe `c33edf1` (`ai_pipeline.cpp`): the same six-node chain on
  `EngineConfig::stream(4, 64)`, string-keyed port maps, drop-on-full instead of
  blocking

Windows and WSL2 run the same tree: `2f2d021` plus the passive-source fix
(`2593711`) and the comparison changes; the WSL2 build is an rsync of that tree
into `/root/nf7`, same CMake flags. Per-run rows, not just the medians below,
are in `benchmarks/comparison/results/`.

### Conditions

| | Windows | WSL2 |
| --- | --- | --- |
| OS | Windows 11 Pro 10.0.26200 | Kali Rolling, kernel 6.18.58.1-microsoft-standard-WSL2 |
| CPU | Core i7-14700KF, 8 P + 12 E, 28 logical | same, 28 logical visible |
| NexusFlow build | MSVC 14.40 (VS 17.10), `/MD /O2 /Ob2 /DNDEBUG`, CMake `Release` | g++-11 11.4.0, `-O3 -DNDEBUG`, CMake `Release` |
| Comparison programs | MSVC 14.40, `/O2 /DNDEBUG /std:c++20` | g++-11 11.4.0, `-O3 -DNDEBUG -std=c++17` (Taskflow `-std=c++20`) |
| ai-pipe build | `c33edf1` (2026-02-12), MSVC 14.40 `/O2 /Ob2 /DNDEBUG /std:c++20`, static `ai_pipe.lib` | same tree, g++-11 `-O3 -DNDEBUG -std=c++20`, static `libai_pipe.a` |
| TBB | oneTBB 2021.13 (`tbb12.dll`) | oneTBB 2021.13, built from `v2021.13.0` into `/root/onetbb-install` (`libtbb.so.12.13`) |
| Taskflow | `9d9cea0` (2026-10-14), header-only | same tree |
| Clock | TSC 3.41762 GHz, 1 ms timer resolution held for the run | TSC 3.41762 GHz |
| Pin | first 8 logical CPUs (`--cpus 8`, process mask) | first 8 of 28 (`sched_setaffinity`) |

Both sides are compiled the way each platform's CMake `Release` compiles the
NexusFlow library: MSVC has no `-O3`, `/O2` *is* its maximise-speed switch and
CMake passes `/O2 /DNDEBUG`; GCC's `Release` is `-O3 -DNDEBUG`. The comparison
programs therefore use exactly those flags. An earlier revision of this
comparison built the WSL2 programs with `-O2` and no `NDEBUG`, one optimisation
level below the NexusFlow build on the same machine.

One caveat applies to the per-platform tables below:

* Run-to-run spread is real on this box: the same raw binary measured
  11.09–12.00 M fps over three Windows runs and 9.72–11.81 M fps over three WSL2
  runs. Compare rows inside one session; do not mix sessions or reuse a number
  from an older build.
* ai-pipe is a drop engine, not a blocking one: when the first edge is full it
  discards (oldest first) and the producer keeps offering. At four workers that
  discarded 54% of pushes on Windows and 4% on WSL2 in the empty case; the
  `dropped` column is the three-run total. Throughput still counts only frames
  that reached the sink, so the rows are comparable, but ai-pipe never had to
  slow the producer the way the other five do.
* ai-pipe is 6.2× slower on Windows than on WSL2 (85.5 against 11.7 µs of CPU
  per frame). The vendor's own probe on this machine reproduces it (34.5 K fps,
  141 µs/frame unpinned), so the row is the library on this platform, not the
  probe.

## Table 1 — empty stage (framework cost only)

`--iters 0`, so every number is per-message overhead: routing, queues,
scheduling, latency stamps.

| implementation | Windows fps | µs/frame | µs CPU/frame | cores | WSL2 fps | µs/frame | µs CPU/frame | cores |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| raw | 11.70 M | 0.09 | 0.53 | 6.16 | 10.92 M | 0.09 | 0.58 | 6.33 |
| Taskflow | 3.84 M | 0.26 | 1.01 | 3.90 | 4.74 M | 0.21 | 0.84 | 3.97 |
| NexusFlow (default threads) | 2.97 M | 0.34 | 1.97 | 5.80 | 3.46 M | 0.29 | 1.66 | 5.75 |
| NexusFlow (pool 4) | 2.37 M | 0.42 | 1.67 | 3.95 | 2.96 M | 0.34 | 1.35 | 4.00 |
| TBB parallel_pipeline | 2.04 M | 0.49 | 1.94 | 3.95 | 3.09 M | 0.32 | 1.29 | 4.00 |
| TBB flow::graph | 1.12 M | 0.90 | 2.66 | 2.95 | 1.11 M | 0.90 | 2.71 | 3.00 |
| ai-pipe | 0.055 M | 18.27 | 85.52 | 4.69 | 0.342 M | 2.93 | 11.65 | 4.00 |

Raw is the floor: a frame crosses five SPSC rings and nothing else. Every
framework pays 2–10× that for what it adds; ai-pipe pays 203× on Windows and 33×
on WSL2. Taskflow is the cheapest because it adds no queue at all; TBB's graph is
the most expensive of the queued frameworks because every hop crosses a queue
node and the scheduler. NexusFlow's default setting sits between them, and it
buys that position with cores: 5.75–5.80 against Taskflow's 3.9. `--pool-size 4`
brings the core count down to 3.95–4.00 but keeps only 80% (Windows) / 86% (WSL2)
of the default throughput, because a pool slot has to be scheduled between turns.

## Table 2 — 500-iteration compute per stage

| implementation | Windows fps | µs/frame | µs CPU/frame | cores | WSL2 fps | µs/frame | µs CPU/frame | cores |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| raw | 450 K | 2.21 | 13.32 | 6.02 | 408 K | 2.45 | 15.11 | 6.17 |
| Taskflow | 483 K | 2.07 | 7.97 | 3.85 | 324 K | 3.09 | 12.14 | 3.92 |
| NexusFlow (default threads) | 415 K | 2.41 | 14.28 | 5.90 | 403 K | 2.48 | 14.69 | 5.91 |
| NexusFlow (pool 4) | 282 K | 3.55 | 13.96 | 3.93 | 221 K | 4.53 | 18.14 | 4.00 |
| TBB parallel_pipeline | 483 K | 2.07 | 8.19 | 3.96 | 335 K | 2.98 | 11.94 | 4.00 |
| TBB flow::graph | 301 K | 3.32 | 9.80 | 2.95 | 217 K | 4.62 | 13.85 | 3.00 |
| ai-pipe | 54 K | 18.42 | 88.80 | 4.78 | 105 K | 9.52 | 45.71 | 4.81 |

NexusFlow's default setting uses 5.90–5.91 cores because every module has its own
thread; with `--pool-size 4` it drops to 3.93–4.00 cores and 68% (Windows) / 55% (WSL2)
of the throughput.

**This table is not a framework ranking.** The identical loop compiles
differently. On MSVC, Taskflow's lambda is inlined into the pipe body while
NexusFlow's `RunCompute` stays a separate frame: 7.97 vs 14.28 µs of CPU per
frame, 1.8×. On WSL2 with g++-11 the same pair is 12.14 vs 14.69 µs, 1.2×. So
the Windows loaded table is partly a codegen artifact; the WSL2 pair is the
fairer read, and there NexusFlow's default setting is the fastest of the six
implementations. ai-pipe's Windows row barely moves from empty to compute
because its engine cost (85.5 µs/frame) is twice the stage work; on WSL2 the row
is work-bound (45.7 against 11.7 µs of CPU per frame).

## Latency (p50 / p95 / p99, µs)

| implementation | Windows | WSL2 |
| --- | --- | --- |
| raw | 288 / 454 / 554 | 381 / 777 / 1,060 |
| Taskflow | 6 / 11 / 15 | 11 / 15 / 30 |
| TBB parallel_pipeline | 131 / 183 / 217 | 197 / 232 / 279 |
| TBB flow::graph | 178 / 309 / 341 | 286 / 359 / 497 |
| NexusFlow (default) | 498 / 692 / 748 | 470 / 716 / 1,020 |
| NexusFlow (pool 4) | 684 / 1,020 / 1,176 | 948 / 1,096 / 1,576 |
| ai-pipe | 1,319 / 1,761 / 1,940 | 720 / 918 / 1,050 |

These are the 500-iteration runs, measured at the sink from the stamp the source
put on the frame. Taskflow's latency is low because a token stays on its line —
there is no queue between stages. Raw's rings add a hop but no batching, so it
sits above Taskflow and below TBB's graph, whose unbounded queue grows with the
backlog. NexusFlow is the highest of the blocking five: the batch of four makes
a frame wait for three others at every hop, the source module runs on its own
thread and pushes into the first queue, and pool 4 adds the wait for a slot on
top of that. ai-pipe sits above everything, and its Windows p50 (1.3 ms) is 1.8×
its WSL2 p50 (0.72 ms), the same platform gap its throughput shows.

## Per-frame bill

The same runs as Tables 1 and 2, one row per implementation and platform:
throughput, CPU per frame, the same cost in cycles, frames dropped,
and the process working set. `cycles/frame` is CPU seconds × the TSC rate
`pipeline_benchmark` prints (3.4176 GHz); the NexusFlow rows use the harness's
own `cyclesPerMsg`. `dropped` is the total over the three runs of a cell. For
NexusFlow it is the harness's `droppedAtStop` (its `droppedWhileRunning` is zero
everywhere), so nothing is lost in the steady state; the four blocking programs
report 0 by construction. ai-pipe drops by design — its cell is the window total,
4% of pushes on WSL2 and 54% on Windows in the empty case, about 60% under load
on either platform. RSS is the working set at the end of a run, so it includes
the framework's own footprint.

### Empty stage (`--iters 0`)

| implementation | platform | throughput (M frames/s) | µs CPU/frame | cycles/frame | dropped | RSS MiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| raw | Windows | 11.703 | 0.53 | 1,800 | 0 | 5.7 |
| raw | WSL2 | 10.923 | 0.58 | 1,982 | 0 | 4.3 |
| Taskflow | Windows | 3.837 | 1.01 | 3,449 | 0 | 6.0 |
| Taskflow | WSL2 | 4.739 | 0.84 | 2,864 | 0 | 4.4 |
| NexusFlow (default) | Windows | 2.967 | 1.97 | 6,723 | 51 | 5.3 |
| NexusFlow (default) | WSL2 | 3.459 | 1.66 | 5,662 | 155 | 4.7 |
| NexusFlow (pool 4) | Windows | 2.368 | 1.67 | 5,724 | 8 | 5.3 |
| NexusFlow (pool 4) | WSL2 | 2.962 | 1.35 | 4,616 | 16 | 4.7 |
| TBB parallel_pipeline | Windows | 2.040 | 1.94 | 6,613 | 0 | 6.7 |
| TBB parallel_pipeline | WSL2 | 3.093 | 1.29 | 4,420 | 0 | 5.3 |
| TBB flow::graph | Windows | 1.115 | 2.66 | 9,081 | 0 | 6.8 |
| TBB flow::graph | WSL2 | 1.107 | 2.71 | 9,263 | 0 | 5.4 |
| ai-pipe | Windows | 0.055 | 85.52 | 292,285 | 6,116,669 | 7.3 |
| ai-pipe | WSL2 | 0.342 | 11.65 | 39,803 | 1,203,976 | 4.9 |

### 500-iteration compute per stage

| implementation | platform | throughput (M frames/s) | µs CPU/frame | cycles/frame | dropped | RSS MiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| raw | Windows | 0.452 | 13.32 | 45,526 | 0 | 5.7 |
| raw | WSL2 | 0.408 | 15.11 | 51,637 | 0 | 4.5 |
| Taskflow | Windows | 0.483 | 7.97 | 27,252 | 0 | 6.0 |
| Taskflow | WSL2 | 0.324 | 12.14 | 41,490 | 0 | 4.6 |
| NexusFlow (default) | Windows | 0.415 | 14.28 | 48,798 | 126 | 5.9 |
| NexusFlow (default) | WSL2 | 0.403 | 14.69 | 50,194 | 96 | 5.5 |
| NexusFlow (pool 4) | Windows | 0.282 | 13.96 | 47,696 | 0 | 5.8 |
| NexusFlow (pool 4) | WSL2 | 0.221 | 18.14 | 61,997 | 0 | 5.4 |
| TBB parallel_pipeline | Windows | 0.483 | 8.19 | 27,976 | 0 | 6.7 |
| TBB parallel_pipeline | WSL2 | 0.335 | 11.94 | 40,799 | 0 | 5.5 |
| TBB flow::graph | Windows | 0.301 | 9.80 | 33,505 | 0 | 6.7 |
| TBB flow::graph | WSL2 | 0.217 | 13.85 | 47,320 | 0 | 5.6 |
| ai-pipe | Windows | 0.054 | 88.80 | 303,482 | 17,482,136 | 7.2 |
| ai-pipe | WSL2 | 0.105 | 45.71 | 156,214 | 29,675,773 | 5.0 |

## Complete table

All 28 measured cells in one place: medians of three runs per cell, both
workloads, both platforms.

| workload | implementation | platform | throughput (M frames/s) | µs/frame | µs CPU/frame | cores | cycles/frame | p50 | p95 | p99 | dropped | RSS MiB |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| empty | raw | Windows | 11.703 | 0.09 | 0.53 | 6.16 | 1,800 | 14 | 29 | 30 | 0 | 5.7 |
| empty | raw | WSL2 | 10.923 | 0.09 | 0.58 | 6.33 | 1,982 | 25 | 31 | 49 | 0 | 4.3 |
| empty | Taskflow | Windows | 3.837 | 0.26 | 1.01 | 3.90 | 3,449 | 0 | 1 | 1 | 0 | 6.0 |
| empty | Taskflow | WSL2 | 4.739 | 0.21 | 0.84 | 3.97 | 2,864 | 0 | 1 | 1 | 0 | 4.4 |
| empty | TBB `parallel_pipeline` | Windows | 2.040 | 0.49 | 1.94 | 3.95 | 6,613 | 0 | 35 | 39 | 0 | 6.7 |
| empty | TBB `parallel_pipeline` | WSL2 | 3.093 | 0.32 | 1.29 | 4.00 | 4,420 | 0 | 24 | 30 | 0 | 5.3 |
| empty | TBB `flow::graph` | Windows | 1.115 | 0.90 | 2.66 | 2.95 | 9,081 | 45 | 60 | 70 | 0 | 6.8 |
| empty | TBB `flow::graph` | WSL2 | 1.107 | 0.90 | 2.71 | 3.00 | 9,263 | 3 | 64 | 75 | 0 | 5.4 |
| empty | NexusFlow (default) | Windows | 2.967 | 0.34 | 1.97 | 5.80 | 6,723 | 62 | 94 | 141 | 51 | 5.3 |
| empty | NexusFlow (default) | WSL2 | 3.459 | 0.29 | 1.66 | 5.75 | 5,662 | 32 | 78 | 161 | 155 | 4.7 |
| empty | NexusFlow (pool 4) | Windows | 2.368 | 0.42 | 1.67 | 3.95 | 5,724 | 76 | 106 | 131 | 8 | 5.3 |
| empty | NexusFlow (pool 4) | WSL2 | 2.962 | 0.34 | 1.35 | 4.00 | 4,616 | 66 | 80 | 118 | 16 | 4.7 |
| empty | ai-pipe | Windows | 0.055 | 18.27 | 85.52 | 4.69 | 292,285 | 1,639 | 2,172 | 2,388 | 6,116,669 | 7.3 |
| empty | ai-pipe | WSL2 | 0.342 | 2.93 | 11.65 | 4.00 | 39,803 | 339 | 483 | 566 | 1,203,976 | 4.9 |
| compute | raw | Windows | 0.452 | 2.21 | 13.32 | 6.02 | 45,526 | 288 | 454 | 554 | 0 | 5.7 |
| compute | raw | WSL2 | 0.408 | 2.45 | 15.11 | 6.17 | 51,637 | 381 | 777 | 1,060 | 0 | 4.5 |
| compute | Taskflow | Windows | 0.483 | 2.07 | 7.97 | 3.85 | 27,252 | 6 | 11 | 15 | 0 | 6.0 |
| compute | Taskflow | WSL2 | 0.324 | 3.09 | 12.14 | 3.92 | 41,490 | 11 | 15 | 30 | 0 | 4.6 |
| compute | TBB `parallel_pipeline` | Windows | 0.483 | 2.07 | 8.19 | 3.96 | 27,976 | 131 | 183 | 217 | 0 | 6.7 |
| compute | TBB `parallel_pipeline` | WSL2 | 0.335 | 2.98 | 11.94 | 4.00 | 40,799 | 197 | 232 | 279 | 0 | 5.5 |
| compute | TBB `flow::graph` | Windows | 0.301 | 3.32 | 9.80 | 2.95 | 33,505 | 178 | 309 | 341 | 0 | 6.7 |
| compute | TBB `flow::graph` | WSL2 | 0.217 | 4.62 | 13.85 | 3.00 | 47,320 | 286 | 359 | 497 | 0 | 5.6 |
| compute | NexusFlow (default) | Windows | 0.415 | 2.41 | 14.28 | 5.90 | 48,798 | 498 | 692 | 748 | 126 | 5.9 |
| compute | NexusFlow (default) | WSL2 | 0.403 | 2.48 | 14.69 | 5.91 | 50,194 | 470 | 716 | 1,020 | 96 | 5.5 |
| compute | NexusFlow (pool 4) | Windows | 0.282 | 3.55 | 13.96 | 3.93 | 47,696 | 684 | 1,020 | 1,176 | 0 | 5.8 |
| compute | NexusFlow (pool 4) | WSL2 | 0.221 | 4.53 | 18.14 | 4.00 | 61,997 | 948 | 1,096 | 1,576 | 0 | 5.4 |
| compute | ai-pipe | Windows | 0.054 | 18.42 | 88.80 | 4.78 | 303,482 | 1,319 | 1,761 | 1,940 | 17,482,136 | 7.2 |
| compute | ai-pipe | WSL2 | 0.105 | 9.52 | 45.71 | 4.81 | 156,214 | 720 | 918 | 1,050 | 29,675,773 | 5.0 |

### Reading the CPU numbers

The harness counts process CPU seconds, so a thread that polls while waiting is
part of the bill. On the empty chain that makes `µs CPU/frame` partly a measure
of the wait strategy: raw's stages `yield` in a tight loop (0.53 µs/frame, 6.2
cores for six threads), NexusFlow spins and then parks (1.97), TBB's graph
blocks on its queue yet still pays 2.66 because every hop enters the scheduler.
Under the 500-iteration load the threads are busy and the number is real work:
13.3 (raw), 8.0 (Taskflow), 8.2 (TBB pipeline), 9.8 (TBB graph), 14.3
(NexusFlow default) µs/frame on Windows.

Throughput per core on the WSL2 compute runs separates "cheap per frame" from
"cheap per core": Taskflow 83 K fps/core and TBB's pipeline 84 K, against
NexusFlow default 68 K and raw 66 K. Taskflow's line model wins that ratio by
using exactly four workers; NexusFlow pays for its sixth thread and its queues.
ai-pipe is last on both platforms: 22 K fps/core on WSL2 and 11 K on Windows,
where one frame costs 85–89 µs of CPU.

## What this comparison says

1. **Framework overhead per frame**: raw ≪ Taskflow < NexusFlow < TBB
   parallel_pipeline < TBB flow::graph < ai-pipe on both platforms (Table 1).
   Taskflow is the cheapest framework because nothing queues; TBB's graph is the
   most expensive of the queued ones because every hop crosses a queue node and
   its scheduler; ai-pipe pays 203× raw on Windows and 33× on WSL2.
2. **Latency**: Taskflow ≪ TBB parallel_pipeline < TBB flow::graph < raw <
   NexusFlow < ai-pipe. If tail latency matters more than throughput, that
   ordering is the one to look at; NexusFlow's batching is what pushes it to the
   back, and ai-pipe's per-edge queues push it further.
3. **Core efficiency**: Taskflow and TBB's pipeline get the most frames per
   core (3.9 cores for 2–4 M fps); NexusFlow's default one-thread-per-module
   setting spends 5.8–5.9 cores on less work.
4. **Thread pool**: `--pool-size 4` matches the other frameworks' worker count
   but costs 14–20% of the throughput on the empty shape and 32–45% on the
   compute-heavy one (see `PERFORMANCE.md` Part II §14.6 for the same trade in
   the pure-throughput case).

## Where the cost comes from

The throughput numbers above are symptoms; this section is the bill. All of it
was measured on the same machine, same run shape, same 64-byte frame; the
per-frame numbers are tabulated in the previous section.

### Taskflow: what it does not pay for

1. **A token stays on its line.** With `--workers` lines each held by one
   worker, a frame is processed by the same thread from pipe 1 to pipe 5. No
   queue, no wake-up, no cross-core hand-off.
2. **No per-message allocation or atomics.** Frames live in a per-line
   `std::array` indexed by `Pipeflow::line()`; the steady state touches no heap
   and no shared counter except the sink's own.
3. **A cheap executor.** Work-stealing workers with per-worker queues make the
   continuation of a line a local push, not a synchronization event.

On the 500-iteration compute shape it costs **12.1 µs of CPU per frame** at
4 threads (WSL2), against NexusFlow's 14.7 µs.

### NexusFlow: the three bills it pays for generality

| What it pays | Where | Why it exists |
|---|---|---|
| `Message` refcount per hop | `std::shared_ptr` payload across each edge | one payload, many consumers (fan-out, join, sink) |
| A queue crossing per hop | SPSC edge queue, atomic publish even same-thread | decoupling, backpressure, pool scheduling |
| Pool dispatch per turn | `StepOnce`/`OutputRoom`/counters in `ThreadPool::Loop` | `--pool-size n`, metrics, observable state |

Those three are what a general DAG engine buys: dynamic shapes, fan-in/fan-out,
external injection with blocking backpressure, per-module counters, and a
shutdown that drains. Taskflow has none of them, which is exactly why its bill
is smaller.

### ai-pipe: what it pays for runtime flexibility

Measured on the same empty chain, ai-pipe spends **39,803 cycles per frame on
WSL2** — 7.0× NexusFlow's 5,662 — and **292,285 on Windows**, where it is 6.2×
slower than its own WSL2 build (85.5 against 11.7 µs of CPU per frame) and
delivers 55 K fps. It also drops by design: 4% of pushes on WSL2, 54% on Windows.
The code points at four causes:

1. **String-keyed, type-erased ports.** `std::map<std::string, std::any>` is
   consulted per hop; the payload itself goes through `std::any`, so every stage
   pays a type-id check and possibly a copy.
2. **Edge scan per frame.** `ExecutionEngine::Impl::routeToDownstream`
   (`src/execution_engine_impl.cpp:1492`) resolves the destination port at
   runtime instead of pre-compiling it into "the n-th queue".
3. **Per-edge statistics (and join coordination when enabled).** Every message
   bumps per-edge counters; the probe turns the sync coordinator off but keeps
   statistics on, because they are what supplies the drop count.
4. **Busy-wait and drop on a full queue.** `forcePush` spins and evicts; the
   drop path itself burns CPU. At two workers it drops 56.5% of frames.

Its scaling says the same thing without any profiling: an earlier 5 s probe on
this box gave 2 → 4 → 8 workers = 174 K → 353 K → 78.9 K fps, and the 4-worker
WSL2 cell above lands at 342 K, consistent with that peak. More threads make it
slower; that is shared state contending, not cores running out.

### The trade, stated once

| Framework | Model | Wins on | Pays with |
|---|---|---|---|
| raw | hand-written rings, one thread per stage | the lowest per-frame cost measured | no reuse, no metrics, no dynamic shape |
| Taskflow | static pipeline, one thread per line | zero hand-off, zero allocation | no queue, no fan-in/out, no injection, no runtime graph edits |
| TBB parallel_pipeline | static filter chain, tokens | cheap ordering, no graph nodes | fixed chain, no fan-in/out, no injection, no observable queue |
| TBB flow::graph | dataflow nodes + unbounded queues | composability, flow control nodes | a queue and scheduler hop per edge |
| DeepStream | GStreamer plugins over NVDEC + TensorRT `nvinfer`, batched | GPU decode/infer throughput, multi-stream batching, tracker/OSD out of the box | NVIDIA-only, GStreamer/plugin complexity, batch latency, no generic CPU DAG |
| ai-pipe | runtime graph, typed ports, drops | dynamic topology, drop policies, joins | string keys, `std::any`, coordination, drops |
| NexusFlow | static DAG, per-edge queues, opt-in pool | injection/backpressure, join by key, metrics, pool | refcount + queue + dispatch per hop |

None of these is "better"; the benchmark exists to show what each choice costs
on the same shape, so the choice can be made on purpose.

### GPU pipelines

The tables above measure the executor with no GPU work; they are not a claim
that NexusFlow is CPU-only. `extensions/aifootball` is a real GPU pipeline on the
same framework — TensorRT stages wired as ordinary NexusFlow modules — and the
executor does not change for GPU stages. That application is not a benchmark:
its numbers describe one sport, one clip, one ROI. The GPU comparison therefore
has its own plan and directory, `benchmarks/gpu/README.md`: YOLOv8/YOLO11
detection with YOLO-pose or HRNet-W32 second stages, fp16 only, 1–16 streams,
300 s windows, engines built with `--builderOptimizationLevel=3 --noTF32`.

Why the DeepStream row still has no number: it needs the same clip, the same
ONNX→engine builds and the same ROI semantics. `nvinfer` hosts YOLO and ResNet
fine, but its tracker is NvDCF, not ByteTrack, so preset D is not the same
pipeline. It also needs the ~15 GB container and a running Docker engine —
Docker Desktop is installed here but not running, and CUDA, TensorRT, FFmpeg and
the engines are not present. Until that setup exists, the DeepStream row in the
trade table below is architecture, not measurement.

> Planned additions: OpenCV G-API (streaming DAG).
> HPX and Arrow Acero are not measured: neither builds in this WSL2 image
> without a source build, and a 30-minute build per framework would not change
> the conclusion above.

## Known issues found while measuring

* `--topology inject` with `--pool-size 4` first produced `delivered = 64`:
  the pool marked the passive source's output edge as thread-local because the
  source worker and its consumer shared a slot, but the real producer is the
  thread calling `Pipeline::Push`. A local edge does not block when full, so
  `Push` returned `kQueueFull` and the harness stopped the window. Fixed by
  `Worker::SetExternalProducer()` / `ThreadPool::Start()` skipping such edges;
  the numbers above are with the fix.
* The first WSL2 numbers came from a NexusFlow build made out of a snapshot
  (`/root/nf8`) that already had the passive-source fix while the benchmark
  branch did not, and with the image's default g++ 9.4 while Taskflow used
  g++-11. Everything above was re-measured after merging the fix, from one
  tree, with g++-11 for every WSL2 program.
* `tbb::parallel_pipeline` first took `--workers` as `max_live_tokens`, which
  left the arena at its default 28 threads (7.1 cores measured) and the chain
  at four tokens instead of the capacity. It now bounds the arena with
  `global_control` and sizes the tokens from `--capacity`.
* The four blocking comparison programs share `--warmup-ms` (1 s) and count a
  frame only if it reaches the sink inside the window; NexusFlow's harness
  discards one full repetition instead. Warm-up and shutdown drain are excluded
  on both sides. Per-run rows are kept in `results/windows.csv` and
  `results/wsl.csv`.
* ai-pipe runs through `ai_pipeline.cpp`, written for this comparison: same
  `Work` loop, same warm-up/window/pinning, its own sink so latency lands in the
  same histogram, and a `drop,` line because ai-pipe drops instead of blocking.
  The vendor probe (`ai_pipe_match.exe`) reproduces the Windows/WSL2 gap
  (34.5 K fps, 141 µs/frame unpinned), so the row is the library on this box,
  not the probe.
