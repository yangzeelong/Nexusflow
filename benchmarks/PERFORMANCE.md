# CPU measurements — reference desktop

The formal record of measurements on the reference desktop (i7-14700KF), in two
parts:

- **Part I — cross-framework comparison** (NexusFlow / raw / TBB ×2 / Taskflow /
  ai-pipe), measured 2026-11-26;
- **Part II — NexusFlow's own parameter sweeps** (depth, capacity, batch, payload,
  fan-out/join, external injection, thread pool, `--queue-full`, timing
  attribution), from 2026-11-18 on.

English version; the Chinese companion is
[`PERFORMANCE.zh-CN.md`](PERFORMANCE.zh-CN.md). The narrative and per-cause
analysis of Part I live in [`COMPARISON.md`](COMPARISON.md); raw per-run rows are
in [`comparison/results/`](comparison/results/).

The GPU benchmark plan is separate: [`gpu/README.md`](gpu/README.md).

## Part I — cross-framework comparison (2026-11-26)

### Compared repositories

| project | repository | version under test |
| --- | --- | --- |
| NexusFlow | https://github.com/yangzeelong/Nexusflow | `2f2d021` + passive-source fix `2593711` (local, not committed) |
| oneTBB | https://github.com/uxlfoundation/oneTBB | tag `v2021.13.0` |
| Taskflow | https://github.com/taskflow/taskflow | `9d9cea0` (2026-10-14), header-only, C++20 |
| ai-pipe | https://github.com/yangzeelong/ai-pipe | `c33edf1` (2026-02-12), static library |

### Test machine (reference desktop)

| item | value |
| --- | --- |
| CPU | Intel Core i7-14700KF, 8 P + 12 E cores, 28 logical threads |
| RAM | 31.8 GiB |
| OS | Windows 11 Pro 10.0.26200 (build 26200) |
| GPU | NVIDIA GeForce RTX 4070 SUPER, 12,282 MiB, driver 616.56 |
| WSL2 | Kali GNU/Linux Rolling, kernel 6.18.58.1-microsoft-standard-WSL2 |
| Clock | TSC 3.41762 GHz; 1 ms timer resolution held during every run |
| Pinning | first 8 logical CPUs (`--cpus 8`): Windows process mask, WSL `sched_setaffinity` |

This desktop is the reference test machine for future runs. Comparisons across
machines are invalid; comparisons across sessions on this machine carry the
run-to-run spread noted below.

### Toolchain and build options

| side | compiler | options |
| --- | --- | --- |
| NexusFlow (Windows) | MSVC 14.40 (VS 17.10), CMake `Release` | `/MD /O2 /Ob2 /DNDEBUG` |
| NexusFlow (WSL2) | g++-11 11.4.0, CMake `Release` | `-O3 -DNDEBUG` |
| raw / TBB / Taskflow (Windows) | MSVC 14.40 | `/O2 /DNDEBUG /std:c++20` |
| raw / TBB / Taskflow (WSL2) | g++-11 11.4.0 | `-O3 -DNDEBUG -std=c++17` (Taskflow `-std=c++20`) |
| ai-pipe (Windows) | MSVC 14.40, static `ai_pipe.lib` | `/O2 /Ob2 /DNDEBUG /std:c++20`, `/MD` |
| ai-pipe (WSL2) | g++-11 11.4.0, static `libai_pipe.a` | `-O3 -DNDEBUG -std=c++20` |

Both platforms use the same flags their CMake `Release` build uses for
NexusFlow. MSVC has no `-O3`; `/O2` is its maximise-speed switch.

### Test cases

Both cases run the same linear graph; they differ only in the per-stage work.

| case | modules | edges | per-stage work | window |
| --- | ---: | ---: | --- | --- |
| empty | 6 (source + 4 stages + sink) | 5 | none (`--iters 0`) | 30 s × 3 runs |
| compute | 6 (source + 4 stages + sink) | 5 | 500 iterations of `sin(x)*cos(x)` per frame | 60 s × 3 runs |

Common parameters: queue capacity 64 per edge (`--capacity 64`), 4 workers, 8
pinned logical CPUs, 1 s warm-up discarded for the comparison programs and one
full discarded repetition for NexusFlow, delivered frames counted only inside the
window, drain excluded, RSS sampled at the end. `cycles/frame` = CPU seconds ×
3.4176 GHz. NexusFlow's edges also accept an unbounded queue (capacity ≤ 0 falls
back to `ConcurrentQueue`), but the comparison keeps 64 so that NexusFlow, TBB
`parallel_pipeline`, ai-pipe and raw's ring all have the same depth; Part II §4
shows throughput is already flat from capacity 4 upward, so the cap costs
nothing measurable.

| implementation | threads / workers | specifics |
| --- | --- | --- |
| raw | 6 (producer + 4 stages + sink) | bounded SPSC ring per edge (64), `yield` on full/empty |
| Taskflow | 4 workers, 5 serial pipes | token stays on its line; no queue between stages |
| TBB `parallel_pipeline` | 4 arena threads | `max_live_tokens` = capacity (64), `global_control` bounds arena |
| TBB `flow::graph` | 4 (arena) | four `function_node<Frame, Frame, serial>`, unbounded node buffers |
| NexusFlow (default) | 6 module threads | active source module + 4 stages + sink, one thread per module, capacity 64 per edge, batch 4 |
| NexusFlow (pool 4) | 4 pool slots | same graph, `--pool-size 4` (the source is pooled too) |
| ai-pipe | 4 workers | `EngineConfig::stream(4, 64)`, statistics on, sync coordination off, drop-oldest on full |

### Results — empty case (framework cost only)

Medians of three runs. `dropped (running)` is steady-state loss; `droppedAtStop`
is what was still in flight when the window's `Stop()` ran.

| implementation | platform | throughput (M frames/s) | delivered (M frames) | µs/frame | µs CPU/frame | cores | cycles/frame | p50/p95/p99 µs | dropped (running) | droppedAtStop | RSS MiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: |
| raw | Windows | 11.703 | 351.1 | 0.09 | 0.53 | 6.16 | 1,800 | 14/29/30 | 0 | 0 | 5.7 |
| raw | WSL2 | 10.923 | 327.7 | 0.09 | 0.58 | 6.33 | 1,982 | 25/31/49 | 0 | 0 | 4.3 |
| Taskflow | Windows | 3.837 | 115.1 | 0.26 | 1.01 | 3.90 | 3,449 | 0/1/1 | 0 | 0 | 6.0 |
| Taskflow | WSL2 | 4.739 | 142.2 | 0.21 | 0.84 | 3.97 | 2,864 | 0/1/1 | 0 | 0 | 4.4 |
| TBB parallel_pipeline | Windows | 2.040 | 61.2 | 0.49 | 1.94 | 3.95 | 6,613 | 0/35/39 | 0 | 0 | 6.7 |
| TBB parallel_pipeline | WSL2 | 3.093 | 92.8 | 0.32 | 1.29 | 4.00 | 4,420 | 0/24/30 | 0 | 0 | 5.3 |
| TBB flow::graph | Windows | 1.115 | 33.4 | 0.90 | 2.66 | 2.95 | 9,081 | 45/60/70 | 0 | 0 | 6.8 |
| TBB flow::graph | WSL2 | 1.107 | 33.2 | 0.90 | 2.71 | 3.00 | 9,263 | 3/64/75 | 0 | 0 | 5.4 |
| NexusFlow (default) | Windows | 2.967 | 89.0 | 0.34 | 1.97 | 5.80 | 6,723 | 62/94/141 | 0 | 51 | 5.3 |
| NexusFlow (default) | WSL2 | 3.459 | 103.8 | 0.29 | 1.66 | 5.75 | 5,662 | 32/78/161 | 0 | 155 | 4.7 |
| NexusFlow (pool 4) | Windows | 2.368 | 71.1 | 0.42 | 1.67 | 3.95 | 5,724 | 76/106/131 | 0 | 8 | 5.3 |
| NexusFlow (pool 4) | WSL2 | 2.962 | 88.9 | 0.34 | 1.35 | 4.00 | 4,616 | 66/80/118 | 0 | 16 | 4.7 |
| ai-pipe | Windows | 0.055 | 1.6 | 18.27 | 85.52 | 4.69 | 292,285 | 1,639/2,172/2,388 | 6,116,669 | 0 | 7.3 |
| ai-pipe | WSL2 | 0.342 | 10.2 | 2.93 | 11.65 | 4.00 | 39,803 | 339/483/566 | 1,203,976 | 0 | 4.9 |

### Results — compute case (500 iterations per stage)

| implementation | platform | throughput (M frames/s) | delivered (M frames) | µs/frame | µs CPU/frame | cores | cycles/frame | p50/p95/p99 µs | dropped (running) | droppedAtStop | RSS MiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: |
| raw | Windows | 0.452 | 27.1 | 2.21 | 13.32 | 6.02 | 45,526 | 288/454/554 | 0 | 0 | 5.7 |
| raw | WSL2 | 0.408 | 24.5 | 2.45 | 15.11 | 6.17 | 51,637 | 381/777/1,060 | 0 | 0 | 4.5 |
| Taskflow | Windows | 0.483 | 29.0 | 2.07 | 7.97 | 3.85 | 27,252 | 6/11/15 | 0 | 0 | 6.0 |
| Taskflow | WSL2 | 0.324 | 19.4 | 3.09 | 12.14 | 3.92 | 41,490 | 11/15/30 | 0 | 0 | 4.6 |
| TBB parallel_pipeline | Windows | 0.483 | 29.0 | 2.07 | 8.19 | 3.96 | 27,976 | 131/183/217 | 0 | 0 | 6.7 |
| TBB parallel_pipeline | WSL2 | 0.335 | 20.1 | 2.98 | 11.94 | 4.00 | 40,799 | 197/232/279 | 0 | 0 | 5.5 |
| TBB flow::graph | Windows | 0.301 | 18.1 | 3.32 | 9.80 | 2.95 | 33,505 | 178/309/341 | 0 | 0 | 6.7 |
| TBB flow::graph | WSL2 | 0.217 | 13.0 | 4.62 | 13.85 | 3.00 | 47,320 | 286/359/497 | 0 | 0 | 5.6 |
| NexusFlow (default) | Windows | 0.415 | 24.9 | 2.41 | 14.28 | 5.90 | 48,798 | 498/692/748 | 0 | 126 | 5.9 |
| NexusFlow (default) | WSL2 | 0.403 | 24.2 | 2.48 | 14.69 | 5.91 | 50,194 | 470/716/1,020 | 0 | 96 | 5.5 |
| NexusFlow (pool 4) | Windows | 0.282 | 16.9 | 3.55 | 13.96 | 3.93 | 47,696 | 684/1,020/1,176 | 0 | 0 | 5.8 |
| NexusFlow (pool 4) | WSL2 | 0.221 | 13.2 | 4.53 | 18.14 | 4.00 | 61,997 | 948/1,096/1,576 | 0 | 0 | 5.4 |
| ai-pipe | Windows | 0.054 | 3.3 | 18.42 | 88.80 | 4.78 | 303,482 | 1,319/1,761/1,940 | 17,482,136 | 0 | 7.2 |
| ai-pipe | WSL2 | 0.105 | 6.3 | 9.52 | 45.71 | 4.81 | 156,214 | 720/918/1,050 | 29,675,773 | 0 | 5.0 |

### Notes

* CPU/frame on the empty case includes polling: raw `yield`s in a loop,
  NexusFlow spins then parks, TBB blocks. Under load it is real work.
* NexusFlow's `dropped (running)` is zero in all 24 runs (the `cov` rows report
  `midDropped = 0`); `droppedAtStop` is what was still in flight — in queues or
  in a module — when the window's `Stop()` ran, never steady-state loss. The four
  blocking programs report 0 by construction.
* ai-pipe drops by design (drop-oldest): 4% of pushes on WSL2, 54% on Windows
  in the empty case, about 60% under load on either platform.
  Throughput still counts only frames that reached the sink.
* ai-pipe is 6.2× slower on Windows than on WSL2; the vendor probe
  (`ai_pipe_match.exe`) reproduces this on the same machine, so it is the
  library on this platform, not the probe.
* Run-to-run spread: the raw binary alone measured 11.09–12.00 M fps across its
  three Windows runs and 9.72–11.81 M across its three WSL2 runs. Compare rows
  inside one session.
* The Windows compute table is partly a codegen artifact: MSVC inlines
  Taskflow's lambda (7.97 µs CPU/frame) but not NexusFlow's `RunCompute`
  (14.28 µs); on WSL2 the same pair is 12.14 vs 14.69 µs.

### Reproduce

```bash
# NexusFlow library + harness (both platforms)
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel 16

# comparison programs
benchmarks/comparison/build_win.bat          # Windows: cmp_raw, cmp_tbb, cmp_tbbpipe, cmp_tf, cmp_ai
benchmarks/comparison/build_wsl.sh           # WSL2:  /root/cmp_*

# one cell each
build/benchmarks/Release/pipeline_benchmark.exe --topology linear --modules 6 \
  --capacity 64 --cpus 8 --repeat 1 --workload none --duration 30 --label nf-empty-default-r1
benchmarks/comparison/cmp_raw.exe --workers 4 --capacity 64 --cpus 8 \
  --iters 0 --seconds 30 --label empty-r1
```

Per-run CSV rows: `comparison/results/windows.csv`, `comparison/results/wsl.csv`
(header lines document the session; `cmp,`/`csv,` rows are measurements, `rss,`
is the working set, `drop,` exists only for ai-pipe).

## Part II — NexusFlow's own parameter sweeps (same reference machine)

Sections 0–16 below are NexusFlow's own parameter sweeps. The machine is the same as in
Part I (i7-14700KF, 20 physical / 28 logical cores), but the tool parameters, commit and
measurement date are noted in each section; the numbers hold only for this machine and
this commit, and only trends are comparable across machines.

### 0. Environment and protocol

| item | value |
| --- | --- |
| machine | reference desktop; Intel Core i7-14700KF, **20 physical / 28 logical cores** (8 P cores with SMT + 12 E cores) |
| RAM | 32 GiB |
| OS | Windows 11 (build 26200) |
| toolchain | Visual Studio 17 2022 (v143), Release / x64, MSVC 19.40 |
| code | commit `31b4420` (v1.0.0) |
| measured | 2026-11-18 |
| matrix protocol | `--topology linear`, `--capacity 8`, empty payload (capacity / payload / fan-out dimensions are noted per section) |
| repetitions | one discarded warm-up + 3 runs per step (5 runs for the 18/20/24 knee re-test); median and `[min .. max]` reported |
| window | **5 s** |
| observation | `--observe full` (§3 adds an A/B) |

This appendix covers **116 stat cells / 489 coverage assertions** (every `cov,` row is ok;
no run dropped a message).

Three isomorphic baselines (`--baseline`; none of them batches, so the framework side is
compared at `--batch 1`):

| baseline | what a message looks like | purpose |
| --- | --- | --- |
| `raw` | plain struct, **no allocation** | lower bound for any framework cost |
| `raw-alloc` | `make_shared` a payload wrapper per message | subtracts the "allocation" item |
| `raw-message` | the framework's `nexusflow::Message` directly | subtracts the "Message itself" item |

#### 0.1 Terms (read this before the tables)

| term | meaning |
| --- | --- |
| **module / module count** | equals the thread count. `--modules 3` = source + 1 middle module + sink (source and sink each count as a module). |
| **per extra module** | one more middle module on the chain (one thread + one queue + one copy); how much extra CPU each message pays. 2 modules have 1 hop, 16 modules have 15. |
| **step** | one value in the ladder, e.g. "the `--modules 4` step"; "extra per step" = CPU/message difference between adjacent steps. |
| **allocation** | the one heap allocation made per produced message. Computed as `raw-alloc − raw`, per **message**, independent of module count. |
| **Message** | the cost of copying a message once (shared_ptr + metadata). Computed as `raw-message − raw-alloc`, per **extra module**. |
| **dispatch** | the framework's extra cost for moving a message from upstream to downstream. Computed as `framework − raw-message`, per **extra module**. |
| **batch** | how many messages a worker takes per turn (`maxBatchSize`, default 4). `batch1` = one at a time, the same protocol as the raw baselines. |
| **high-resolution timer** | the delay workload uses `CREATE_WAITABLE_TIMER_HIGH_RESOLUTION` on Windows instead of `sleep_for`; the latter is coalesced into the 15.6 ms tick on long runs (end of §8). |

### 1. Linear depth ladder (CPU, µs/message, median)

| modules | batch1 throughput (M msg/s) | batch1 CPU/message (µs) | extra per step (µs) | batch4 throughput (M msg/s) | batch4 CPU/message (µs) | extra per step (µs) | stable |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 2 | 2.26 | 0.876 | — | 2.67 | 0.739 | — | ok |
| 3 | 2.05 | 1.449 | +0.573 | 2.49 | 1.161 | +0.422 | ok |
| 4 | 1.91 | 2.052 | +0.603 | 2.47 | 1.568 | +0.407 | ok |
| 5 | 1.87 | 2.580 | +0.528 | 2.43 | 1.981 | +0.413 | ok |
| 6 | 1.82 | 3.173 | +0.592 | 2.38 | 2.439 | +0.458 | ok |
| 8 | 1.79 | 4.278 | +1.106 | 2.37 | 3.292 | +0.852 | ok |
| 10 | 1.80 | 5.364 | +1.085 | 2.29 | 4.074 | +0.783 | ok |
| 12 | 1.77 | 6.481 | +1.117 | 2.37 | 4.934 | +0.859 | ok |
| 16 | 1.76 | 8.755 | +2.274 | 2.24 | 6.791 | +1.857 | ok |
| 18 | 1.04 | 12.622 | +3.867 | 1.49 | 10.050 | +3.259 | ok |
| 20 | 1.46 | 11.766 | -0.856 | 2.13 | 8.779 | -1.271 | ★ |
| 24 | 1.21 | 16.447 | +4.681 | 0.81 | 18.177 | +9.398 | ★ |
| 28 | 0.43 | 35.607 | +19.160 | 0.45 | 31.757 | +13.580 | ★ |
| 32 | 0.33 | 46.649 | +11.042 | 0.42 | 39.405 | +7.648 | ok |
| 40 | 0.23 | 77.149 | +30.499 | 0.25 | 71.053 | +31.648 | ok |

The raw baselines on the same ladder, CPU/message only (µs):

| modules | raw | raw-alloc | raw-message |
| --- | --- | --- | --- |
| 2 | 0.234 ★ | 0.471 | 0.564 |
| 3 | 0.515 | 0.784 | 0.980 |
| 4 | 0.863 | 1.012 | 1.306 |
| 5 | 1.143 | 1.485 | 1.758 |
| 6 | 1.565 ★ | 1.996 ★ | 1.906 |
| 8 | 2.668 | 2.372 | 2.854 |
| 10 | 3.614 | 3.511 | 3.452 |
| 12 | 4.786 | 4.717 | 4.512 |
| 16 | 6.437 | 7.030 | 5.998 |
| 18 | 8.247 ★ | — | 7.978 ★ |
| 20 | 10.237 ★ | 11.754 ★ | 8.471 ★ |
| 24 | 14.254 ★ | 28.693 ★ | 26.162 ★ |
| 28 | 22.346 ★ | 33.810 | 24.040 ★ |
| 32 | 48.657 ★ | 57.755 ★ | 43.264 ★ |
| 40 | 68.379 | 70.113 | 78.890 ★ |

★ = `unstable` (spread > 10%); that cell is not read and did not enter §2.

Note: "extra per step" is the total difference between adjacent rows: after 6 modules each
step spans 2 modules, 20→24, 24→28 and 28→32 span 4, and 32→40 spans 8. Divide by the span
to get the cost per extra module; the table keeps the total so that a knee at a jump is not
flattened away.

Two conclusions:

1. **The knee is at 16→18 modules**: 16 modules at batch1 is 8.755 µs/message, 1.76M msg/s;
   18 modules jumps to 12.622 µs (+1.93 µs per extra module, about 3.4× the 0.56 µs of the
   2→16 range) and throughput falls to 1.04M (59%). The 18–28 range is largely `unstable`
   (P/E mixed scheduling), so starred cells are not quoted as single values; 32 and 40
   modules are stable again: 46.65 / 77.15 µs, 0.33M / 0.23M msg/s.
2. **The knee is the machine filling up, not framework dispatch**: at 40 modules raw is
   68.38 µs/message and the framework is 77.15 µs, a difference of 8.77 µs (about 0.23 µs
   per module, the same order as the framework increment below 16 modules); raw threads
   themselves go from 6.44 µs at 16 modules to 68.38 µs at 40 (about 10.6×), so the jump is
   on the raw-thread side. 8 P cores × SMT = 16 P threads; the 17th module thread starts
   landing on E cores and the slowest E-core stage sets the pace of the whole chain. batch4
   pushes the knee back: 20 modules still shows 8.78 µs (2.13M, spread 5.6%), and 32 modules
   only reaches 39.41 µs.

> **2026-11-20 re-test correction**: neither conclusion reproduced — 18 modules is only 2.6%
> slower than 16, the real collapse is at 28 modules, and pinning to P cores makes it worse.
> See §1.1.

#### 1.1 Knee re-test (2026-11-20)

Same machine (i7-14700KF), same parameters (linear, cap 8, `--batch 1`, 5 s × 1 warm-up +
median of 5) over the knee range; the result disagrees with the table above:

| modules | throughput this run | CPU/message this run | spread | table above |
| --- | --- | --- | --- | --- |
| 16 | 1.742 M msg/s | 8.79 µs | 2.9% | 1.76 M / 8.755 µs |
| 18 | 1.697 M | 10.076 µs | 1.3% | **1.04 M / 12.622 µs** |
| 20 | 1.681 M | 11.483 µs | 26.1% | 1.46 M / 11.766 µs |
| 24 | 1.632 M | 13.594 µs | 1.3% | 1.21 M / 16.447 µs |
| 28 | 0.856 M | 23.501 µs | 47.8% | 0.43 M / 35.607 µs |

(16 and 18 were each re-run once: 16 = 1.744 M / 8.783 µs, 18 = 1.695 M / 10.438 µs.)

Three conclusions:

1. **18 is not a knee**: today 18 modules is only 2.6% below 16 (1.697 vs 1.742 M), and
   16→24 loses only 6% in total; the −41% in the 18 row above is an unstable sample and
   should not be quoted as a conclusion.
2. **The real collapse is at 28 modules** (= logical core count): throughput falls to 49% of
   16 modules (0.856 M), CPU/message doubles to 23.50 µs, spread 47.8%.
3. **Pinning to P cores does not help, it makes things worse**: `--cpus 16` squeezes 18
   module threads onto 16 P threads, giving 0.419 M (−75%) at 24.902 µs; so extra threads
   spilling onto E cores are absorbing the overflow, not causing it. Attributing this to E
   cores needs more direct evidence (`EfficiencyClass` accounting from
   `GetSystemCpuSetInformation`); until then the P/E explanation in conclusion 2 of §1 is a
   hypothesis.

#### 1.2 cycles per message (secondary metric, 2026-11-20)

`--batch` default 4, linear, median of 5 s × 3; TSC calibration 3.4176 GHz (each run prints
a `tsc` line). `cycles/msg = cpu/msg × TSC`: this removes the clock from the number, so it
stays comparable across machines and clock points.

| modules | capacity | throughput msg/s | cores | CPU/msg µs | cycles/msg |
| --- | --- | --- | --- | --- | --- |
| 3 | 8 | 3.440 M | 2.99 | 0.864 | 2,954 |
| 4 | 16 | 3.220 M | 3.91 | 1.215 | 4,152 |
| 16 | 8 | 2.250 M | 15.60 | 6.805 | 23,257 |

Reading: 3 modules is 2 hops, about 1,477 cycles/hop; 16 modules is 15 hops, about 1,550
cycles/hop. **The per-hop cost of a shallow chain barely changes with depth** — 23K is just
"15 hops × 1.5K per hop" accumulated linearly, not the deep chain itself getting more
expensive. For the definition and the three identities see `benchmarks/README.md`, section
§4 of the measurement protocol.

### 2. Three-layer split (extra cost per extra module, µs)

Only shallow steps where both ends are stable and the raw baselines do not cross:

| modules | allocation/message | Message per extra module | dispatch per extra module (batch1) | dispatch per extra module (batch4) |
| --- | --- | --- | --- | --- |
| 3 | 0.269 | 0.098 | 0.234 | 0.091 |
| 4 | 0.149 | 0.098 | 0.249 | 0.087 |
| 5 | 0.343 | 0.068 | 0.206 | 0.056 |

Reading: allocation is "per message"; the other two columns are "per extra module". From 6
modules on, `raw` and `raw-alloc` start to cross (the 5 s × 3 noise swallows the
difference), so the table stops there:

- **Message costs about 0.098 µs per extra module** (consistent across the three steps);
  this is the ceiling for reworking the message layer.
- **Dispatch costs about 0.21–0.25 µs per extra module (batch1), 0.06–0.09 µs (batch4)**;
  batching cuts the dispatch chain by about 65%.
- **Allocation costs about 0.15–0.34 µs per message** and does not grow with module count.

### 3. Observation overhead (what the harness itself spends)

| measurement | throughput (M msg/s) | CPU/message (µs) | p50 (µs) |
| --- | --- | --- | --- |
| `--observe full` | 2.60 | 1.114 | 3.016 |
| `--observe none` | 2.61 | 1.131 | — (latency not recorded) |

Difference: **CPU/message +0.02 µs, inside the ±0.02 µs run-to-run noise — not measurable**.
So this appendix needs no "subtract the observation" correction. `--micro-observer` was not
run on this desktop; an earlier machine measured about 40 ns/message on the same
micro-benchmark, which serves as an order-of-magnitude reference.

### 4. Queue capacity (linear 3 modules, batch 4 default)

| capacity | throughput (msg/s) | CPU/message (µs) | extra per step (µs) | p50 (µs) | p99 (µs) | stable |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 1,675,373 | 1.767 | — | 2.016 | 5.031 | ok |
| 2 | 2,033,582 | 1.467 | -0.300 | 2.016 | 5.031 | ok |
| 4 | 2,651,823 | 1.118 | -0.349 | 2.016 | 5.031 | ok |
| 8 | 2,632,877 | 1.115 | -0.003 | 2.016 | 6.031 | ok |
| 16 | 2,740,867 | 1.068 | -0.047 | 3.016 | 10.062 | ok |
| 64 | 2,683,154 | 1.100 | +0.032 | 7.031 | 31.125 | ok |
| 256 | 2,747,269 | 1.074 | -0.026 | 46.250 | 112.500 | ★ |

Two conclusions:

1. **Capacity 4 is already saturated**: 1→4 raises throughput by 58%; beyond 4 up to 64 it
   hovers between 2.63–2.74M (CPU/message 1.07–1.12 µs).
2. **Opening it further only buys tail latency**: p50 rises from 2 µs at capacity 4 to 7 µs
   at 64 and 46 µs at 256; p99 rises from 5 µs to 112 µs, and the 256 step has 21.9% spread
   and is marked `unstable`.

### 5. Batch size (linear 3 modules, capacity 8)

| batch | throughput (msg/s) | CPU/message (µs) | extra per step (µs) | p50 (µs) | p99 (µs) | stable |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 2,571,834 | 1.153 | — | 3.016 | 7.031 | ok |
| 2 | 2,919,243 | 0.988 | -0.165 | 3.016 | 5.031 | ok |
| 4 | 3,155,601 | 0.931 | -0.057 | 2.016 | 5.031 | ok |
| 8 | 2,767,708 | 1.071 | +0.140 | 4.031 | 8.062 | ok |
| 16 | 2,025,843 | 1.453 | +0.381 | 9.062 | 16.125 | ok |

Two conclusions:

1. **Batch 4 is the sweet spot**: 19% less CPU/message and 23% more throughput than batch1;
   batch 8 already falls back 12%, and batch 16 loses 36% with p50 up to 9 µs.
2. **The cause is waiting to fill a batch**: capacity is only 8, so a batch of 16 waits for
   the producer to refill the queue several times, and `PullBatchMessage` delivers only
   after a timeout of up to 100 ms; a batch should therefore not be much larger than the
   capacity.

### 6. Payload (linear 3 modules, capacity 4)

One frame buffer reused (framework-cost protocol):

| payload | frame size | throughput (msg/s) | CPU/message (µs) | extra over none (µs) | p99 (µs) |
| --- | --- | --- | --- | --- | --- |
| none | 0 | 2,481,762 | 1.169 | +0.000 | 4.031 |
| 1080p-yuv420 | 2.97 MiB | 2,361,255 | 1.251 | +0.082 | 4.031 |
| 1080p-rgb | 5.93 MiB | 2,377,994 | 1.240 | +0.070 | 5.031 |
| 2k-yuv420 | 5.27 MiB | 2,380,797 | 1.208 | +0.039 | 5.031 |
| 2k-rgb | 10.55 MiB | 2,382,874 | 1.228 | +0.058 | 5.031 |

A fresh frame per message (decoder protocol, including allocation and zero fill):

| payload | frame size | throughput (frames/s) | CPU/message (µs) | p50 (µs) | p99 (µs) |
| --- | --- | --- | --- | --- | --- |
| 1080p-rgb | 5.93 MiB | 692 | 1797.610 | 3600.000 | 7264.000 |
| 2k-rgb | 10.55 MiB | 476 | 2472.190 | 4960.000 | 9792.000 |

Two conclusions:

1. **In reuse mode the frame size barely affects framework cost**: throughput from 0 to
   10.55 MiB stays at 2.36–2.48M (about 5% apart) and CPU/message at 1.17–1.25 µs — the
   measured proof that copying a Message is just a refcount.
2. **A fresh frame per message falls to 692 / 476 frames/s** (about 3400×), CPU/message
   1797.6 / 2472.2 µs, p50 3.6 / 4.96 ms: this is the cost of malloc + zero fill, not of the
   framework.

### 7. Fan-out and fusion (diamond, capacity 8, 5 s × 3)

Not fused (the sink reads each branch message separately; one branch message counts as one):

| branches | msg/s | frames/s | CPU per frame (µs) | extra per step (µs) | p50 (µs) | p99 (µs) | stable |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 2,583,181 | 2,583,181 | 1.143 | — | 2.016 | 6.031 | ok |
| 2 | 3,992,591 | 1,996,296 | 1.958 | +0.815 | 2.016 | 11.062 | ok |
| 3 | 4,515,944 | 1,505,315 | 3.255 | +1.297 | 3.016 | 15.062 | ok |
| 4 | 4,753,142 | 1,188,286 | 4.886 | +1.630 | 3.016 | 21.125 | ok |
| 6 | 4,938,732 | 823,122 | 9.298 | +4.412 | 4.031 | 32.250 | ok |
| 8 | 4,949,565 | 618,696 | 14.877 | +5.579 | 5.031 | 53.250 | ok |

Fused (`--sync-inputs`; the sink receives one fused message per frame):

| branches | frames/s | CPU per frame (µs) | extra per step (µs) | p50 (µs) | p99 (µs) | stable |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 1,144,116 | 2.564 | — | 17.125 | 23.125 | ok |
| 2 | 854,930 | 4.594 | +2.029 | 23.125 | 31.125 | ok |
| 4 | 594,751 | 9.999 | +5.406 | 34.250 | 43.250 | ok |
| 8 | 361,133 | 26.749 | +16.750 | 55.250 | 72.500 | ★ |

Two conclusions:

1. **Unfused fan-out only adds cost, not frame rate**: each extra branch raises CPU per
   frame from 1.14 to 14.88 µs (about 13×) and drops the frame rate from 2.58M to 618K; the
   sink receives all branches serially on one thread, so the more branches, the longer each
   frame waits.
2. **Fusion is more expensive**: 2 branches give 855K frames/s vs 2.00M unfused, CPU per
   frame 4.59 vs 1.96 µs (2.3×); 8 branches give 361K frames/s at 26.75 µs/frame, and that
   step has 13.9% spread and is marked `unstable`.

### 8. Workload nodes (linear 3 modules, capacity 4, `--batch 1`)

Replacing "framework idling" with "real work per frame": compute is a busy loop (sin × cos
iterations), memory is memcpy, delay is waiting.

| workload | parameter | throughput (msg/s) | CPU/message (µs) | extra per step (µs) | p50 (µs) | p99 (µs) | stable |
| --- | --- | --- | --- | --- | --- | --- | --- |
| compute | 500 iterations | 311,026 | 9.619 | — | 22.125 | 28.125 | ok |
| compute | 2000 iterations | 51,148 | 53.499 | +43.880 | 67.500 | 225.000 | ok |
| compute | 5000 iterations | 17,868 | 107.669 | +54.170 | 219.000 | 572.000 | ok |
| memory | 4096 B | 1,847,891 | 1.587 | — | 3.016 | 4.031 | ok |
| memory | 65536 B | 385,248 | 7.683 | +6.096 | 18.125 | 25.125 | ok |
| memory | 524288 B | 57,902 | 48.839 | +41.156 | 108.500 | 211.000 | ok |
| delay | 2 ms | 419 | 81.342 | — | 14400.000 | 15552.000 | ok |
| delay | 5 ms | 188 | 65.582 | -15.759 | 31872.000 | 33024.000 | ok |
| delay | 10 ms | 98 | 95.468 | +29.886 | 61696.000 | 63232.000 | ok |

Two conclusions:

1. **Compute / memory throughput falls roughly as the inverse of the work**: compute
   500→5000 iterations takes throughput 311K→17.9K msg/s (CPU/message 9.62→107.67 µs);
   memory 4 KiB→512 KiB takes 1.85M→57.9K (CPU/message 1.59→48.84 µs).
2. **Delay costs waiting, not CPU**: 2/5/10 ms gives 419/188/98 msg/s at 0.034/0.012/0.009
   cores; p50 14.4/31.9/61.7 ms is larger than the delay itself because the capacity-4
   queues are backing up, not because of sleep precision — this uses the high-resolution
   waitable timer.

The "extra per step" can go negative on the delay rows: waiting itself burns no CPU, and
CPU/message is dominated by queueing and spinning. Read only throughput, cores and p50 for
this dimension.

### 9. External injection (`--topology inject`, 3 modules, capacity 8)

The main thread plays the external decoder and feeds a passive source through
`Pipeline::Push` / `TryPush`.

| mode | throughput (msg/s) | CPU/message (µs) | p99 (µs) | push attempts | refused retries | shutdown (ms) |
| --- | --- | --- | --- | --- | --- | --- |
| block (`Push` blocks) | 2,802,874 | 1.048 | 6.031 | 14,014,535 | 0 | 8.37 |
| try (`TryPush` retries) | 2,927,649 | 0.998 | 6.031 | 14,638,413 | 5,743,614 | 14.65 |

Reading: the two modes are almost identical in throughput and CPU/message (2.803M vs
2.927M, within noise); in `try` mode about 39% of attempts hit a full queue and retry, but
nothing turns into a dropped frame — every `cov,` row is ok.

### 10. Memory and shutdown (spot checks)

| configuration | RSS start (MiB) | RSS peak (MiB) | shutdown (ms) | max edge depth |
| --- | --- | --- | --- | --- |
| capacity 4, none | 5.8 | 5.8 | 0.19 | 4 |
| capacity 64, none | 5.8 | 5.9 | 0.20 | 64 |
| capacity 256, none | 5.8 | 5.9 | 0.21 | 256 |
| capacity 4, 1080p-rgb | 5.8 | 11.8 | 0.22 | 4 |
| capacity 4, 2k-rgb | 5.8 | 16.4 | 0.23 | 4 |
| 16 modules, capacity 8 | 6.0 | 6.3 | 15.60 | 8 |
| 8 branches | 5.9 | 6.1 | 0.71 | 8 |
| 8 branches fused | 6.0 | 6.2 | 0.67 | 8 |

Reading: the empty payload rests at about 5.8 MiB; with a reused frame, 1080p-rgb only
rises to 11.8 and 2k-rgb to 16.4 MiB — the queues hold references to the same frame, not
one copy per slot. The 15.6 ms shutdown of 16 modules is clearly above a shallow chain
(about 0.2 ms) because it waits for 15 workers to drain and exit.

### 11. Known-unstable / not measured

- **Any step marked `unstable` is not read**: `d2-raw` (14.8%), `d6-raw` (16.7%),
  `d6-rawalloc` (29.6%), `capacity 256` (21.9%), `join 8 branches` (13.9%). The remaining
  steps are within 10% after a re-run.
- **Depth knee region, 18–28 modules**: in the first round and the median-of-5 re-test most
  arms had 15–126% spread, caused by thread placement changing between runs under P/E mixed
  scheduling; `d18-fw-b1` is stable after the re-test (8.3%) and `d20-fw-b4` as well (5.6%);
  the rest are treated per the ★ in the table and no single value is quoted.
- **The three-layer split uses shallow steps only**: from 6 modules on, the raw baselines
  cross (noise exceeds the difference), so §2 samples 3/4/5 modules.
- **`--micro-observer` was not run**: the observation micro-benchmark was measured only on
  an earlier machine (about 40 ns/message).
- **5 s × 3 is the uniform protocol here**: no 10 s long run was done.
- **This appendix is valid for the desktop only**: do not mix its data with other machines;
  only trends compare.
- **The thread pool (v1.1 `--pool-size`) is measured**: see §14; the conclusion is threads
  traded for throughput, and the default remains one thread per module.
- **Next-version optimisation list**: the framework problems this report exposed (cross-P-core
  cliff, low-rate timer floor, fixed per-message cost, fan/join, batching semantics) are
  collected in [`NEXT-VERSION.md`](../NEXT-VERSION.md) §3.4 with acceptance thresholds.

### 12. Reproduce

```powershell
# depth (framework batch1, same protocol as the raw baselines; use --repeat 5 for 18/20/24)
.\build\benchmarks\Release\pipeline_benchmark.exe --topology linear --modules 16 --capacity 8 --duration 5 --repeat 3 --batch 1
.\build\benchmarks\Release\pipeline_benchmark.exe --topology linear --modules 18 --capacity 8 --duration 5 --repeat 5 --batch 1
.\build\benchmarks\Release\pipeline_benchmark.exe --topology linear --modules 40 --capacity 8 --duration 5 --repeat 3 --batch 1

# the three raw baselines
.\build\benchmarks\Release\pipeline_benchmark.exe --topology linear --modules 8 --capacity 8 --duration 5 --repeat 3 --baseline raw
.\build\benchmarks\Release\pipeline_benchmark.exe --topology linear --modules 8 --capacity 8 --duration 5 --repeat 3 --baseline raw-alloc
.\build\benchmarks\Release\pipeline_benchmark.exe --topology linear --modules 8 --capacity 8 --duration 5 --repeat 3 --baseline raw-message

# capacity / batch / payload / observation
.\build\benchmarks\Release\pipeline_benchmark.exe --topology linear --modules 3 --capacity 16 --duration 5 --repeat 3 --batch 4
.\build\benchmarks\Release\pipeline_benchmark.exe --topology linear --modules 3 --capacity 8 --duration 5 --repeat 3 --batch 8
.\build\benchmarks\Release\pipeline_benchmark.exe --topology linear --modules 3 --payload 1080p-rgb --payload-fresh --duration 5 --repeat 3 --batch 4
.\build\benchmarks\Release\pipeline_benchmark.exe --topology linear --modules 3 --capacity 8 --duration 5 --repeat 3 --batch 4 --observe none

# fan-out / fusion
.\build\benchmarks\Release\pipeline_benchmark.exe --topology diamond --branches 8 --capacity 8 --duration 5 --repeat 3
.\build\benchmarks\Release\pipeline_benchmark.exe --topology diamond --branches 4 --capacity 8 --duration 5 --repeat 3 --sync-inputs

# workloads / injection
.\build\benchmarks\Release\pipeline_benchmark.exe --topology linear --modules 3 --capacity 4 --duration 5 --repeat 3 --batch 1 --workload compute --compute-iters 2000
.\build\benchmarks\Release\pipeline_benchmark.exe --topology linear --modules 3 --capacity 4 --duration 5 --repeat 3 --batch 1 --workload delay --delay-us 2000
.\build\benchmarks\Release\pipeline_benchmark.exe --topology inject --modules 3 --capacity 8 --duration 5 --repeat 3 --push-mode block

# reading: grep "^stat," for the ladder; grep "^cov," for coverage; grep "^csv," for single samples
```

### 13. Workspace

The benchmark-tool changes behind this appendix shipped with `31b4420` (v1.0.0); every number
in the report comes from the `pipeline_benchmark` built at that commit. `docs/` outside
`benchmarks/` holds local notes and is not in the repository.

### 14. Thread pool (v1.1 experiment, `--pool-size N`)

`PipelineContext::WithThreadPoolSize(N)` hands non-fused modules to N shared threads: modules
are pinned to thread `index % N` in registration order, and each thread advances the modules
in its slot with non-blocking `StepOnce()`; fused modules (`--sync-inputs`) always keep a
dedicated thread. `N = 0` (the default) is 1.0's "one thread per module".

A shared thread must not block on push (the consumer of a full edge may be the same thread),
so pool mode uses **exact-quota backpressure**: `Worker::OutputRoom()` takes the tightest
output edge's free slots, and `StepOnce()` takes at most `min(batch, room)` per turn. Each
edge has a single producer, so room only grows — no overflow, no dropped frames; every
pooled configuration's `cov` is `ok`.

The table below is the same machine, same code, 5 s × 1; the two platform columns are
Windows/MSVC and the same machine's WSL2/g++ 9.4.

| scenario (linear) | Win pool 0 | Win pool 4 | WSL pool 0 | WSL pool 4 | WSL pool 8 |
| --- | --- | --- | --- | --- | --- |
| 4 modules cap 16 | 3.141 M | **1.742 M (55%)** | 3.147 M | **2.808 M (89%)** | — |
| 4 modules cap 16, pool 2 | 3.141 M | 2.177 M (69%) | 3.147 M | 2.248 M (71%) | — |
| 4 modules cap 1024 | 3.540 M | 3.140 M (89%) | 3.638 M | 3.399 M (93%) | — |
| 16 modules cap 8 | 2.229 M | 0.946 M (42%) | 1.274 M | 0.950 M (75%) | 1.378 M (108%) |
| 4 modules cap 16 + compute 500 | 0.639 M | 0.441 M (69%) | 0.409 M | 0.399 M (98%) | — |

Conclusion: pooling is **threads traded for throughput**, and how much it costs **depends
strongly on the OS scheduler**.

- **Windows/MSVC**: shallow edges + deep chains lose the most — 16 modules cap 8 with pool 8
  keeps only 23% (0.514 M vs 2.229 M), and 4 modules cap 16 keeps 55%. The bottleneck is the
  hand-off on a full edge: a blocked slot is rescheduled only on the next turn, and a deep
  chain accumulates that delay hop by hop.
- **WSL2/g++**: the same code does much better — 89% at 4 modules cap 16, 93% at cap 1024,
  and 16 modules cap 8 with 8 threads **beats** the baseline by 8% (1.378 M vs 1.274 M).
- **The platforms differ by 20–34 percentage points**: not because the code path differs
  (`cov` is ok everywhere) but because "how soon a blocked thread is rescheduled" differs:
  pool threads stay runnable on Linux (non-blocking + `pause`), while MSVC spends more time
  in context switches and ready queues.
- **The default remains one thread per module** (`pool = 0`); pooling is opt-in. Getting
  "fewer threads without losing throughput" still needs one more step: a per-module
  completion signal plus bounded blocking (the L2 next step in `NEXT-VERSION.md` §3.1).

#### 14.1 Same-machine A/B: v1.0.1 vs pooling (re-measured 2026-11-20)

The previous section was a rough 5 s × 1. Here are two binaries from the same machine and
source family: `v1.0.1` (tag build) and the branch build, each 5 s × 1 warm-up + median of 3.
First the "does it slow down without changing configuration" question: **`pool = 0` takes the
same code path as v1.0.1, and the result is inside the noise** (+2.0% / +1.1%, same p50).

| scenario (linear) | v1.0.1 | branch pool 0 | branch pool 2 | branch pool 4 | branch pool 8 |
| --- | --- | --- | --- | --- | --- |
| 4 modules cap 16 / MSVC | 3.226 M | 3.291 M (+2.0%) | 2.134 M (65%) | 1.662 M (50%) | — |
| 16 modules cap 8 / MSVC | 2.221 M | 2.246 M (+1.1%) | — | 0.855 M (38%) | 0.480 M (21%) |
| 4 modules cap 16 / WSL2 | — | 3.483 M | 2.482 M (71%) | 3.127 M (90%) | 3.014 M (87%) |
| 16 modules cap 8 / WSL2 | — | 1.130 M (spread 25%, unstable) | 0.668 M (59%) | 1.123 M (99%) | 1.478 M (131%) |

Two new facts:

- **WSL2, 16 modules cap 8: pool 4 matches pool 0's throughput using 3.98 cores vs 11.61**
  (1.123 M vs 1.130 M), at the cost of p50 rising from 29 µs to 58 µs. Same delivery, a third
  of the CPU.
- **WSL2, same scenario, pool 8 reaches 1.478 M (+31%)**, but pool 0's median has 25%
  spread, so read it as a range (930 K–1.215 M).

The conclusion stands: pooling is still a net loss on MSVC (deep chains −62% … −79%), and on
Linux it goes from "roughly break-even" to "fewer threads without losing throughput, and deep
chains even higher"; the default `pool = 0` is unchanged.

#### 14.2 Regression check: pool = 0 vs v1.0.1 (2026-11-20, after the merge)

After v1.0.1 was merged into the branch, `pool = 0` in the same binary takes the old path. To
confirm the merge did not slow the old path, the v1.0.1 tag build and the merged binary were
run in a same-machine matrix (`--duration 5 --repeat 3`, median; M msg/s):

| configuration | v1.0.1 | merged pool 0 | Δ |
| --- | --- | --- | --- |
| linear 3 cap 8 | 3.315 | 3.330 | +0.4% |
| linear 4 cap 16 | 3.322 | 3.390 | +2.1% |
| linear 8 cap 16 | 2.824 | 2.883 | +2.1% |
| linear 16 cap 8 | 2.224 | 2.177 | -2.1% |
| linear 4 cap 64 | 3.440 | 3.321 | -3.4% |
| linear 4 cap 16, batch 1 | 2.680 | 2.688 | +0.3% |
| linear 4 cap 16, 1080p | 2.989 | 2.941 | -1.6% |
| linear 4 cap 16, compute 500 | 0.634 | 0.632 | -0.3% |
| diamond 2 cap 8, sync | 1.230 | 1.285 | +4.5% |
| inject 6 cap 64 | 3.581 | 3.345 | -6.6% |

The six configurations beyond ±2% were re-run with `--repeat 7` (alternating the order):
L4c16 +2.9%, L8c16 -0.5%, L16c8 +1.5%, L4c64 +1.3%, D2c8 +4.4%, I6c64 -3.7%; all min–max
ranges overlap (e.g. I6c64 is 3.291–3.634 vs 3.303–3.542) and the signs are inconsistent.
Conclusion: **merged `pool = 0` and v1.0.1 fall in the same noise band — no systematic
regression**; single 5 s p50 values flip between two modes (2 µs and 8–10 µs), so the latency
column can only be compared as ranges, not as single medians.

#### 14.3 The pooled hot path: what four cuts bought (2026-11-20)

The pooled path (pool > 0) spends extra time in exactly four places: the slot loop itself
(`ThreadPool::Loop`), `Worker::StepOnce()`, the `OutputRoom()` scan once per turn, and the
per-turn batch vector. Each got one cut, all affecting only the pooled path:

1. `StepOnce()` measures `OutputRoom()` once per call instead of once per turn. Each edge has
   a single producer, so "decrement the local budget by what this turn produced" is exact for
   that semantic; `ThreadPool::Loop` no longer re-scans every edge at the end either, reading
   `Worker::BlockedOnOutputs()` instead.
2. The pull path uses a flat vector of input queues instead of hashing queue names as strings
   every turn.
3. `StepOnce()` reuses one batch buffer; previously it created a vector per turn (the
   allocator accounted for 6.3% of the pooled profile).
4. Edges whose producer and consumer land in the same slot are marked local: their queues skip
   the `seq_cst` publish store and the wake-up check because both ends are the same thread.
   `SpscQueue::SetLocalMode()` is called by `ThreadPool::Start()` before any worker thread
   exists.

| configuration (5 s × 3) | pool 0 | pool 4 | pooled / serial |
| --- | --- | --- | --- |
| L4cap16, MSVC | 3.468 M | 2.197 M | 63% |
| L16cap8, MSVC | 2.470 M | 1.002 M | 41% |
| L4cap16, WSL2 | 3.689 M | 3.472 M | 94% |
| L16cap8, WSL2 | 1.575 M | 1.855 M | 118% |

This table adds two things the earlier ones did not say:

- **The gap comes from backpressure, not from routine per-message work.** With capacity
  raised to 1024, where edges are almost never full, pool4 is 89–93% of serial on both
  platforms; the same binary at capacity 16 drops to 63% on MSVC. A per-message tax does not
  change with capacity; backpressure does.
- **Windows and Linux disagree, and all the loss is on Windows.** On WSL2 pooling is roughly
  break-even (L4) or even ahead (L16); on MSVC it is only 40–63%. The difference is the
  blocked branch (`Pause()` spinning) and how Windows schedules four spinning threads, not
  the queue itself.

Cut four alone once showed +24.6% (Windows L16cap8 pool4, 0.794 → 0.989 M), but the same pair
of binaries in the next batch showed only +0.3% (0.999 → 1.002 M), so that cut is currently
**unconfirmed**; the first two cuts (unified clock read, single room measurement) are far
above the noise, this one is not.

Measurement notice (deliberately in the document): re-running the same binary at different
times drifts by up to 70% (L4cap16 pool4 has been seen in two clusters, 1.24–1.42 M and
2.07–2.10 M), so differences below 10% in this section are not credible; every number in the
table is the median of three 5 s runs, and the pool4 rows are taken with the process pinned to
8 P threads.

#### 14.4 Fix: the idle backoff unit was off by an order of magnitude (2026-11-21)

§14.3 attributed the pooled-vs-p0 gap to "backpressure + dispatch", but p4/p0 was only 63–68%
on MSVC while WSL2 broke even, and that platform difference stayed unexplained. The root cause
is not pooling's inherent cost but the idle backoff in `ThreadPool::Loop()`:

- `sleep_for(10us)` / `sleep_for(50us)` are rounded up by `chrono::ceil<milliseconds>` to
  `Sleep(1)` on MSVC, and `Sleep(1)` waits for the system tick — measured at **1.907 ms** on
  average (with `timeBeginPeriod(1)`), and **15.374 ms** at default resolution;
- pool threads rely on polling, and `push` does not wake them, so one park stops that pipeline
  stage for one tick and the whole queue behind a capacity-16 edge jams;
- diagnostic counts: the sink thread did `sleep10=756` times in 5 s, `tscIdle ≈ 1.96 s`
  (about 40% of wall time).

Fix `8e628b0`: the backoff ladder became "4096 rounds of PAUSE → 4096 rounds of yield → 1 ms
park", with thresholds in rounds rather than microseconds.

| configuration (5 s × 3, cpus 8) | before | after | change |
|---|---|---|---|
| L4cap16 p4 | 2.221 M / 3.475 cores / p50 9.06 µs | 3.019 M / 3.958 cores / p50 8.06 µs | +36% |
| L4cap16 p0 | 3.267 M / 3.875 cores | 3.225 M / 3.876 cores | within noise |
| L16cap8 p4 | 0.980 M / 3.486 cores | 1.411 M / 3.961 cores | +44% |
| p4 / p0 | 68.0% | **93.6%** | — |

The residual 6–7% is the polling design itself (empty `StepOnce` + only about 2.4 messages per
batch), not this bug. For the mechanism, the API comparison and a terminal side-by-side see
[`TIMERS.md`](TIMERS.md).

#### 14.5 Re-test after the fix: pooled vs non-pooled, both platforms (2026-11-22)

Protocol: the matrix uses `--duration 300 --repeat 2` (plus one warm-up, 3 × 5 minutes per
configuration); the sweep section uses `--duration 60 --repeat 2` only as a window-length
control. `pool 0` is not pinned; `pool N` is pinned to 2N logical cores (`--cpus 2N`). Both
platforms are the same HEAD (with the `8e628b0` backoff fix and the `98d3790` wrapper). Every
pooled run reports `coverage: ok`.

**Table 1 · 300 s matrix (median; two-run range in parentheses)**

| configuration (linear) | Windows | pool/p0 | WSL2 | pool/p0 |
| --- | --- | --- | --- | --- |
| L4cap16 p0 | 2,876,440 (2.845–2.907 M) | — | 3,742,427 (3.717–3.768 M) | — |
| L4cap16 p4 | **3,015,422** (2.939–3.092 M) | **104.8%** | 3,633,264 (3.600–3.666 M) | 97.1% |
| L16cap8 p0 | 2,625,565 (2.587–2.664 M) | — | **767,206** (750–784 K) | — |
| L16cap8 p4 | 1,489,342 (1.470–1.509 M) | 56.7% | 1,930,952 (1.915–1.947 M) | 251.7% |
| L16cap8 p8 | 2,109,795 (2.093–2.126 M) | 80.4% | 2,573,464 (2.569–2.578 M) | 335.4% |

**Table 2 · 60 s sweep (same batch, same build; window control only)**

| configuration | Windows | WSL2 |
| --- | --- | --- |
| L16cap8 p0 | 2,688,464 | 691,074 |
| L16cap8 p4 | 1,422,041 (52.9%) | 1,894,718 (274%) |

The Windows 60 s to 300 s ratio differs by only 3.8 percentage points (52.9% vs 56.7%), so
**window length is not the cause**; the WSL2 deep-chain p0 lands in the slow state in both
windows (691 K / 767 K).

**Table 3 · Throughput per core (M msg/s/core) and cycles per message (cycles is the last csv
column; the TSC calibration is in §1.2)**

| configuration | Win per core | Win cycles | WSL per core | WSL cycles |
| --- | --- | --- | --- | --- |
| L4cap16 p0 | 0.728 | 4,706 | 0.945 | 3,616 |
| L4cap16 p4 | 0.757 | 4,513 | 0.908 | 3,764 |
| L16cap8 p0 | 0.169 | 20,249 | 0.075 | 45,354 |
| L16cap8 p4 | 0.374 | 9,138 | 0.483 | 7,083 |
| L16cap8 p8 | 0.265 | 12,909 | 0.322 | 10,627 |

**WSL2 batch-to-batch difference (this is what "slow state" means above)**

- The same binary with the same parameters (`L16cap8 p0`) measured two clusters on WSL2 today:
  **0.69–0.90 M** (four independent measurements: 691 K, 742 K, 901 K, 767 K) and
  **2.39–2.59 M** (the 12:00 batch, 60 s × 3). Windows stays at 2.63–2.69 M for the same
  configuration.
- This is not caused by window length (both 60 s and 300 s land in the same cluster), nor by
  the VM lifetime: after `wsl --shutdown` recreated the VM it still ranged from 742 K to
  901 K and never returned to the fast cluster.
- Host-side sampling (Windows `Get-Counter '\Processor Information(*)\% Processor Time'`,
  P cores = 0–15, E cores = 16–27, core type confirmed first by `GetSystemCpuSetInformation`):
  idle is about 20% on P and 4% on E; while the slow cluster runs, **all 12 E cores are busy at
  about 60%** and P is only about 45%; the fast pooled batch (p4 1.86 M) has E at about 20%.
  The slow cluster does push a lot of work onto E cores, but the fast batch also uses E cores,
  so this is **correlation, not causation**.
- Therefore: WSL2 deep-chain `p0` absolutes are quoted as a range, not a single value; the
  pooled side (p4 / p8) is stable in both clusters (1.89–1.93 M / 2.57 M) and unaffected.

**Acceptance protocol (set 2026-11-22)**: judge by **Windows p0 / p4 and WSL2 p4**; WSL2
deep-chain `p0` is recorded as a range only and does not enter the verdict. Shallow chains are
accepted on parity (≥95%); deep chains are accepted on per-core efficiency rather than
throughput ratio (p4 ≥2× per core, p8 ≥1.5×), otherwise "4 cores delivering 4 cores' worth"
would be misjudged as failure.

#### 14.6 WSL2's two states and a thread-count sweep (2026-11-23)

§14.5 left a phenomenon not fully explained: on WSL2 the same binary with the same parameters
produced two throughput clusters 3× apart for the deep chain at `pool 0`. This section turns it
into a decidable metric and lists thread counts 8 / 12 / 16 separately.

Protocol: `--duration 300 --repeat 1` (one 5-minute warm-up window + one 5-minute measurement
window, about 10 minutes per configuration); `pool 0` is not pinned, `pool N` is pinned to 2N
logical cores (`--cpus 2N`). Both platforms are the same HEAD. Every pooled run reports
`coverage: ok`.

**Table A: Windows / MSVC (300 s, same batch)**

| configuration | throughput (msg/s) | cores | p50 (µs) | p95 (µs) | p99 (µs) | µs/message |
| --- | --- | --- | --- | --- | --- | --- |
| L4cap16 p0 | 2,858,586 | 3.95 | 7.03 | 14.06 | 15.06 | 1.383 |
| L4cap16 p4 | 3,002,675 (105.0%) | 3.98 | 7.03 | 11.06 | 12.06 | 1.325 |
| L16cap8 p0 | 2,625,253 | 15.59 | 40.25 | 53.25 | 63.25 | 5.940 |
| L16cap8 p8 | 2,035,490 (77.5%) | 7.96 | 22.12 | 41.25 | 47.25 | 3.912 |
| L16cap8 p16 | 2,527,911 (96.3%) | 15.92 | 31.12 | 40.25 | 44.25 | 6.297 |

**Table B: WSL2 (300 s, same batch; both states appeared within it)**

| configuration | throughput (msg/s) | cores | p50 (µs) | p95 (µs) | p99 (µs) | µs/message |
| --- | --- | --- | --- | --- | --- | --- |
| L4cap16 p0 | 3,645,733 | 3.94 | 2.02 | 9.06 | 15.06 | 1.080 |
| L4cap16 p4 | 3,529,947 (96.8%) | 4.00 | 8.06 | 9.06 | 16.12 | 1.133 |
| L16cap8 p0a (slow state) | 776,331 | 10.12 | 27.12 | 434.00 | 564.00 | 13.037 |
| L16cap8 p0b (slow state) | 658,593 | 9.82 | 27.12 | 454.00 | 580.00 | 14.917 |
| L16cap8 p8 | 2,587,774 | 8.00 | 25.12 | 34.25 | 48.25 | 3.092 |
| L16cap8 p12 | 2,313,519 | 12.00 | 20.12 | 38.25 | 55.25 | 5.187 |
| L16cap8 p16 | 2,863,602 | 16.00 | 26.12 | 39.25 | 56.25 | 5.587 |

**Reading 1: the slow state shows in the tail, not the median.** Both `p0` runs have p50
27.12 µs, almost the same as pooling (25.12 / 26.12 µs); but p95 / p99 are 434 / 564 µs and
454 / 580 µs, while pooling is only 34–39 / 48–56 µs. CPU per message likewise rises from
3.1–5.6 µs to 13–15 µs. So the anomaly is not "every message got slower" but **a small subset
of messages waiting for a wake-up** — consistent with the §16 profile: non-pooled parks and
wakes every hop, and WSL2's wake-up latency is 43–46 µs even at p50 (the `handoff_lat` probe).
The pooled path does not park, so both batches are stable (2.588 M vs 2.580 M, the latter from
the earlier 60 s p8 batch).

> Annotation: p50 / p95 / p99 / µs/message are **measured**; "the slow state is caused by
> park/wake and host placement" is a **hypothesis**, supported by the host sampling in §14.5
> (all 12 E cores busy in the slow batch) and by `wsl --shutdown` not clearing it, but no
> causal experiment has been run.

**Reading 2: 8 / 12 / 16 threads.** Same 16 modules:

- **16 threads is fastest**: 2,863,602 msg/s;
- **8 threads uses the least CPU**: 3.092 µs/message (44.7% less than 16 threads), only 9.6%
  less throughput, and tighter tail latency (p99 48.25 vs 56.25 µs);
- **12 threads loses both ways**: 10.6% slower than 8 threads while spending 67.7% more CPU per
  message (5.187 vs 3.092 µs). `poolstats` shows 51.5% idle turns — fewer threads than modules,
  but not few enough, so half the turns spin empty.

Conclusion: on WSL2 use **8 or 16**, not 12; pick 16 for throughput and 8 for CPU.

**Reading 3: Windows is only a control.** The shallow chain `p4` is 105.0% (lossless); the deep
chain with 16 threads is 96.3% (nearly lossless, and p50 / p99 are better than serial:
31.12 / 44.25 vs 40.25 / 63.25 µs), and the deep chain with 8 threads is 77.5%. Windows'
pooling loss is a separate problem (candidate cause: each hand-off is more expensive with fewer
cores); the tables here are only a control and are not explained in this section.

**Acceptance protocol (per this section's data)**

| scenario | threshold | measured |
| --- | --- | --- |
| shallow chain (4 modules) | ≥95% | Windows 105.0% / WSL2 96.8% |
| deep chain (16 modules), 16 threads | ≥95% | Windows 96.3% / WSL2 110.6% (vs 8 threads) |
| deep chain, 8 threads | −20% allowed for 45% less CPU | WSL2 break-even; Windows 77.5% (fails) |

**Reproduce**

```bash
# Windows
build/benchmarks/Release/pipeline_benchmark.exe --topology linear --modules 16 --capacity 8 \
    --pool-size 16 --cpus 28 --duration 300 --repeat 1 --label W-L16cap8-p16
# WSL2 (same source)
/root/nf7/build-wsl/benchmarks/pipeline_benchmark --topology linear --modules 16 --capacity 8 \
    --pool-size 8 --cpus 16 --duration 300 --repeat 1 --label S-L16cap8-p8
```

Raw logs: `accept_win.log`, `accept_wsl.log` (local temp directory, not in the repository).

### 15. Queue-full policy: `--queue-full drop` (v1.1 experiment)

#### 15.1 What it does

`--queue-full drop` enables `QueueFullPolicy::kDrop`: before broadcasting, a module checks all
output queues and **drops the whole message if any one of them is full**, counting it in
`ModuleSnapshot::droppedTotal`, instead of blocking in `queue->push()`. The default `block`
keeps the historical behaviour: block, and a slow downstream exerts backpressure on upstream.

The drop point is not inside the queue but on the module's send path: `Dispatcher::Broadcast`
first reads `queue->capacity()/getSize()` and `return`s when full, so the message **never
enters the queue**. Messages already queued are not evicted (this is the essential difference
from ai-pipe's DropHead/DropTail: that evicts inside the queue, this refuses before enqueue).

All-or-nothing is deliberate: if a message entered only some output queues, the join group
would wait forever for the other half and downstream would hang until `joinTimeoutMs` expires.
So "drop" here does not mean "let the fast branch proceed" but "this frame is abandoned".

#### 15.2 Measurements (i7-14700KF, MSVC, median of 5 s × 3)

| scenario | policy | delivered/s | p50 µs | cores | dropped while running | `cov` |
| --- | --- | --- | --- | --- | --- | --- |
| diamond 2 cap 4, branch_0 2 ms slower (no join) | block | 843 | 12,608 | 0.05 | 0 | ok |
| same | drop | 814 | 10,048 | **1.05** | **30,945,511** | ok |
| diamond 2 cap 4, branch_0 2 ms slower (join) | block | 407 | 17,792 | 0.99 | 0 | ok |
| same | drop | 410 | 15,424 | **2.00** | **30,736,662** | ok |
| linear 4 cap 4, 1 ms delay per stage | block | 678 | 19,584 | 0.03 | 0 | ok |
| same | drop | 667 | 18,304 | **1.03** | **30,679,573** | ok |
| inject 6 cap 4 (healthy chain) | block | 3.105 M | 6 | 5.88 | 0 | ok |
| same | drop | 2.950 M | 5 | 5.93 | 1,839,919 | ok |

#### 15.3 Conclusion: keep `block` as the default

1. **Throughput did not improve at all** — it fell 5% on the healthy chain. That is the direct
   consequence of all-or-nothing: deliverable frames are still set by the slowest path, and
   dropping only changes who waits, not who is fast.
2. **Liveness is bought with a whole core**: in the jammed scenarios CPU rises from 0.05 to
   1.05 cores, because the producer is no longer blocked by the queue and spins through about
   2 million drops per second (30 million dropped in 15 s). This is not free insurance.
3. **It costs more on the join chain** (2.00 cores): every dropped frame would otherwise have
   taken one enqueue opportunity on each branch.
4. `cov` is `ok` under both policies: with `--queue-full drop`, drops are expected and the
   harness counts them in the `cov,` row; under the default policy any drop while running is
   still a MISMATCH.

If this is pursued, the right direction is not "drop more" but ① throttle dropping (back off a
tick after a drop instead of spinning), ② push the policy down to **single edges** (allow
partial delivery on non-join fan-out), or ③ simply rate-limit at the source with
`--push-mode try`. None of the three is implemented yet.

### 16. Attribution case study: two clock reads on the hot path (2026-11-20)

#### 16.1 Problem: who owns the p0 vs p4 gap

On `L4cap16` (linear 4 modules, capacity 16), MSVC measured `pool=0` at 3.287 M msg/s and
`pool=4` at 1.946 M msg/s, a 40.8% gap. Until then there was only the guess "pooling
overhead", with no observable evidence.

#### 16.2 Method: sampling that can see stacks

- Windows xperf/wpr exports produced no usable call stacks (a 407 MB ETL with no symbolized
  export), so the profiling was done on WSL2.
- **Frame pointers are mandatory**: `-DCMAKE_CXX_FLAGS_RELEASE="-O2 -g
  -fno-omit-frame-pointer"`. Without them the stack is 2–3 frames deep and full of
  `[unknown]`, showing only "which function is expensive", never "who calls it".
- Sampling: `perf record -e cpu-clock -F 1999 -g --call-graph fp`, 5 s per configuration,
  about 78.5 K samples; `perf script` exports and a script draws the flame graph (bottom-up,
  width = sample share).
- Two profiled processes: `L4cap16 pool=0` (1.185 µs/msg) and `pool=4` (1.323 µs/msg).

#### 16.3 Finding: both sides pay the same bill

Self time (share of that process's samples):

| symbol | p0 | p4 | belongs to |
|---|---|---|---|
| `SpscQueue::PutIfSpace` | 21.98% | 19.72% | enqueue hand-off, paid by both models |
| `__vdso_clock_gettime` | **14.74%** | **9.55%** | pure waste, see 16.4 |
| `SpscQueue::waitAndPopFor` | 12.64% | — | p0's empty wait (1 ms timeout) |
| `Worker::PullBatchMessage` / `PullBatchNow` | 11.48% | 15.19% | batch pull |
| `ThreadPool::Loop` | — | 10.31% | the pool scheduler itself |
| `Worker::StepOnce` | — | 6.50% | single-step dispatch |
| `Module::Broadcast` | 6.20% | 5.24% | send path |
| `Worker::OutputRoom` | — | 2.49% | scans output edges every step |
| `__schedule` | — | 2.68% | thread switched out |

In absolute cost:

- the pooled-only part (`Loop`+`StepOnce`+`OutputRoom`+`__schedule`) = 22.0% × 1.323 µs ≈
  **0.29 µs/message**;
- the p0-only part (`PullBatchMessage`+`waitAndPopFor`+`WorkLoop`) = 27.7% × 1.185 µs ≈
  **0.33 µs/message**.

**The two roughly cancel.** So "the 22% exactly explains p4 being 0.14 µs slower" is wrong —
0.29 ≠ 0.14; the real gap comes from batch-fill pattern, cross-thread scheduling and cache
effects, and cannot be booked under the single heading "pooling overhead".

#### 16.4 Fix: if timings are off, do not read the clock

`Module::ProcessTimed` (`src/module/Module.cpp`) read `steady_clock::now()` twice per message
unconditionally (`started` and `elapsed`), even when `EnableModuleTimings` was never enabled.
It now reads the clock only when `CapturesModuleTimings()` is true, otherwise
`elapsedMs = 0` (the TRACE log is kept).

#### 16.5 Results (MSVC, median of 5 s × 3; WSL2 check 5 s × 1)

| configuration | before | after | Δ | µs/msg before → after | p50 before → after |
|---|---|---|---|---|---|
| `L4cap16-p0` | 3.287 M | 3.698 M | **+12.5%** | 1.184 → 1.064 | 3.02 → 2.02 µs |
| `L4cap16-p4` | 1.946 M | 2.052 M | +5.5% | 1.852 → 1.655 | 9.06 → 8.06 µs |
| `L16cap8-p0` | 2.223 M | 2.484 M | +11.7% | 6.823 → 6.364 | 39.25 → 34.25 µs |
| `L16cap8-p4` | 0.952 M | 1.352 M | **+42.0%** | 3.709 → 2.716 | 52.25 → 36.25 µs |

WSL2 check: `L4cap16-p0` 3.292 → 3.563 M (+8.2%), `p4` 2.970 → 3.258 M (+9.7%).

Seven-repeat check (same machine, alternating order): `L4cap16-p4` 2.123 → 2.241 M (+5.6%),
`L16cap8-p4` 0.853 → 1.202 M (+40.9%). The ranges still overlap for the former (2.025–2.288 vs
1.892–2.309), but µs/message 1.660 → 1.545 and p50 10.06 → 8.06 agree, so it is judged a real
gain.

p4 gains relatively more because its fixed per-message cost is a larger share. After removing
the same clock bill, absolute throughput rose on both, but the p4/p0 ratio went from 59.2%
(1.946/3.287) to 55.5% (2.052/3.698) — the one-thread-per-module model gains more, so pooling's
**relative** cost actually grew slightly.

#### 16.6 What remains, and the lessons

- After the fix `clock_gettime` in the profile falls 14.74%→8.04% (p0), 9.55%→3.07% (p4); the
  remainder comes from the 1 ms timeout wait in `SpscQueue::waitAndPopFor`. The next candidate
  is "pull when there is a message, look at the clock only when there is not", or reusing one
  timestamp.
- Lesson one: **cost assumptions in comments go stale**. The original comment in
  `Module::ProcessTimed` asserted that "copying the timing map is the biggest hot-path cost";
  sampling showed that the timings-off path never makes that copy, while the clock was still
  read.
- Lesson two: sampling without frame pointers shows only top-level self time, not call chains,
  and it is easy to misread "some leaf function takes 20%" as "this function should be
  optimised" when what should really be removed is an unwanted API call.

#### 16.7 Reproduce

```bash
cmake -S . -B build-perf -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_CXX_FLAGS_RELEASE="-O2 -g -fno-omit-frame-pointer"
cmake --build build-perf --target pipeline_benchmark -j
perf record -e cpu-clock -F 1999 -g --call-graph fp -o p0.data -- \
  ./build-perf/benchmarks/pipeline_benchmark --topology linear --modules 4 --capacity 16 --duration 5
perf script -i p0.data > p0.txt
```

#### 16.8 Two more attempts, neither kept

The next time an intuition says "there is still a clock on the hot path", read this section
first.

**Attempt one: when `waitAndPopFor()` reads the clock for its deadline.** The old code did
`const auto deadline = steady_clock::now() + timeout;` **before** spinning, so even a message
obtained within the 20 µs spin paid for that clock read. It was changed to read only after the
spin fails and the thread is about to park; semantically it adds only one thing: once the
thread really parks, it may wait up to one extra spin budget (`kSpinBudgetMicros = 20` µs).

| configuration (median of 5 s × 3) | Windows before | after | change | WSL2 before | after | change |
| --- | --- | --- | --- | --- | --- | --- |
| L4cap16 p0 | 3.524 M | 3.464 M | −1.7% | 3.676 M | 3.605 M | −2.0% |
| L4cap16 p4 | 2.241 M | 2.269 M | +1.2% | 3.355 M | 3.273 M | −2.4% |
| L16cap8 p0 | 2.463 M | 2.415 M | −2.0% | 1.475 M | 1.428 M | −3.2% |
| L16cap8 p4 | 1.104 M | 1.091 M | −1.1% | 1.896 M | 1.875 M | −1.1% |

Across two platforms and four configurations the signs wobble around zero; nothing exceeds this
machine's repeat error.

**Attempt two: the per-round clock read in Phase 2 of `PullBatchMessage()`.** There the
`steady_clock::now()` only checks the batch timeout (default 100 ms) yet ran on every round in
which the queue happened to be momentarily empty. It was changed to sample once every 4 rounds,
adding at worst 4 × 1 ms of park.

| configuration (median of 5 s × 3) | Windows before | after | change | WSL2 before | after | change |
| --- | --- | --- | --- | --- | --- | --- |
| L4cap16 p0 | 3.420 M | 3.473 M | +1.5% | 3.426 M | 3.482 M | +1.6% |
| L4cap16 p4 | 2.225 M | 2.080 M | −6.5% | 3.211 M | 2.899 M | −9.7% |
| L16cap8 p0 | 2.146 M | 1.860 M | −13.3% | 0.33 M (unstable) | 1.50 M | — |
| L16cap8 p4 | 1.283 M | 1.258 M | −1.9% | 1.654 M | 1.829 M | +10.6% |

In this round the WSL2 L16cap8 baseline itself scattered over 0.33–1.48 M (16 modules has
always been unstable on WSL, noted in §14.3), and Windows −13.3% points the opposite way from
WSL2 +10.6%, so this can only be judged **noise**.

**Why neither bought throughput:** re-profiling after the changes (WSL2, p0, 5 s, 78 K
samples) shows `clock_gettime` at 8.04% → 7.53% in total, while the `PullBatchMessage` bucket
is still 2.55% — because the real clock read is the deadline in `waitAndPopFor()`, which
belongs to attempt one and was reverted. About 3 points of the remaining clock is **the harness
itself**: `SourceModule` stamps `creationTime` on every message and `SinkModule` reads the
clock per message to compute latency, both under `Module::ProcessTimed`'s `Process`. Driving
that to zero would mean making latency measurement itself optional (`--no-latency`), which is a
measurement cost, not a framework cost.

Both changes were reverted; what remains is the one fix in 16.4 (no timing, no clock read).
