# NexusFlow: A High-Performance, Modern C++ Dataflow Pipeline Framework

[![Build Status](https://img.shields.io/badge/build-passing-brightgreen)](https://github.com/your_username/nexusflow)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![C++_Version](https://img.shields.io/badge/C++-14%2B-blue.svg)]()

**NexusFlow** is a high-performance, highly-decoupled, and dynamically configurable dataflow pipeline framework designed for modern C++. It aims to help developers easily build complex, multi-stage data processing tasks, such as video analytics pipelines, real-time ETL, industrial sensor data processing, and more.

The core idea of the framework is to decompose complex processing workflows into a series of independent **Modules**, which can then be connected like LEGO bricks into a powerful **Directed Acyclic Graph (DAG)** using simple configurations.

## Documentation map

| File | What it covers |
| --- | --- |
| [`AGENTS.md`](AGENTS.md) | Rules for anyone (human or agent) changing this repo: build, tests, counter conventions, hard rules. |
| [`TODO.md`](TODO.md) | Long-term roadmap: finished work, open items, known pitfalls. |
| [`NEXT-VERSION.md`](NEXT-VERSION.md) | The next release's work order: items, decisions to settle first, acceptance criteria. |
| [`benchmarks/README.md`](benchmarks/README.md) | Benchmark numbers, measurement protocol, and the machine they were taken on. |
| [`extensions/README.md`](extensions/README.md) | Extension layout and how to add a new one. |

## Core Features

*   **Modular Design, Highly Decoupled**: Each module focuses exclusively on its own business logic. Modules communicate asynchronously via message queues, remaining completely unaware of each other's existence.
*   **Dynamic Configuration, Powerful & Flexible**: Use simple YAML files to define the entire pipeline topology, module types, and parameters. Reconfigure and refactor complex business flows without recompiling your code.
*   **Automatic Concurrency, Simplified Development**: The framework automatically assigns dedicated threads to drive each module (or uses a defined strategy). You can focus on your business logic without manually managing complex thread synchronization and lifecycles.
*   **Clean API, Easy to Use**: Provides two primary ways to construct a pipeline: a **programmatic approach (`PipelineBuilder`)** for rapid development and testing, and a **declarative approach (from YAML)** for production environments.
*   **High-Performance & Modern C++**: Core components are designed for performance, leveraging modern C++ features. Its core data container, `nexusflow::Message`, uses **Copy-On-Write (COW)** semantics for extreme efficiency in broadcast scenarios.

---

## Extensions

Optional functionality ships as extensions under `extensions/`. The framework
core does not depend on any of them: an extension is built only when it is
listed in `-DNEXUSFLOW_EXTENSIONS=<name>` (semicolon-separated for several).

| Extension | Description | Requirements |
| --- | --- | --- |
| [`aifootball`](extensions/aifootball/README.md) | AI-Football algorithm SDK (`aifootball_sdk`), offline demo app, and the Python touch-evaluation toolchain. | CUDA, TensorRT, FFmpeg |

See [`extensions/README.md`](extensions/README.md) for the extension layout and
how to add a new one.

---

## Quick Start

The following example demonstrates how to build a pipeline with four modules: one input node distributes data to two parallel processing nodes, and one output node gathers the results.

### Option 1: Declarative Build via YAML (Recommended)

This is the most powerful and recommended way to use NexusFlow. It allows you to define your entire system through a configuration file, providing maximum flexibility.

#### 1. Create a Configuration File (`graph.yaml`)

```yaml
graph:
  name: "VideoAnalyticsPipeline"

  # 1. Define all module instances
  modules:
    - name: "InputNode"          # A unique instance name for the module
      class: "MockInputModule"   # The class name registered in the ModuleFactory

    - name: "ProcessNode1"
      class: "MockProcessModule"

    - name: "ProcessNode2"
      class: "MockProcessModule"

    - name: "OutputNode"
      class: "MockOutputModule"

  # 2. Define the connections (the dataflow topology)
  connections:
    - from: "InputNode"
      to: "ProcessNode1"
    - from: "InputNode"
      to: "ProcessNode2"
    - from: "ProcessNode1"
      to: "OutputNode"
    - from: "ProcessNode2"
      to: "OutputNode"
```

#### 2. Write the C++ Entry Point (`example.cpp`)

```cpp
#include "nexusflow/Pipeline.hpp"
#include "nexusflow/ModuleFactory.hpp"
#include "my_module/MockInputModule.hpp"    // Include your custom module headers
#include "my_module/MockProcessModule.hpp"
#include "my_module/MockOutputModule.hpp"
#include "nexusflow/Logging.hpp"               // Your logging utility

#include <iostream>
#include <chrono>
#include <thread>
#include <stdexcept>
#include <memory>

using namespace nexusflow;

// Register all custom modules with the factory at application startup.
void registerAllModules() {
    NEXUSFLOW_REGISTER_MODULE(MockInputModule);
    NEXUSFLOW_REGISTER_MODULE(MockProcessModule);
    NEXUSFLOW_REGISTER_MODULE(MockOutputModule);
}

// A helper function to encapsulate the pipeline execution and teardown logic.
void executePipeline(Pipeline& pipeline) {
    if (pipeline.Init() != ErrorCode::SUCCESS) {
        throw std::runtime_error("Pipeline initialization failed.");
    }

    pipeline.Start();
    LOG_INFO("Pipeline running for 10 seconds...");
    std::this_thread::sleep_for(std::chrono::seconds(10));
    pipeline.Stop();
    pipeline.DeInit();
}

int main(int argc, char* argv[]) {
    // ... Initialize logger ...

    if (argc < 2) {
        LOG_ERROR("Usage: {} <path_to_graph_yaml>", argv[0]);
        return 1;
    }

    try {
        registerAllModules();
        std::string configPath = argv[1];
        
        // The Pipeline class provides a static factory method.
        auto pipeline = Pipeline::CreateFromYaml(configPath);
        if (!pipeline) {
            throw std::runtime_error("Failed to create pipeline from YAML config.");
        }

        executePipeline(*pipeline);

    } catch (const std::exception& e) {
        LOG_CRITICAL("An exception occurred: {}", e.what());
        return 1;
    }

    LOG_INFO("Execution finished successfully.");
    return 0;
}
```

### Option 2: Programmatic Build via `PipelineBuilder`

This approach is suitable for simple applications, unit tests, or scenarios where the topology needs to be generated dynamically in code.

```cpp
#include "nexusflow/Pipeline.hpp"
#include "nexusflow/PipelineBuilder.hpp"
// ... other necessary headers ...

void runWithBuilder() {
    auto inputModule    = std::make_shared<MockInputModule>("InputNode");
    auto process1Module = std::make_shared<MockProcessModule>("ProcessNode1");
    auto process2Module = std::make_shared<MockProcessModule>("ProcessNode2");
    auto outputModule   = std::make_shared<MockOutputModule>("OutputNode");

    auto pipeline = PipelineBuilder()
                        .AddModule(inputModule)
                        .AddModule(process1Module)
                        .AddModule(process2Module)
                        .AddModule(outputModule)
                        .Connect("InputNode", "ProcessNode1")
                        .Connect("InputNode", "ProcessNode2")
                        .Connect("ProcessNode1", "OutputNode")
                        .Connect("ProcessNode2", "OutputNode")
                        .Build();
    if (!pipeline) {
        throw std::runtime_error("Failed to build pipeline.");
    }
    
    executePipeline(*pipeline);
}
```

---

## Runtime Tuning and Metrics

Every graph edge gets its own bounded message queue, and the queue capacity is the
backpressure depth of the pipeline: how many messages may be in flight between two
stages. Depth is a property of the pipeline, not of the framework, so there is no
default: raise it for throughput-heavy stages and lower it for latency-sensitive
ones.

In YAML (required):

```yaml
graph:
  name: "VideoAnalyticsPipeline"
  messageQueueCapacity: 64   # required, must be positive
  modules:
    # ...
```

Programmatically:

```cpp
auto pipeline = nexusflow::PipelineBuilder()
    .AddModule(source)
    .AddModule(sink)
    .Connect("source", "sink")
    .Build(64);   // required: the builder will not guess an edge capacity
```

`Pipeline::GetEdgeCapacity()` reports the depth the built pipeline used.

`Pipeline::GetSnapshot()` returns a snapshot of the runtime counters without
stopping the pipeline:

```cpp
const nexusflow::PipelineSnapshot snapshot = pipeline->GetSnapshot();

for (const auto& edge : snapshot.edges) {
    // edge.from, edge.to, edge.capacity, edge.size, edge.maxSize,
    // edge.pushedTotal, edge.poppedTotal, edge.discardedTotal
}
for (const auto& module : snapshot.modules) {
    // module.name, module.processedTotal, module.droppedTotal,
    // module.fusionExpiredTotal, module.failedTotal
}
std::cout << snapshot.ToString() << std::endl;   // one line, greppable
```

An edge reports `capacity == 0` when it is unbounded, and `maxSize` is the deepest
backlog observed so far. `ResetMetrics()` clears only those water marks: the
`*Total` counters are cumulative for the life of the pipeline and never go back, so
differencing two snapshots stays a rate. The modules are read before the edges, so a
reader can take a module and then the edge it writes into without seeing a message
that left the module but has not reached the edge yet.

## Stopping a Pipeline

There are two ways to stop, and the difference is what happens to the messages that
are already in flight between stages. Both are synchronous and both leave the
pipeline reusable, so a later `Start()` runs it again:

| Call | Messages still in an edge |
| --- | --- |
| `Stop()` | **drained**: sources are joined first, then each stage takes the rest of its input before it leaves, so every edge is empty when `Stop()` returns. |
| `Cancel()` | **discarded**: every stage is told to stop at once and every edge is closed first (which releases any thread parked in a push or a pull), then whatever was still buffered is destroyed. |

```cpp
pipeline->Stop();     // graceful: delivers the backlog
pipeline->Start();    // ...and runs again

pipeline->Cancel();   // immediate: drops the backlog
```

Discarding is accounted for on the edge, not on a module: `EdgeSnapshot::discardedTotal`
counts the messages `Cancel()` destroyed, so

```
pushedTotal == poppedTotal + size + discardedTotal
```

holds for every edge at all times. `Stop()` drains everything and therefore never
moves `discardedTotal`. Charging a discard to a module would double-count it — the
producer already counted it as processed and pushed, while no consumer ever popped it.

Neither call takes a timeout. After the producers are joined an empty input edge is a
state each stage reaches by itself, so the only thing that can still block a stop is a
`Process()` that never returns — and that is a bug in the module, not something a
deadline should paper over.

Where the stop went is in the next snapshot. `PipelineSnapshot::stopPipelineMs` is the wall
time of the last `Stop()`/`Cancel()`, and each `ModuleSnapshot` carries the part that
stage owned: `drainInputsMs` is how long the stop waited for its input edges to empty
(zero for a source, and zero after `Cancel()`, which never drains) and `exitLoopMs` is how
long it then waited for the module to leave its loop. Both are cleared by `Start()`, so
a snapshot taken mid-run never reports the previous run's numbers.

```cpp
pipeline->Stop();
const auto snapshot = pipeline->GetSnapshot();
LOG_INFO("stop took {} ms", snapshot.stopPipelineMs);
for (const auto& module : snapshot.modules) {
    if (module.drainInputsMs > 0 || module.exitLoopMs > 0) {
        LOG_INFO("{}: drain {} ms, exit {} ms", module.name, module.drainInputsMs, module.exitLoopMs);
    }
}
```

A stage with a large `drainInputsMs` is what made the stop slow: since stages are torn down
producers-first, everything downstream of it waits for that drain too. A large `exitLoopMs`
means the stage was still busy when its turn came, usually finishing the batch it had
already pulled.

## Pipeline Context

Each pipeline owns one context object that its modules can reach through
`Module::GetContext()`. It bundles the services that are genuinely shared by a
whole pipeline, instead of forcing them through process-wide singletons:

```cpp
auto pipeline = nexusflow::Pipeline::CreateFromYaml("graph.yaml");
pipeline->Init();

auto context = pipeline->GetContext();
context->SetSink(customSink);          // logs from this pipeline's module threads
auto frames = context->Intern("detect.frames");  // resolve the name once
context->Record(frames, 1.0);          // module-defined metric samples
const auto metrics = context->GetReportedMetrics();
const bool stopping = context->StopRequested();
```

- `SetSink` routes records produced on this pipeline's module threads to the given
  sink. Without one, the process-wide facade sink is used, so existing programs do
  not change.
- `Intern` resolves a metric name to a handle and `Record` adds a sample to it,
  accumulating `samples`, `total` and `last`. Holding the handle keeps the hot path
  to one vector index instead of hashing a string per sample; `Record(name, value)`
  is the convenient form. Read the samples back with `GetReportedMetrics()`, which
  copies the list; `GetReportedMetricCount()`, `FindMetric(handle)` and
  `ForEachMetric(fn)` let a poller walk them without that copy.
  `Pipeline::GetSnapshot()` reports the framework edges and module counters.
- Metric names are prefixed with the module name (`"detect.frames"`), because the
  context is shared by the whole pipeline and a bare `"latency"` from two modules
  would be indistinguishable.
- `RequestStop` is called by `Pipeline::Stop()` / `Pipeline::Cancel()` before the queues
  are shut down, so long-running modules can leave their loop cooperatively.
- `Profiler()` returns a per-pipeline `nexusflow::profiling::MetricsCollector`, so two
  pipelines that use identical module names no longer merge their timing samples.

The context is created by `Pipeline::Impl` during `Init()`; modules that never call
`GetContext()` behave exactly as before.

## Profiling a Module

`nexusflow::profiling::MetricsCollector` collects timing samples. Inside a module,
the `TIMER_START_*` macros time a scope and report on the policy they name:

```cpp
#include "nexusflow/MetricsCollector.hpp"

{
    TIMER_START_AVERAGE_MS("detect.infer", 1000);   // report every second
    RunInference(frame);
    TIMER_STOP_AVERAGE("detect.infer");
}

auto& collector = GetContext()->Profiler();        // per pipeline, not global
nexusflow::profiling::MetricSamples samples;
if (collector.GetStats("detect.infer", samples)) {
    // samples.items, samples.sumMs, samples.AvgItemMs(), samples.p95Ms,
    // samples.p99Ms, samples.windowItems, samples.windowSpanMs,
    // samples.minMs, samples.maxMs
}
```

Every duration in `MetricSamples` describes a single item, so the average and the
tail stay comparable even when one call covers a batch. The p95/p99 cover a recent
window (`windowItems` samples spanning `windowSpanMs` milliseconds) while min/max
cover the whole run. Both percentiles come out of one partition over that window,
so adding p99 costs no extra sort — and `windowItems` travels with the number
because a p99 over 20 items is just the slowest of those 20, not a tail.

There is deliberately no throughput field: a rate is `items` divided by wall-clock
time, and these samples are cumulative, so they cannot tell how long the run has
been going. Take two snapshots and divide the deltas:

```cpp
const auto start = std::chrono::steady_clock::now();
snapshotNow();                                     // read items here
// ... run for a while ...
const double wallS = std::chrono::duration<double>(
    std::chrono::steady_clock::now() - start).count();
const double itemsPerSecond = (itemsNow - itemsThen) / wallS;
```

## Core Component: The `nexusflow::Message`

The `nexusflow::Message` is the universal, thread-safe data wrapper that flows through the pipeline. It is designed to be both highly performant and exceptionally safe.

### Design Philosophy

1.  **Type-Erasure**: A `Message` can hold an object of **any data type**, allowing different modules to communicate seamlessly.
2.  **Copy-On-Write (COW)**: This is the core performance feature.
    *   **Copying is cheap**: Copying a `Message` is a fast `shared_ptr` operation, ideal for broadcasting data to multiple downstream modules.
    *   **Mutation is safe**: When you attempt to *modify* a shared `Message`, the framework automatically performs a deep copy of the data *before* the modification. This ensures that changes in one branch of the pipeline do not accidentally affect others.
3.  **Expressive & Safe Accessors**: Instead of traditional getters, `Message` uses a `Borrow`/`Mut` naming convention inspired by Rust to make the developer's intent crystal clear.

### How to Use `Message`

#### Creating a Message
The recommended way is to use the `nexusflow::MakeMessage` factory function.
```cpp
#include "nexusflow/Message.hpp"

// Create a message containing a string
auto msg1 = nexusflow::MakeMessage(std::string("Hello, Pipeline!"));

// Create a message containing a vector, specifying its source
auto msg2 = nexusflow::MakeMessage(std::vector<int>{1, 2, 3}, "SensorModule");
```

#### Immutable Access (Borrowing)
Use "borrowing" for **read-only** access. This operation is always fast and **will never** trigger a copy.

*   **`Borrow<T>()`**: Returns a `const` reference. Throws `std::runtime_error` on type mismatch.
*   **`BorrowPtr<T>()`**: Returns a `const` pointer. Returns `nullptr` on type mismatch (no-throw).

```cpp
// Safely check for type with BorrowPtr
if (const auto* content_ptr = msg1.BorrowPtr<std::string>()) {
    std::cout << "Content: " << *content_ptr << std::endl;
} else {
    std::cout << "Message does not contain a string." << std::endl;
}
```

#### Mutable Access (Mutating)
Use "mutating" for **read-write** access. This **will trigger a Copy-On-Write** if the data is shared.

*   **`Mut<T>()`**: Returns a non-`const` reference. Throws `std::runtime_error` on type mismatch.
*   **`MutPtr<T>()`**: Returns a non-`const` pointer. Returns `nullptr` on type mismatch (no-throw).

```cpp
// Safely get a mutable pointer
if (auto* vec_ptr = msg2.MutPtr<std::vector<int>>()) {
    vec_ptr->push_back(4); // This might trigger a COW
}
```

### Copy-On-Write in Action

```cpp
// 1. Create an original message (SharedCount: 1)
auto original_msg = nexusflow::MakeMessage(std::vector<int>{10, 20, 30});

// 2. Create a shared copy (SharedCount becomes 2)
auto shared_copy = original_msg;

// 3. One module modifies its copy. This triggers COW.
// shared_copy creates its own data. Its SharedCount becomes 1.
// original_msg's SharedCount reverts to 1.
shared_copy.Mut<std::vector<int>>()[1] = 99;

// 4. The data has diverged safely.
std::cout << original_msg.Borrow<std::vector<int>>()[1]; // -> 20
std::cout << shared_copy.Borrow<std::vector<int>>()[1];  // -> 99
```

---

## How to Write a Custom Module

Creating a new module is a simple two-step process:

#### 1. Inherit from `nexusflow::Module` and implement `Process`

```cpp
// modules/MultiplierModule.hpp
#include "nexusflow/Module.hpp"
#include "nexusflow/Message.hpp"

class MultiplierModule : public nexusflow::Module {
public:
    // Every module must have a constructor that accepts a name.
    explicit MultiplierModule(std::string name) : nexusflow::Module(std::move(name)) {}

    // Implement the core processing logic.
    void Process(nexusflow::Message& msg) override {
        // Use MutPtr for safe, mutable access.
        if (auto* data = msg.MutPtr<int>()) {
            // Multiply the received number by 2.
            *data *= 2;
            
            // Broadcast the modified message downstream.
            // The message is implicitly shared, no extra copy needed.
            Broadcast(msg);
        }
    }
};
```

#### 2. Register it with the `ModuleFactory` in your application

```cpp
// main.cpp
#include "nexusflow/ModuleFactory.hpp"
#include "modules/MultiplierModule.hpp"

void registerAllModules() {
    // Register the MultiplierModule with the factory.
    NEXUSFLOW_REGISTER_MODULE(MultiplierModule);
}
```
That's it! You can now use `class: "MultiplierModule"` in your `graph.yaml` file.

## Building the Project

The project uses CMake (3.15+) and is written in C++14.

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel 16
```

| Option | Default | Meaning |
| --- | --- | --- |
| `WITH_TESTING` | `ON` | Build the GoogleTest suite (`nexusflow_tests`). |
| `WITH_EXAMPLES` | `ON` | Build the example pipelines under `examples/`. |
| `WITH_BENCHMARK` | `ON` | Build the Google Benchmark targets under `benchmarks/`. |
| `NEXUSFLOW_EXTENSIONS` | empty | Semicolon-separated extensions to build from `extensions/<name>` (see [Extensions](#extensions)). |

Third-party dependencies (spdlog, yaml-cpp, jsoncpp, GoogleTest, Google
Benchmark) are pulled in with CMake `FetchContent`. On an offline machine point
`FETCHCONTENT_SOURCE_DIR_<NAME>` at local checkouts.

With the default Unix generator artifacts go to `build/bin/` and `build/lib/`;
multi-config generators (Visual Studio) use per-configuration directories such
as `build/src/Release/`.

```bash
./build/bin/nexusflow_tests                                        # unit tests
./build/bin/nexusflow_how_to_use examples/1-how-to-use/config.yaml
```

### Installing and Exporting the Package

```bash
cmake --install build --prefix <install-dir>
```

The install rule publishes:

- `lib/libnexusflow.a` (or the MSVC `.lib`) — the framework library;
- `include/nexusflow/**` — public headers, including the header-only utilities in
  `include/nexusflow/common/` (`Any`, `ConcurrentQueue`, `Optional`, `ViewPtr`);
- `lib/cmake/Nexusflow/` — `NexusflowConfig.cmake`,
  `NexusflowConfigVersion.cmake` and `nexusflowTargets.cmake` (namespace
  `Nexusflow::`).

The bundled `spdlog` and `yaml-cpp` builds are installed into the same prefix, so
the exported targets can resolve their dependencies without a second install.

A downstream CMake project can then write:

```cmake
find_package(Nexusflow CONFIG REQUIRED)
target_link_libraries(my_app PRIVATE Nexusflow::nexusflow)
```

`Nexusflow::nexusflow_common` is the header-only INTERFACE target for the
containers and utilities and is exported as well. The exported targets link
`spdlog::spdlog` and `yaml-cpp::yaml-cpp`, so a consumer must be able to resolve
those targets; `nexusflowConfig.cmake` calls `find_dependency` when they are
missing.

## Contributing

Contributions of any kind are welcome! If you have ideas, suggestions, or have found a bug, please feel free to open a Pull Request or create an Issue.

## License

This project is licensed under the **MIT License**. See the [LICENSE](LICENSE) file for details.
