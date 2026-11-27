// Taskflow implementation of the same 4-stage streaming chain.
//
// Taskflow's model is task-graph rather than message-queue: a `tf::Pipeline`
// runs `num_lines` tokens concurrently through the same sequence of pipes, and
// each pipe is a task. There are no user-visible queues, so `--capacity` has no
// meaning here; `--workers` becomes the number of parallel lines.
//
// Tokens do not carry data; each line owns a Frame slot (`buffer[pf.line()]`),
// which is the pattern the Taskflow pipeline examples use.

#include <array>
#include <atomic>
#include <cstdio>
#include <thread>

#include <taskflow/taskflow.hpp>
#include <taskflow/algorithm/pipeline.hpp>

#include "common.hpp"

namespace
{
constexpr std::size_t kMaxLines = 64;
}  // namespace

int main(int argc, char** argv)
{
    const cmp::Options opt = cmp::ParseOptions(argc, argv);
    const cmp::TimerResolutionGuard timerGuard;
    cmp::PinToLogicalCpus(opt.cpus);

    const std::size_t lines = static_cast<std::size_t>(opt.workers > 0 ? opt.workers : 1);
    if (lines > kMaxLines)
    {
        std::fprintf(stderr, "--workers must be <= %zu\n", kMaxLines);
        return 1;
    }

    std::array<cmp::Frame, kMaxLines> buffer{};
    std::atomic<bool> measuring{false};
    std::atomic<bool> stop{false};
    std::atomic<std::uint64_t> delivered{0};
    cmp::LatencyHistogram histogram;

    tf::Executor executor(lines);
    tf::Taskflow taskflow;

    // Pipes share one signature (`tf::Pipeflow&`) and keep state per line.
    tf::Pipeline pl(
        lines,
        tf::Pipe{tf::PipeType::SERIAL, [&](tf::Pipeflow& pf) {
            if (stop.load(std::memory_order_relaxed))
            {
                pf.stop();
                return;
            }
            buffer[pf.line()] = cmp::MakeFrame(pf.token());
        }},
        tf::Pipe{tf::PipeType::SERIAL, [&](tf::Pipeflow& pf) {
            buffer[pf.line()].payload[1] = cmp::Work(buffer[pf.line()].seq + 1, opt.iters);
            if (opt.slowMs > 0)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(opt.slowMs));
            }
        }},
        tf::Pipe{tf::PipeType::SERIAL, [&](tf::Pipeflow& pf) {
            buffer[pf.line()].payload[2] = cmp::Work(buffer[pf.line()].seq + 2, opt.iters);
            if (opt.slowMs > 0)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(opt.slowMs));
            }
        }},
        tf::Pipe{tf::PipeType::SERIAL, [&](tf::Pipeflow& pf) {
            buffer[pf.line()].payload[3] = cmp::Work(buffer[pf.line()].seq + 3, opt.iters);
            if (opt.slowMs > 0)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(opt.slowMs));
            }
        }},
        tf::Pipe{tf::PipeType::SERIAL, [&](tf::Pipeflow& pf) {
            buffer[pf.line()].payload[4] = cmp::Work(buffer[pf.line()].seq + 4, opt.iters);
            if (opt.slowMs > 0)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(opt.slowMs));
            }
            if (measuring.load(std::memory_order_relaxed))
            {
                histogram.Add(cmp::NowNs() - buffer[pf.line()].stampNs);
                delivered.fetch_add(1, std::memory_order_relaxed);
            }
        }});

    taskflow.composed_of(pl);
    auto future = executor.run(taskflow);

    std::this_thread::sleep_for(std::chrono::milliseconds(opt.warmupMs));
    histogram.Reset();
    delivered.store(0, std::memory_order_relaxed);
    measuring.store(true, std::memory_order_relaxed);

    const double cpu0 = cmp::CpuSeconds();
    const std::uint64_t wall0 = cmp::NowNs();
    std::this_thread::sleep_for(std::chrono::duration<double>(opt.seconds));
    const std::uint64_t wall1 = cmp::NowNs();
    const double cpu1 = cmp::CpuSeconds();

    measuring.store(false, std::memory_order_relaxed);
    stop.store(true, std::memory_order_relaxed);
    future.wait();

    const double wall = static_cast<double>(wall1 - wall0) * 1e-9;
    cmp::PrintCsv("taskflow", opt, wall, delivered.load(std::memory_order_relaxed), cpu1 - cpu0, histogram);
    return 0;
}
