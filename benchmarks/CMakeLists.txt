# Every benchmark lives directly in benchmarks/ (see AGENTS.md). Two targets:
#
# 1) Google Benchmark cases. A file named Benchmark*.cpp holds cases only, and
#    they all link into one binary next to benchmark_main.cpp.
file(GLOB_RECURSE ALL_BENCHMARK_SOURCES CONFIGURE_DEPENDS "Benchmark*.cpp")
add_executable(nexusflow_benchmarks benchmark_main.cpp ${ALL_BENCHMARK_SOURCES})
nexusflow_enable_warnings(nexusflow_benchmarks)
target_link_libraries(nexusflow_benchmarks PRIVATE nexusflow benchmark::benchmark)

add_test(NAME nexusflow_benchmarks COMMAND nexusflow_benchmarks)

# 2) End-to-end throughput harness. It owns its main() and its own CLI
#    (--capacity), so it is a separate program and it deliberately does not match
#    the Benchmark*.cpp glob above.
add_executable(pipeline_benchmark pipeline_benchmark.cpp)
nexusflow_enable_warnings(pipeline_benchmark)
target_link_libraries(pipeline_benchmark PRIVATE nexusflow)
