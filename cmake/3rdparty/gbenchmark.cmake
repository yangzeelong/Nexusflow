include(FetchContent)
FetchContent_Declare(
    GoogleBenchmark
    GIT_REPOSITORY https://github.com/google/benchmark.git
    GIT_TAG v1.8.0
)
# googlebenchmark ships its own gtest-based test suite and builds it by default.
# We only want the library: those tests added a dozen extra targets to every
# build, which is why WITH_BENCHMARK=ON used to be expensive enough to keep out
# of CI.
set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(GoogleBenchmark)
