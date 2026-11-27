#!/usr/bin/env bash
# Builds all comparison programs inside WSL2. Not part of the CMake build.
# g++-11 and -O3 -DNDEBUG are what CMake's Release configuration uses for the
# NexusFlow side, so both sides of the comparison see the same code generator
# and the same optimisation level. oneTBB is the same 2021.13 as on Windows.
set -eu
SRC="$(cd "$(dirname "$0")" && pwd)"
TF=/mnt/c/Code/3rdparty/taskflow
CXX=${CXX:-g++-11}
TBB=/root/onetbb-install

"$CXX" -O3 -DNDEBUG -std=c++17 -pthread -I "$TBB/include" "$SRC/tbb_graph.cpp" \
    -o /root/cmp_tbb -L "$TBB/lib" -ltbb -Wl,-rpath,"$TBB/lib"
"$CXX" -O3 -DNDEBUG -std=c++17 -pthread -I "$TBB/include" "$SRC/tbb_pipeline.cpp" \
    -o /root/cmp_tbbpipe -L "$TBB/lib" -ltbb -Wl,-rpath,"$TBB/lib"
"$CXX" -O3 -DNDEBUG -std=c++17 -pthread "$SRC/raw_pipeline.cpp" -o /root/cmp_raw
"$CXX" -O3 -DNDEBUG -std=c++20 -pthread -I "$TF" "$SRC/tf_pipeline.cpp" -o /root/cmp_tf

# ai-pipe: configure and build only the library (tests/examples/benchmarks off),
# then link the comparison program against the static lib.
AIPIPE=/root/ai-pipe-ref
AIPIPE_BUILD=/root/ai-pipe-build
if [ ! -f "$AIPIPE_BUILD/x86_64/lib/libai_pipe.a" ]; then
    cmake -S "$AIPIPE" -B "$AIPIPE_BUILD" -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_CXX_COMPILER="$CXX" -DBUILD_SHARED_LIBS=OFF \
        -DBUILD_AI_PIPE_TESTS=OFF -DBUILD_AI_PIPE_EXAMPLES=OFF -DBUILD_AI_PIPE_BENCHMARKS=OFF
    cmake --build "$AIPIPE_BUILD" --target ai_pipe -j "$(nproc)"
fi
"$CXX" -O3 -DNDEBUG -std=c++20 -pthread -I "$AIPIPE/src/api" "$SRC/ai_pipeline.cpp" \
    -o /root/cmp_ai "$AIPIPE_BUILD"/*/lib/libai_pipe.a
echo build-wsl-ok
