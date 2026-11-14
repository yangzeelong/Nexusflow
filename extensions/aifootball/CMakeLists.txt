# =============================================================================
# Nexusflow extension: AI-Football
#
# Ships the AI-Football algorithm SDK (RF-DETR detector, ByteTracker, HRNet
# pose, keypoint smoother, football tracker), the offline demo app and the
# touch-eval tooling. Everything AI-Football specific lives in this directory;
# the framework only knows about extensions/<name>.
#
# Enable with: -DNEXUSFLOW_EXTENSIONS=aifootball
#
# Optional dependency roots (auto-detect by default):
#   -DAIFOOTBALL_CUDA_ROOT=/path/to/cuda
#   -DAIFOOTBALL_TRT_ROOT=/path/to/TensorRT
#   -DAIFOOTBALL_FFMPEG_ROOT=/path/to/ffmpeg
# =============================================================================

if(NOT TARGET nexusflow)
    message(FATAL_ERROR
        "The AI-Football extension must be configured as part of a Nexusflow "
        "build: pass -DNEXUSFLOW_EXTENSIONS=aifootball.")
endif()

# --- Options ----------------------------------------------------------------
option(AIFOOTBALL_BUILD_APP
       "Build the offline aifootball demo app (requires FFmpeg development files)" ON)
option(AIFOOTBALL_BUILD_BENCHMARKS
       "Build the AI-Football pose benchmark (requires FFmpeg development files)" ON)
set(AIFOOTBALL_CUDA_ROOT "" CACHE PATH
    "CUDA Toolkit root (default: auto-detect)")
set(AIFOOTBALL_TRT_ROOT "" CACHE PATH
    "TensorRT root containing include/NvInfer.h and lib/libnvinfer.*")
set(AIFOOTBALL_FFMPEG_ROOT "" CACHE PATH
    "FFmpeg development root containing include/ and lib/")

# --- CUDA -------------------------------------------------------------------
# CUDA is needed by the detector preprocessing kernel and the TensorRT glue.
# When it is missing we still configure and build a source-only SDK shell, so
# the extension can be inspected on machines without a GPU.
set(AIFOOTBALL_CUDA_AVAILABLE FALSE)
set(AIFOOTBALL_CUDA_INCLUDE_DIRS "")
set(AIFOOTBALL_CUDA_LIBRARIES "")

include(CheckLanguage)
check_language(CUDA)
if(NOT CMAKE_CUDA_COMPILER AND AIFOOTBALL_CUDA_ROOT)
    find_program(AIFOOTBALL_NVCC nvcc
        HINTS "${AIFOOTBALL_CUDA_ROOT}/bin" NO_DEFAULT_PATH)
    if(AIFOOTBALL_NVCC)
        set(CMAKE_CUDA_COMPILER "${AIFOOTBALL_NVCC}" CACHE FILEPATH "CUDA compiler")
        check_language(CUDA)
    endif()
endif()

if(CMAKE_CUDA_COMPILER)
    enable_language(CUDA)
    set(AIFOOTBALL_CUDA_AVAILABLE TRUE)
    if(AIFOOTBALL_CUDA_ROOT)
        list(APPEND AIFOOTBALL_CUDA_INCLUDE_DIRS
             "${AIFOOTBALL_CUDA_ROOT}/include")
    elseif(CMAKE_CUDA_COMPILER_TOOLKIT_ROOT)
        list(APPEND AIFOOTBALL_CUDA_INCLUDE_DIRS
             "${CMAKE_CUDA_COMPILER_TOOLKIT_ROOT}/include")
    endif()
    set(AIFOOTBALL_CUDA_LIBRARIES CUDA::cudart)
    message(STATUS "AI-Football: CUDA compiler ${CMAKE_CUDA_COMPILER}")
else()
    message(WARNING
        "AI-Football: CUDA Toolkit not found; building a source-only SDK shell "
        "(CUDA preprocessing and TensorRT inference disabled). Set "
        "AIFOOTBALL_CUDA_ROOT to enable them.")
endif()

# --- TensorRT ---------------------------------------------------------------
set(AIFOOTBALL_TRT_FOUND FALSE)
set(AIFOOTBALL_TRT_INCLUDE_DIR "")
set(AIFOOTBALL_TRT_NVINFER_LIB "")
set(AIFOOTBALL_TRT_PLUGIN_LIB "")

if(AIFOOTBALL_CUDA_AVAILABLE)
    find_path(AIFOOTBALL_TRT_INCLUDE_DIR NvInfer.h
        HINTS "${AIFOOTBALL_TRT_ROOT}/include"
        PATHS /usr/include/x86_64-linux-gnu /usr/include /usr/local/include)
    find_library(AIFOOTBALL_TRT_NVINFER_LIB nvinfer
        HINTS "${AIFOOTBALL_TRT_ROOT}/lib" "${AIFOOTBALL_TRT_ROOT}/lib64"
        PATHS /usr/lib/x86_64-linux-gnu /usr/lib /usr/local/lib)
    find_library(AIFOOTBALL_TRT_PLUGIN_LIB nvinfer_plugin
        HINTS "${AIFOOTBALL_TRT_ROOT}/lib" "${AIFOOTBALL_TRT_ROOT}/lib64"
        PATHS /usr/lib/x86_64-linux-gnu /usr/lib /usr/local/lib)
    if(AIFOOTBALL_TRT_INCLUDE_DIR AND AIFOOTBALL_TRT_NVINFER_LIB)
        set(AIFOOTBALL_TRT_FOUND TRUE)
        message(STATUS "AI-Football: TensorRT ${AIFOOTBALL_TRT_NVINFER_LIB}")
    else()
        message(WARNING
            "AI-Football: TensorRT not found; building a source-only SDK shell "
            "(inference backend disabled). Set AIFOOTBALL_TRT_ROOT to enable it.")
    endif()
endif()

# --- Algorithm SDK ----------------------------------------------------------
file(GLOB_RECURSE AIFOOTBALL_SDK_SOURCES
     "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp")

if(NOT AIFOOTBALL_TRT_FOUND)
    list(FILTER AIFOOTBALL_SDK_SOURCES EXCLUDE
         REGEX ".*/inference/TensorRTEngine\\.cpp$")
endif()

if(AIFOOTBALL_CUDA_AVAILABLE AND AIFOOTBALL_TRT_FOUND)
    list(APPEND AIFOOTBALL_SDK_SOURCES
         "${CMAKE_CURRENT_SOURCE_DIR}/src/detector/RFDetrCudaPreprocess.cu")
endif()

add_library(aifootball_sdk STATIC ${AIFOOTBALL_SDK_SOURCES})
add_library(Nexusflow::aifootball_sdk ALIAS aifootball_sdk)

target_include_directories(aifootball_sdk
    PUBLIC
        "${CMAKE_CURRENT_SOURCE_DIR}/include"
    PRIVATE
        "${CMAKE_CURRENT_SOURCE_DIR}/src"
        "${PROJECT_SOURCE_DIR}/src")

if(AIFOOTBALL_CUDA_INCLUDE_DIRS)
    target_include_directories(aifootball_sdk PRIVATE
        ${AIFOOTBALL_CUDA_INCLUDE_DIRS})
endif()
if(AIFOOTBALL_TRT_INCLUDE_DIR)
    target_include_directories(aifootball_sdk PRIVATE
        ${AIFOOTBALL_TRT_INCLUDE_DIR})
endif()

target_compile_definitions(aifootball_sdk PRIVATE
    $<$<BOOL:${AIFOOTBALL_TRT_FOUND}>:WITH_TENSORRT>
    $<$<BOOL:${AIFOOTBALL_CUDA_AVAILABLE}>:WITH_CUDA>
    $<$<BOOL:${AIFOOTBALL_TRT_FOUND}>:$<$<BOOL:${AIFOOTBALL_CUDA_AVAILABLE}>:WITH_CUDA_KERNELS>>)

target_link_libraries(aifootball_sdk PUBLIC nexusflow)
if(AIFOOTBALL_CUDA_LIBRARIES)
    target_link_libraries(aifootball_sdk PUBLIC ${AIFOOTBALL_CUDA_LIBRARIES})
endif()
if(AIFOOTBALL_TRT_NVINFER_LIB)
    target_link_libraries(aifootball_sdk PUBLIC ${AIFOOTBALL_TRT_NVINFER_LIB})
endif()
if(AIFOOTBALL_TRT_PLUGIN_LIB)
    target_link_libraries(aifootball_sdk PUBLIC ${AIFOOTBALL_TRT_PLUGIN_LIB})
endif()

if(AIFOOTBALL_CUDA_AVAILABLE)
    set_target_properties(aifootball_sdk PROPERTIES
        CUDA_STANDARD 14
        CUDA_STANDARD_REQUIRED ON)
    if(CMAKE_VERSION VERSION_GREATER_EQUAL 3.24)
        set_property(TARGET aifootball_sdk PROPERTY CUDA_ARCHITECTURES native)
    endif()
endif()

install(TARGETS aifootball_sdk
    ARCHIVE DESTINATION lib
    LIBRARY DESTINATION lib
    RUNTIME DESTINATION bin)
install(DIRECTORY include/ DESTINATION include)

# --- FFmpeg (offline app and benchmark only) --------------------------------
set(AIFOOTBALL_FFMPEG_FOUND FALSE)
set(AIFOOTBALL_FFMPEG_INCLUDE_DIR "")
set(AIFOOTBALL_FFMPEG_AVFORMAT_LIB "")
set(AIFOOTBALL_FFMPEG_AVCODEC_LIB "")
set(AIFOOTBALL_FFMPEG_AVUTIL_LIB "")
set(AIFOOTBALL_FFMPEG_SWSCALE_LIB "")

if(AIFOOTBALL_BUILD_APP OR AIFOOTBALL_BUILD_BENCHMARKS)
    find_path(AIFOOTBALL_FFMPEG_INCLUDE_DIR libavcodec/avcodec.h
        HINTS "${AIFOOTBALL_FFMPEG_ROOT}/include"
        PATHS /usr/include/x86_64-linux-gnu /usr/local/include/x86_64-linux-gnu
              /usr/include /usr/local/include)

    foreach(_ffmpeg_lib avformat avcodec avutil swscale)
        string(TOUPPER "${_ffmpeg_lib}" _ffmpeg_lib_upper)
        find_library(AIFOOTBALL_FFMPEG_${_ffmpeg_lib_upper}_LIB ${_ffmpeg_lib}
            HINTS "${AIFOOTBALL_FFMPEG_ROOT}/lib"
            PATHS /usr/lib/x86_64-linux-gnu /usr/local/lib/x86_64-linux-gnu
                  /usr/lib /usr/local/lib)
    endforeach()
    unset(_ffmpeg_lib)
    unset(_ffmpeg_lib_upper)

    if(AIFOOTBALL_FFMPEG_INCLUDE_DIR AND
       AIFOOTBALL_FFMPEG_AVFORMAT_LIB AND AIFOOTBALL_FFMPEG_AVCODEC_LIB AND
       AIFOOTBALL_FFMPEG_AVUTIL_LIB AND AIFOOTBALL_FFMPEG_SWSCALE_LIB)
        set(AIFOOTBALL_FFMPEG_FOUND TRUE)
        message(STATUS "AI-Football: FFmpeg ${AIFOOTBALL_FFMPEG_INCLUDE_DIR}")
    else()
        message(WARNING
            "AI-Football: FFmpeg development files not found; skipping the "
            "offline app and benchmark. Set AIFOOTBALL_FFMPEG_ROOT to enable them.")
    endif()
endif()

if(AIFOOTBALL_BUILD_APP AND AIFOOTBALL_FFMPEG_FOUND)
    # JSON output (observations.jsonl) is app-only; the SDK does not depend on it.
    include("${PROJECT_SOURCE_DIR}/cmake/3rdparty/jsoncpp.cmake")
    add_subdirectory(app)
endif()

if(AIFOOTBALL_BUILD_BENCHMARKS AND AIFOOTBALL_FFMPEG_FOUND)
    add_subdirectory(benchmarks)
endif()

# --- Status -----------------------------------------------------------------
message(STATUS "AI-Football extension:")
message(STATUS "  aifootball_sdk      : ${CMAKE_CURRENT_SOURCE_DIR}")
message(STATUS "  CUDA                : ${AIFOOTBALL_CUDA_AVAILABLE}")
message(STATUS "  TensorRT            : ${AIFOOTBALL_TRT_FOUND}")
message(STATUS "  FFmpeg (app/bench)  : ${AIFOOTBALL_FFMPEG_FOUND}")
