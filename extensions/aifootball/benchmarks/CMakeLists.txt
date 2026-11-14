# Pose benchmark: measures HRNet inference latency/throughput on the TensorRT
# engine used by the offline pipeline. It reads a real frame through the app's
# FFmpeg reader and drives the pose estimator directly.
add_executable(pose_benchmark
    pose_benchmark.cpp
    "${CMAKE_CURRENT_SOURCE_DIR}/../app/VideoReader.cpp")

target_include_directories(pose_benchmark PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}/../app"
    "${CMAKE_CURRENT_SOURCE_DIR}/../src"
    ${AIFOOTBALL_FFMPEG_INCLUDE_DIR})

target_link_libraries(pose_benchmark PRIVATE
    aifootball_sdk
    ${AIFOOTBALL_FFMPEG_AVFORMAT_LIB}
    ${AIFOOTBALL_FFMPEG_AVCODEC_LIB}
    ${AIFOOTBALL_FFMPEG_AVUTIL_LIB}
    ${AIFOOTBALL_FFMPEG_SWSCALE_LIB})

install(TARGETS pose_benchmark RUNTIME DESTINATION bin)
