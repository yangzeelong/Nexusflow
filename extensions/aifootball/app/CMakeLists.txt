add_executable(aifootball_app
    main.cpp
    AppConfig.cpp
    VideoReader.cpp
    JsonlWriter.cpp
    VideoRenderer.cpp)

target_include_directories(aifootball_app PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}"
    ${AIFOOTBALL_FFMPEG_INCLUDE_DIR})

target_link_libraries(aifootball_app PRIVATE
    aifootball_sdk
    JsonCpp::JsonCpp
    ${AIFOOTBALL_FFMPEG_AVFORMAT_LIB}
    ${AIFOOTBALL_FFMPEG_AVCODEC_LIB}
    ${AIFOOTBALL_FFMPEG_AVUTIL_LIB}
    ${AIFOOTBALL_FFMPEG_SWSCALE_LIB})

install(TARGETS aifootball_app RUNTIME DESTINATION bin)
