include(FetchContent)

FetchContent_Declare(
    spdlog
    GIT_REPOSITORY https://github.com/gabime/spdlog.git
    GIT_TAG v1.10.0
)
# spdlog turns its install rules off when nested (SPDLOG_MASTER_PROJECT is
# false), but the exported NexusFlow package references spdlog::spdlog. Keep
# spdlog installable into the same prefix so find_dependency(spdlog) resolves.
set(SPDLOG_INSTALL ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(spdlog)



