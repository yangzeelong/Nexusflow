include(FetchContent)

# Pinned to 0.8.0, not 0.7.0. yaml-cpp 0.7.0 declares its install rules through
# cmake_dependent_option(YAML_CPP_INSTALL ... "CMAKE_SOURCE_DIR STREQUAL
# PROJECT_SOURCE_DIR" OFF), a condition that is false for every nested
# FetchContent build. The dependent option then replaces the cached value with
# its OFF fallback, so the FORCE below had no effect and yaml-cpp never joined
# an export set: configuring died at generate time with
#   install(EXPORT "nexusflowTargets" ...) includes target "nexusflow" which
#   requires target "yaml-cpp" that is not in any export set.
# 0.8.0 declares it as a plain option(), which the FORCE below does turn on, and
# it exports yaml-cpp with "NAMESPACE yaml-cpp::" -- exactly the target name
# NexusflowConfig.cmake.in looks for.
FetchContent_Declare(yaml URL
	https://github.com/jbeder/yaml-cpp/archive/refs/tags/0.8.0.tar.gz)
# Same as spdlog: nested builds skip yaml-cpp's install rules, which would make
# the exported NexusFlow package reference a target nobody installs.
set(YAML_CPP_INSTALL ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(yaml)


