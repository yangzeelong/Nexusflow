include(FetchContent)

FetchContent_Declare(yaml URL
	https://github.com/jbeder/yaml-cpp/archive/refs/tags/yaml-cpp-0.7.0.tar.gz)
# Same as spdlog: nested builds skip yaml-cpp's install rules, which would make
# the exported NexusFlow package reference a target nobody installs.
set(YAML_CPP_INSTALL ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(yaml)


