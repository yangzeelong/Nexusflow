file(GLOB_RECURSE SRC_FILES CONFIGURE_DEPENDS "**.cpp")
add_executable(nexusflow_how_to_use ${SRC_FILES})
target_link_libraries(nexusflow_how_to_use PRIVATE nexusflow)
nexusflow_enable_warnings(nexusflow_how_to_use)
