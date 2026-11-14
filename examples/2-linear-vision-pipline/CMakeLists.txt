file(GLOB_RECURSE SRC_FILES CONFIGURE_DEPENDS "**.cpp")
add_executable(nexusflow_linear_pipeline ${SRC_FILES})
target_link_libraries(nexusflow_linear_pipeline PRIVATE nexusflow)
nexusflow_enable_warnings(nexusflow_linear_pipeline)
