file(GLOB_RECURSE NEXUSFLOW_ALL_FILES CONFIGURE_DEPENDS "**.cpp")

# Split source files and test files. 
separate_files_by_suffix("Test.cpp" "${NEXUSFLOW_ALL_FILES}" NEXUSFLOW_SRC_FILES NEXUSFLOW_TEST_FILES)
message(STATUS "NEXUSFLOW_ALL_FILES : ${NEXUSFLOW_ALL_FILES}")
message(STATUS "NEXUSFLOW_SRC_FILES : ${NEXUSFLOW_SRC_FILES}")
message(STATUS "NEXUSFLOW_TEST_FILES : ${NEXUSFLOW_TEST_FILES}")

if(NEXUSFLOW_TEST_FILES)
    set_property(GLOBAL APPEND PROPERTY GLOBAL_TEST_SOURCES ${NEXUSFLOW_TEST_FILES})
endif()


add_library(nexusflow STATIC ${NEXUSFLOW_SRC_FILES})
nexusflow_enable_warnings(nexusflow)
target_include_directories(nexusflow PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
)
target_include_directories(nexusflow PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/../include
)
target_link_libraries(nexusflow PUBLIC
    spdlog::spdlog
    yaml-cpp::yaml-cpp
)

install(TARGETS nexusflow
    ARCHIVE DESTINATION lib
    LIBRARY DESTINATION lib
    RUNTIME DESTINATION bin)
install(DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/../include/nexusflow"
    DESTINATION include)


