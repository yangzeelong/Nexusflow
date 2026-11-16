file(GLOB_RECURSE NEXUSFLOW_ALL_FILES CONFIGURE_DEPENDS "**.cpp")

# Everything under src/ builds the library, and every unit test lives in
# tests/test/ (see AGENTS.md). The split stays as a guard: a file that ends in
# Test.cpp and still sits under src/ is dropped from the library instead of
# dragging gtest into it.
separate_files_by_suffix("Test.cpp" "${NEXUSFLOW_ALL_FILES}" NEXUSFLOW_SRC_FILES NEXUSFLOW_TEST_FILES)
message(STATUS "NEXUSFLOW_ALL_FILES : ${NEXUSFLOW_ALL_FILES}")
message(STATUS "NEXUSFLOW_SRC_FILES : ${NEXUSFLOW_SRC_FILES}")


include(GNUInstallDirs)

# Header-only utilities live under include/nexusflow/common. They get their own
# INTERFACE target so consumers can link just those headers, and so the install
# and export rules have a single place to publish them.
add_library(nexusflow_common INTERFACE)
target_compile_features(nexusflow_common INTERFACE cxx_std_14)
target_include_directories(nexusflow_common INTERFACE
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

add_library(nexusflow STATIC ${NEXUSFLOW_SRC_FILES})
nexusflow_enable_warnings(nexusflow)
target_include_directories(nexusflow PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
)
target_link_libraries(nexusflow PUBLIC
    nexusflow_common
    spdlog::spdlog
    yaml-cpp::yaml-cpp
)

install(TARGETS nexusflow nexusflow_common
    EXPORT nexusflowTargets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(DIRECTORY "${PROJECT_SOURCE_DIR}/include/nexusflow"
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

install(EXPORT nexusflowTargets
    NAMESPACE Nexusflow::
    FILE nexusflowTargets.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/Nexusflow)

include(CMakePackageConfigHelpers)
configure_package_config_file(
    "${PROJECT_SOURCE_DIR}/cmake/nexusflowConfig.cmake.in"
    "${CMAKE_CURRENT_BINARY_DIR}/NexusflowConfig.cmake"
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/Nexusflow)
write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/NexusflowConfigVersion.cmake"
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion)
install(FILES
    "${CMAKE_CURRENT_BINARY_DIR}/NexusflowConfig.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/NexusflowConfigVersion.cmake"
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/Nexusflow)
