# Every unit test lives directly in tests/test/ (see AGENTS.md). The glob picks
# up a new *Test.cpp without touching this file.
file(GLOB_RECURSE CUR_TEST_SOURCES CONFIGURE_DEPENDS "**Test.cpp")

if(NOT CUR_TEST_SOURCES)
    message(WARNING "No test sources found in tests/test – will not build unit tests.")
    return()
endif()

add_executable(nexusflow_tests gtest_main.cpp ${CUR_TEST_SOURCES})
# Some tests exercise framework internals (src/...) through plain includes such as
# "base/Graph.hpp", so add the src root to the include path.
target_include_directories(nexusflow_tests PRIVATE ${PROJECT_SOURCE_DIR}/src)
nexusflow_enable_warnings(nexusflow_tests)

target_link_libraries(nexusflow_tests PRIVATE nexusflow GTest::gtest)

add_test(NAME nexusflow_tests COMMAND nexusflow_tests)
