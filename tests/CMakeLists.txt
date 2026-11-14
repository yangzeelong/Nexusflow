get_property(ALL_TEST_SOURCES GLOBAL PROPERTY GLOBAL_TEST_SOURCES)

if(NOT ALL_TEST_SOURCES)
    message(WARNING "No test sources found – will not build unit tests, ALL_TEST_SOURCES: ${ALL_TEST_SOURCES}")
    return()
endif()



file(GLOB_RECURSE CUR_TEST_SOURCES CONFIGURE_DEPENDS "**Test.cpp")

add_executable(nexusflow_tests gtest_main.cpp ${CUR_TEST_SOURCES} ${ALL_TEST_SOURCES})
# Some tests exercise framework internals (src/...) through relative includes, so add
# the src root to the include path.
target_include_directories(nexusflow_tests PRIVATE ${PROJECT_SOURCE_DIR}/src)
nexusflow_enable_warnings(nexusflow_tests)

target_link_libraries(nexusflow_tests PRIVATE nexusflow GTest::gtest)

add_test(NAME nexusflow_tests COMMAND nexusflow_tests)
