# ---------------------------------------------------------------------------
# Module-level tests (concurrency, integration)
# ---------------------------------------------------------------------------

add_executable(test_concurrent test_concurrent.cpp)
target_link_libraries(test_concurrent
    PRIVATE
        runtime
        GTest::gtest_main
        GTest::gtest
        Folly::folly
        Threads::Threads
)
target_include_directories(test_concurrent PRIVATE ${FOLLY_INCLUDE_DIR})
if(FOLLY_BUILD_INCLUDE_DIR)
    target_include_directories(test_concurrent PRIVATE ${FOLLY_BUILD_INCLUDE_DIR})
endif()
sanitizer_add(test_concurrent)
include(GoogleTest)
gtest_discover_tests(test_concurrent)
