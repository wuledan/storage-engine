# ---------------------------------------------------------------------------
# Module-level tests (concurrency, integration)
# ---------------------------------------------------------------------------

function(add_module_test TEST_NAME TEST_SOURCE)
    add_executable(${TEST_NAME} ${TEST_SOURCE})
    target_link_libraries(${TEST_NAME}
        PRIVATE
            runtime
            GTest::gtest_main
            GTest::gtest
            Folly::folly
            Threads::Threads
    )
    target_include_directories(${TEST_NAME}
        PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}
            ${FOLLY_INCLUDE_DIR}
    )
    if(FOLLY_BUILD_INCLUDE_DIR)
        target_include_directories(${TEST_NAME} PRIVATE ${FOLLY_BUILD_INCLUDE_DIR})
    endif()
    sanitizer_add(${TEST_NAME})
    include(GoogleTest)
    gtest_discover_tests(${TEST_NAME})
endfunction()

add_module_test(test_concurrent test_concurrent.cpp)
add_module_test(test_mock_io_poller test_mock_io_poller.cpp)
