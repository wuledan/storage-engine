# ---------------------------------------------------------------------------
# Existing build-health test
# ---------------------------------------------------------------------------
add_executable(unit_tests test_build.cpp)
target_link_libraries(unit_tests PRIVATE GTest::gtest_main GTest::gtest)
target_include_directories(unit_tests PRIVATE ${FOLLY_INCLUDE_DIR})
if(FOLLY_BUILD_INCLUDE_DIR)
    target_include_directories(unit_tests PRIVATE ${FOLLY_BUILD_INCLUDE_DIR})
endif()
sanitizer_add(unit_tests)
include(GoogleTest)
gtest_discover_tests(unit_tests)

# ---------------------------------------------------------------------------
# Unit tests for storage-engine runtime
# ---------------------------------------------------------------------------

# Helper: create a test executable for each source file
function(add_runtime_test TEST_NAME TEST_SOURCE)
    add_executable(${TEST_NAME} ${TEST_SOURCE})
    target_link_libraries(${TEST_NAME}
        PRIVATE
            runtime
            GTest::gtest_main
            GTest::gtest
            Folly::folly
            Threads::Threads
    )
    target_include_directories(${TEST_NAME} PRIVATE ${FOLLY_INCLUDE_DIR})
    if(FOLLY_BUILD_INCLUDE_DIR)
        target_include_directories(${TEST_NAME} PRIVATE ${FOLLY_BUILD_INCLUDE_DIR})
    endif()
    sanitizer_add(${TEST_NAME})
    gtest_discover_tests(${TEST_NAME})
endfunction()

add_runtime_test(test_types              test_types.cpp)
add_runtime_test(test_local_queue        test_local_queue.cpp)
add_runtime_test(test_batched_spsc_queue test_batched_spsc_queue.cpp)
add_runtime_test(test_work_queues        test_work_queues.cpp)
add_runtime_test(test_scheduling         test_scheduling.cpp)
add_runtime_test(test_adaptive_idle      test_adaptive_idle.cpp)
add_runtime_test(test_scheduler          test_scheduler.cpp)

# Phase 5-6: Worker, OnlineWorker, OnlineGroup, Runtime
add_runtime_test(test_worker            test_worker.cpp)
add_runtime_test(test_online_worker     test_online_worker.cpp)
add_runtime_test(test_online_group      test_online_group.cpp)
add_runtime_test(test_runtime           test_runtime.cpp)
