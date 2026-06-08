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

add_runtime_test(test_coroutine          test_coroutine.cpp)

# Phase 5-6: Worker, OnlineWorker, OnlineGroup, Runtime
add_runtime_test(test_worker            test_worker.cpp)
add_runtime_test(test_online_worker     test_online_worker.cpp)
add_runtime_test(test_online_group      test_online_group.cpp)
add_runtime_test(test_runtime           test_runtime.cpp)

add_runtime_test(test_coroutine_worker  test_coroutine_worker.cpp)
add_runtime_test(test_affinity_routing  test_affinity_routing.cpp)

add_runtime_test(test_memory_pool        test_memory_pool.cpp)

add_runtime_test(test_perf               test_perf.cpp)
add_runtime_test(test_perf_comprehensive  test_perf_comprehensive.cpp)
add_runtime_test(test_trace_config       test_trace_config.cpp)

add_runtime_test(test_decision_system     test_decision_system.cpp)

add_runtime_test(test_executor            test_executor.cpp)

add_runtime_test(test_feedback_loop       test_feedback_loop.cpp)

# IO types test (links io library + runtime)
add_executable(test_io_types test_io_types.cpp)
target_link_libraries(test_io_types
    PRIVATE
        io
        runtime
        GTest::gtest_main
        GTest::gtest
        Folly::folly
        Threads::Threads
)
target_include_directories(test_io_types PRIVATE ${FOLLY_INCLUDE_DIR})
if(FOLLY_BUILD_INCLUDE_DIR)
    target_include_directories(test_io_types PRIVATE ${FOLLY_BUILD_INCLUDE_DIR})
endif()
sanitizer_add(test_io_types)
gtest_discover_tests(test_io_types)

# IO uring test (links io library + runtime + uring)
add_executable(test_io_uring test_io_uring.cpp)
target_link_libraries(test_io_uring
    PRIVATE
        io
        runtime
        GTest::gtest_main
        GTest::gtest
        Folly::folly
        Threads::Threads
)
if(LIBURING)
    target_link_libraries(test_io_uring PRIVATE ${LIBURING})
endif()
target_include_directories(test_io_uring PRIVATE ${FOLLY_INCLUDE_DIR})
if(FOLLY_BUILD_INCLUDE_DIR)
    target_include_directories(test_io_uring PRIVATE ${FOLLY_BUILD_INCLUDE_DIR})
endif()
sanitizer_add(test_io_uring)
gtest_discover_tests(test_io_uring)

# IO backend test (libaio + SPDK skeleton)
add_executable(test_io_backends test_io_backends.cpp)
target_link_libraries(test_io_backends
    PRIVATE
        io
        runtime
        GTest::gtest_main
        GTest::gtest
        Folly::folly
        Threads::Threads
)
if(LIBURING)
    target_link_libraries(test_io_backends PRIVATE ${LIBURING})
endif()
if(LIBAIO)
    target_link_libraries(test_io_backends PRIVATE ${LIBAIO})
endif()
target_include_directories(test_io_backends PRIVATE ${FOLLY_INCLUDE_DIR})
if(FOLLY_BUILD_INCLUDE_DIR)
    target_include_directories(test_io_backends PRIVATE ${FOLLY_BUILD_INCLUDE_DIR})
endif()
sanitizer_add(test_io_backends)
gtest_discover_tests(test_io_backends)

# Error codes test
add_executable(test_error_codes test_error_codes.cpp)
target_link_libraries(test_error_codes
    PRIVATE
        io
        runtime
        GTest::gtest_main
        GTest::gtest
        Folly::folly
        Threads::Threads
)
if(LIBURING)
    target_link_libraries(test_error_codes PRIVATE ${LIBURING})
endif()
if(LIBAIO)
    target_link_libraries(test_error_codes PRIVATE ${LIBAIO})
endif()
target_include_directories(test_error_codes PRIVATE ${FOLLY_INCLUDE_DIR})
if(FOLLY_BUILD_INCLUDE_DIR)
    target_include_directories(test_error_codes PRIVATE ${FOLLY_BUILD_INCLUDE_DIR})
endif()
sanitizer_add(test_error_codes)
gtest_discover_tests(test_error_codes)
