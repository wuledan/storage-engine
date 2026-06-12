# ---------------------------------------------------------------------------
# Stress and benchmark tests
# ---------------------------------------------------------------------------

# Helper: create a stress/benchmark test executable
function(add_stress_test TEST_NAME TEST_SOURCE)
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
    include(GoogleTest)
    gtest_discover_tests(${TEST_NAME})
endfunction()

add_stress_test(test_stress     test_stress.cpp)

# Benchmark test (also links io for IO benchmarks)
add_executable(test_benchmark test_benchmark.cpp)
target_link_libraries(test_benchmark
    PRIVATE
        io
        runtime
        GTest::gtest_main
        GTest::gtest
        Folly::folly
        Threads::Threads
)
if(LIBURING)
    target_link_libraries(test_benchmark PRIVATE ${LIBURING})
endif()
target_include_directories(test_benchmark PRIVATE ${FOLLY_INCLUDE_DIR})
if(FOLLY_BUILD_INCLUDE_DIR)
    target_include_directories(test_benchmark PRIVATE ${FOLLY_BUILD_INCLUDE_DIR})
endif()
sanitizer_add(test_benchmark)
include(GoogleTest)
gtest_discover_tests(test_benchmark)

# Work-stealing benchmark (bthread-style throughput + steal efficiency)
add_executable(test_work_stealing_bench test_work_stealing_bench.cpp)
target_link_libraries(test_work_stealing_bench
    PRIVATE
        runtime
        GTest::gtest_main
        GTest::gtest
        Folly::folly
        Threads::Threads
)
target_include_directories(test_work_stealing_bench PRIVATE ${FOLLY_INCLUDE_DIR})
if(FOLLY_BUILD_INCLUDE_DIR)
    target_include_directories(test_work_stealing_bench PRIVATE ${FOLLY_BUILD_INCLUDE_DIR})
endif()
sanitizer_add(test_work_stealing_bench)
include(GoogleTest)
gtest_discover_tests(test_work_stealing_bench)

# IO stress test (links io + runtime + uring)
add_executable(test_io_stress test_io_stress.cpp)
target_link_libraries(test_io_stress
    PRIVATE
        io
        runtime
        GTest::gtest_main
        GTest::gtest
        Folly::folly
        Threads::Threads
)
if(LIBURING)
    target_link_libraries(test_io_stress PRIVATE ${LIBURING})
endif()
target_include_directories(test_io_stress PRIVATE ${FOLLY_INCLUDE_DIR})
if(FOLLY_BUILD_INCLUDE_DIR)
    target_include_directories(test_io_stress PRIVATE ${FOLLY_BUILD_INCLUDE_DIR})
endif()
sanitizer_add(test_io_stress)
include(GoogleTest)
gtest_discover_tests(test_io_stress)

# Idle degradation test
add_executable(test_idle_degradation test_idle_degradation.cpp)
target_link_libraries(test_idle_degradation
    PRIVATE
        runtime
        GTest::gtest_main
        GTest::gtest
        Folly::folly
        Threads::Threads
)
target_include_directories(test_idle_degradation PRIVATE ${FOLLY_INCLUDE_DIR})
if(FOLLY_BUILD_INCLUDE_DIR)
    target_include_directories(test_idle_degradation PRIVATE ${FOLLY_BUILD_INCLUDE_DIR})
endif()
sanitizer_add(test_idle_degradation)
include(GoogleTest)
gtest_discover_tests(test_idle_degradation)
