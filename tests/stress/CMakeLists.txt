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
            Threads::Threads
    )
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
        Threads::Threads
)
if(LIBURING)
    target_link_libraries(test_benchmark PRIVATE ${LIBURING})
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
        Threads::Threads
)
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
        Threads::Threads
)
if(LIBURING)
    target_link_libraries(test_io_stress PRIVATE ${LIBURING})
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
        Threads::Threads
)
sanitizer_add(test_idle_degradation)
include(GoogleTest)
gtest_discover_tests(test_idle_degradation)

# Online IO longevity test (20min, 4 workers, QD=32)
add_executable(test_longevity_online test_longevity_online.cpp)
target_link_libraries(test_longevity_online
    PRIVATE
        io
        runtime
        GTest::gtest_main
        GTest::gtest
        Threads::Threads
)
if(LIBURING)
    target_link_libraries(test_longevity_online PRIVATE ${LIBURING})
endif()
sanitizer_add(test_longevity_online)
include(GoogleTest)
gtest_discover_tests(test_longevity_online)

# Offline CPU longevity test (20min, 8 workers, random CPU tasks)
add_executable(test_longevity_offline test_longevity_offline.cpp)
target_link_libraries(test_longevity_offline
    PRIVATE
        runtime
        GTest::gtest_main
        GTest::gtest
        Threads::Threads
)
sanitizer_add(test_longevity_offline)
include(GoogleTest)
gtest_discover_tests(test_longevity_offline)

# Pool throughput benchmark (lock-free MemoryPool)
add_executable(test_pool_bench test_pool_bench.cpp)
target_link_libraries(test_pool_bench
    PRIVATE
        runtime
        GTest::gtest_main
        GTest::gtest
        Threads::Threads
)
sanitizer_add(test_pool_bench)
include(GoogleTest)
gtest_discover_tests(test_pool_bench)

# Per-NUMA shared pool multi-thread correctness & scaling tests
add_stress_test(test_pool_numa test_pool_numa.cpp)

# Mixed online + offline longevity test (20min, 2 online + 4 offline workers, cross-group sync)
add_executable(test_longevity_mixed test_longevity_mixed.cpp)
target_link_libraries(test_longevity_mixed
    PRIVATE
        runtime
        GTest::gtest_main
        GTest::gtest
        Threads::Threads
)
sanitizer_add(test_longevity_mixed)
include(GoogleTest)
gtest_discover_tests(test_longevity_mixed)
