add_test([=[BenchmarkIO.CoroutinePipeline]=]  /home/wuledan/work/proj/storage-engine/build_sanitized/tests/stress/test_benchmark [==[--gtest_filter=BenchmarkIO.CoroutinePipeline]==] --gtest_also_run_disabled_tests)
set_tests_properties([=[BenchmarkIO.CoroutinePipeline]=]  PROPERTIES WORKING_DIRECTORY /home/wuledan/work/proj/storage-engine/build_sanitized/tests/stress SKIP_REGULAR_EXPRESSION [==[\[  SKIPPED \]]==])
set(  test_benchmark_TESTS BenchmarkIO.CoroutinePipeline)
