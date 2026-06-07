add_executable(unit_tests test_build.cpp)

target_link_libraries(unit_tests
    PRIVATE
        GTest::gtest_main
        GTest::gtest
)

# We only need folly headers for the build test; avoid linking the full
# dependency closure of the prebuilt static libfolly.a.
target_include_directories(unit_tests PRIVATE ${FOLLY_INCLUDE_DIR})
if(FOLLY_BUILD_INCLUDE_DIR)
    target_include_directories(unit_tests PRIVATE ${FOLLY_BUILD_INCLUDE_DIR})
endif()

# Apply sanitizers if requested
sanitizer_add(unit_tests)

include(GoogleTest)
gtest_discover_tests(unit_tests)
