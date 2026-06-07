enable_testing()

# GTest already found in top-level, but ensure variables are available
if(NOT TARGET GTest::gtest)
    message(FATAL_ERROR "GTest not found")
endif()

add_subdirectory(unit)
add_subdirectory(module)
add_subdirectory(stress)
