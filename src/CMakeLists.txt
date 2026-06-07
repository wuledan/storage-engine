# src/CMakeLists.txt
# Runtime and core library targets will be added here.

# Core runtime sources (exclude adapt/ components that need quant_invest build env)
file(GLOB RUNTIME_SOURCES "runtime/*.cc")
list(FILTER RUNTIME_SOURCES EXCLUDE REGEX ".*/(offline_group|adapt/.*)\\.cc$")
add_library(runtime STATIC ${RUNTIME_SOURCES})

target_include_directories(runtime PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}")
target_link_libraries(runtime PUBLIC Folly::folly Hwloc::hwloc Threads::Threads)
sanitizer_add(runtime)
