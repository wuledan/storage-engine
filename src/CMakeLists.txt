# src/CMakeLists.txt
# Runtime and core library targets will be added here.

# Core runtime sources
file(GLOB RUNTIME_SOURCES "runtime/*.cc")
# Exclude adapt/ .cc files (they need quant_invest build environment alignment)
list(FILTER RUNTIME_SOURCES EXCLUDE REGEX ".*/adapt/.*\\.cc$")
# Exclude offline_group.cc (depends on WorkStealingExecutor which may not compile standalone)
list(FILTER RUNTIME_SOURCES EXCLUDE REGEX ".*/offline_group\\.cc$")
# Include memory_pool.cc (needed by Worker memory pool integration)
list(APPEND RUNTIME_SOURCES "runtime/adapt/memory_pool.cc")
add_library(runtime STATIC ${RUNTIME_SOURCES})

target_include_directories(runtime PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}")
target_link_libraries(runtime PUBLIC Folly::folly Hwloc::hwloc Threads::Threads event)
sanitizer_add(runtime)
