# src/CMakeLists.txt
# Runtime and core library targets will be added here.

add_library(storage_runtime STATIC
    runtime/adaptive_idle.cc
)

target_include_directories(storage_runtime PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(storage_runtime PUBLIC
    Threads::Threads
)
