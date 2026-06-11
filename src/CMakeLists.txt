# src/CMakeLists.txt
# Runtime and core library targets will be added here.

# Core runtime sources
file(GLOB RUNTIME_SOURCES "runtime/*.cc")
# Include IO backend core files (needed by OnlineWorker::init_io_backend)
list(APPEND RUNTIME_SOURCES "io/io_backend.cc" "io/io_engine.cc" "io/io_uring_backend.cc" "io/libaio_backend.cc" "io/spdk_backend.cc")
add_library(runtime STATIC ${RUNTIME_SOURCES})

target_include_directories(runtime PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}" "${CMAKE_CURRENT_SOURCE_DIR}/..")
find_library(LIBURING uring)
find_library(LIBAIO aio)
# ── SPDK for runtime (same conditional detection) ──
set(SPDK_LIBS "")
find_package(PkgConfig QUIET)
if(PkgConfig_FOUND)
    pkg_check_modules(SPDK spdk QUIET)
endif()
if(SPDK_FOUND)
    set(SPDK_LIBS ${SPDK_LIBRARIES})
endif()

target_link_libraries(runtime PUBLIC
    Folly::folly Hwloc::hwloc Threads::Threads event
    ${LIBURING} ${LIBAIO} ${SPDK_LIBS})
sanitizer_add(runtime)
