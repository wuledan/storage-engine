add_library(io STATIC
    io_backend.cc
    io_engine.cc
    io_uring_backend.cc
    libaio_backend.cc
    spdk_backend.cc
)
target_include_directories(io PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../")
target_link_libraries(io PUBLIC runtime)

# ── liburing ──
find_library(LIBURING uring)
if(LIBURING)
    target_link_libraries(io PUBLIC ${LIBURING})
    target_compile_definitions(io PUBLIC HAS_LIBURING)
endif()

# ── libaio ──
find_library(LIBAIO aio)
if(LIBAIO)
    target_link_libraries(io PUBLIC ${LIBAIO})
    target_compile_definitions(io PUBLIC HAS_LIBAIO)
endif()

# ── SPDK ──
find_package(PkgConfig QUIET)
if(PkgConfig_FOUND)
    pkg_check_modules(SPDK spdk QUIET)
endif()
if(SPDK_FOUND)
    target_link_libraries(io PUBLIC ${SPDK_LIBRARIES})
    target_include_directories(io PUBLIC ${SPDK_INCLUDE_DIRS})
    target_compile_definitions(io PUBLIC HAS_SPDK)
    message(STATUS "Found SPDK: ${SPDK_LIBRARIES}")
else()
    message(STATUS "SPDK not found — SPDK backend will be stubbed out")
endif()

sanitizer_add(io)
