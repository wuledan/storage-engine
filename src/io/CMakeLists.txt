add_library(io STATIC
    io_backend.cc
    io_engine.cc
    io_uring_backend.cc
    libaio_backend.cc
    spdk_backend.cc
)
target_include_directories(io PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../")
target_link_libraries(io PUBLIC runtime)
# 检查 liburing 是否存在
find_library(LIBURING uring)
if(LIBURING)
    target_link_libraries(io PUBLIC ${LIBURING})
    target_compile_definitions(io PUBLIC HAS_LIBURING)
endif()
# 检查 libaio 是否存在
find_library(LIBAIO aio)
if(LIBAIO)
    target_link_libraries(io PUBLIC ${LIBAIO})
    target_compile_definitions(io PUBLIC HAS_LIBAIO)
endif()
sanitizer_add(io)
