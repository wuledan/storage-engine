cmake_minimum_required(VERSION 3.20)
project(storage-engine CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Global compile options
add_compile_options(-Wall -Wextra -Wpedantic -g)

# Include sanitizers module
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
include(Sanitizers)

# Find dependencies
find_package(Threads REQUIRED)
find_package(GTest REQUIRED)

# folly: try pkg-config, then config mode, then known paths
find_package(PkgConfig QUIET)
if(PkgConfig_FOUND)
    pkg_check_modules(FOLLY QUIET folly)
endif()

# Note: find_package(folly CONFIG) skipped because prebuilt configs
# reference non-installed paths. We use manual fallback instead.

if(NOT FOLLY_FOUND)
    # Manual fallback using quant_invest prebuilt artifacts
    find_path(FOLLY_INCLUDE_DIR
        NAMES folly/Executor.h
        PATHS
            /home/wuledan/work/proj/quant_invest/build/_deps/folly-src
            /home/wuledan/work/3FS/third_party/folly
        NO_DEFAULT_PATH
    )
    find_library(FOLLY_LIBRARY
        NAMES folly
        PATHS
            /home/wuledan/work/proj/quant_invest/build/_deps/folly-build
            /home/wuledan/work/3FS/build/third_party/folly
        NO_DEFAULT_PATH
    )
    if(FOLLY_INCLUDE_DIR AND FOLLY_LIBRARY)
        add_library(Folly::folly UNKNOWN IMPORTED)
        # folly-config.h is generated in the build dir, so we need both dirs
        set(FOLLY_BUILD_INCLUDE_DIR "")
        if(EXISTS "/home/wuledan/work/proj/quant_invest/build/_deps/folly-build/folly/folly-config.h")
            set(FOLLY_BUILD_INCLUDE_DIR "/home/wuledan/work/proj/quant_invest/build/_deps/folly-build")
        elseif(EXISTS "/home/wuledan/work/3FS/build/third_party/folly/folly/folly-config.h")
            set(FOLLY_BUILD_INCLUDE_DIR "/home/wuledan/work/3FS/build/third_party/folly")
        endif()
        set_target_properties(Folly::folly PROPERTIES
            IMPORTED_LOCATION "${FOLLY_LIBRARY}"
            INTERFACE_INCLUDE_DIRECTORIES "${FOLLY_INCLUDE_DIR};${FOLLY_BUILD_INCLUDE_DIR}"
        )
        set(FOLLY_FOUND TRUE)
        message(STATUS "Found folly (manual): ${FOLLY_LIBRARY}")
    else()
        message(FATAL_ERROR "folly not found. Please install libfolly-dev or set FOLLY_ROOT.")
    endif()
endif()

# hwloc: try pkg-config, then manual
if(PkgConfig_FOUND)
    pkg_check_modules(HWLOC QUIET hwloc)
endif()
if(NOT HWLOC_FOUND)
    find_path(HWLOC_INCLUDE_DIR NAMES hwloc.h)
    find_library(HWLOC_LIBRARY NAMES hwloc)
    if(HWLOC_INCLUDE_DIR AND HWLOC_LIBRARY)
        add_library(Hwloc::hwloc UNKNOWN IMPORTED)
        set_target_properties(Hwloc::hwloc PROPERTIES
            IMPORTED_LOCATION "${HWLOC_LIBRARY}"
            INTERFACE_INCLUDE_DIRECTORIES "${HWLOC_INCLUDE_DIR}"
        )
        set(HWLOC_FOUND TRUE)
        message(STATUS "Found hwloc (manual): ${HWLOC_LIBRARY}")
    else()
        message(FATAL_ERROR "hwloc not found. Please install libhwloc-dev.")
    endif()
endif()

# jemalloc
find_library(JEMALLOC_LIBRARY NAMES jemalloc libjemalloc.so.2)
if(NOT JEMALLOC_LIBRARY)
    message(FATAL_ERROR "jemalloc not found. Please install libjemalloc-dev.")
endif()
find_path(JEMALLOC_INCLUDE_DIR NAMES jemalloc/jemalloc.h
    PATHS
        /home/wuledan/work/3FS/third_party/jemalloc/include
        /home/wuledan/work/3FS/build/third_party/jemalloc/include
    NO_DEFAULT_PATH
)
if(NOT JEMALLOC_INCLUDE_DIR)
    # Not fatal: some usages may not need the header at skeleton stage
    message(WARNING "jemalloc header not found; only library will be linked.")
endif()

# Preset-like build type configurations
set(CMAKE_CONFIGURATION_TYPES "Release;Debug;Sanitized" CACHE STRING "Available build types" FORCE)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Release" CACHE STRING "Build type" FORCE)
endif()

set(CMAKE_BUILD_TYPE "${CMAKE_BUILD_TYPE}" CACHE STRING "Build type" FORCE)
set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS Release Debug Sanitized)

# Sanitized build type flags
set(CMAKE_CXX_FLAGS_SANITIZED "-O1 -g -fno-omit-frame-pointer" CACHE STRING "Flags for Sanitized build" FORCE)
set(CMAKE_C_FLAGS_SANITIZED "-O1 -g -fno-omit-frame-pointer" CACHE STRING "Flags for Sanitized build" FORCE)

# Enable testing before adding tests subdirectory so CTest files are generated
# in the build root.
enable_testing()

# Add subdirectories
add_subdirectory(src)
add_subdirectory(tests)
