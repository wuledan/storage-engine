# Sanitizers.cmake
# Usage: cmake -DSANITIZE=address,thread,undefined ...
# Then call sanitizer_add(<target>) after defining the target.

function(sanitizer_add target)
    if(NOT SANITIZE)
        return()
    endif()

    string(REPLACE "," ";" SANITIZER_LIST "${SANITIZE}")
    set(SANITIZER_FLAGS "")

    foreach(SAN ${SANITIZER_LIST})
        string(TOLOWER "${SAN}" SAN_LOWER)
        if(SAN_LOWER STREQUAL "address")
            list(APPEND SANITIZER_FLAGS "-fsanitize=address")
        elseif(SAN_LOWER STREQUAL "thread")
            list(APPEND SANITIZER_FLAGS "-fsanitize=thread")
        elseif(SAN_LOWER STREQUAL "undefined")
            list(APPEND SANITIZER_FLAGS "-fsanitize=undefined")
        else()
            message(WARNING "Unknown sanitizer: ${SAN}")
        endif()
    endforeach()

    if(SANITIZER_FLAGS)
        target_compile_options(${target} PRIVATE ${SANITIZER_FLAGS})
        target_link_options(${target} PRIVATE ${SANITIZER_FLAGS})
    endif()
endfunction()
