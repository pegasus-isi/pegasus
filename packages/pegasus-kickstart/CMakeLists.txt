cmake_minimum_required(VERSION 3.20)
project(pegasus-kickstart C)

# Generate version.h from git state at build time
add_custom_command(
    OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/version.h"
    COMMAND "${CMAKE_SOURCE_DIR}/release-tools/getversion" --header
            > "${CMAKE_CURRENT_BINARY_DIR}/version.h"
    DEPENDS "${CMAKE_SOURCE_DIR}/release-tools/getversion"
    COMMENT "Generating kickstart version.h")
add_custom_target(kickstart_version_h
    DEPENDS "${CMAKE_CURRENT_BINARY_DIR}/version.h")

set(KICKSTART_SRCS
    getif.c utils.c useinfo.c statinfo.c jobinfo.c limitinfo.c
    machine.c machine/basic.c appinfo.c parse.c mysystem.c mylist.c
    invoke.c pegasus-kickstart.c procinfo.c sha2.c checksum.c)

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    list(APPEND KICKSTART_SRCS machine/linux.c syscall.c)
    add_compile_definitions(LINUX)
    # Large-file support
    execute_process(COMMAND getconf LFS_CFLAGS
        OUTPUT_VARIABLE _lfs_cflags OUTPUT_STRIP_TRAILING_WHITESPACE ERROR_QUIET)
    if(_lfs_cflags)
        string(REPLACE " " ";" _lfs_cflags_list "${_lfs_cflags}")
        add_compile_options(${_lfs_cflags_list})
    endif()
elseif(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    list(APPEND KICKSTART_SRCS machine/darwin.c)
    add_compile_definitions(DARWIN)
endif()

add_executable(pegasus-kickstart ${KICKSTART_SRCS})
add_dependencies(pegasus-kickstart kickstart_version_h)
target_compile_options(pegasus-kickstart PRIVATE -Wall -O2 -ggdb -std=gnu99)
target_include_directories(pegasus-kickstart PRIVATE
    "${CMAKE_CURRENT_BINARY_DIR}"
    "${CMAKE_CURRENT_SOURCE_DIR}")
target_link_libraries(pegasus-kickstart m)

# -fpie on GCC >= 14
if(CMAKE_C_COMPILER_ID STREQUAL "GNU")
    execute_process(COMMAND "${CMAKE_C_COMPILER}" -dumpversion
        OUTPUT_VARIABLE _gcc_ver OUTPUT_STRIP_TRAILING_WHITESPACE)
    string(REPLACE "." ";" _gcc_ver_list "${_gcc_ver}")
    list(GET _gcc_ver_list 0 _gcc_major)
    if(_gcc_major GREATER_EQUAL 14)
        target_compile_options(pegasus-kickstart PRIVATE -fpie)
    endif()
endif()

install(TARGETS pegasus-kickstart RUNTIME DESTINATION "${C_BIN_DEST}")

# libinterpose.so: Linux x86_64, non-musl only
if(CMAKE_SYSTEM_NAME STREQUAL "Linux" AND CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64")
    execute_process(COMMAND "${CMAKE_C_COMPILER}" -dumpmachine
        OUTPUT_VARIABLE _cc_triple OUTPUT_STRIP_TRAILING_WHITESPACE)
    if(NOT _cc_triple MATCHES "musl")
        add_library(interpose SHARED interpose.c)
        target_compile_options(interpose PRIVATE -Wall -O2 -ggdb -std=gnu99 -fPIC)
        target_link_libraries(interpose pthread dl)

        # Optional PAPI support
        find_library(_papi_lib papi)
        find_path(_papi_inc papi.h)
        if(_papi_lib AND _papi_inc)
            target_compile_definitions(interpose PRIVATE HAS_PAPI)
            target_link_libraries(interpose "${_papi_lib}")
        endif()

        install(TARGETS interpose LIBRARY DESTINATION "${C_LIB_DEST}")
    endif()
endif()
