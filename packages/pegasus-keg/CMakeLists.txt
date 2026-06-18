cmake_minimum_required(VERSION 3.20)
project(pegasus-keg CXX)

# Skip entirely on musl libc (same logic as Makefile)
execute_process(COMMAND "${CMAKE_CXX_COMPILER}" -dumpmachine
    OUTPUT_VARIABLE _cxx_triple OUTPUT_STRIP_TRAILING_WHITESPACE ERROR_QUIET)
if(_cxx_triple MATCHES "musl")
    message(STATUS "pegasus-keg: skipping build on musl libc (${_cxx_triple})")
    return()
endif()

# Generate version.h at build time via genversion.sh
add_custom_command(
    OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/version.h"
    COMMAND bash "${CMAKE_CURRENT_SOURCE_DIR}/genversion.sh"
            > "${CMAKE_CURRENT_BINARY_DIR}/version.h"
    DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/genversion.sh"
    COMMENT "Generating keg version.h")
add_custom_target(keg_version_h DEPENDS "${CMAKE_CURRENT_BINARY_DIR}/version.h")

set(KEG_SRCS pegasus-keg.cc basic.cc)

if(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    list(APPEND KEG_SRCS darwin.cc)
    add_compile_definitions(DARWIN MACHINE_SPECIFIC=darwin)
    # Optional: sockio.h
    if(EXISTS "/usr/include/sys/sockio.h")
        add_compile_definitions(HAS_SYS_SOCKIO=1)
    endif()
elseif(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    list(APPEND KEG_SRCS linux.cc)
    add_compile_definitions(LINUX MACHINE_SPECIFIC=linux)
    # Large-file support
    execute_process(COMMAND getconf LFS_CFLAGS
        OUTPUT_VARIABLE _lfs_cflags OUTPUT_STRIP_TRAILING_WHITESPACE ERROR_QUIET)
    if(_lfs_cflags)
        string(REPLACE " " ";" _lfs_cflags_list "${_lfs_cflags}")
        add_compile_options(${_lfs_cflags_list})
    endif()
    # -fpie on GCC >= 14
    if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        execute_process(COMMAND "${CMAKE_CXX_COMPILER}" -dumpversion
            OUTPUT_VARIABLE _gcc_ver OUTPUT_STRIP_TRAILING_WHITESPACE)
        string(REPLACE "." ";" _gcc_ver_list "${_gcc_ver}")
        list(GET _gcc_ver_list 0 _gcc_major)
        if(_gcc_major GREATER_EQUAL 14)
            add_compile_options(-fpie)
        endif()
    endif()
else()
    message(FATAL_ERROR "pegasus-keg: unsupported platform ${CMAKE_SYSTEM_NAME}")
endif()

# Kernel major version (used by keg for display)
execute_process(COMMAND uname -r OUTPUT_VARIABLE _uname_r OUTPUT_STRIP_TRAILING_WHITESPACE)
string(REGEX MATCH "^([0-9]+)" _kern_major "${_uname_r}")
add_compile_definitions(MAJOR=${_kern_major})

add_executable(pegasus-keg ${KEG_SRCS})
add_dependencies(pegasus-keg keg_version_h)
target_compile_options(pegasus-keg PRIVATE -O -Wall -ffor-scope)
target_include_directories(pegasus-keg PRIVATE
    "${CMAKE_CURRENT_BINARY_DIR}"
    "${CMAKE_CURRENT_SOURCE_DIR}")
target_link_libraries(pegasus-keg m)

install(TARGETS pegasus-keg RUNTIME DESTINATION "${C_BIN_DEST}")
