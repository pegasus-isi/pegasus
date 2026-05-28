# Skip on musl libc — keg does not build there (mirrors Makefile MUSLLIBC check)
execute_process(
    COMMAND ${CMAKE_CXX_COMPILER} -dumpmachine
    OUTPUT_VARIABLE _MACHINE
    OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_QUIET)
if(_MACHINE MATCHES "musl")
    message(STATUS "musl libc detected — skipping pegasus-keg")
    return()
endif()

# Generate version.h with the git hash + short date (mirrors genversion.sh output)
execute_process(
    COMMAND git log -1 --date=short --pretty=format:"%H %ad"
    WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}"
    OUTPUT_VARIABLE _KEG_VER
    OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_QUIET)
if(NOT _KEG_VER)
    set(_KEG_VER "unknown")
endif()
file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/version.h"
    "#ifndef VERSION_H\n"
    "#define VERSION_H\n"
    "\n"
    "#define KEG_VERSION \"${_KEG_VER}\"\n"
    "\n"
    "#endif /* VERSION_H */\n")

# Platform-specific source selection (mirrors keg/Makefile SYSTEM detection)
set(KEG_SRCS pegasus-keg.cc basic.cc)
if(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    list(APPEND KEG_SRCS darwin.cc)
    set(_KEG_MACH "darwin")
elseif(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    list(APPEND KEG_SRCS linux.cc)
    set(_KEG_MACH "linux")
else()
    list(APPEND KEG_SRCS gnukfreebsd.cc)
    set(_KEG_MACH "gnukfreebsd")
endif()

add_executable(pegasus-keg ${KEG_SRCS})

target_include_directories(pegasus-keg PRIVATE
    "${CMAKE_CURRENT_BINARY_DIR}"   # generated version.h (takes priority)
    "${CMAKE_CURRENT_SOURCE_DIR}"   # *.hh headers
)

target_compile_options(pegasus-keg PRIVATE -O -Wall)
target_compile_definitions(pegasus-keg PRIVATE "MACHINE_SPECIFIC=${_KEG_MACH}")
target_link_libraries(pegasus-keg PRIVATE m)

install(PROGRAMS $<TARGET_FILE:pegasus-keg>
        DESTINATION "${C_BIN_DEST}"
        RENAME pegasus-keg)
