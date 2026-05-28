# Generate version.h from the project's build.properties version string.
# PEGASUS_VERSION is set by the top-level CMakeLists.txt.
file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/version.h"
    "#ifndef VERSION_H\n"
    "#define VERSION_H\n"
    "\n"
    "#define PEGASUS_VERSION \"${PEGASUS_VERSION}\"\n"
    "\n"
    "#endif /* VERSION_H */\n")

# Detect musl libc — affects PAPI and libinterpose.so availability.
execute_process(
    COMMAND ${CMAKE_C_COMPILER} -dumpmachine
    OUTPUT_VARIABLE _MACHINE
    OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_QUIET)
if(_MACHINE MATCHES "musl")
    set(_IS_MUSL TRUE)
else()
    set(_IS_MUSL FALSE)
endif()

# Platform-specific source files (mirrors kickstart/Makefile OBJS logic)
set(KICKSTART_SRCS
    appinfo.c
    checksum.c
    getif.c
    invoke.c
    jobinfo.c
    limitinfo.c
    machine.c
    machine/basic.c
    mylist.c
    mysystem.c
    parse.c
    pegasus-kickstart.c
    procinfo.c
    sha2.c
    statinfo.c
    useinfo.c
    utils.c
)

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    list(APPEND KICKSTART_SRCS machine/linux.c syscall.c)
elseif(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    list(APPEND KICKSTART_SRCS machine/darwin.c)
endif()

add_executable(pegasus-kickstart ${KICKSTART_SRCS})

target_include_directories(pegasus-kickstart PRIVATE
    "${CMAKE_CURRENT_BINARY_DIR}"   # generated version.h
    "${CMAKE_CURRENT_SOURCE_DIR}"   # local headers
)

target_compile_options(pegasus-kickstart PRIVATE -Wall -O2 -ggdb -std=gnu99)

# Inject platform macro used by machine/*.c (matches Makefile's -D${SYSTEM})
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    target_compile_definitions(pegasus-kickstart PRIVATE LINUX)
elseif(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    target_compile_definitions(pegasus-kickstart PRIVATE DARWIN)
else()
    target_compile_definitions(pegasus-kickstart PRIVATE GNUKFREEBSD)
endif()

target_link_libraries(pegasus-kickstart PRIVATE m)

# LFS (Large File Support) flags on Linux — mirrors `getconf LFS_CFLAGS` call
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    execute_process(COMMAND getconf LFS_CFLAGS
        OUTPUT_VARIABLE _LFS_C OUTPUT_STRIP_TRAILING_WHITESPACE ERROR_QUIET)
    execute_process(COMMAND getconf LFS_LDFLAGS
        OUTPUT_VARIABLE _LFS_LD OUTPUT_STRIP_TRAILING_WHITESPACE ERROR_QUIET)
    if(_LFS_C)
        # getconf may return flags like "-D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64"
        separate_arguments(_LFS_C_LIST UNIX_COMMAND "${_LFS_C}")
        target_compile_options(pegasus-kickstart PRIVATE ${_LFS_C_LIST})
    endif()
    if(_LFS_LD)
        separate_arguments(_LFS_LD_LIST UNIX_COMMAND "${_LFS_LD}")
        target_link_options(pegasus-kickstart PRIVATE ${_LFS_LD_LIST})
    endif()
endif()

# PIE on GCC >= 14 (matches kickstart/Makefile)
if(CMAKE_C_COMPILER_ID STREQUAL "GNU" AND
   CMAKE_C_COMPILER_VERSION VERSION_GREATER_EQUAL "14")
    target_compile_options(pegasus-kickstart PRIVATE -fpie)
    target_link_options(pegasus-kickstart PRIVATE -pie)
endif()

# PAPI support (Linux x86_64, non-musl only — mirrors Makefile ldconfig check)
if(CMAKE_SYSTEM_NAME STREQUAL "Linux" AND
   CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64" AND NOT _IS_MUSL)
    find_library(PAPI_LIB papi)
    find_path(PAPI_INC papi.h)
    if(PAPI_LIB AND PAPI_INC)
        target_compile_definitions(pegasus-kickstart PRIVATE HAS_PAPI)
        target_link_libraries(pegasus-kickstart PRIVATE ${PAPI_LIB})
    endif()
endif()

install(PROGRAMS $<TARGET_FILE:pegasus-kickstart>
        DESTINATION "${C_BIN_DEST}"
        RENAME pegasus-kickstart)

# libinterpose.so — Linux x86_64, non-musl only.
# kickstart's mysystem.c searches for it at:
#   (1) same dir as kickstart binary
#   (2) <kickstart>/../lib/pegasus/libinterpose.so
#   (3) <kickstart>/../lib64/pegasus/libinterpose.so
# With kickstart at <venv>/bin/, C_LIB_DEST resolves to <venv>/lib[64]/pegasus/
# which satisfies path (2) or (3).
if(CMAKE_SYSTEM_NAME STREQUAL "Linux" AND
   CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64" AND NOT _IS_MUSL)
    add_library(interpose SHARED interpose.c)
    target_compile_options(interpose PRIVATE
        -Wall -O2 -ggdb -std=gnu99 -pthread -fPIC)
    target_link_libraries(interpose PRIVATE dl)
    if(PAPI_LIB AND PAPI_INC)
        target_link_libraries(interpose PRIVATE ${PAPI_LIB})
    endif()
    set_target_properties(interpose PROPERTIES
        OUTPUT_NAME "libinterpose"
        PREFIX      ""
        SUFFIX      ".so"
    )
    install(TARGETS interpose LIBRARY DESTINATION "${C_LIB_DEST}")
endif()
