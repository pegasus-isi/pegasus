cmake_minimum_required(VERSION 3.20)
project(pegasus-cluster C)

set(CLUSTER_SRCS
    pegasus-cluster.c tools.c parser.c report.c mysystem.c job.c statinfo.c)

add_executable(pegasus-cluster ${CLUSTER_SRCS})
target_compile_options(pegasus-cluster PRIVATE -Wall -O2 -ggdb -std=gnu99)
target_link_libraries(pegasus-cluster m)

# -fpie on GCC >= 14
if(CMAKE_C_COMPILER_ID STREQUAL "GNU")
    execute_process(COMMAND "${CMAKE_C_COMPILER}" -dumpversion
        OUTPUT_VARIABLE _gcc_ver OUTPUT_STRIP_TRAILING_WHITESPACE)
    string(REPLACE "." ";" _gcc_ver_list "${_gcc_ver}")
    list(GET _gcc_ver_list 0 _gcc_major)
    if(_gcc_major GREATER_EQUAL 14)
        target_compile_options(pegasus-cluster PRIVATE -fpie)
    endif()
endif()

install(TARGETS pegasus-cluster RUNTIME DESTINATION "${C_BIN_DEST}")
