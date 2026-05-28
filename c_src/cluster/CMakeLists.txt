add_executable(pegasus-cluster
    pegasus-cluster.c
    tools.c
    parser.c
    report.c
    mysystem.c
    job.c
    statinfo.c
)

target_compile_options(pegasus-cluster PRIVATE -Wall -O2 -ggdb -std=gnu99)
target_link_libraries(pegasus-cluster PRIVATE m)

# PIE on GCC >= 14 (matches cluster/Makefile)
if(CMAKE_C_COMPILER_ID STREQUAL "GNU" AND
   CMAKE_C_COMPILER_VERSION VERSION_GREATER_EQUAL "14")
    target_compile_options(pegasus-cluster PRIVATE -fpie)
    target_link_options(pegasus-cluster PRIVATE -pie)
endif()

install(PROGRAMS $<TARGET_FILE:pegasus-cluster>
        DESTINATION "${C_BIN_DEST}"
        RENAME pegasus-cluster)
