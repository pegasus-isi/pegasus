find_package(MPI REQUIRED COMPONENTS CXX)

set(PMC_SRCS
    pegasus-mpi-cluster.cpp
    strlib.cpp
    tools.cpp
    failure.cpp
    engine.cpp
    dag.cpp
    master.cpp
    worker.cpp
    protocol.cpp
    mpicomm.cpp
    fdcache.cpp
    log.cpp
    config.cpp
)

add_executable(pegasus-mpi-cluster ${PMC_SRCS})
target_compile_options(pegasus-mpi-cluster PRIVATE -g -Wall -ansi)
target_link_libraries(pegasus-mpi-cluster PRIVATE MPI::MPI_CXX)

# Optional libnuma on Linux (mirrors mpi-cluster/Makefile ldconfig check)
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    find_library(NUMA_LIB numa)
    find_path(NUMA_INC numaif.h PATHS /usr/include)
    if(NUMA_LIB AND NUMA_INC)
        target_compile_definitions(pegasus-mpi-cluster PRIVATE HAS_LIBNUMA)
        target_link_libraries(pegasus-mpi-cluster PRIVATE ${NUMA_LIB})
    endif()
endif()

install(PROGRAMS $<TARGET_FILE:pegasus-mpi-cluster>
        DESTINATION "${C_BIN_DEST}"
        RENAME pegasus-mpi-cluster)
