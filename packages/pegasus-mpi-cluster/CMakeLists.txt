cmake_minimum_required(VERSION 3.20)
project(pegasus-mpi-cluster CXX)

# MPI is required; already checked in parent CMakeLists before add_subdirectory
find_package(MPI REQUIRED COMPONENTS CXX)

# Generate version.h at build time
add_custom_command(
    OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/version.h"
    COMMAND "${CMAKE_SOURCE_DIR}/release-tools/getversion" --header
            > "${CMAKE_CURRENT_BINARY_DIR}/version.h"
    DEPENDS "${CMAKE_SOURCE_DIR}/release-tools/getversion"
    COMMENT "Generating mpi-cluster version.h")
add_custom_target(mpi_cluster_version_h
    DEPENDS "${CMAKE_CURRENT_BINARY_DIR}/version.h")

set(PMC_SRCS
    strlib.cpp tools.cpp failure.cpp engine.cpp dag.cpp master.cpp
    worker.cpp protocol.cpp mpicomm.cpp fdcache.cpp log.cpp config.cpp
    pegasus-mpi-cluster.cpp)

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    add_compile_definitions(LINUX)
    # Optional libnuma
    find_library(_libnuma numa)
    find_path(_numaif numaif.h)
    if(_libnuma AND _numaif)
        add_compile_definitions(HAS_LIBNUMA)
        set(_numa_libs "${_libnuma}")
    endif()
elseif(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    add_compile_definitions(DARWIN)
else()
    add_compile_definitions(GNUKFREEBSD)
endif()

add_executable(pegasus-mpi-cluster ${PMC_SRCS})
add_dependencies(pegasus-mpi-cluster mpi_cluster_version_h)
target_compile_options(pegasus-mpi-cluster PRIVATE -g -Wall -ansi)
target_include_directories(pegasus-mpi-cluster PRIVATE
    "${CMAKE_CURRENT_BINARY_DIR}"
    "${CMAKE_CURRENT_SOURCE_DIR}"
    ${MPI_CXX_INCLUDE_DIRS})
target_link_libraries(pegasus-mpi-cluster MPI::MPI_CXX ${_numa_libs})

# Darwin code-signing (best-effort, non-fatal)
if(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    add_custom_command(TARGET pegasus-mpi-cluster POST_BUILD
        COMMAND codesign -s "Pegasus Development" "$<TARGET_FILE:pegasus-mpi-cluster>" || true
        COMMENT "Code-signing pegasus-mpi-cluster")
endif()

install(TARGETS pegasus-mpi-cluster RUNTIME DESTINATION "${C_BIN_DEST}")
