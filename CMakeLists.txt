cmake_minimum_required(VERSION 3.20)
project(pegasus-wms LANGUAGES C CXX)

include(GNUInstallDirs)

find_package(Python3 COMPONENTS Interpreter QUIET)

# Read version from build.properties
file(READ "${CMAKE_SOURCE_DIR}/build.properties" _props)
string(REGEX MATCH "pegasus\\.version *= *([^\n\r]+)" _ "${_props}")
string(STRIP "${CMAKE_MATCH_1}" PEGASUS_VERSION)
message(STATUS "Pegasus version: ${PEGASUS_VERSION}")

# Build metadata — shared by java_src/ and worker_package/
execute_process(
    COMMAND git log -1 --format=%H
    WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}"
    OUTPUT_VARIABLE PEGASUS_GIT_HASH
    OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_QUIET)
execute_process(
    COMMAND "${CMAKE_SOURCE_DIR}/release-tools/getsystem"
    OUTPUT_VARIABLE PEGASUS_PLATFORM
    OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_QUIET)
if(NOT PEGASUS_PLATFORM)
    set(PEGASUS_PLATFORM "unknown")
endif()

# Feature flags; honour PEGASUS_NO_C / PEGASUS_NO_JAVA / PEGASUS_NO_WORKER
option(PEGASUS_BUILD_C      "Compile C execution tools"               ON)
option(PEGASUS_BUILD_JAVA   "Compile Java planner JARs"               ON)
option(PEGASUS_BUILD_MPI    "Compile pegasus-mpi-cluster (optional)"  OFF)
option(PEGASUS_BUILD_WORKER "Build the worker package tarball"        ON)

if("$ENV{PEGASUS_NO_C}" STREQUAL "1")
    set(PEGASUS_BUILD_C OFF)
endif()
if("$ENV{PEGASUS_NO_JAVA}" STREQUAL "1")
    set(PEGASUS_BUILD_JAVA OFF)
endif()
if("$ENV{PEGASUS_NO_WORKER}" STREQUAL "1")
    set(PEGASUS_BUILD_WORKER OFF)
endif()

# Install destinations.
# When invoked by scikit-build-core the SKBUILD_* variables are set and map
# directly into the wheel layout.  For standalone `cmake --install` builds
# they fall back to the standard GNUInstallDirs paths so that
#   cmake --install _cmake_build --prefix ./
# produces ./bin/, ./lib/, etc. at the project root.

if(DEFINED SKBUILD_SCRIPTS_DIR)
    # Wheel scripts section → <venv>/bin/ on install
    set(C_BIN_DEST "${SKBUILD_SCRIPTS_DIR}")
else()
    # Relative path so `cmake --install --prefix <dir>` works correctly.
    # Produces <prefix>/bin/ (e.g. ./bin/ when prefix is the project root).
    set(C_BIN_DEST "${CMAKE_INSTALL_BINDIR}")
endif()

if(DEFINED SKBUILD_DATA_DIR)
    # libinterpose.so must be at <venv>/lib[64]/pegasus/ so that kickstart's
    # runtime search (<kickstart>/../lib/pegasus/libinterpose.so) finds it.
    set(C_LIB_DEST "${SKBUILD_DATA_DIR}/${CMAKE_INSTALL_LIBDIR}/pegasus")
else()
    set(C_LIB_DEST "${CMAKE_INSTALL_LIBDIR}/pegasus")
endif()

if(DEFINED SKBUILD_PLATLIB_DIR)
    # Keep JARs at the same path as before: site-packages/Pegasus/data/java/
    # so that _java.py's get_pegasus_data_dir() requires no changes.
    set(JAVA_DEST "${SKBUILD_PLATLIB_DIR}/Pegasus/data/java")
else()
    set(JAVA_DEST "${CMAKE_INSTALL_DATAROOTDIR}/pegasus/java")
endif()

if(DEFINED SKBUILD_PLATLIB_DIR)
    set(WORKER_DEST "${SKBUILD_PLATLIB_DIR}/Pegasus/data/worker-packages")
else()
    set(WORKER_DEST "${CMAKE_INSTALL_DATAROOTDIR}/pegasus/worker-packages")
endif()

if(PEGASUS_BUILD_C)
    add_subdirectory(c_src/kickstart)
    add_subdirectory(c_src/cluster)
    add_subdirectory(c_src/keg)
    if(PEGASUS_BUILD_MPI)
        find_package(MPI)
        if(MPI_CXX_FOUND)
            add_subdirectory(c_src/mpi-cluster)
        else()
            message(WARNING "MPI not found — skipping pegasus-mpi-cluster")
        endif()
    endif()
endif()

if(PEGASUS_BUILD_JAVA)
    add_subdirectory(java_src)
endif()

if(PEGASUS_BUILD_WORKER)
    add_subdirectory(worker_package)
endif()
