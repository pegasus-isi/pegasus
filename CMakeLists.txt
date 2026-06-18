cmake_minimum_required(VERSION 3.20)
project(pegasus-wms LANGUAGES C CXX)

include(GNUInstallDirs)

find_package(Python3 COMPONENTS Interpreter QUIET)

# Read version from build.properties
file(READ "${CMAKE_SOURCE_DIR}/build.properties" _props)
string(REGEX MATCH "pegasus\\.version *= *([^\n\r]+)" _ "${_props}")
string(STRIP "${CMAKE_MATCH_1}" PEGASUS_VERSION)
message(STATUS "Pegasus version: ${PEGASUS_VERSION}")

# Build metadata
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
    set(_data_base "${SKBUILD_PLATLIB_DIR}/Pegasus/data")
else()
    set(_data_base "${CMAKE_INSTALL_DATAROOTDIR}/pegasus")
endif()
# Keep JARs at <data>/java so _java.py's get_pegasus_data_dir() finds them.
set(JAVA_DEST    "${_data_base}/java")
set(WORKER_DEST  "${_data_base}/worker-packages")
set(SCHEMA_DEST  "${_data_base}/schema")
set(ETC_DEST     "${_data_base}/etc")

# Python sub-packages: merge pegasus-common, pegasus-api, and pegasus-worker
# into the wheel alongside pegasus-python (handled by wheel.packages).
# pegasus-worker Python is included here for local CLI access (pegasus transfer,
# pegasus s3, etc.). The isolated worker tarball for remote hosts is built
# separately by the worker_package CMake target.
if(DEFINED SKBUILD_PLATLIB_DIR)
    install(DIRECTORY "${CMAKE_SOURCE_DIR}/packages/pegasus-common/src/Pegasus/"
            DESTINATION "${SKBUILD_PLATLIB_DIR}/Pegasus")
    install(DIRECTORY "${CMAKE_SOURCE_DIR}/packages/pegasus-api/src/Pegasus/"
            DESTINATION "${SKBUILD_PLATLIB_DIR}/Pegasus")
    install(DIRECTORY "${CMAKE_SOURCE_DIR}/packages/pegasus-worker/src/Pegasus/"
            DESTINATION "${SKBUILD_PLATLIB_DIR}/Pegasus")
endif()

if(PEGASUS_BUILD_C)
    add_subdirectory(packages/pegasus-kickstart)
    add_subdirectory(packages/pegasus-cluster)
    add_subdirectory(packages/pegasus-keg)
    if(PEGASUS_BUILD_MPI)
        find_package(MPI)
        if(MPI_CXX_FOUND)
            add_subdirectory(packages/pegasus-mpi-cluster)
        else()
            message(WARNING "MPI not found — skipping pegasus-mpi-cluster")
        endif()
    endif()
endif()

# ── Java JARs ─────────────────────────────────────────────────────────────────
# Compiles Java source using CMake's UseJava module (no Ant).
# Produces: pegasus.jar (planner/common/ikcap), vdl.jar, pegasus-aws-batch.jar.
# Third-party JARs already live in share/pegasus/java/ and are installed as-is.
if(PEGASUS_BUILD_JAVA)
    find_package(Java 1.8 REQUIRED COMPONENTS Development)
    include(UseJava)

    # Java 8 source/target compatibility; UTF-8 encoding
    set(CMAKE_JAVA_COMPILE_FLAGS "--release" "8" "-encoding" "UTF-8" "-Xlint:none")

    # Third-party classpath JARs (already in source tree, not compiled)
    file(GLOB _cp_jars     "${CMAKE_SOURCE_DIR}/share/pegasus/java/*.jar")
    file(GLOB _aws_cp_jars "${CMAKE_SOURCE_DIR}/share/pegasus/java/aws/*.jar")

    # ── Generate pegasus.build.properties (embedded in pegasus.jar) ──────────
    string(TIMESTAMP _build_ts "%Y%m%d%H%M%SZ" UTC)
    file(WRITE "${CMAKE_BINARY_DIR}/pegasus.build.properties"
        "pegasus.build.version=${PEGASUS_VERSION}\n"
        "pegasus.build.platform=${PEGASUS_PLATFORM}\n"
        "pegasus.build.timestamp=${_build_ts}\n"
        "pegasus.build.git.hash=${PEGASUS_GIT_HASH}\n")

    # ── pegasus.jar: common + planner + ikcap + vdl ──────────────────────────
    # Compile all non-AWS sources together into a single JAR.
    # The Ant build compiled these to a shared output dir (so VDL could see
    # common classes and vice versa). UseJava isolates each add_jar(), so we
    # merge all mutually-dependent sources into one compilation unit.
    # The runtime classpath loader globs *.jar, so no separate vdl.jar is needed.
    file(GLOB_RECURSE _pegasus_srcs
        "${CMAKE_SOURCE_DIR}/src/edu/isi/pegasus/common/*.java"
        "${CMAKE_SOURCE_DIR}/src/edu/isi/pegasus/planner/*.java"
        "${CMAKE_SOURCE_DIR}/src/edu/isi/ikcap/*.java"
        "${CMAKE_SOURCE_DIR}/src/org/griphyn/vdl/*.java")
    list(FILTER _pegasus_srcs EXCLUDE REGEX ".*Test\\.java$")
    list(FILTER _pegasus_srcs EXCLUDE REGEX ".*/pegasus/aws/.*")

    add_jar(pegasus_jar
        SOURCES ${_pegasus_srcs}
        INCLUDE_JARS ${_cp_jars}
        OUTPUT_NAME pegasus
        OUTPUT_DIR "${CMAKE_BINARY_DIR}/jars")

    # Embed the generated build properties into pegasus.jar
    get_target_property(_pegasus_jar_file pegasus_jar JAR_FILE)
    add_custom_command(TARGET pegasus_jar POST_BUILD
        COMMAND "${Java_JAR_EXECUTABLE}" uf "${_pegasus_jar_file}"
                -C "${CMAKE_BINARY_DIR}" "pegasus.build.properties"
        COMMENT "Embedding pegasus.build.properties into pegasus.jar")

    install_jar(pegasus_jar DESTINATION "${JAVA_DEST}")

    # ── pegasus-aws-batch.jar: AWS Batch integration ──────────────────────────
    file(GLOB_RECURSE _aws_srcs
        "${CMAKE_SOURCE_DIR}/src/edu/isi/pegasus/aws/*.java")
    list(FILTER _aws_srcs EXCLUDE REGEX ".*Test\\.java$")

    add_jar(pegasus_aws_batch_jar
        SOURCES ${_aws_srcs}
        INCLUDE_JARS pegasus_jar ${_cp_jars} ${_aws_cp_jars}
        OUTPUT_NAME pegasus-aws-batch
        OUTPUT_DIR "${CMAKE_BINARY_DIR}/jars")

    get_target_property(_aws_jar_file pegasus_aws_batch_jar JAR_FILE)
    add_custom_command(TARGET pegasus_aws_batch_jar POST_BUILD
        COMMAND "${Java_JAR_EXECUTABLE}" uf "${_aws_jar_file}"
                -C "${CMAKE_BINARY_DIR}" "pegasus.build.properties"
        COMMENT "Embedding pegasus.build.properties into pegasus-aws-batch.jar")

    install_jar(pegasus_aws_batch_jar DESTINATION "${JAVA_DEST}")

    # ── Third-party JARs (pre-built, just install) ───────────────────────────
    install(DIRECTORY "${CMAKE_SOURCE_DIR}/share/pegasus/java/"
            DESTINATION "${JAVA_DEST}"
            FILES_MATCHING PATTERN "*.jar" PATTERN "aws" EXCLUDE)
    install(DIRECTORY "${CMAKE_SOURCE_DIR}/share/pegasus/java/aws/"
            DESTINATION "${JAVA_DEST}/aws"
            FILES_MATCHING PATTERN "*.jar")

    # ── Runtime data Java tools need ─────────────────────────────────────────
    install(DIRECTORY "${CMAKE_SOURCE_DIR}/share/pegasus/schema/"
            DESTINATION "${SCHEMA_DEST}")
    install(DIRECTORY "${CMAKE_SOURCE_DIR}/etc/"
            DESTINATION "${ETC_DEST}")
endif()

# ── Worker tarball ────────────────────────────────────────────────────────────
# Bundles packages/pegasus-worker + runtime deps (boto3, globus-sdk) into a
# self-contained platform tarball that the planner stages to remote worker nodes.
if(PEGASUS_BUILD_WORKER)
    find_package(Python3 COMPONENTS Interpreter REQUIRED)

    set(_worker_tarball "pegasus-worker-${PEGASUS_VERSION}-${PEGASUS_PLATFORM}.tar.gz")
    set(_worker_staging "${CMAKE_BINARY_DIR}/worker_staging")

    add_custom_target(build_worker_tarball ALL
        COMMENT "Building worker package tarball"
        COMMAND ${CMAKE_COMMAND} -E rm -rf "${_worker_staging}"
        COMMAND ${CMAKE_COMMAND} -E make_directory "${_worker_staging}"
        COMMAND "${Python3_EXECUTABLE}" -m pip install
                --no-deps --target "${_worker_staging}"
                "${CMAKE_SOURCE_DIR}/packages/pegasus-worker"
        COMMAND "${Python3_EXECUTABLE}" -m pip install
                --target "${_worker_staging}"
                "boto3>1.12" "globus-sdk>=3.23.0,<4"
        COMMAND find "${_worker_staging}" -name "__pycache__" -type d -exec rm -rf {} +
        COMMAND find "${_worker_staging}" -name "*.dist-info" -type d -exec rm -rf {} +
        COMMAND ${CMAKE_COMMAND} -E tar czf
                "${CMAKE_BINARY_DIR}/${_worker_tarball}"
                -C "${_worker_staging}" .
        WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}"
        VERBATIM)

    install(FILES "${CMAKE_BINARY_DIR}/${_worker_tarball}"
            DESTINATION "${WORKER_DEST}")
endif()
