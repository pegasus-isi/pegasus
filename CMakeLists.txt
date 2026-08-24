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

# Feature flags; honour PEGASUS_NO_C / PEGASUS_NO_JAVA / PEGASUS_NO_GO / PEGASUS_NO_WORKER
option(PEGASUS_BUILD_C      "Compile C execution tools"               ON)
option(PEGASUS_BUILD_JAVA   "Compile Java planner JARs"               ON)
option(PEGASUS_BUILD_GO     "Compile pegasus-transfer (Go)"           ON)
option(PEGASUS_BUILD_MPI    "Compile pegasus-mpi-cluster (optional)"  OFF)
option(PEGASUS_BUILD_WORKER "Build the worker package tarball"        OFF)

if("$ENV{PEGASUS_NO_C}" STREQUAL "1")
    set(PEGASUS_BUILD_C OFF)
endif()
if("$ENV{PEGASUS_NO_JAVA}" STREQUAL "1")
    set(PEGASUS_BUILD_JAVA OFF)
endif()
if("$ENV{PEGASUS_NO_GO}" STREQUAL "1")
    set(PEGASUS_BUILD_GO OFF)
endif()
if("$ENV{PEGASUS_NO_WORKER}" STREQUAL "1")
    set(PEGASUS_BUILD_WORKER OFF)
endif()

# Pin the x86-64 baseline so the artifacts run on any x86-64 CPU.
if(CMAKE_SYSTEM_PROCESSOR MATCHES "^(x86_64|AMD64)$")
    add_compile_options(-march=x86-64 -mtune=generic)

    # Record the ISA level the code actually requires in the ELF's GNU property
    # note.
    if(CMAKE_C_COMPILER_ID STREQUAL "GNU" AND CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        include(CheckCCompilerFlag)
        check_c_compiler_flag("-mneeded" PEGASUS_HAVE_MNEEDED)
        if(PEGASUS_HAVE_MNEEDED)
            add_compile_options(-mneeded)
        endif()
    endif()
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
# pegasus-worker's Python content here is now just namespace-package
# boilerplate (see packages/pegasus-worker/CLAUDE.md) — every CLI tool that
# used to live in pegasus-worker (transfer, s3, checkpoint, integrity,
# globus-online(-init)) has been rewritten in Go and is installed as a
# compiled binary further down instead. The isolated, Python-free worker
# tarball for remote hosts is built separately by the build_worker_tarball
# CMake target below.
if(DEFINED SKBUILD_PLATLIB_DIR)
    install(DIRECTORY "${CMAKE_SOURCE_DIR}/packages/pegasus-common/src/Pegasus/"
            DESTINATION "${SKBUILD_PLATLIB_DIR}/Pegasus")
    install(DIRECTORY "${CMAKE_SOURCE_DIR}/packages/pegasus-api/src/Pegasus/"
            DESTINATION "${SKBUILD_PLATLIB_DIR}/Pegasus")
    install(DIRECTORY "${CMAKE_SOURCE_DIR}/packages/pegasus-worker/src/Pegasus/"
            DESTINATION "${SKBUILD_PLATLIB_DIR}/Pegasus")
endif()

# Static runtime data — always installed regardless of Java/C build flags.
# The Java planner and Python tools look these up via pegasus.home.sharedstatedir
# which maps to Pegasus/data/ in the installed wheel.
install(DIRECTORY "${CMAKE_SOURCE_DIR}/share/pegasus/sh/"
        DESTINATION "${_data_base}/sh"
        USE_SOURCE_PERMISSIONS)
install(DIRECTORY "${CMAKE_SOURCE_DIR}/share/pegasus/notification/"
        DESTINATION "${_data_base}/notification")
install(DIRECTORY "${CMAKE_SOURCE_DIR}/share/pegasus/htcondor/"
        DESTINATION "${_data_base}/htcondor")

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

# ── pegasus-transfer (Go) ────────────────────────────────────────────────────
# A single static (CGO_ENABLED=0) binary implementing all pegasus-transfer
# protocols, replacing the old Python packages/pegasus-worker/src/Pegasus/
# transfer.py + s3.py. Installed to bin/ like the C tools; PEGASUS_TRANSFER_GO_BIN
# / PEGASUS_TRANSFER_GO_TARGET (set by the subdirectory) let the worker
# tarball below stage the compiled binary alongside pegasus-kickstart.
if(PEGASUS_BUILD_GO)
    add_subdirectory(packages/pegasus-transfer)
    add_subdirectory(packages/pegasus-checkpoint)
    add_subdirectory(packages/pegasus-integrity)
    add_subdirectory(packages/pegasus-globus-online)
endif()

# ── Java JARs ─────────────────────────────────────────────────────────────────
# Compiles Java source using CMake's UseJava module.
# Produces: pegasus.jar (planner/common/ikcap), vdl.jar, pegasus-aws-batch.jar.
# Third-party JARs already live in share/pegasus/java/ and are installed as-is.
if(PEGASUS_BUILD_JAVA)
    # Require runtime Java components
    find_package(Java 1.8 REQUIRED COMPONENTS Runtime)
    if(NOT Java_JAVAC_EXECUTABLE OR NOT Java_JAR_EXECUTABLE)
        message(FATAL_ERROR
            "Building the Pegasus JARs requires a JDK providing javac and jar; "
            "found Java ${Java_VERSION} but javac/jar are missing "
            "(javac='${Java_JAVAC_EXECUTABLE}', jar='${Java_JAR_EXECUTABLE}').")
    endif()
    include(UseJava)

    # Java 8 bytecode target, UTF-8 encoding. `--release 8` (JDK 9+) both sets
    # the target and constrains the API to Java 8, but javac has no --release
    # flag on a JDK 8 toolchain (e.g. RHEL 8's default /usr/bin/javac), so fall
    # back to `-source/-target 8` there — same Java 8 bytecode. Probe javac
    # itself rather than the java runtime in case the two versions differ.
    execute_process(
        COMMAND "${Java_JAVAC_EXECUTABLE}" -version
        OUTPUT_VARIABLE _javac_version ERROR_VARIABLE _javac_version
        OUTPUT_STRIP_TRAILING_WHITESPACE)
    if(_javac_version MATCHES "javac 1\\.[0-8]")
        # JDK 8 and older: no --release support
        set(CMAKE_JAVA_COMPILE_FLAGS "-source" "8" "-target" "8" "-encoding" "UTF-8" "-Xlint:none")
    else()
        set(CMAKE_JAVA_COMPILE_FLAGS "--release" "8" "-encoding" "UTF-8" "-Xlint:none")
    endif()

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

endif()

# Schema and etc are needed by both Java planner and Python validation tools.
install(DIRECTORY "${CMAKE_SOURCE_DIR}/share/pegasus/schema/"
        DESTINATION "${SCHEMA_DEST}")
install(DIRECTORY "${CMAKE_SOURCE_DIR}/etc/"
        DESTINATION "${ETC_DEST}")

# ── Worker tarball ────────────────────────────────────────────────────────────
# Bundles the compiled C tools and Go binaries into a self-contained
# platform tarball that the planner stages to remote worker nodes.
#
# As of the Go rewrite of pegasus-transfer/pegasus-checkpoint/
# pegasus-integrity/pegasus-globus-online(-init) (see
# packages/pegasus-transfer/CLAUDE.md for the decision record), the worker
# package contains NO Python at all: no `pip install`, no interpreter
# dependency, no runtime deps (globus-sdk included) — completing the
# original goal of pegasus-isi/pegasus#2237. Every worker-side tool is now a
# statically-linked binary staged directly into bin/, the same way
# pegasus-kickstart always was.
if(PEGASUS_BUILD_WORKER)
    set(_worker_tarball "pegasus-worker-${PEGASUS_VERSION}-${PEGASUS_PLATFORM}.tar.gz")
    set(_worker_pkg_name "pegasus-${PEGASUS_VERSION}")
    set(_worker_staging "${CMAKE_BINARY_DIR}/worker_staging")
    set(_worker_payload "${_worker_staging}/${_worker_pkg_name}")
    set(_worker_bin "${_worker_payload}/bin")

    # PegasusLite (share/pegasus/sh/pegasus-lite-common.sh) requires the
    # worker tarball to contain a single top-level directory so it can be
    # renamed per-system after extraction; tarring "." directly would leave
    # payload files at the tarball root and break that logic. The actual
    # tar happens in a POST_BUILD step below, after every available binary
    # has been staged into bin/ by the C/Go blocks that follow.
    add_custom_target(build_worker_tarball ALL
        COMMENT "Building worker package tarball"
        COMMAND ${CMAKE_COMMAND} -E rm -rf "${_worker_staging}"
        COMMAND ${CMAKE_COMMAND} -E make_directory "${_worker_bin}"
        WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}"
        VERBATIM)

    # Bundle the compiled execution wrappers (pegasus-kickstart, pegasus-cluster,
    # pegasus-keg) so the worker package is self-contained on remote nodes,
    # matching the old ant dist-worker target (which depended on compile-c).
    if(PEGASUS_BUILD_C)
        # pegasus-keg is skipped on musl libc (Alpine), so its target may not
        # exist; only stage it into the worker package when it was built.
        set(_worker_c_tools pegasus-kickstart pegasus-cluster)
        if(TARGET pegasus-keg)
            list(APPEND _worker_c_tools pegasus-keg)
        endif()
        add_dependencies(build_worker_tarball ${_worker_c_tools})
        foreach(_c_tool ${_worker_c_tools})
            add_custom_command(TARGET build_worker_tarball POST_BUILD
                COMMAND ${CMAKE_COMMAND} -E copy
                        "$<TARGET_FILE:${_c_tool}>" "${_worker_bin}/"
                COMMAND chmod 755 "${_worker_bin}/${_c_tool}"
                COMMENT "Staging ${_c_tool} into worker package")
        endforeach()

        # kickstart looks for libinterpose.so at ../lib/pegasus/ relative to
        # its own bin/ dir (see packages/pegasus-kickstart/mysystem.c).
        if(TARGET interpose)
            add_custom_command(TARGET build_worker_tarball POST_BUILD
                COMMAND ${CMAKE_COMMAND} -E make_directory "${_worker_payload}/lib/pegasus"
                COMMAND ${CMAKE_COMMAND} -E copy
                        "$<TARGET_FILE:interpose>" "${_worker_payload}/lib/pegasus/"
                COMMENT "Staging libinterpose.so into worker package")
        endif()
    endif()

    # Stage the compiled Go binaries, same as the C tools above.
    # PEGASUS_*_GO_BIN/_TARGET are set by each package's own CMakeLists.txt
    # (packages/pegasus-transfer, pegasus-checkpoint, pegasus-integrity,
    # pegasus-globus-online) when PEGASUS_BUILD_GO is ON.
    if(PEGASUS_BUILD_GO)
        add_dependencies(build_worker_tarball "${PEGASUS_TRANSFER_GO_TARGET}")
        add_custom_command(TARGET build_worker_tarball POST_BUILD
            COMMAND ${CMAKE_COMMAND} -E copy
                    "${PEGASUS_TRANSFER_GO_BIN}" "${_worker_bin}/"
            COMMAND chmod 755 "${_worker_bin}/pegasus-transfer"
            COMMENT "Staging pegasus-transfer (Go) into worker package")

        add_dependencies(build_worker_tarball "${PEGASUS_CHECKPOINT_GO_TARGET}")
        add_custom_command(TARGET build_worker_tarball POST_BUILD
            COMMAND ${CMAKE_COMMAND} -E copy
                    "${PEGASUS_CHECKPOINT_GO_BIN}" "${_worker_bin}/"
            COMMAND chmod 755 "${_worker_bin}/pegasus-checkpoint"
            COMMENT "Staging pegasus-checkpoint (Go) into worker package")

        add_dependencies(build_worker_tarball "${PEGASUS_INTEGRITY_GO_TARGET}")
        add_custom_command(TARGET build_worker_tarball POST_BUILD
            COMMAND ${CMAKE_COMMAND} -E copy
                    "${PEGASUS_INTEGRITY_GO_BIN}" "${_worker_bin}/"
            COMMAND chmod 755 "${_worker_bin}/pegasus-integrity"
            COMMENT "Staging pegasus-integrity (Go) into worker package")

        add_dependencies(build_worker_tarball "${PEGASUS_GLOBUS_ONLINE_GO_TARGET}")
        add_custom_command(TARGET build_worker_tarball POST_BUILD
            COMMAND ${CMAKE_COMMAND} -E copy
                    "${PEGASUS_GLOBUS_ONLINE_GO_BIN}" "${PEGASUS_GLOBUS_ONLINE_INIT_GO_BIN}" "${_worker_bin}/"
            COMMAND chmod 755 "${_worker_bin}/pegasus-globus-online" "${_worker_bin}/pegasus-globus-online-init"
            COMMENT "Staging pegasus-globus-online(-init) (Go) into worker package")
    endif()

    # Tar up whatever ended up in bin/ — the C and Go blocks above populate
    # it via POST_BUILD hooks, which must all run before this final step,
    # hence this being POST_BUILD too (POST_BUILD hooks on the same target
    # run in the order they were added).
    add_custom_command(TARGET build_worker_tarball POST_BUILD
        COMMAND "${CMAKE_COMMAND}" -E chdir "${_worker_staging}"
                "${CMAKE_COMMAND}" -E tar czf "${CMAKE_BINARY_DIR}/${_worker_tarball}" "${_worker_pkg_name}"
        COMMENT "Packing worker package tarball"
        VERBATIM)

    install(FILES "${CMAKE_BINARY_DIR}/${_worker_tarball}"
            DESTINATION "${WORKER_DEST}")
endif()
