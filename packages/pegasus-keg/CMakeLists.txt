cmake_minimum_required(VERSION 3.20)
project(pegasus-keg NONE)

# pegasus-keg is a Go module, built via `go build` rather than any
# native CMake language support (there is no CMake "Go" language). It has
# no module dependencies, so no network access is needed to build it. The
# MPI variant (pegasus-mpi-keg) is still C++ and lives in mpi/ with its own
# Makefile; it is not part of the CMake build.
find_program(GO_EXECUTABLE go)
if(NOT GO_EXECUTABLE)
    message(FATAL_ERROR
        "Building pegasus-keg requires a Go toolchain (go binary not "
        "found in PATH). Install Go, or configure with -DPEGASUS_BUILD_GO=OFF "
        "/ PEGASUS_NO_GO=1 to skip it.")
endif()

set(_pk_src_dir "${CMAKE_CURRENT_SOURCE_DIR}")
set(_pk_bin "${CMAKE_CURRENT_BINARY_DIR}/pegasus-keg")

file(GLOB_RECURSE _pk_sources
    "${_pk_src_dir}/cmd/*.go"
    "${_pk_src_dir}/internal/*.go")
list(FILTER _pk_sources EXCLUDE REGEX "_test\\.go$")

# GOCACHE/GOPATH default to $HOME, which may not exist or be writable in
# minimal build containers; keep Go's caches inside the build tree instead.
set(_pk_gocache "${CMAKE_CURRENT_BINARY_DIR}/go-cache")
set(_pk_gopath  "${CMAKE_CURRENT_BINARY_DIR}/go-path")

# PEGASUS_GO_MACOS_LDFLAG is probed once by the top-level CMakeLists.txt
# (Go version >=1.27 required; see the comment there) and is empty/undefined
# wherever it doesn't apply.
set(_pk_ldflags "-s -w")
if(DEFINED PEGASUS_GO_MACOS_LDFLAG)
    string(APPEND _pk_ldflags " ${PEGASUS_GO_MACOS_LDFLAG}")
endif()

add_custom_command(
    OUTPUT "${_pk_bin}"
    COMMAND ${CMAKE_COMMAND} -E env
            "GOCACHE=${_pk_gocache}"
            "GOPATH=${_pk_gopath}"
            "CGO_ENABLED=0"
            "${GO_EXECUTABLE}" build -ldflags "${_pk_ldflags}" -o "${_pk_bin}" ./cmd/pegasus-keg
    WORKING_DIRECTORY "${_pk_src_dir}"
    DEPENDS ${_pk_sources} "${_pk_src_dir}/go.mod"
    COMMENT "Building pegasus-keg (Go)"
    VERBATIM)

add_custom_target(pegasus-keg-go ALL DEPENDS "${_pk_bin}")

# Exposed to the parent scope so it can be staged into the worker tarball
# the same way pegasus-kickstart/pegasus-transfer are (see top-level
# CMakeLists.txt's build_worker_tarball target).
set(PEGASUS_KEG_GO_BIN "${_pk_bin}" PARENT_SCOPE)
set(PEGASUS_KEG_GO_TARGET "pegasus-keg-go" PARENT_SCOPE)

install(PROGRAMS "${_pk_bin}" DESTINATION "${C_BIN_DEST}")
