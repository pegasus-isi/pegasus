cmake_minimum_required(VERSION 3.20)
project(pegasus-globus-online NONE)

# pegasus-globus-online is a Go module producing two binaries
# (pegasus-globus-online, pegasus-globus-online-init) built via `go build`
# rather than any native CMake language support (there is no CMake "Go"
# language). Modules are resolved from the network at build time (no
# vendoring, matching pegasus-transfer's precedent) — this module depends
# on golang.org/x/oauth2, so a from-scratch build requires outbound network
# access the first time (subsequent builds reuse the module cache below).
find_program(GO_EXECUTABLE go)
if(NOT GO_EXECUTABLE)
    message(FATAL_ERROR
        "Building pegasus-globus-online requires a Go toolchain (go binary "
        "not found in PATH). Install Go, or configure with "
        "-DPEGASUS_BUILD_GO=OFF / PEGASUS_NO_GO=1 to skip it.")
endif()

set(_pgo_src_dir "${CMAKE_CURRENT_SOURCE_DIR}")
set(_pgo_bin "${CMAKE_CURRENT_BINARY_DIR}/pegasus-globus-online")
set(_pgo_init_bin "${CMAKE_CURRENT_BINARY_DIR}/pegasus-globus-online-init")

file(GLOB_RECURSE _pgo_sources
    "${_pgo_src_dir}/cmd/*.go"
    "${_pgo_src_dir}/internal/*.go")

# GOCACHE/GOPATH default to $HOME, which may not exist or be writable in
# minimal build containers; keep Go's caches inside the build tree instead.
set(_pgo_gocache "${CMAKE_CURRENT_BINARY_DIR}/go-cache")
set(_pgo_gopath  "${CMAKE_CURRENT_BINARY_DIR}/go-path")

add_custom_command(
    OUTPUT "${_pgo_bin}"
    COMMAND ${CMAKE_COMMAND} -E env
            "GOCACHE=${_pgo_gocache}"
            "GOPATH=${_pgo_gopath}"
            "CGO_ENABLED=0"
            "${GO_EXECUTABLE}" build -o "${_pgo_bin}" ./cmd/pegasus-globus-online
    WORKING_DIRECTORY "${_pgo_src_dir}"
    DEPENDS ${_pgo_sources} "${_pgo_src_dir}/go.mod" "${_pgo_src_dir}/go.sum"
    COMMENT "Building pegasus-globus-online (Go)"
    VERBATIM)

add_custom_command(
    OUTPUT "${_pgo_init_bin}"
    COMMAND ${CMAKE_COMMAND} -E env
            "GOCACHE=${_pgo_gocache}"
            "GOPATH=${_pgo_gopath}"
            "CGO_ENABLED=0"
            "${GO_EXECUTABLE}" build -o "${_pgo_init_bin}" ./cmd/pegasus-globus-online-init
    WORKING_DIRECTORY "${_pgo_src_dir}"
    DEPENDS ${_pgo_sources} "${_pgo_src_dir}/go.mod" "${_pgo_src_dir}/go.sum"
    COMMENT "Building pegasus-globus-online-init (Go)"
    VERBATIM)

add_custom_target(pegasus-globus-online-go ALL DEPENDS "${_pgo_bin}" "${_pgo_init_bin}")

# Exposed to the parent scope so it can be staged into the worker tarball
# the same way pegasus-kickstart/pegasus-transfer/pegasus-checkpoint/
# pegasus-integrity are (see top-level CMakeLists.txt's build_worker_tarball
# target).
set(PEGASUS_GLOBUS_ONLINE_GO_BIN "${_pgo_bin}" PARENT_SCOPE)
set(PEGASUS_GLOBUS_ONLINE_INIT_GO_BIN "${_pgo_init_bin}" PARENT_SCOPE)
set(PEGASUS_GLOBUS_ONLINE_GO_TARGET "pegasus-globus-online-go" PARENT_SCOPE)

install(PROGRAMS "${_pgo_bin}" "${_pgo_init_bin}" DESTINATION "${C_BIN_DEST}")
