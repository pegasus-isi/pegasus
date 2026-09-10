cmake_minimum_required(VERSION 3.20)
project(pegasus-transfer NONE)

# pegasus-transfer is a Go module, built via `go build` rather than any
# native CMake language support (there is no CMake "Go" language). Modules
# are resolved from the network at build time (no vendoring — see the
# project's decision record), so this target requires outbound network
# access during a from-scratch build, same as any other `go build`/`go mod
# download` invocation.
find_program(GO_EXECUTABLE go)
if(NOT GO_EXECUTABLE)
    message(FATAL_ERROR
        "Building pegasus-transfer requires a Go toolchain (go binary not "
        "found in PATH). Install Go, or configure with -DPEGASUS_BUILD_GO=OFF "
        "/ PEGASUS_NO_GO=1 to skip it.")
endif()

set(_pt_src_dir "${CMAKE_CURRENT_SOURCE_DIR}")
set(_pt_bin "${CMAKE_CURRENT_BINARY_DIR}/pegasus-transfer")

file(GLOB_RECURSE _pt_sources
    "${_pt_src_dir}/cmd/*.go"
    "${_pt_src_dir}/internal/*.go")

# GOCACHE/GOPATH default to $HOME, which may not exist or be writable in
# minimal build containers; keep Go's caches inside the build tree instead.
set(_pt_gocache "${CMAKE_CURRENT_BINARY_DIR}/go-cache")
set(_pt_gopath  "${CMAKE_CURRENT_BINARY_DIR}/go-path")

add_custom_command(
    OUTPUT "${_pt_bin}"
    COMMAND ${CMAKE_COMMAND} -E env
            "GOCACHE=${_pt_gocache}"
            "GOPATH=${_pt_gopath}"
            "CGO_ENABLED=0"
            "${GO_EXECUTABLE}" build -o "${_pt_bin}" ./cmd/pegasus-transfer
    WORKING_DIRECTORY "${_pt_src_dir}"
    DEPENDS ${_pt_sources} "${_pt_src_dir}/go.mod" "${_pt_src_dir}/go.sum"
    COMMENT "Building pegasus-transfer (Go)"
    VERBATIM)

add_custom_target(pegasus-transfer-go ALL DEPENDS "${_pt_bin}")

# Exposed to the parent scope so it can be staged into the worker tarball
# the same way pegasus-kickstart/pegasus-cluster are (see top-level
# CMakeLists.txt's build_worker_tarball target).
set(PEGASUS_TRANSFER_GO_BIN "${_pt_bin}" PARENT_SCOPE)
set(PEGASUS_TRANSFER_GO_TARGET "pegasus-transfer-go" PARENT_SCOPE)

install(PROGRAMS "${_pt_bin}" DESTINATION "${C_BIN_DEST}")
