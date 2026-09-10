cmake_minimum_required(VERSION 3.20)
project(pegasus-checkpoint NONE)

# pegasus-checkpoint is a Go module, built via `go build` rather than any
# native CMake language support (there is no CMake "Go" language). Modules
# are resolved from the network at build time (no vendoring, matching
# pegasus-transfer's precedent), so this target requires outbound network
# access during a from-scratch build.
find_program(GO_EXECUTABLE go)
if(NOT GO_EXECUTABLE)
    message(FATAL_ERROR
        "Building pegasus-checkpoint requires a Go toolchain (go binary not "
        "found in PATH). Install Go, or configure with -DPEGASUS_BUILD_GO=OFF "
        "/ PEGASUS_NO_GO=1 to skip it.")
endif()

set(_pc_src_dir "${CMAKE_CURRENT_SOURCE_DIR}")
set(_pc_bin "${CMAKE_CURRENT_BINARY_DIR}/pegasus-checkpoint")

file(GLOB_RECURSE _pc_sources
    "${_pc_src_dir}/cmd/*.go"
    "${_pc_src_dir}/internal/*.go")

# GOCACHE/GOPATH default to $HOME, which may not exist or be writable in
# minimal build containers; keep Go's caches inside the build tree instead.
set(_pc_gocache "${CMAKE_CURRENT_BINARY_DIR}/go-cache")
set(_pc_gopath  "${CMAKE_CURRENT_BINARY_DIR}/go-path")

add_custom_command(
    OUTPUT "${_pc_bin}"
    COMMAND ${CMAKE_COMMAND} -E env
            "GOCACHE=${_pc_gocache}"
            "GOPATH=${_pc_gopath}"
            "CGO_ENABLED=0"
            "${GO_EXECUTABLE}" build -o "${_pc_bin}" ./cmd/pegasus-checkpoint
    WORKING_DIRECTORY "${_pc_src_dir}"
    DEPENDS ${_pc_sources} "${_pc_src_dir}/go.mod"
    COMMENT "Building pegasus-checkpoint (Go)"
    VERBATIM)

add_custom_target(pegasus-checkpoint-go ALL DEPENDS "${_pc_bin}")

# Exposed to the parent scope so it can be staged into the worker tarball
# the same way pegasus-kickstart/pegasus-transfer are (see top-level
# CMakeLists.txt's build_worker_tarball target).
set(PEGASUS_CHECKPOINT_GO_BIN "${_pc_bin}" PARENT_SCOPE)
set(PEGASUS_CHECKPOINT_GO_TARGET "pegasus-checkpoint-go" PARENT_SCOPE)

install(PROGRAMS "${_pc_bin}" DESTINATION "${C_BIN_DEST}")
