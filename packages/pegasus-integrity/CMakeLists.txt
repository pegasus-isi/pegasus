cmake_minimum_required(VERSION 3.20)
project(pegasus-integrity NONE)

# pegasus-integrity is a Go module, built via `go build` rather than any
# native CMake language support (there is no CMake "Go" language). Modules
# are resolved from the network at build time (no vendoring, matching
# pegasus-transfer's precedent) — this module has no third-party
# dependencies though, so a from-scratch build only needs the Go toolchain
# itself, not network access.
find_program(GO_EXECUTABLE go)
if(NOT GO_EXECUTABLE)
    message(FATAL_ERROR
        "Building pegasus-integrity requires a Go toolchain (go binary not "
        "found in PATH). Install Go, or configure with -DPEGASUS_BUILD_GO=OFF "
        "/ PEGASUS_NO_GO=1 to skip it.")
endif()

set(_pi_src_dir "${CMAKE_CURRENT_SOURCE_DIR}")
set(_pi_bin "${CMAKE_CURRENT_BINARY_DIR}/pegasus-integrity")

file(GLOB_RECURSE _pi_sources
    "${_pi_src_dir}/cmd/*.go"
    "${_pi_src_dir}/internal/*.go")

# GOCACHE/GOPATH default to $HOME, which may not exist or be writable in
# minimal build containers; keep Go's caches inside the build tree instead.
set(_pi_gocache "${CMAKE_CURRENT_BINARY_DIR}/go-cache")
set(_pi_gopath  "${CMAKE_CURRENT_BINARY_DIR}/go-path")

add_custom_command(
    OUTPUT "${_pi_bin}"
    COMMAND ${CMAKE_COMMAND} -E env
            "GOCACHE=${_pi_gocache}"
            "GOPATH=${_pi_gopath}"
            "CGO_ENABLED=0"
            "${GO_EXECUTABLE}" build -o "${_pi_bin}" ./cmd/pegasus-integrity
    WORKING_DIRECTORY "${_pi_src_dir}"
    DEPENDS ${_pi_sources} "${_pi_src_dir}/go.mod"
    COMMENT "Building pegasus-integrity (Go)"
    VERBATIM)

add_custom_target(pegasus-integrity-go ALL DEPENDS "${_pi_bin}")

# Exposed to the parent scope so it can be staged into the worker tarball
# the same way pegasus-kickstart/pegasus-transfer/pegasus-checkpoint are
# (see top-level CMakeLists.txt's build_worker_tarball target).
set(PEGASUS_INTEGRITY_GO_BIN "${_pi_bin}" PARENT_SCOPE)
set(PEGASUS_INTEGRITY_GO_TARGET "pegasus-integrity-go" PARENT_SCOPE)

install(PROGRAMS "${_pi_bin}" DESTINATION "${C_BIN_DEST}")
