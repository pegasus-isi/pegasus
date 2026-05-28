if(NOT Python3_EXECUTABLE)
    message(FATAL_ERROR
        "Python 3 interpreter not found — required to build worker package.\n"
        "Install Python 3 or set Python3_EXECUTABLE.")
endif()

# Python pure-lib subdir relative to an install prefix, e.g. "lib/python3.12/site-packages"
execute_process(
    COMMAND "${Python3_EXECUTABLE}" -c
        "import sysconfig; \
         p = sysconfig.get_path('purelib', vars={'base': '', 'platbase': ''}).lstrip('/'); \
         print(p)"
    OUTPUT_VARIABLE _PY_LIB_SUBDIR
    OUTPUT_STRIP_TRAILING_WHITESPACE)

set(WORKER_TARBALL "pegasus-worker-${PEGASUS_VERSION}-${PEGASUS_PLATFORM}.tar.gz")
set(WORKER_STAGE   "${CMAKE_CURRENT_BINARY_DIR}/stage")

# C binary paths — empty strings when C build is OFF (worker will have no C tools)
set(_KICKSTART_BIN "")
set(_CLUSTER_BIN   "")
set(_KEG_BIN       "")
set(_C_DEPS        "")
if(PEGASUS_BUILD_C)
    set(_KICKSTART_BIN "$<TARGET_FILE:pegasus-kickstart>")
    set(_CLUSTER_BIN   "$<TARGET_FILE:pegasus-cluster>")
    set(_KEG_BIN       "$<TARGET_FILE:pegasus-keg>")
    set(_C_DEPS        pegasus-kickstart pegasus-cluster pegasus-keg)
endif()

add_custom_command(
    OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/${WORKER_TARBALL}"
    COMMAND ${CMAKE_COMMAND}
        -DPYTHON_EXECUTABLE=${Python3_EXECUTABLE}
        -DSOURCE_DIR=${CMAKE_SOURCE_DIR}
        -DWORKER_STAGE=${WORKER_STAGE}
        -DPEGASUS_VERSION=${PEGASUS_VERSION}
        -DPEGASUS_PLATFORM=${PEGASUS_PLATFORM}
        "-DPY_LIB_SUBDIR=${_PY_LIB_SUBDIR}"
        "-DKICKSTART_BIN=${_KICKSTART_BIN}"
        "-DCLUSTER_BIN=${_CLUSTER_BIN}"
        "-DKEG_BIN=${_KEG_BIN}"
        -DOUTPUT_DIR=${CMAKE_CURRENT_BINARY_DIR}
        -P "${CMAKE_CURRENT_SOURCE_DIR}/assemble.cmake"
    DEPENDS
        ${_C_DEPS}
        "${CMAKE_SOURCE_DIR}/src/requirements.txt"
        "${CMAKE_SOURCE_DIR}/pyproject.toml"
    COMMENT "Assembling ${WORKER_TARBALL}"
    VERBATIM
)

add_custom_target(pegasus-worker-package ALL
    DEPENDS "${CMAKE_CURRENT_BINARY_DIR}/${WORKER_TARBALL}"
)

install(FILES "${CMAKE_CURRENT_BINARY_DIR}/${WORKER_TARBALL}"
        DESTINATION "${WORKER_DEST}")
