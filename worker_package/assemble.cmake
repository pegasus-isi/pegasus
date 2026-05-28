# assemble.cmake — invoked as: cmake -P assemble.cmake
# Variables passed via -D flags:
#   PYTHON_EXECUTABLE   Python 3 interpreter
#   SOURCE_DIR          Repo root (CMAKE_SOURCE_DIR)
#   WORKER_STAGE        Staging directory (cleared on each run)
#   PEGASUS_VERSION     e.g. "5.2.0-dev.0"
#   PEGASUS_PLATFORM    e.g. "x86_64_ubuntu_24"
#   PY_LIB_SUBDIR       e.g. "lib/python3.12/site-packages"
#   KICKSTART_BIN       Path to pegasus-kickstart (empty if C build off)
#   CLUSTER_BIN         Path to pegasus-cluster   (empty if C build off)
#   KEG_BIN             Path to pegasus-keg        (empty if C build off)
#   OUTPUT_DIR          Directory to write the final .tar.gz into

set(WD "${WORKER_STAGE}/pegasus-${PEGASUS_VERSION}")

# 1. Clean and create staging directory skeleton
file(REMOVE_RECURSE "${WORKER_STAGE}")
file(MAKE_DIRECTORY
    "${WD}/bin"
    "${WD}/${PY_LIB_SUBDIR}"
    "${WD}/lib/pegasus/externals/python"
    "${WD}/share/pegasus")

# 2. Copy C binaries (skipped when path is empty)
foreach(_bin IN ITEMS "${KICKSTART_BIN}" "${CLUSTER_BIN}" "${KEG_BIN}")
    if(_bin)
        file(COPY "${_bin}" DESTINATION "${WD}/bin"
             FILE_PERMISSIONS
                 OWNER_READ OWNER_WRITE OWNER_EXECUTE
                 GROUP_READ GROUP_EXECUTE
                 WORLD_READ WORLD_EXECUTE)
    endif()
endforeach()

# 3. Copy Pegasus Python package from source (pure Python — no C extensions).
#    Exclude large or worker-irrelevant data subdirs to keep the tarball small.
file(COPY "${SOURCE_DIR}/src/Pegasus"
     DESTINATION "${WD}/${PY_LIB_SUBDIR}"
     REGEX "/data/bin$"              EXCLUDE   # old C binary location
     REGEX "/data/java$"             EXCLUDE   # planner JARs (submit-host only)
     REGEX "/data/worker-packages$"  EXCLUDE   # would embed a tarball inside a tarball
     PATTERN "__pycache__"           EXCLUDE
     PATTERN "*.pyc"                 EXCLUDE)

# 4. Generate Python worker scripts in bin/
set(_SCRIPT_NAMES
    pegasus-transfer pegasus-s3 pegasus-exitcode pegasus-integrity pegasus-checkpoint)
set(_FUNC_NAMES
    pegasus_transfer pegasus_s3 pegasus_exitcode pegasus_integrity pegasus_checkpoint)

list(LENGTH _SCRIPT_NAMES _N)
math(EXPR _LAST "${_N} - 1")
foreach(_i RANGE ${_LAST})
    list(GET _SCRIPT_NAMES ${_i} _script_name)
    list(GET _FUNC_NAMES   ${_i} _func_name)
    file(WRITE "${WD}/bin/${_script_name}"
        "#!/usr/bin/env python3\n"
        "from Pegasus.cli.main import ${_func_name}\n"
        "if __name__ == '__main__':\n"
        "    ${_func_name}()\n")
    file(CHMOD "${WD}/bin/${_script_name}"
         PERMISSIONS
             OWNER_READ OWNER_WRITE OWNER_EXECUTE
             GROUP_READ GROUP_EXECUTE
             WORLD_READ WORLD_EXECUTE)
endforeach()

# 5. Install external Python dependencies
# Isolated build environments (scikit-build-core) ship Python without pip;
# ensurepip bootstraps it from the stdlib before we use it.
execute_process(
    COMMAND "${PYTHON_EXECUTABLE}" -m ensurepip --upgrade
    RESULT_VARIABLE _ensurepip_rc
    OUTPUT_QUIET ERROR_QUIET)

message(STATUS "Installing worker external dependencies (this may take a minute)...")
execute_process(
    COMMAND "${PYTHON_EXECUTABLE}" -m pip install
        --target "${WD}/lib/pegasus/externals/python"
        --no-deps
        -r "${SOURCE_DIR}/src/requirements.txt"
    RESULT_VARIABLE _pip_rc
    OUTPUT_VARIABLE _pip_out
    ERROR_VARIABLE  _pip_err)
if(_pip_rc)
    message(FATAL_ERROR
        "pip install worker externals failed (exit ${_pip_rc}):\n${_pip_err}")
endif()

# 6. Create lib/pegasus/python symlink pointing to the Python package directory
#    From lib/pegasus/, going up two levels reaches the worker root, then back
#    down into PY_LIB_SUBDIR (e.g. "../../lib/python3.12/site-packages")
file(CREATE_LINK "../../${PY_LIB_SUBDIR}"
     "${WD}/lib/pegasus/python" SYMBOLIC)

# 7. Copy share data directories
foreach(_dir IN ITEMS sh notification htcondor schema)
    if(EXISTS "${SOURCE_DIR}/src/Pegasus/data/${_dir}")
        file(COPY "${SOURCE_DIR}/src/Pegasus/data/${_dir}"
             DESTINATION "${WD}/share/pegasus")
    endif()
endforeach()

# 8. Copy LICENSE
file(COPY "${SOURCE_DIR}/LICENSE" DESTINATION "${WD}")

# 9. Create the gzipped tarball
set(_TARBALL "pegasus-worker-${PEGASUS_VERSION}-${PEGASUS_PLATFORM}.tar.gz")
message(STATUS "Creating ${_TARBALL}...")
execute_process(
    COMMAND ${CMAKE_COMMAND} -E tar czf
        "${OUTPUT_DIR}/${_TARBALL}"
        "pegasus-${PEGASUS_VERSION}"
    WORKING_DIRECTORY "${WORKER_STAGE}"
    RESULT_VARIABLE _tar_rc)
if(_tar_rc)
    message(FATAL_ERROR "tar creation failed (exit ${_tar_rc})")
endif()

message(STATUS "Worker package written to: ${OUTPUT_DIR}/${_TARBALL}")
