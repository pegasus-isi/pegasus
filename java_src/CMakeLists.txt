find_package(Java COMPONENTS Development QUIET)

if(NOT Java_FOUND OR NOT Java_JAVAC_EXECUTABLE OR NOT Java_JAR_EXECUTABLE)
    message(WARNING
        "JDK not found (javac/jar missing) — skipping Java compilation.\n"
        "Install a JDK and set JAVA_HOME to build the Pegasus planner.")
    return()
endif()

include(UseJava)

message(STATUS "Found javac: ${Java_JAVAC_EXECUTABLE}")
message(STATUS "Found jar:   ${Java_JAR_EXECUTABLE}")

# Match build.xml: source/target 1.8, UTF-8 encoding, debug symbols
set(CMAKE_JAVA_COMPILE_FLAGS
    "-source" "1.8"
    "-target" "1.8"
    "-encoding" "UTF-8"
    "-g"
)

# Generate pegasus.build.properties — embedded inside pegasus.jar so the
# planner can query its build metadata at runtime.
string(TIMESTAMP _TS "%Y%m%d%H%M%SZ" UTC)
file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/pegasus.build.properties"
    "pegasus.build.version=${PEGASUS_VERSION}\n"
    "pegasus.build.platform=${PEGASUS_PLATFORM}\n"
    "pegasus.build.timestamp=${_TS}\n"
    "pegasus.build.git.hash=${PEGASUS_GIT_HASH}\n")

# Pre-compiled third-party dependency JARs
file(GLOB DEP_JARS     "${CMAKE_SOURCE_DIR}/share/pegasus/java/*.jar")
file(GLOB AWS_DEP_JARS "${CMAKE_SOURCE_DIR}/share/pegasus/java/aws/*.jar")

# Java sources for the main planner JAR (all non-AWS packages)
file(GLOB_RECURSE PLANNER_SRCS
    "${CMAKE_SOURCE_DIR}/java_src/edu/isi/pegasus/planner/*.java"
    "${CMAKE_SOURCE_DIR}/java_src/edu/isi/pegasus/common/*.java"
    "${CMAKE_SOURCE_DIR}/java_src/edu/isi/ikcap/*.java"
    "${CMAKE_SOURCE_DIR}/java_src/org/griphyn/*.java")

# Java sources for the AWS Batch JAR (separate so it can depend on pegasus.jar)
file(GLOB_RECURSE AWS_SRCS
    "${CMAKE_SOURCE_DIR}/java_src/edu/isi/pegasus/aws/*.java")

# pegasus.jar — planner + common + VDL.
add_jar(pegasus
    SOURCES      ${PLANNER_SRCS}
    INCLUDE_JARS ${DEP_JARS}
    OUTPUT_NAME  pegasus
)

# Embed pegasus.build.properties at the JAR root using `jar uf` (update).
# The file is in CMAKE_CURRENT_BINARY_DIR, so -C switches to that directory
# before adding it, which places it at the archive root.
get_target_property(_PEGASUS_JAR pegasus JAR_FILE)
add_custom_command(TARGET pegasus POST_BUILD
    COMMAND ${Java_JAR_EXECUTABLE} uf "${_PEGASUS_JAR}"
            -C "${CMAKE_CURRENT_BINARY_DIR}" "pegasus.build.properties"
    COMMENT "Embedding pegasus.build.properties in pegasus.jar"
    VERBATIM
)

# pegasus-aws-batch.jar — AWS Batch client.
# Listed after pegasus so it appears on the classpath during compilation.
add_jar(pegasus-aws-batch
    SOURCES      ${AWS_SRCS}
    INCLUDE_JARS ${DEP_JARS} ${AWS_DEP_JARS} pegasus
    OUTPUT_NAME  pegasus-aws-batch
)

# Install compiled JARs to Pegasus/data/java/ — same path as before so
# _java.py's get_pegasus_data_dir() needs no changes.
install_jar(pegasus           DESTINATION "${JAVA_DEST}")
install_jar(pegasus-aws-batch DESTINATION "${JAVA_DEST}")

# Install pre-compiled dependency JARs alongside (the planner loads them
# at startup via the classpath built in _java.py).
install(FILES ${DEP_JARS}     DESTINATION "${JAVA_DEST}")
install(FILES ${AWS_DEP_JARS} DESTINATION "${JAVA_DEST}/aws")
