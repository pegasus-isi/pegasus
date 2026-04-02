"""
Java discovery, heap configuration, and classpath management.

This module is a Python port of share/pegasus/sh/java.sh, providing
the same functionality for discovering Java, computing heap sizes,
and building classpaths for the Pegasus Java planner tools.
"""

import os
import platform
import re
import resource
import shutil
import subprocess
import sys
import sysconfig
from glob import glob
from pathlib import Path

import click


def find_java():
    """Discover the Java executable.

    Search order:
    1. JAVA_HOME environment variable
    2. System default locations (Linux)
    3. macOS java_home utility
    4. PATH lookup via ``which java``

    Returns the path to the java executable.
    Raises click.ClickException if Java cannot be found.
    """
    java_home = os.environ.get("JAVA_HOME")

    if not java_home:
        # Try system defaults (Linux)
        for target in [
            "/usr/lib/jvm/default-java",
            "/usr/lib/jvm/java-openjdk",
            "/usr/lib/jvm/jre-openjdk",
            "/usr/lib/jvm/java-sun",
            "/usr/lib/jvm/jre-sun",
        ]:
            java_bin = os.path.join(target, "bin", "java")
            if os.path.isdir(target) and os.access(java_bin, os.X_OK):
                java_home = target
                break

        # macOS: use java_home utility
        if not java_home and platform.system() == "Darwin":
            java_home_cmd = "/usr/libexec/java_home"
            if os.access(java_home_cmd, os.X_OK):
                try:
                    result = subprocess.run(
                        [java_home_cmd, "-version", "1.8+"],
                        capture_output=True,
                        text=True,
                    )
                    if result.returncode == 0 and result.stdout.strip():
                        java_home = result.stdout.strip()
                except OSError:
                    pass

    # Construct path from JAVA_HOME
    if java_home:
        java = os.path.join(java_home, "bin", "java")
        if os.access(java, os.X_OK):
            return java

    # Fallback to PATH
    java = shutil.which("java")
    if java:
        return java

    raise click.ClickException(
        "Java not found. Please set JAVA_HOME or ensure 'java' is in your PATH."
    )


def compute_heap_args():
    """Compute JVM heap memory arguments.

    Mirrors the logic from java.sh:
    - If JAVA_HEAPMIN/JAVA_HEAPMAX are set, use them
    - Otherwise, auto-detect from system memory (1/3 of total, capped at 16GB)
    - Apply ulimit constraints

    Returns a list of JVM arguments (e.g., ['-Xms256m', '-Xmx512m']).
    """
    heap_min_env = os.environ.get("JAVA_HEAPMIN")
    heap_max_env = os.environ.get("JAVA_HEAPMAX")

    if heap_min_env or heap_max_env:
        args = []
        if heap_min_env:
            args.append(f"-Xms{heap_min_env}m")
        if heap_max_env:
            args.append(f"-Xmx{heap_max_env}m")
        return args

    # Auto-detect heap size
    heap_max = 512  # default in MB

    system = platform.system()

    if system == "Linux":
        try:
            with open("/proc/meminfo") as f:
                for line in f:
                    match = re.match(r"MemTotal:\s+(\d+)\s+kB", line)
                    if match:
                        mem_total_kb = int(match.group(1))
                        heap_max = mem_total_kb // 1024 // 3
                        break
        except OSError:
            pass

    elif system == "Darwin":
        try:
            result = subprocess.run(
                ["/usr/sbin/sysctl", "-n", "hw.memsize"],
                capture_output=True,
                text=True,
            )
            if result.returncode == 0:
                mem_bytes = int(result.stdout.strip())
                heap_max = mem_bytes // 1024 // 1024 // 3
        except (OSError, ValueError):
            pass

    # Upper limit: 16GB
    if heap_max > 16384:
        heap_max = 16384

    # Apply ulimit constraints
    try:
        # RLIMIT_AS (virtual memory) or RLIMIT_RSS (resident set)
        for limit_type in (resource.RLIMIT_AS, resource.RLIMIT_RSS):
            soft, _ = resource.getrlimit(limit_type)
            if soft > 0 and soft != resource.RLIM_INFINITY:
                mem_limit_mb = soft // 1024 // 1024
                if mem_limit_mb > 0:
                    heap_max = min(heap_max, mem_limit_mb // 2)
                    break
    except (ValueError, AttributeError):
        pass

    # Ensure minimum viable heap
    if heap_max < 64:
        heap_max = 64

    # min is 1/2 of max
    heap_min = heap_max // 2

    return [f"-Xms{heap_min}m", f"-Xmx{heap_max}m"]


def get_pegasus_data_dir():
    """Get the path to Pegasus package data directory.

    Uses importlib.resources for installed packages, falls back to
    filesystem-relative path for development installs.
    """
    try:
        from importlib.resources import files

        data_dir = files("Pegasus.data")
        # Convert to a path if possible
        data_path = Path(str(data_dir))
        if data_path.is_dir():
            return data_path
    except (ImportError, TypeError, ModuleNotFoundError):
        pass

    # Fallback: look relative to this file's location
    # In development: packages/pegasus-python/src/Pegasus/cli/_java.py
    # Data might be at: share/pegasus/ or src/Pegasus/data/
    cli_dir = Path(__file__).resolve().parent
    pegasus_dir = cli_dir.parent

    # Check for src/Pegasus/data/
    data_dir = pegasus_dir / "data"
    if data_dir.is_dir():
        return data_dir

    # Check for repo-root share/pegasus/ (development mode)
    # Walk up to find the repo root
    candidate = pegasus_dir
    for _ in range(10):
        share_dir = candidate / "share" / "pegasus"
        if share_dir.is_dir():
            return share_dir
        candidate = candidate.parent

    raise click.ClickException(
        "Cannot locate Pegasus data directory. "
        "Ensure Pegasus is properly installed."
    )


def get_java_dir():
    """Get the directory containing Pegasus Java JARs."""
    data_dir = get_pegasus_data_dir()

    # In new layout: data/java/
    java_dir = data_dir / "java"
    if java_dir.is_dir():
        return java_dir

    # Fallback for old layout: share/pegasus/java/
    if data_dir.name == "pegasus":
        java_dir = data_dir / "java"
        if java_dir.is_dir():
            return java_dir

    raise click.ClickException(
        f"Cannot locate Java JARs directory under {data_dir}"
    )


def get_schema_dir():
    """Get the directory containing Pegasus schemas."""
    data_dir = get_pegasus_data_dir()

    for candidate in [data_dir / "schema", data_dir / "share" / "schema"]:
        if candidate.is_dir():
            return candidate

    raise click.ClickException(
        f"Cannot locate schema directory under {data_dir}"
    )


def build_classpath():
    """Build the Java classpath for Pegasus.

    Includes all JARs from the java/ directory and java/aws/ subdirectory,
    plus any existing CLASSPATH from the environment.

    Returns a classpath string with entries separated by ':'.
    """
    java_dir = get_java_dir()

    # Collect all JARs
    jars = sorted(glob(str(java_dir / "*.jar")))

    # Add AWS JARs
    aws_dir = java_dir / "aws"
    if aws_dir.is_dir():
        jars.extend(sorted(glob(str(aws_dir / "*.jar"))))

    classpath = ":".join(jars)

    # Append existing CLASSPATH
    env_classpath = os.environ.get("CLASSPATH", "")
    if env_classpath:
        classpath = classpath + ":" + env_classpath

    return classpath


def parse_java_args(args):
    """Separate JVM options (-X*, -D*) from application arguments.

    Mirrors the argument parsing logic from java.sh.

    Returns (jvm_args, app_args) tuple.
    """
    jvm_args = []
    app_args = []

    args = list(args)
    i = 0
    while i < len(args):
        arg = args[i]
        if re.match(r"-[XD][_a-zA-Z]", arg):
            jvm_args.append(arg)
        elif arg == "-D" and i + 1 < len(args):
            i += 1
            next_arg = args[i]
            if "=" in next_arg:
                jvm_args.append(f"-D{next_arg}")
            else:
                app_args.extend(["-D", next_arg])
        else:
            app_args.append(arg)
        i += 1

    return jvm_args, app_args


def run_java_tool(main_class, args=(), system_properties=None):
    """Launch a Java tool via subprocess.

    Args:
        main_class: Fully qualified Java class name to run.
        args: Command-line arguments to pass to the Java tool.
        system_properties: Dict of -D system properties to set.

    Exits with the same return code as the Java process.
    """
    java = find_java()
    heap_args = compute_heap_args()
    classpath = build_classpath()

    # Parse args to separate JVM options from app arguments
    extra_jvm_args, app_args = parse_java_args(args)

    cmd = [java]
    cmd.extend(heap_args)
    cmd.extend(extra_jvm_args)

    # Add system properties
    if system_properties:
        for key, value in system_properties.items():
            cmd.append(f"-D{key}={value}")

    cmd.extend(["-cp", classpath, main_class])
    cmd.extend(app_args)

    result = subprocess.run(cmd)
    sys.exit(result.returncode)


def get_system_properties():
    """Build the standard Pegasus system properties dict for Java tools.

    Returns a dict with keys like 'pegasus.home.sysconfdir', etc.
    """
    data_dir = get_pegasus_data_dir()

    # Determine paths based on data directory layout
    if (data_dir / "java").is_dir():
        # New layout: data/ contains java/, schema/, etc.
        share_dir = str(data_dir)
        schema_dir = str(data_dir / "schema")
        conf_dir = str(data_dir / "etc")
    else:
        # Old layout: share/pegasus/
        share_dir = str(data_dir)
        schema_dir = str(data_dir / "schema")
        base = data_dir.parent.parent  # share/pegasus -> root
        conf_dir = str(base / "etc")

    # bindir must point to where Python console scripts are installed
    # (the venv or system bin/ directory), not data/bin/ which only
    # contains C binaries.  The Java planner constructs absolute paths
    # to tools like pegasus-run, pegasus-exitcode, etc. from this.
    #
    # Prefer shutil.which("pegasus") to find the actual installed location,
    # since sysconfig.get_path("scripts") can be wrong for --user installs
    # and some conda setups.
    pegasus_exe = shutil.which("pegasus")
    if pegasus_exe:
        bin_dir = str(Path(pegasus_exe).resolve().parent)
    else:
        bin_dir = sysconfig.get_path("scripts")

    return {
        "pegasus.home.sysconfdir": conf_dir,
        "pegasus.home.bindir": bin_dir,
        "pegasus.home.sharedstatedir": share_dir,
        "pegasus.home.schemadir": schema_dir,
    }
