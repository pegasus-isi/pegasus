"""
Custom build commands for compiling Java and C components during pip install.

This setup.py supplements pyproject.toml with custom build commands that
compile the Java planner JARs and C execution tools when installing from
source (sdist). Pre-built wheels skip these steps since they already contain
the compiled artifacts.
"""

import datetime
import os
import shutil
import subprocess
import sys
from glob import glob
from pathlib import Path

from setuptools import setup
from setuptools.command.build import build

# Root of the repository
ROOT_DIR = Path(__file__).resolve().parent

# Where compiled artifacts should go (inside the package for inclusion in wheels)
DATA_DIR = ROOT_DIR / "src" / "Pegasus" / "data"


class BuildJava(build):
    """Compile Java source into JARs.

    Runs javac on the Java source tree and creates:
    - pegasus.jar (planner + common + VDL)
    - pegasus-aws-batch.jar (AWS Batch client)

    Skips if:
    - PEGASUS_NO_JAVA=1 environment variable is set
    - Pre-compiled JARs already exist in data/java/
    - javac is not available
    """

    description = "compile Java planner source into JARs"

    def run(self):
        if os.environ.get("PEGASUS_NO_JAVA", "0") == "1":
            print("PEGASUS_NO_JAVA=1: skipping Java compilation")
            return

        java_data_dir = DATA_DIR / "java"

        # Check if JARs are already pre-built
        if java_data_dir.exists():
            existing_jars = list(java_data_dir.glob("pegasus.jar"))
            if existing_jars:
                print("Pre-compiled JARs found, skipping Java compilation")
                return

        # Check for javac
        javac = shutil.which("javac")
        if javac is None:
            java_home = os.environ.get("JAVA_HOME")
            if java_home:
                javac = os.path.join(java_home, "bin", "javac")
                if not os.access(javac, os.X_OK):
                    javac = None

        if javac is None:
            print(
                "WARNING: javac not found. Java planner will not be available.\n"
                "Set JAVA_HOME or install a JDK to enable Java compilation.",
                file=sys.stderr,
            )
            return

        jar_cmd = shutil.which("jar")
        if jar_cmd is None:
            java_home = os.environ.get("JAVA_HOME")
            if java_home:
                jar_cmd = os.path.join(java_home, "bin", "jar")

        if jar_cmd is None:
            print(
                "WARNING: jar command not found. Skipping Java compilation.",
                file=sys.stderr,
            )
            return

        java_src_dir = ROOT_DIR / "java_src"
        if not java_src_dir.exists():
            # Try legacy location
            java_src_dir = ROOT_DIR / "src"
            if not (java_src_dir / "edu").exists():
                print("Java source not found, skipping Java compilation")
                return

        print("Compiling Java source...")

        # Ensure output directories exist
        java_data_dir.mkdir(parents=True, exist_ok=True)
        build_dir = ROOT_DIR / "build" / "java" / "classes"
        build_dir.mkdir(parents=True, exist_ok=True)

        # Collect all dependency JARs for classpath
        dep_jars = []
        share_java = ROOT_DIR / "share" / "pegasus" / "java"
        if share_java.exists():
            dep_jars.extend(glob(str(share_java / "*.jar")))
            aws_dir = share_java / "aws"
            if aws_dir.exists():
                dep_jars.extend(glob(str(aws_dir / "*.jar")))

        # Also check data dir for pre-existing deps
        if java_data_dir.exists():
            dep_jars.extend(glob(str(java_data_dir / "*.jar")))

        classpath = ":".join(dep_jars) if dep_jars else ""

        # Find all Java source files
        java_files = list(java_src_dir.rglob("*.java"))
        if not java_files:
            print("No Java source files found")
            return

        # Write source file list for javac
        sources_file = build_dir / "sources.txt"
        with open(sources_file, "w") as f:
            for jf in java_files:
                f.write(str(jf) + "\n")

        # Compile
        cmd = [javac, "-source", "1.8", "-target", "1.8", "-d", str(build_dir)]
        if classpath:
            cmd.extend(["-cp", classpath])
        cmd.extend(["@" + str(sources_file)])

        print(f"Running: {' '.join(cmd[:6])}...")
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"Java compilation failed:\n{result.stderr}", file=sys.stderr)
            return

        # Generate pegasus.build.properties (mirrors build.xml lines 157-163)
        version = "5.2.0-dev"
        build_props_file = ROOT_DIR / "build.properties"
        if build_props_file.exists():
            with open(build_props_file) as f:
                for line in f:
                    if line.startswith("pegasus.version"):
                        version = line.split("=", 1)[1].strip()

        timestamp = datetime.datetime.now(datetime.timezone.utc).strftime(
            "%Y%m%d%H%M%SZ"
        )

        platform_str = "unknown"
        getsystem = ROOT_DIR / "release-tools" / "getsystem"
        if getsystem.exists() and os.access(str(getsystem), os.X_OK):
            gs_result = subprocess.run([str(getsystem)], capture_output=True, text=True)
            if gs_result.returncode == 0 and gs_result.stdout.strip():
                platform_str = gs_result.stdout.strip()

        git_hash = ""
        try:
            gs_result = subprocess.run(
                ["git", "log", "-1", "--format=%H"],
                capture_output=True,
                text=True,
                cwd=str(ROOT_DIR),
            )
            if gs_result.returncode == 0:
                git_hash = gs_result.stdout.strip()
        except FileNotFoundError:
            pass

        props_content = (
            f"pegasus.build.version={version}\n"
            f"pegasus.build.platform={platform_str}\n"
            f"pegasus.build.timestamp={timestamp}\n"
            f"pegasus.build.git.hash={git_hash}\n"
        )
        with open(build_dir / "pegasus.build.properties", "w") as f:
            f.write(props_content)
        print("Generated pegasus.build.properties")

        # Create pegasus.jar
        pegasus_jar = java_data_dir / "pegasus.jar"
        cmd = [jar_cmd, "cf", str(pegasus_jar), "-C", str(build_dir), "."]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"JAR creation failed:\n{result.stderr}", file=sys.stderr)
            return

        print(f"Created {pegasus_jar}")

        # Copy dependency JARs to data dir
        if share_java.exists():
            for jar in glob(str(share_java / "*.jar")):
                dest = java_data_dir / Path(jar).name
                if not dest.exists():
                    shutil.copy2(jar, dest)

            # AWS JARs
            aws_src = share_java / "aws"
            aws_dest = java_data_dir / "aws"
            if aws_src.exists():
                aws_dest.mkdir(exist_ok=True)
                for jar in glob(str(aws_src / "*.jar")):
                    dest = aws_dest / Path(jar).name
                    if not dest.exists():
                        shutil.copy2(jar, dest)

        print("Java compilation complete")


class BuildC(build):
    """Compile C/C++ tools via make.

    Builds:
    - pegasus-kickstart (C)
    - pegasus-cluster (C)
    - pegasus-keg (C++)
    - pegasus-mpi-cluster (C++, optional, requires MPI)

    Skips if:
    - PEGASUS_NO_C=1 environment variable is set
    - Pre-compiled binaries already exist in data/bin/
    - make/gcc are not available
    """

    description = "compile C/C++ execution tools"

    C_TOOLS = [
        ("pegasus-kickstart", "kickstart"),
        ("pegasus-cluster", "cluster"),
        ("pegasus-keg", "keg"),
    ]

    OPTIONAL_TOOLS = [
        ("pegasus-mpi-cluster", "mpi-cluster"),
    ]

    def run(self):
        if os.environ.get("PEGASUS_NO_C", "0") == "1":
            print("PEGASUS_NO_C=1: skipping C compilation")
            return

        bin_dir = DATA_DIR / "bin"

        # Check if binaries already exist
        if bin_dir.exists():
            existing = [f.name for f in bin_dir.iterdir() if f.is_file()]
            if "pegasus-kickstart" in existing:
                print("Pre-compiled C binaries found, skipping C compilation")
                return

        # Check for make and gcc
        make = shutil.which("make")
        gcc = shutil.which("gcc")
        if make is None or gcc is None:
            print(
                "WARNING: make or gcc not found. C tools will not be available.\n"
                "Install build-essential (Debian/Ubuntu) or gcc (RHEL) to compile C tools.",
                file=sys.stderr,
            )
            return

        bin_dir.mkdir(parents=True, exist_ok=True)

        # Build C tools from c_src/ or packages/
        for binary_name, dir_name in self.C_TOOLS:
            self._build_c_tool(binary_name, dir_name, bin_dir, make)

        # Optional tools (MPI)
        for binary_name, dir_name in self.OPTIONAL_TOOLS:
            mpicxx = shutil.which("mpicxx")
            if mpicxx is None:
                print(f"mpicxx not found, skipping {binary_name}")
                continue
            self._build_c_tool(binary_name, dir_name, bin_dir, make)

        # Also copy C binaries to the Python scripts directory so they
        # are co-located with the Python console scripts.  The Java planner
        # uses pegasus.home.bindir (which points here) to find executables.
        import sysconfig as _sysconfig

        scripts_dir = Path(_sysconfig.get_path("scripts"))
        if scripts_dir.exists() and scripts_dir != bin_dir:
            for binary in bin_dir.iterdir():
                if binary.is_file():
                    dest = scripts_dir / binary.name
                    if not dest.exists():
                        shutil.copy2(str(binary), str(dest))
                        os.chmod(str(dest), 0o755)

        print("C compilation complete")

    def _build_c_tool(self, binary_name, dir_name, bin_dir, make):
        # Try new layout first, then legacy
        for src_dir in [
            ROOT_DIR / "c_src" / dir_name,
            ROOT_DIR / "packages" / f"pegasus-{dir_name}",
        ]:
            if src_dir.exists() and (src_dir / "Makefile").exists():
                break
        else:
            print(f"Source for {binary_name} not found, skipping")
            return

        print(f"Building {binary_name} from {src_dir}...")

        # Create a temporary prefix for make install
        prefix_dir = ROOT_DIR / "build" / "c" / dir_name
        prefix_dir.mkdir(parents=True, exist_ok=True)
        (prefix_dir / "bin").mkdir(exist_ok=True)

        cmd = [
            make,
            "-C",
            str(src_dir),
            f"prefix={prefix_dir}",
            f"datadir={prefix_dir / 'share'}",
            "install",
        ]

        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(
                f"WARNING: Building {binary_name} failed:\n{result.stderr}",
                file=sys.stderr,
            )
            return

        # Copy binary to data/bin/
        built_binary = prefix_dir / "bin" / binary_name
        if built_binary.exists():
            dest = bin_dir / binary_name
            shutil.copy2(str(built_binary), str(dest))
            os.chmod(str(dest), 0o755)
            print(f"  Installed {binary_name}")
        else:
            print(f"  WARNING: {binary_name} binary not found after build")


class BuildWorkerPackage(build):
    """Create a worker package tarball for deployment to compute nodes.

    The worker package is a self-contained tarball that the planner
    deploys to remote compute sites.  It contains C binaries
    (kickstart, cluster, keg), Python CLI tools (pegasus-transfer,
    etc.), the Pegasus Python package, and shell helpers.

    Mirrors the Ant ``dist-worker`` target in build.xml.

    Skips if:
    - A worker package tarball already exists in data/worker-packages/
    - C binaries are not available in data/bin/
    """

    description = "create worker package tarball for remote deployment"

    def _get_version(self):
        """Read the Pegasus version from build.properties."""
        version = "5.2.0-dev"
        build_props_file = ROOT_DIR / "build.properties"
        if build_props_file.exists():
            with open(build_props_file) as f:
                for line in f:
                    if line.startswith("pegasus.version"):
                        version = line.split("=", 1)[1].strip()
        return version

    def _get_platform(self):
        """Determine the build platform string."""
        getsystem = ROOT_DIR / "release-tools" / "getsystem"
        if getsystem.exists() and os.access(str(getsystem), os.X_OK):
            result = subprocess.run([str(getsystem)], capture_output=True, text=True)
            if result.returncode == 0 and result.stdout.strip():
                return result.stdout.strip()
        return "unknown"

    def run(self):
        wp_dir = DATA_DIR / "worker-packages"

        version = self._get_version()
        platform_str = self._get_platform()
        tarball_name = f"pegasus-worker-{version}-{platform_str}.tar.gz"

        # Check if tarball already exists
        if wp_dir.exists() and (wp_dir / tarball_name).exists():
            print("Worker package tarball already exists, skipping")
            return

        bin_dir = DATA_DIR / "bin"
        if not bin_dir.exists() or not list(bin_dir.iterdir()):
            print(
                "WARNING: No C binaries in data/bin/. "
                "Worker package requires compiled C tools.",
                file=sys.stderr,
            )
            # Still create the directory so the planner doesn't crash
            wp_dir.mkdir(parents=True, exist_ok=True)
            return

        import tarfile
        import tempfile

        print("Creating worker package tarball...")

        dist_name = f"pegasus-{version}"

        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir) / dist_name

            # bin/ — C binaries
            dest_bin = base / "bin"
            dest_bin.mkdir(parents=True)
            for binary in bin_dir.iterdir():
                if binary.is_file():
                    dest = dest_bin / binary.name
                    shutil.copy2(str(binary), str(dest))
                    os.chmod(str(dest), 0o755)

            # bin/ — Python console-script wrappers needed on worker nodes
            self._create_worker_scripts(dest_bin)

            # lib/ — Python package
            lib_subdir = self._determine_lib_subdir()
            dest_lib = base / lib_subdir
            dest_lib.mkdir(parents=True)

            src_pegasus = ROOT_DIR / "src" / "Pegasus"
            if src_pegasus.exists():
                shutil.copytree(
                    str(src_pegasus),
                    str(dest_lib / "Pegasus"),
                    ignore=shutil.ignore_patterns(
                        "__pycache__",
                        "*.pyc",
                        "*.pyo",
                        "java",
                        "*.jar",
                    ),
                )

            # lib/pegasus/python symlink (expected by pegasus-lite scripts)
            pegasus_lib = base / "lib" / "pegasus"
            pegasus_lib.mkdir(parents=True, exist_ok=True)
            python_link = pegasus_lib / "python"
            rel_target = os.path.relpath(str(dest_lib), str(pegasus_lib))
            python_link.symlink_to(rel_target)

            # lib/pegasus/externals/python/ — external deps
            ext_dir = pegasus_lib / "externals" / "python"
            ext_dir.mkdir(parents=True, exist_ok=True)
            # Install minimal external deps needed on worker nodes
            self._install_worker_externals(ext_dir)

            # share/pegasus/ — shell helpers and schemas
            dest_share = base / "share" / "pegasus"
            for subdir in ["sh", "schema", "notification", "htcondor"]:
                src_sub = DATA_DIR / subdir
                if src_sub.exists():
                    shutil.copytree(str(src_sub), str(dest_share / subdir))

            # LICENSE
            license_file = ROOT_DIR / "LICENSE"
            if license_file.exists():
                shutil.copy2(str(license_file), str(base / "LICENSE"))

            # Create tarball
            wp_dir.mkdir(parents=True, exist_ok=True)
            tarball_path = wp_dir / tarball_name

            with tarfile.open(str(tarball_path), "w:gz") as tar:
                tar.add(str(base), arcname=dist_name)

        print(f"Created worker package: {tarball_path}")

    def _create_worker_scripts(self, dest_bin):
        """Create minimal wrapper scripts for Python CLI tools needed on workers."""
        # These are the tools referenced by PEGASUS_WORKER_EXECUTABLES
        # pegasus-transfer and pegasus-integrity are Python entry points
        worker_tools = {
            "pegasus-transfer": "Pegasus.cli.main:transfer",
            "pegasus-s3": "Pegasus.cli.main:s3",
            "pegasus-integrity": "Pegasus.cli.main:integrity",
            "pegasus-exitcode": "Pegasus.cli.main:exitcode",
            "pegasus-checkpoint": "Pegasus.cli.main:checkpoint",
        }

        for tool_name, entry_point in worker_tools.items():
            module, func = entry_point.rsplit(":", 1)
            script = dest_bin / tool_name
            script.write_text(
                f"#!/usr/bin/env python3\n"
                f"import sys\n"
                f"from {module} import {func}\n"
                f"sys.exit({func}())\n"
            )
            os.chmod(str(script), 0o755)

    def _determine_lib_subdir(self):
        """Determine the Python lib subdirectory (e.g. lib/python3.12/site-packages)."""
        v = sys.version_info
        return f"lib/python{v.major}.{v.minor}/site-packages"

    def _install_worker_externals(self, ext_dir):
        """Install external Python dependencies needed on worker nodes."""
        # The worker needs boto3 and its deps for S3 transfers,
        # plus globus-sdk for Globus transfers
        req_file = ROOT_DIR / "src" / "requirements.txt"
        if not req_file.exists():
            return

        python = sys.executable
        result = subprocess.run(
            [
                python,
                "-m",
                "pip",
                "install",
                "-t",
                str(ext_dir),
                "--no-deps",
                "-r",
                str(req_file),
            ],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            print(
                f"WARNING: Installing worker externals failed:\n{result.stderr}",
                file=sys.stderr,
            )


class CustomBuild(build):
    """Custom build that compiles Java and C before the standard build."""

    def run(self):
        # Run Java and C compilation first
        self.run_command("build_java")
        self.run_command("build_c")
        self.run_command("build_worker_package")
        # Then run the standard build
        super().run()


setup(
    cmdclass={
        "build": CustomBuild,
        "build_java": BuildJava,
        "build_c": BuildC,
        "build_worker_package": BuildWorkerPackage,
    },
)
