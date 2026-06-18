PYTHON      ?= python3
CMAKE       ?= cmake
BUILD_DIR   ?= _cmake_build
WORKER_DATA := packages/pegasus-python/src/Pegasus/data/worker-packages

# Java tooling — override via JAVA_HOME or explicit variable
ifdef JAVA_HOME
JAVA     ?= $(JAVA_HOME)/bin/java
JAVAC    ?= $(JAVA_HOME)/bin/javac
JAR_TOOL ?= $(JAVA_HOME)/bin/jar
else
JAVA     ?= java
JAVAC    ?= javac
JAR_TOOL ?= jar
endif

# Derive tox environment name from the active Python (e.g. py313)
PY_VERSION  := $(shell $(PYTHON) -c "import sys; print('py{}{}'.format(*sys.version_info[:2]))")

_JAVA_TEST_CLASSES := $(BUILD_DIR)/java-test-classes
_JUNIT_REPORT_DIR  := test-reports/junit

.PHONY: build dev build-c build-java build-worker clean clean-java clean-c clean-worker \
        clean-test test test-python test-java test-c help

# Build a distributable wheel.  scikit-build-core drives cmake internally.
build: build-worker
	$(PYTHON) -m build

# Editable (development) install.
# Python changes take effect immediately; C/Java are compiled once.
# Re-run after C or Java source changes to recompile.
dev:
	$(PYTHON) -m pip install -e . --no-build-isolation

# Re-build only C tools (skip Java and worker)
build-c:
	$(CMAKE) -B $(BUILD_DIR) -S . \
	    -DCMAKE_BUILD_TYPE=Release \
	    -DPEGASUS_BUILD_C=ON \
	    -DPEGASUS_BUILD_JAVA=OFF \
	    -DPEGASUS_BUILD_WORKER=OFF \
	    -DPEGASUS_BUILD_MPI=OFF
	$(CMAKE) --build $(BUILD_DIR)

# Re-build only Java JARs (skip C and worker)
build-java:
	$(CMAKE) -B $(BUILD_DIR) -S . \
	    -DCMAKE_BUILD_TYPE=Release \
	    -DPEGASUS_BUILD_C=OFF \
	    -DPEGASUS_BUILD_JAVA=ON \
	    -DPEGASUS_BUILD_WORKER=OFF
	$(CMAKE) --build $(BUILD_DIR)

# Build the worker package tarball (pegasus-worker-VERSION-PLATFORM.tar.gz)
# and stage it into the Python source tree so that `make build` includes it
# in the wheel as Pegasus/data/worker-packages/<tarball>.
# Slow: runs pip install for external deps.
build-worker:
	$(CMAKE) -B $(BUILD_DIR) -S . \
	    -DCMAKE_BUILD_TYPE=Release \
	    -DPEGASUS_BUILD_C=OFF \
	    -DPEGASUS_BUILD_JAVA=OFF \
	    -DPEGASUS_BUILD_WORKER=ON
	$(CMAKE) --build $(BUILD_DIR) --target build_worker_tarball
	mkdir -p $(WORKER_DATA)
	cp $(BUILD_DIR)/pegasus-worker-*.tar.gz $(WORKER_DATA)/

# Remove worker build artifacts (cmake staging dir and the staged tarball in the
# Python source tree; the latter must be removed to avoid stale tarballs in the wheel).
clean-worker:
	rm -rf $(BUILD_DIR)/worker_staging
	rm -f $(WORKER_DATA)/pegasus-worker-*.tar.gz

# Remove only Java build artifacts — forces recompilation on next build.
clean-java:
	rm -rf $(BUILD_DIR)/jars \
	       $(BUILD_DIR)/java-test-classes \
	       $(BUILD_DIR)/CMakeFiles/pegasus_jar.dir \
	       $(BUILD_DIR)/CMakeFiles/pegasus_aws_batch_jar.dir

# Remove only C build artifacts — forces recompilation on next build.
clean-c:
	rm -rf $(BUILD_DIR)/packages

# Remove test output artifacts only (reports, coverage data, compiled test classes).
# Does not remove tox virtualenvs — use 'make clean' to nuke everything.
clean-test:
	rm -rf $(_JAVA_TEST_CLASSES) $(BUILD_DIR)/jars/pegasus-test.jar test-reports/
	rm -f packages/pegasus-*/.coverage
	rm -rf packages/pegasus-*/test-reports
	find packages -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null; true

# Remove all build artifacts: cmake build dir, wheel output, egg-info, caches.
clean:
	rm -rf $(BUILD_DIR) dist/ test-reports/
	rm -f $(WORKER_DATA)/pegasus-worker-*.tar.gz
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null; true
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null; true

# Run all tests (Python + Java + C).
# Java tests require: make build-java
# C tests require:    make build-c
test: test-python test-java test-c

# Run Python test suites for all four packages via tox.
# Each package manages its own virtualenv; no prior install needed.
test-python:
	cd packages/pegasus-common && tox -e $(PY_VERSION)
	cd packages/pegasus-api    && tox -e $(PY_VERSION)
	cd packages/pegasus-worker && tox -e $(PY_VERSION)
	cd packages/pegasus-python && tox -e $(PY_VERSION)

# Run Java unit tests (JUnit 5).
# Requires: make build-java   (produces $(BUILD_DIR)/jars/pegasus.jar)
# JUnit JARs are committed at release-tools/jars/target/dependency/.
test-java:
	@if [ ! -f "$(BUILD_DIR)/jars/pegasus.jar" ]; then \
		echo "Skipping Java tests: run 'make build-java' first"; \
	elif [ ! -d "release-tools/jars/target/dependency" ]; then \
		echo "Skipping Java tests: JUnit JARs not found in release-tools/jars/target/dependency/"; \
	else \
		echo "--- Java unit tests ---"; \
		mkdir -p "$(CURDIR)/$(_JAVA_TEST_CLASSES)" "$(CURDIR)/$(_JUNIT_REPORT_DIR)"; \
		find test/junit -name "*.java" > "$(CURDIR)/$(_JAVA_TEST_CLASSES)/sources.txt"; \
		$(JAVAC) -encoding UTF-8 --release 11 \
		    -cp "$(CURDIR)/share/pegasus/java/*:$(CURDIR)/share/pegasus/java/aws/*:$(CURDIR)/release-tools/jars/target/dependency/*:$(CURDIR)/$(BUILD_DIR)/jars/pegasus.jar:$(CURDIR)/$(BUILD_DIR)/jars/pegasus-aws-batch.jar" \
		    -d "$(CURDIR)/$(_JAVA_TEST_CLASSES)" \
		    @"$(CURDIR)/$(_JAVA_TEST_CLASSES)/sources.txt" && \
		$(JAR_TOOL) cf "$(CURDIR)/$(BUILD_DIR)/jars/pegasus-test.jar" \
		    -C "$(CURDIR)/$(_JAVA_TEST_CLASSES)" . && \
		$(JAVA) \
		    --add-opens=java.base/java.util=ALL-UNNAMED \
		    --add-opens=java.base/java.lang=ALL-UNNAMED \
		    -Dpegasus.home.schemadir="$(CURDIR)/share/pegasus/schema" \
		    -Dpegasus.home.bindir="$(CURDIR)/$(BUILD_DIR)" \
		    -Dpegasus.home.sysconfdir="$(CURDIR)/etc" \
		    -Dpegasus.home.sharedstatedir="$(CURDIR)/share/pegasus" \
		    -cp "$(CURDIR)/share/pegasus/java/*:$(CURDIR)/share/pegasus/java/aws/*:$(CURDIR)/release-tools/jars/target/dependency/*:$(CURDIR)/$(BUILD_DIR)/jars/pegasus.jar:$(CURDIR)/$(BUILD_DIR)/jars/pegasus-aws-batch.jar:$(CURDIR)/$(BUILD_DIR)/jars/pegasus-test.jar" \
		    org.junit.platform.console.ConsoleLauncher execute \
		    --fail-if-no-tests \
		    --scan-classpath="$(CURDIR)/$(_JAVA_TEST_CLASSES)" \
		    --reports-dir="$(CURDIR)/$(_JUNIT_REPORT_DIR)"; \
	fi

# Run C integration tests.
# kickstart tests require: make build-c  (populates $(BUILD_DIR)/packages/pegasus-kickstart/)
# mpi-cluster tests require: mpicxx in PATH and make build-c with -DPEGASUS_BUILD_MPI=ON
test-c:
	@if [ -d "$(BUILD_DIR)/packages/pegasus-kickstart" ]; then \
		echo "--- pegasus-kickstart tests ---"; \
		_py_bin=$$($(PYTHON) -c "import sys,os; print(os.path.dirname(sys.executable))"); \
		if [ -f "$$_py_bin/pegasus-integrity" ]; then \
			ln -sf "$$_py_bin/pegasus-integrity" \
			    "$(CURDIR)/$(BUILD_DIR)/packages/pegasus-kickstart/pegasus-integrity"; \
		else \
			echo "WARNING: pegasus-integrity not found in $$_py_bin — run 'make dev' first"; \
		fi; \
		cd packages/pegasus-kickstart/test && \
		PEGASUS_BIN_DIR="$(CURDIR)/$(BUILD_DIR)/packages/pegasus-kickstart" ./test.sh; \
	else \
		echo "Skipping kickstart tests: run 'make build-c' first"; \
	fi
	@if command -v mpicxx >/dev/null 2>&1 && [ -f "$(BUILD_DIR)/packages/pegasus-mpi-cluster/pegasus-mpi-cluster" ]; then \
		echo "--- pegasus-mpi-cluster tests ---"; \
		$(MAKE) -C packages/pegasus-mpi-cluster test; \
	else \
		echo "Skipping mpi-cluster tests: requires mpicxx and 'make build-c' with -DPEGASUS_BUILD_MPI=ON"; \
	fi

help:
	@echo "Pegasus WMS build targets (scikit-build-core / CMake):"
	@echo ""
	@echo "  build         Build a distributable wheel into dist/"
	@echo "  dev           Editable install (Python live; C/Java compile on first run)"
	@echo "  build-c       Re-build only C tools (pegasus-kickstart, cluster, keg)"
	@echo "  build-java    Re-build only Java JARs (pegasus.jar, pegasus-aws-batch.jar)"
	@echo "  build-worker  Build worker package tarball (slow: runs pip install)"
	@echo "  clean         Remove all build artifacts"
	@echo "  clean-test    Remove test output (reports, coverage, compiled test classes)"
	@echo "  clean-java    Remove only Java cmake build output"
	@echo "  clean-c       Remove only C cmake build output"
	@echo "  clean-worker  Remove only worker package cmake build output"
	@echo ""
	@echo "Test targets:"
	@echo "  test          Run all tests (Python + Java + C)"
	@echo "  test-python   Run tox test suites for all four Python packages"
	@echo "  test-java     Run Java unit tests via JUnit 5 (needs 'make build-java')"
	@echo "  test-c        Run C integration tests (needs 'make build-c')"
	@echo ""
	@echo "Variables:"
	@echo "  PYTHON=$(PYTHON)     (override with PYTHON=/path/to/python)"
	@echo "  CMAKE=$(CMAKE)       (override with CMAKE=/path/to/cmake)"
	@echo "  JAVA=$(JAVA)         (override with JAVA=/path/to/java)"
	@echo "  BUILD_DIR=$(BUILD_DIR)  (cmake build directory)"
	@echo ""
	@echo "Environment overrides:"
	@echo "  PEGASUS_NO_C=1      Skip C compilation"
	@echo "  PEGASUS_NO_JAVA=1   Skip Java compilation"
	@echo "  PEGASUS_NO_WORKER=1 Skip worker package build"
	@echo ""
	@echo "Standalone cmake (produces ./bin/ at project root):"
	@echo "  cmake -B $(BUILD_DIR) && cmake --install $(BUILD_DIR) --prefix ."
