PYTHON    ?= python3
CMAKE     ?= cmake
BUILD_DIR ?= _cmake_build

.PHONY: build dev build-c build-java build-worker clean clean-java clean-c clean-worker help

# Build a distributable wheel.  scikit-build-core drives cmake internally.
build:
	$(PYTHON) -m build

# Editable (development) install.
# Python changes take effect immediately; C/Java are compiled once.
# Re-run after C or Java source changes to recompile.
dev:
	$(PYTHON) -m pip install -e . --no-build-isolation

# Re-build only C tools (skip Java)
build-c:
	$(CMAKE) -B $(BUILD_DIR) -S . \
	    -DCMAKE_BUILD_TYPE=Release \
	    -DPEGASUS_BUILD_JAVA=OFF
	$(CMAKE) --build $(BUILD_DIR)

# Re-build only Java JARs (skip C)
build-java:
	$(CMAKE) -B $(BUILD_DIR) -S . \
	    -DCMAKE_BUILD_TYPE=Release \
	    -DPEGASUS_BUILD_C=OFF
	$(CMAKE) --build $(BUILD_DIR)

# Build the worker package tarball (pegasus-worker-VERSION-PLATFORM.tar.gz).
# Slow: runs pip install for external deps.
build-worker:
	$(CMAKE) -B $(BUILD_DIR) -S . \
	    -DCMAKE_BUILD_TYPE=Release \
	    -DPEGASUS_BUILD_WORKER=ON
	$(CMAKE) --build $(BUILD_DIR) --target pegasus-worker-package

# Remove only worker build artifacts.
clean-worker:
	rm -rf $(BUILD_DIR)/worker_package

# Remove only Java build artifacts — forces recompilation on next build.
clean-java:
	rm -rf $(BUILD_DIR)/java_src

# Remove only C build artifacts — forces recompilation on next build.
clean-c:
	rm -rf $(BUILD_DIR)/c_src \
	       $(BUILD_DIR)/c_src

# Remove all build artifacts: cmake build dir, wheel output, egg-info, caches.
clean: clean-java clean-c clean-worker
	rm -rf $(BUILD_DIR) dist/ src/*.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null; true

help:
	@echo "Pegasus WMS build targets (scikit-build-core / CMake):"
	@echo ""
	@echo "  build         Build a distributable wheel into dist/"
	@echo "  dev           Editable install (Python live; C/Java compile on first run)"
	@echo "  build-c       Re-build only C tools (pegasus-kickstart, cluster, keg)"
	@echo "  build-java    Re-build only Java JARs (pegasus.jar, pegasus-aws-batch.jar)"
	@echo "  build-worker  Build worker package tarball (slow: runs pip install)"
	@echo "  clean         Remove all build artifacts"
	@echo "  clean-java    Remove only Java cmake build output"
	@echo "  clean-c       Remove only C cmake build output"
	@echo "  clean-worker  Remove only worker package cmake build output"
	@echo ""
	@echo "Variables:"
	@echo "  PYTHON=$(PYTHON)     (override with PYTHON=/path/to/python)"
	@echo "  CMAKE=$(CMAKE)       (override with CMAKE=/path/to/cmake)"
	@echo "  BUILD_DIR=$(BUILD_DIR)  (cmake build directory)"
	@echo ""
	@echo "Environment overrides:"
	@echo "  PEGASUS_NO_C=1      Skip C compilation"
	@echo "  PEGASUS_NO_JAVA=1   Skip Java compilation"
	@echo "  PEGASUS_NO_WORKER=1 Skip worker package build"
	@echo ""
	@echo "Standalone cmake (produces ./bin/ at project root):"
	@echo "  cmake -B $(BUILD_DIR) && cmake --install $(BUILD_DIR) --prefix ."
