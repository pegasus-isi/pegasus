PYTHON ?= python3

# Compiled artifacts that trigger recompilation when absent
JAVA_JAR     := src/Pegasus/data/java/pegasus.jar
C_BINARIES   := src/Pegasus/data/bin/pegasus-kickstart \
                src/Pegasus/data/bin/pegasus-cluster \
                src/Pegasus/data/bin/pegasus-keg \
                src/Pegasus/data/bin/pegasus-mpi-cluster

.PHONY: build dev clean clean-java clean-c help

# Build a wheel. setup.py skips Java/C compilation when artifacts already
# exist, so run `make clean` (or clean-java / clean-c) first to force
# recompilation after source changes.
build:
	$(PYTHON) -m build --wheel

# Editable (development) install. Python changes take effect immediately
# without reinstalling. Java/C are compiled on first install; delete their
# artifacts (make clean-java / clean-c) before re-running to recompile.
dev:
	$(PYTHON) -m pip install -e .

# Remove only Java build artifacts — forces javac to run on next install.
clean-java:
	rm -f $(JAVA_JAR)
	rm -rf build/java/

# Remove only C build artifacts — forces make/gcc to run on next install.
clean-c:
	rm -f $(C_BINARIES)
	rm -rf build/c/

# Remove everything: wheel, egg-info, all compiled artifacts, caches.
clean: clean-java clean-c
	rm -rf build/ dist/ src/*.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null; true

help:
	@echo "Pegasus WMS build targets:"
	@echo ""
	@echo "  build       Build a distributable wheel into dist/"
	@echo "  dev         Editable install (Python changes are live immediately;"
	@echo "              Java/C compile only when artifacts are missing)"
	@echo "  clean       Remove all build artifacts: wheel, egg-info, Java JARs,"
	@echo "              C binaries, and __pycache__ directories"
	@echo "  clean-java  Remove only Java artifacts (pegasus.jar) to force"
	@echo "              recompilation on next install"
	@echo "  clean-c     Remove only C binaries (kickstart, cluster, keg) to force"
	@echo "              recompilation on next install"
	@echo "  help        Show this message"
	@echo ""
	@echo "Development workflow:"
	@echo "  Python change : no action needed after 'make dev'"
	@echo "  Java change   : make clean-java && make dev"
	@echo "  C change      : make clean-c   && make dev"
	@echo "  Release wheel : make clean     && make build"
	@echo ""
	@echo "Variables:"
	@echo "  PYTHON=$(PYTHON)  (override with PYTHON=/path/to/python)"
