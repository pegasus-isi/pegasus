# Minimal makefile for Sphinx documentation
#

# You can set these variables from the command line.
SPHINXOPTS    =
SPHINXBUILD   = sphinx-build
SOURCEDIR     = .
BUILDDIR      = _build
SHELL         = /bin/bash

# Put it first so that "make" without argument is like "make help".
help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)


.PHONY: help Makefile

# Catch-all target: route all unknown targets to Sphinx using the new
# "make mode" option.  $(O) is meant as a shortcut for $(SPHINXOPTS).
%: Makefile
	mkdir -p _static
	@rm -f python/modules.rst # not used - remove so we do not get warnings
	@rm -f build.log warn.log
	set -o pipefail; $(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O) 2>&1 | tee build.log
	@cat build.log \
		| grep -i warn \
		| grep -v "Not copying tabs assets" \
		| grep -v "build succeeded" \
		| grep -v "The app.add_javascript() is deprecated" \
		| grep -v "Pegasus.api.mixins" \
		| grep -v "HTTP status code must be an integer" \
		| grep -v "reference-guide/configuration.rst" \
		| grep -v "user-guide/_basic-properties.rst" \
		| grep -v "python/Pegasus.rst" \
		| grep -v "Problem in http domain" \
		| grep -v "Package sphinx Warning:" \
		| grep -v "LaTeX Warning:" \
		| grep -v "Package rerunfilecheck Warning:" \
		| grep -v "Generating output file PegasusWMS" \
		> warn.log || true
	@if [ -s warn.log ]; then echo "Documentation build contained warnings, which we do not allow. Summary (more above):"; cat warn.log; exit 1; fi

