# Pegasus WMS Build System Migration TODO

Migration from Ant-based build to unified pip-based build with Click CLI.

## Phase 1: Click CLI Rewrite

- [x] Create `src/Pegasus/cli/main.py` — Click group with lazy subcommand loading
- [x] Create `src/Pegasus/cli/_java.py` — Port `share/pegasus/sh/java.sh` to Python (Java discovery, heap, classpath)
- [x] Migrate `pegasus-status.py` → `_status.py` (already Click, remove PEGASUS_PYTHONPATH boilerplate)
- [x] Migrate `pegasus-run.py` → `_run.py` (already Click)
- [x] Migrate `pegasus-analyzer.py` → `_analyzer.py` (already Click)
- [x] Migrate `pegasus-statistics.py` → `_statistics.py` (already Click)
- [x] Create Java wrapper subcommands: `_plan.py`, `_version.py`, `_rc_client.py`, `_tc_converter.py`, `_rc_converter.py`, `_sc_converter.py`, `_aws_batch.py`
- [x] Wrap argparse scripts: `_monitord.py`, `_dagman.py`, `_db_admin.py`, `_exitcode.py`, `_graphviz.py`, `_init.py`, `_metadata.py`, `_preflight_check.py`, `_remove.py`, `_service.py`, `_submitdir.py`, `_cwl_converter.py`, `_em.py`, `_config.py`
- [x] Wrap worker scripts: `_transfer.py`, `_s3.py`, `_integrity.py`, `_checkpoint.py`, `_globus_online.py`, `_globus_online_init.py`
- [x] Convert `pegasus-halt` from bash to Python Click subcommand
- [x] Ship `pegasus-configure-glite` as data file with Click wrapper
- [x] Test: `pegasus --help` shows all subcommands

## Phase 2: Build Backend and pyproject.toml

- [x] Drop Python 3.6 support — set `requires-python >= 3.9`
- [x] Merge 4 Python packages into single `src/Pegasus/` tree
- [x] Remove `pkgutil.extend_path` from all `__init__.py`
- [x] Move Java source to `java_src/`
- [x] Move C source to `c_src/`
- [x] Move JAR deps + schemas + shell helpers to `src/Pegasus/data/`
- [x] Write `pyproject.toml` with consolidated deps and `console_scripts`
- [x] Write `setup.py` with `BuildJava`, `BuildC`, and `BuildWorkerPackage` custom commands
- [x] Write `MANIFEST.in`
- [x] Rewrite `pegasus-config` to use `importlib.resources`
- [x] Consolidate test directories into single `test/` with unified `tox.ini`
- [x] Update all internal path references (`PEGASUS_SHARE_DIR`, etc.)
- [x] Test: `pip install .` succeeds and all tests pass

## Phase 3: Cleanup

- [x] Remove old `packages/` directory (C/C++ source moved to `c_src/`, Python merged to `src/Pegasus/`)
- [x] Remove `bin/` directory (shell wrappers replaced by Click CLI, `pegasus-configure-glite` moved to `src/Pegasus/data/share/`)
- [x] Update `build.xml` to use new source locations (`java_src/`, `c_src/`, top-level `tox.ini`)
- [x] Fix Python references to `bin/pegasus-version` (replaced with `importlib.metadata`)
- [x] Update `CLAUDE.md` and `MANIFEST.in`

## Phase 4: Testing and final polishes

- [ ] Fix: condor submit bug
- [ ] Click CLI testing (Add `--help` test for each subcommands)
- [ ] Check `tox` testing
- [ ] Add Make build that parallels Ant build
- [ ] CRITICAL: Create a new brach from main, and edit/delete/move files while preserving git history. (Ex. `git rm`, `git mv`, etc.)

## Phase 5: PyPI Publishing

- [ ] Set up cibuildwheel for multi-platform wheel builds (Linux x86_64/aarch64, macOS x86_64/arm64)
- [ ] Configure CI for sdist + wheel + PyPI upload
- [ ] Publish legacy redirect stub packages (`pegasus-wms.common`, `pegasus-wms.api`, `pegasus-wms.worker`)
- [ ] Add `ant pip-dist` target for transitional compatibility
- [ ] Update RPM/DEB packaging for new structure
- [ ] Test: `pip install pegasus-wms` from PyPI works end-to-end

## Phase 6: Future Improvements

- [ ] Add deprecation warnings for standalone `pegasus-<tool>` commands
