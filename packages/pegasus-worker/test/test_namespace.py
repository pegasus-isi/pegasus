# This package's CLI-tool test coverage has migrated to Go alongside the
# tools themselves: pegasus-transfer/pegasus-s3 -> packages/pegasus-transfer,
# pegasus-checkpoint -> packages/pegasus-checkpoint, pegasus-integrity ->
# packages/pegasus-integrity, pegasus-globus-online(-init) ->
# packages/pegasus-globus-online (see each package's own `go test ./...`).
# This package now contains no CLI tools at all, only namespace-package
# plumbing — see packages/pegasus-worker/CLAUDE.md, which flags it as a
# candidate for retiring entirely.
#
# Until/unless that happens, this keeps `tox`/`pytest test/` from failing
# outright with "no tests collected" now that the last real test file
# (test_pegasus_integrity.py) is gone, and asserts the one bit of real
# logic still living in this package: pkgutil.extend_path-based namespace
# merging works.
import Pegasus
import Pegasus.cli


def test_pegasus_namespace_package_extends():
    assert Pegasus.__path__
    assert Pegasus.cli.__path__
