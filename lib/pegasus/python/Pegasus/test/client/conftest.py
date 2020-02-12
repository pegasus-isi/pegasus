import os
import stat

from pathlib import Path

import pytest

# _client.Client needs pegasus-version in PATH
# this creates a fake one for local testing in the event it
# is not already part of the user's PATH
@pytest.fixture(scope="function")
def pegasus_version_file(monkeypatch):
    path = Path(os.getcwd()) / "pegasus-version"
    with path.open("w") as f:
        f.write("fake pegasus-version script")
    
    os.chmod(str(path), stat.S_IEXEC)
    monkeypatch.setenv("PATH", os.getcwd(), prepend=os.pathsep)

    yield
    path.unlink()