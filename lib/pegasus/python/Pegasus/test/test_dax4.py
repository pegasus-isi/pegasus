"""
"""

import pytest
from Pegasus.DAX4 import File


@pytest.mark.parametrize("lfn", [("a",), ("例",)])
def _test_valid_file(lfn: str):
    assert isinstance(File(lfn), File)


@pytest.mark.parametrize("lfn", [("[]",)])
def _test_invalid_file(lfn: str):
    with pytest.raises(ValueError):
        raise ValueError("lfn can't contain <[>")
