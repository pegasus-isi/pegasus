import io
from typing import Dict

import attr
import pytest

from Pegasus.braindump import Braindump, dump, dumps, load, loads

fields = attr.fields_dict(Braindump)


@pytest.mark.parametrize(
    "s, obj",
    (
        ("user: a", {"user": "a"}),
        ('user: "a"', {"user": "a"}),
        ('{"user": "a"}', {"user": "a"}),
        ('{"uses_pmc": true}', {"uses_pmc": "true"}),
        ('{"uses_pmc": true}', {"uses_pmc": True}),
    ),
)
def test_load(s, obj):
    fp = io.StringIO(s)
    b = load(fp)

    assert isinstance(b, Braindump)

    for k, v in obj.items():
        assert k in fields
        assert isinstance(getattr(b, k), fields[k].type)
        assert isinstance(getattr(b, k), fields[k].type)


@pytest.mark.parametrize(
    "s, obj",
    (
        ("user: a", {"user": "a"}),
        ('user: "a"', {"user": "a"}),
        ('{"user": "a"}', {"user": "a"}),
        ('{"uses_pmc": true}', {"uses_pmc": "true"}),
        ('{"uses_pmc": true}', {"uses_pmc": True}),
    ),
)
def test_loads(s, obj):
    b = loads(s)

    assert isinstance(b, Braindump)

    for k, v in obj.items():
        assert k in fields
        assert isinstance(getattr(b, k), fields[k].type)
        assert isinstance(getattr(b, k), fields[k].type)


def test_loads_fail():
    with pytest.raises(TypeError) as e:
        loads("userr: a")
    assert "'userr'" in str(e)


@pytest.mark.parametrize(
    "obj",
    (
        {"user": "a"},
        {
            "dax": "a.dax",
            "basedir": "/basedir",
            "submit_dir": "/submit-dir",
            "bindir": "/bin-dir",
            "planner": "/plan",
        },
        {"uses_pmc": "true"},
    ),
)
def test_dump(obj: Dict):
    b = Braindump(**obj)
    fp = io.StringIO()

    for k in obj.keys():
        assert k in fields
        assert isinstance(getattr(b, k), fields[k].type)

    dump(b, fp)

    fp = fp.getvalue()
    for attr_name, attr_val in obj.items():
        assert "{}: {}".format(attr_name, attr_val) in fp


@pytest.mark.parametrize(
    "obj",
    (
        {"user": "a"},
        {
            "dax": "a.dax",
            "basedir": "/basedir",
            "submit_dir": "/submit-dir",
            "bindir": "/bin-dir",
            "planner": "/plan",
        },
        {"uses_pmc": "true"},
    ),
)
def test_dumps(obj: Dict):
    b = Braindump(**obj)

    for k in obj:
        assert k in fields
        assert isinstance(getattr(b, k), fields[k].type)

    rv = dumps(b)
    for attr_name, attr_val in obj.items():
        assert "{}: {}".format(attr_name, attr_val) in rv
