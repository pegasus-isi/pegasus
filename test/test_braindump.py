import io
from dataclasses import fields
from typing import Dict

import pytest

from Pegasus.braindump import Braindump, dump, dumps, load, loads

cls_fields = {field.name: field for field in fields(Braindump)}


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
        assert k in cls_fields
        assert isinstance(getattr(b, k), cls_fields[k].type)
        assert isinstance(getattr(b, k), cls_fields[k].type)


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
        assert k in cls_fields
        assert isinstance(getattr(b, k), cls_fields[k].type)
        assert isinstance(getattr(b, k), cls_fields[k].type)


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
        assert k in cls_fields
        assert isinstance(getattr(b, k), cls_fields[k].type)

    dump(b, fp)

    fp = fp.getvalue()
    for attr_name, attr_val in obj.items():
        assert f"{attr_name}: {attr_val}" in fp


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
        assert k in cls_fields
        assert isinstance(getattr(b, k), cls_fields[k].type)

    rv = dumps(b)
    for attr_name, attr_val in obj.items():
        assert f"{attr_name}: {attr_val}" in rv
