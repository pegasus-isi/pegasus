from configparser import DEFAULTSECT
from tempfile import TemporaryFile

from Pegasus.api.properties import Properties
from Pegasus.properties import *


def test_load():
    with TemporaryFile(mode="w+") as f:
        f.write(
            """env.PEGASUS_HOME = HOME
env.pegasus_home = home
"""
        )
        f.seek(0)
        props = load(f)

    assert props._conf[DEFAULTSECT] == {
        "env.PEGASUS_HOME": "HOME",
        "env.pegasus_home": "home",
    }


def test_loads():
    s = """env.PEGASUS_HOME = HOME
env.pegasus_home = home
"""
    props = loads(s)

    assert props._conf[DEFAULTSECT] == {
        "env.PEGASUS_HOME": "HOME",
        "env.pegasus_home": "home",
    }


def test_dump():
    props = Properties()
    props["env.PEGASUS_HOME"] = "HOME"
    props["env.pegasus_home"] = "home"

    with TemporaryFile(mode="w+") as f:
        dump(props, f)
        f.seek(0)
        assert (
            f.read()
            == """env.PEGASUS_HOME = HOME
env.pegasus_home = home

"""
        )


def test_dumps():
    props = Properties()
    props["env.PEGASUS_HOME"] = "HOME"
    props["env.pegasus_home"] = "home"

    assert (
        dumps(props)
        == """env.PEGASUS_HOME = HOME
env.pegasus_home = home

"""
    )
