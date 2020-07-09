import json
from tempfile import NamedTemporaryFile, TemporaryFile

import pytest

import Pegasus
from Pegasus import yaml
from Pegasus.api.site_catalog import (
    OS,
    Arch,
    Directory,
    FileServer,
    Grid,
    Operation,
    Scheduler,
    Site,
    SiteCatalog,
    SupportedJobs,
)
from Pegasus.api.writable import _CustomEncoder
from Pegasus.site_catalog import _to_sc, dump, dumps, load, loads


@pytest.fixture(scope="module")
def sc1():
    return SiteCatalog().add_sites(
        Site(
            "local", arch=Arch.X86_64, os_type=OS.LINUX, os_release="1", os_version="1",
        )
        .add_directories(
            Directory(Directory.LOCAL_SCRATCH, "/path").add_file_servers(
                FileServer("url", Operation.ALL).add_dagman_profile(retry=1)
            )
        )
        .add_dagman_profile(retry=1)
        .add_grids(
            Grid(
                Grid.CONDOR,
                "contact",
                Scheduler.CONDOR,
                job_type=SupportedJobs.REGISTER,
            )
        )
    )


@pytest.fixture(scope="module")
def sc2():
    return SiteCatalog().add_sites(
        Site("local",)
        .add_directories(
            Directory(Directory.LOCAL_SCRATCH, "/path").add_file_servers(
                FileServer("url", Operation.ALL)
            )
        )
        .add_grids(Grid(Grid.CONDOR, "contact", Scheduler.CONDOR,))
    )


def test_to_sc_with_optional_args_set(sc1):
    expected = json.loads(json.dumps(sc1, cls=_CustomEncoder))
    result = json.loads(json.dumps(_to_sc(expected), cls=_CustomEncoder))
    assert result == expected


def test_to_sc_without_optional_args(sc2):
    expected = json.loads(json.dumps(sc2, cls=_CustomEncoder))
    result = json.loads(json.dumps(_to_sc(expected), cls=_CustomEncoder))
    assert result == expected


@pytest.mark.parametrize("_format", [("yml"), ("json")])
def test_load(sc1, _format):
    # write to tempfile as _format
    with TemporaryFile(mode="w+") as f:
        sc1.write(f, _format=_format)
        f.seek(0)

        # load into new sc object
        new_sc = load(f)

    # assert that what was loaded is equal to original
    result = json.loads(json.dumps(new_sc, cls=_CustomEncoder))
    expected = json.loads(json.dumps(sc1, cls=_CustomEncoder))

    assert result == expected


def test_loads_json(sc1):
    # dump sc1 to str, then load into new sc
    new_sc = loads(json.dumps(sc1, cls=_CustomEncoder))

    # assert that what was loaded is equal to the original
    result = json.loads(json.dumps(new_sc, cls=_CustomEncoder))
    expected = json.loads(json.dumps(sc1, cls=_CustomEncoder))

    assert result == expected


def test_loads_yaml(sc1):
    # dump sc1 to str, then load into new sc
    new_sc = loads(yaml.dump(json.loads(json.dumps(sc1, cls=_CustomEncoder))))

    # assert that what was loaded is equal to the original
    result = json.loads(json.dumps(new_sc, cls=_CustomEncoder))
    expected = json.loads(json.dumps(sc1, cls=_CustomEncoder))

    assert result == expected


def test_dump(mocker, sc1):
    mocker.patch("Pegasus.api.writable.Writable.write")
    with NamedTemporaryFile(mode="w") as f:
        dump(sc1, f, _format="yml")
        Pegasus.api.writable.Writable.write.assert_called_once_with(f, _format="yml")


def test_dumps(sc1):
    assert yaml.load(dumps(sc1)) == json.loads(json.dumps(sc1, cls=_CustomEncoder))
