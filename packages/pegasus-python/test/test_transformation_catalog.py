import json
from tempfile import NamedTemporaryFile, TemporaryFile

import pytest

import Pegasus
from Pegasus import yaml
from Pegasus.api.mixins import EventType
from Pegasus.api.site_catalog import OS, Arch
from Pegasus.api.transformation_catalog import (
    Container,
    Transformation,
    TransformationCatalog,
    TransformationSite,
)
from Pegasus.api.writable import _CustomEncoder
from Pegasus.transformation_catalog import _to_tc, dump, dumps, load, loads


@pytest.fixture(scope="module")
def tc1():
    return (
        TransformationCatalog()
        .add_transformations(
            Transformation("t1", namespace="test", version="1.0")
            .add_sites(
                TransformationSite(
                    "local",
                    "/pfn",
                    True,
                    arch=Arch.X86_64,
                    os_type=OS.LINUX,
                    os_release="1",
                    os_version="1",
                    container="cont",
                )
                .add_dagman_profile(retry="3")
                .add_metadata(JAVA_HOME="/usr/bin/java")
            )
            .add_requirement("t2", namespace="test", version="1.0")
            .add_shell_hook(EventType.START, "echo hello")
        )
        .add_containers(
            Container(
                "cont",
                Container.DOCKER,
                "docker:///ryan/centos-pegasus:latest",
                mounts=["/Volumes/Work/lfs1:/shared-data/:ro"],
                image_site="local",
            ).add_env(JAVA_HOME="/usr/bin/java")
        )
    )


@pytest.fixture(scope="module")
def tc2():
    return (
        TransformationCatalog()
        .add_transformations(
            Transformation("t1", namespace="test", version="1.0").add_sites(
                TransformationSite("local", "/pfn", True,)
            )
        )
        .add_containers(
            Container(
                "cont",
                Container.DOCKER,
                "docker:///ryan/centos-pegasus:latest",
                mounts=["/Volumes/Work/lfs1:/shared-data/:ro"],
                image_site="local",
            )
        )
    )


def test_to_tc_with_optional_args_set(tc1):
    expected = json.loads(json.dumps(tc1, cls=_CustomEncoder))
    result = json.loads(json.dumps(_to_tc(expected), cls=_CustomEncoder))
    assert result == expected


def test_to_tc_without_optional_args(tc2):
    expected = json.loads(json.dumps(tc2, cls=_CustomEncoder))
    result = json.loads(json.dumps(_to_tc(expected), cls=_CustomEncoder))
    assert result == expected


@pytest.mark.parametrize("_format", [("yml"), ("json")])
def test_load(mocker, tc1, _format):
    # write to tempfile as _format
    with TemporaryFile(mode="w+") as f:
        tc1.write(f, _format=_format)
        f.seek(0)

        # load into new tc object
        new_tc = load(f)

    # assert that what was loaded is equal to original
    result = json.loads(json.dumps(new_tc, cls=_CustomEncoder))
    expected = json.loads(json.dumps(tc1, cls=_CustomEncoder))

    assert result == expected


def test_loads(tc1):
    # dump tc1 to str, then load into new tc
    new_tc = loads(json.dumps(tc1, cls=_CustomEncoder))

    # assert that what was loaded is equal to the original
    result = json.loads(json.dumps(new_tc, cls=_CustomEncoder))
    expected = json.loads(json.dumps(tc1, cls=_CustomEncoder))

    assert result == expected


def test_dump(mocker, tc1):
    mocker.patch("Pegasus.api.writable.Writable.write")
    with NamedTemporaryFile(mode="w") as f:
        dump(tc1, f, _format="yml")
        Pegasus.api.writable.Writable.write.assert_called_once_with(f, _format="yml")


def test_dumps(tc1):
    assert yaml.load(dumps(tc1)) == json.loads(json.dumps(tc1, cls=_CustomEncoder))
