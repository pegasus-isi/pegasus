import os
import json

import pytest

from Pegasus.DAX4.TransformationCatalog import (
    TransformationType,
    TransformationSite,
    ContainerType,
    Container,
    Transformation,
    TransformationCatalog,
)
from Pegasus.DAX4.SiteCatalog import OSType, Arch
from Pegasus.DAX4.Mixins import Namespace, EventType
from Pegasus.DAX4.Errors import DuplicateError, NotFoundError


class TestTransformationSite:
    @pytest.mark.parametrize(
        "name, pfn, transformation_type, kwargs",
        [
            (
                "local",
                "/pfn",
                TransformationType.STAGEABLE,
                {
                    "arch": Arch.X86_64,
                    "os_type": None,
                    "os_release": None,
                    "os_version": None,
                    "glibc": None,
                    "container": None,
                },
            ),
            (
                "local",
                "/pfn",
                TransformationType.STAGEABLE,
                {
                    "arch": Arch.X86_64,
                    "os_type": None,
                    "os_release": None,
                    "os_version": None,
                    "glibc": None,
                    "container": None,
                },
            ),
            (
                "local",
                "/pfn",
                TransformationType.STAGEABLE,
                {
                    "arch": Arch.X86_64,
                    "os_type": OSType.LINUX,
                    "os_release": None,
                    "os_version": None,
                    "glibc": None,
                    "container": None,
                },
            ),
            (
                "local",
                "/pfn",
                TransformationType.STAGEABLE,
                {
                    "arch": Arch.X86_64,
                    "os_type": OSType.LINUX,
                    "os_release": "release",
                    "os_version": "1.1.1",
                    "glibc": "123",
                    "container": "centos-pegasus",
                },
            ),
        ],
    )
    def test_valid_transformation_site(
        self, name: str, pfn: str, transformation_type: TransformationType, kwargs: dict
    ):
        TransformationSite(name, pfn, transformation_type, **kwargs)

    @pytest.mark.parametrize(
        "name, pfn, transformation_type, kwargs",
        [
            (
                "local",
                "/pfn",
                "should be one of TransformationType",
                {
                    "arch": Arch.X86_64,
                    "os_type": None,
                    "os_release": None,
                    "os_version": None,
                    "glibc": None,
                    "container": None,
                },
            ),
            (
                "local",
                "/pfn",
                TransformationType.STAGEABLE,
                {
                    "arch": "should be one of Arch",
                    "os_type": None,
                    "os_release": None,
                    "os_version": None,
                    "glibc": None,
                    "container": None,
                },
            ),
            (
                "local",
                "/pfn",
                TransformationType.STAGEABLE,
                {
                    "arch": Arch.X86_64,
                    "os_type": "should be one of OSType",
                    "os_release": None,
                    "os_version": None,
                    "glibc": None,
                    "container": None,
                },
            ),
        ],
    )
    def test_invalid_transformation_site(
        self, name: str, pfn: str, transformation_type: TransformationType, kwargs: dict
    ):
        with pytest.raises(ValueError):
            TransformationSite(name, pfn, transformation_type, **kwargs)

    @pytest.mark.parametrize(
        "transformation_site, expected_json",
        [
            (
                TransformationSite("local", "/pfn", TransformationType.STAGEABLE),
                {"name": "local", "pfn": "/pfn", "type": "stageable"},
            ),
            (
                TransformationSite(
                    "local",
                    "/pfn",
                    TransformationType.INSTALLED,
                    arch=Arch.X86_64,
                    os_type=OSType.LINUX,
                    os_release="release",
                    os_version="1.1.1",
                    glibc="123",
                    container="centos-pegasus",
                ),
                {
                    "name": "local",
                    "pfn": "/pfn",
                    "type": "installed",
                    "arch": "x86_64",
                    "os.type": "linux",
                    "os.release": "release",
                    "os.version": "1.1.1",
                    "glibc": "123",
                    "container": "centos-pegasus",
                },
            ),
        ],
    )
    def test_tojson_no_profiles(
        self, transformation_site: TransformationSite, expected_json: dict
    ):
        assert transformation_site.__json__() == expected_json

    def test_tojson_with_profiles(self):
        t = TransformationSite(
            "local", "/pfn", TransformationType.INSTALLED
        ).add_profile(Namespace.ENV, "JAVA_HOME", "/java/home")

        assert t.__json__() == {
            "name": "local",
            "pfn": "/pfn",
            "type": TransformationType.INSTALLED.value,
            "profiles": {Namespace.ENV.value: {"JAVA_HOME": "/java/home"}},
        }


class TestTransformation:
    def test_has_site(self):
        t = Transformation("test")
        assert not t.has_site("sitename that doesn't exist")

        t.add_site("local", "/pfn", TransformationType.STAGEABLE)
        assert t.has_site("local")

    def test_add_site(self):
        t = Transformation("test")
        t.add_site("local", "/pfn", TransformationType.STAGEABLE)
        assert t.has_site("local")

    def test_add_duplicate_site(self):
        t = Transformation("test")
        t.add_site("local", "/pfn", TransformationType.STAGEABLE)
        t.add_site("isi", "/pfn", TransformationType.STAGEABLE)

        with pytest.raises(DuplicateError):
            t.add_site("local", "/pfn", TransformationType.STAGEABLE)

    @pytest.mark.parametrize(
        "name, pfn, transformation_type, kwargs",
        [
            ("local", "/pfn", "invalid transformation type", {}),
            ("local", "/pfn", "invalid transformation type", {"arch": "invalid arch"}),
            (
                "local",
                "/pfn",
                TransformationType.STAGEABLE,
                {"arch": Arch.X86_64, "ostype": "invalid os type"},
            ),
            (
                "local",
                "/pfn",
                TransformationType.STAGEABLE,
                {"arch": Arch.X86_64, "ostype": "invalid os type"},
            ),
            (
                "local",
                "/pfn",
                TransformationType.STAGEABLE,
                {"arch": Arch.X86_64, "ostype": "invalid os type"},
            ),
        ],
    )
    def test_add_invalid_site(self, name, pfn, transformation_type, kwargs):
        t = Transformation("test")
        with pytest.raises(ValueError):
            t.add_site(name, pfn, transformation_type, **kwargs)

    def test_remove_site(self):
        t = Transformation("test")
        t.add_site("local", "/pfn", TransformationType.STAGEABLE)
        assert t.has_site("local")

        t.remove_site("local")
        assert not t.has_site("local")

    def test_remove_not_added_site(self):
        t = Transformation("test")
        with pytest.raises(NotFoundError):
            t.remove_site("local")

    def test_add_site_profile(self):
        t = Transformation("test")
        t.add_site("local", "/pfn", TransformationType.STAGEABLE)
        t.add_site_profile("local", Namespace.ENV, "JAVA_HOME", "/java/home")

        t_local_profiles = dict(t.sites["local"].profiles)

        assert Namespace.ENV.value in t_local_profiles
        assert "JAVA_HOME" in t_local_profiles[Namespace.ENV.value]

    def test_add_requirement_as_str(self):
        t = Transformation("test")
        required_transformation_name = "required"
        t.add_requirement(required_transformation_name)

        assert (required_transformation_name, None, None) in t.requires

    def test_add_requirement_as_transformation_object(self):
        t = Transformation("test")
        required = Transformation("required", namespace=None, version=None)

        t.add_requirement(required)
        assert required.get_key() in t.requires

    def test_add_invalid_requirement(self):
        t = Transformation("test")
        with pytest.raises(ValueError):
            t.add_requirement(1)

    def test_add_duplicate_requirement_as_str(self):
        t = Transformation("test")
        required = "required"

        t.add_requirement(required)
        with pytest.raises(DuplicateError):
            t.add_requirement(required)

    def test_add_duplicate_requirement_as_transformation_object(self):
        t = Transformation("test")
        required = Transformation("required", namespace=None, version=None)

        t.add_requirement(required)
        with pytest.raises(DuplicateError):
            t.add_requirement(required)

    def test_has_requirement_as_str(self):
        t = Transformation("test")
        required = "required"

        t.add_requirement(required, "pegasus", "1.1")
        assert (required, "pegasus", "1.1") in t.requires

    def test_has_requirement_as_obj(self):
        t = Transformation("test")
        required = Transformation("required", namespace="pegasus", version="1.1")

        t.add_requirement(required)
        assert required.get_key() in t.requires

    def test_has_invalid_requirement(self):
        t = Transformation("test")
        with pytest.raises(ValueError):
            t.has_requirement(1)

    def test_remove_requirement_as_str(self):
        t = Transformation("test")
        required = "required"

        t.add_requirement(required)
        assert t.has_requirement(required)

        t.remove_requirement(required)
        assert not t.has_requirement(required)
        assert len(t.requires) == 0

    def test_remove_requirement_as_obj(self):
        t = Transformation("test")
        required = Transformation("required", namespace="pegasus", version="1.1")

        t.add_requirement(required)
        assert t.has_requirement(required)

        t.remove_requirement(required)
        assert not t.has_requirement(required)
        assert len(t.requires) == 0

    def test_remove_requirement_not_added(self):
        t = Transformation("test")
        with pytest.raises(NotFoundError):
            t.remove_requirement("123")

        with pytest.raises(NotFoundError):
            t.remove_requirement(Transformation("required"))

    def test_remove_invalid_requirement(self):
        t = Transformation("test")
        with pytest.raises(ValueError):
            t.remove_requirement(123)

    def test_tojson_without_profiles_hooks_metadata(self):
        t = Transformation("test", namespace="pegasus")
        t.add_site("local", "/pfn", TransformationType.STAGEABLE)
        t.add_requirement("required")

        assert t.__json__() == {
            "name": "test",
            "namespace": "pegasus",
            "requires": ["required"],
            "sites": [t.sites["local"].__json__()],
        }

    def test_tojson_with_profiles_hooks_metadata(self):
        t = Transformation("test", namespace="pegasus")
        t.add_site("local", "/pfn", TransformationType.STAGEABLE)
        t.add_site_profile("local", Namespace.ENV, "JAVA_HOME", "/java/home")
        t.add_requirement("required")

        t.add_profile(Namespace.ENV, "JAVA_HOME", "/java/home")
        t.add_shell_hook(EventType.START, "/bin/echo hi")
        t.add_metadata("key", "value")

        assert t.__json__() == {
            "name": "test",
            "namespace": "pegasus",
            "requires": ["required"],
            "sites": [t.sites["local"].__json__()],
            "metadata": {"key": "value"},
            "profiles": {Namespace.ENV.value: {"JAVA_HOME": "/java/home"}},
            "hooks": {"shell": [{"_on": EventType.START.value, "cmd": "/bin/echo hi"}]},
        }


class TestContainer:
    def test_valid_container(self):
        Container("test", ContainerType.DOCKER, "image", "mount")

    def test_invalid_container(self):
        with pytest.raises(ValueError):
            Container("test", "container_type", "image", "mount")

    def test_tojson_no_profiles(self):
        c = Container("test", ContainerType.DOCKER, "image", "mount")

        assert c.__json__() == {
            "name": "test",
            "type": ContainerType.DOCKER.value,
            "image": "image",
            "mount": "mount",
        }

    def test_tojson_with_profiles(self):
        c = Container("test", ContainerType.DOCKER, "image", "mount")
        c.add_profile(Namespace.ENV, "JAVA_HOME", "/java/home")

        assert c.__json__() == {
            "name": "test",
            "type": ContainerType.DOCKER.value,
            "image": "image",
            "mount": "mount",
            "profiles": {Namespace.ENV.value: {"JAVA_HOME": "/java/home"}},
        }


class TestTransformationCatalog:
    pass
