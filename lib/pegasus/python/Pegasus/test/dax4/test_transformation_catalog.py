import os
import json

import pytest
from jsonschema import validate

from Pegasus.dax4.transformation_catalog import TransformationType
from Pegasus.dax4.transformation_catalog import _TransformationSite
from Pegasus.dax4.transformation_catalog import ContainerType
from Pegasus.dax4.transformation_catalog import _Container
from Pegasus.dax4.transformation_catalog import Transformation
from Pegasus.dax4.transformation_catalog import TransformationCatalog
from Pegasus.dax4.transformation_catalog import PEGASUS_VERSION
from Pegasus.dax4.site_catalog import OSType
from Pegasus.dax4.site_catalog import Arch
from Pegasus.dax4.mixins import Namespace
from Pegasus.dax4.mixins import EventType
from Pegasus.dax4.errors import DuplicateError
from Pegasus.dax4.errors import NotFoundError
from Pegasus.dax4.writable import FileFormat


class Test_TransformationSite:
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
        _TransformationSite(name, pfn, transformation_type, **kwargs)

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
            _TransformationSite(name, pfn, transformation_type, **kwargs)

    @pytest.mark.parametrize(
        "transformation_site, expected_json",
        [
            (
                _TransformationSite("local", "/pfn", TransformationType.STAGEABLE),
                {"name": "local", "pfn": "/pfn", "type": "stageable"},
            ),
            (
                _TransformationSite(
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
    def test_tojson_no_profiles_or_metadata(
        self,
        transformation_site: _TransformationSite,
        expected_json: dict,
        convert_yaml_schemas_to_json,
        load_schema,
    ):
        result = transformation_site.__json__()

        transformation_site_schema = load_schema("tc-5.0.json")["$defs"][
            "transformation"
        ]["properties"]["sites"]["items"]

        validate(instance=result, schema=transformation_site_schema)

        assert transformation_site.__json__() == expected_json

    def test_tojson_with_profiles_and_metadata(
        self, convert_yaml_schemas_to_json, load_schema
    ):
        t = (
            _TransformationSite("local", "/pfn", TransformationType.INSTALLED)
            .add_profile(Namespace.ENV, "JAVA_HOME", "/java/home")
            .add_metadata("key", "value")
        )

        result = t.__json__()
        expected = {
            "name": "local",
            "pfn": "/pfn",
            "type": TransformationType.INSTALLED.value,
            "profiles": {Namespace.ENV.value: {"JAVA_HOME": "/java/home"}},
            "metadata": {"key": "value"},
        }

        transformation_site_schema = load_schema("tc-5.0.json")["$defs"][
            "transformation"
        ]["properties"]["sites"]["items"]

        validate(instance=result, schema=transformation_site_schema)

        assert result == expected


class TestTransformation:
    @pytest.mark.parametrize(
        "transformation",
        [
            (Transformation("name")),
            (Transformation("name", namespace="namespace")),
            (Transformation("name", namespace="namespace", version="1.1")),
        ],
    )
    def test_get_key(self, transformation):
        assert transformation._get_key() == (
            transformation.name,
            transformation.namespace,
            transformation.version,
        )

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

    def test_add_site_metadata(self):
        t = Transformation("test")
        t.add_site("local", "/pfn", TransformationType.STAGEABLE)
        t.add_site_metadata("local", "key", "value")

        t_local_metadata = t.sites["local"].metadata

        assert "key" in t_local_metadata
        assert t_local_metadata["key"] == "value"

    def test_add_requirement_as_str(self):
        t = Transformation("test")
        required_transformation_name = "required"
        t.add_requirement(required_transformation_name)

        assert (required_transformation_name, None, None) in t.requires

    def test_add_requirement_as_transformation_object(self):
        t = Transformation("test")
        required = Transformation("required", namespace=None, version=None)

        t.add_requirement(required)
        assert required._get_key() in t.requires

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
        assert required._get_key() in t.requires

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

    def test_chaining(self):
        t = (
            Transformation("test")
            .add_site("local", "/pfn", TransformationType.STAGEABLE)
            .add_requirement("required")
            .add_site_profile("local", Namespace.ENV, "JAVA_HOME", "/java/home")
        )

        assert t.has_site("local")
        assert t.sites["local"].has_profile(Namespace.ENV, "JAVA_HOME", "/java/home")
        assert t.has_requirement("required")

    def test_tojson_without_profiles_hooks_metadata(
        self, convert_yaml_schemas_to_json, load_schema
    ):
        t = Transformation("test", namespace="pegasus")
        t.add_site("local", "/pfn", TransformationType.STAGEABLE)
        t.add_requirement("required")

        result = t.__json__()
        expected = {
            "name": "test",
            "namespace": "pegasus",
            "requires": ["required"],
            "sites": [t.sites["local"].__json__()],
        }

        transformation_schema = load_schema("tc-5.0.json")["$defs"]["transformation"]

        validate(instance=result, schema=transformation_schema)

        assert result == expected

    def test_tojson_with_profiles_hooks_metadata(
        self, convert_yaml_schemas_to_json, load_schema
    ):
        t = Transformation("test", namespace="pegasus")
        t.add_site("local", "/pfn", TransformationType.STAGEABLE)
        t.add_site_profile("local", Namespace.ENV, "JAVA_HOME", "/java/home")
        t.add_requirement("required")

        t.add_profile(Namespace.ENV, "JAVA_HOME", "/java/home")
        t.add_shell_hook(EventType.START, "/bin/echo hi")
        t.add_metadata("key", "value")

        result = t.__json__()
        expected = {
            "name": "test",
            "namespace": "pegasus",
            "requires": ["required"],
            "sites": [t.sites["local"].__json__()],
            "metadata": {"key": "value"},
            "profiles": {Namespace.ENV.value: {"JAVA_HOME": "/java/home"}},
            "hooks": {"shell": [{"_on": EventType.START.value, "cmd": "/bin/echo hi"}]},
        }

        transformation_schema = load_schema("tc-5.0.json")["$defs"]["transformation"]
        validate(instance=result, schema=transformation_schema)

        assert result == expected


class Test_Container:
    def test_valid_container(self):
        _Container("test", ContainerType.DOCKER, "image", ["mount"])

    def test_invalid_container(self):
        with pytest.raises(ValueError):
            _Container("test", "container_type", "image", ["mount"])

    def test_tojson_no_profiles(self, convert_yaml_schemas_to_json, load_schema):
        c = _Container("test", ContainerType.DOCKER, "image", ["mount"])

        result = c.__json__()
        expected = {
            "name": "test",
            "type": ContainerType.DOCKER.value,
            "image": "image",
            "mounts": ["mount"],
        }

        container_schema = load_schema("tc-5.0.json")["$defs"]["container"]
        validate(instance=result, schema=container_schema)

        assert result == expected

    def test_tojson_with_profiles(self, convert_yaml_schemas_to_json, load_schema):
        c = _Container("test", ContainerType.DOCKER, "image", ["mount"])
        c.add_profile(Namespace.ENV, "JAVA_HOME", "/java/home")

        result = c.__json__()
        expected = {
            "name": "test",
            "type": ContainerType.DOCKER.value,
            "image": "image",
            "mounts": ["mount"],
            "profiles": {Namespace.ENV.value: {"JAVA_HOME": "/java/home"}},
        }

        container_schema = load_schema("tc-5.0.json")["$defs"]["container"]
        validate(instance=result, schema=container_schema)

        assert result == expected


class TestTransformationCatalog:
    def test_add_single_transformation(self):
        tc = TransformationCatalog()
        tc.add_transformations(Transformation("test"))

        assert ("test", None, None) in tc.transformations
        assert len(tc.transformations) == 1

    def test_add_multiple_transformations(self):
        tc = TransformationCatalog()

        t1 = Transformation("name")
        t2 = Transformation("name", namespace="namespace")
        t3 = Transformation("name", namespace="namespace", version="version")

        tc.add_transformations(t1, t2, t3)

        assert ("name", None, None) in tc.transformations
        assert ("name", "namespace", None) in tc.transformations
        assert ("name", "namespace", "version") in tc.transformations
        assert len(tc.transformations) == 3

    def test_add_duplicate_transformation(self):
        tc = TransformationCatalog()
        tc.add_transformations(Transformation("name"))
        with pytest.raises(DuplicateError):
            tc.add_transformations(Transformation("name", namespace=None, version=None))

    def test_add_invalid_transformation(self):
        tc = TransformationCatalog()
        with pytest.raises(ValueError):
            tc.add_transformations(1)

    def test_has_transformation_str(self):
        tc = TransformationCatalog()
        tc.add_transformations(Transformation("name", namespace="namespace"))
        assert tc.has_transformation("name", namespace="namespace")

    def test_has_transformation_obj(self):
        tc = TransformationCatalog()
        t = Transformation("name", namespace="namespace")
        tc.add_transformations(t)
        assert tc.has_transformation(t)

    def test_has_invalid_transformation(self):
        tc = TransformationCatalog()
        with pytest.raises(ValueError):
            tc.has_transformation(1)

    def test_add_container(self):
        tc = TransformationCatalog()
        tc.add_container("container", ContainerType.DOCKER, "image", ["mount"])

        assert len(tc.containers) == 1
        assert "container" in tc.containers

    def test_add_duplicate_container(self):
        tc = TransformationCatalog()
        tc.add_container("container", ContainerType.DOCKER, "image", ["mount"])
        with pytest.raises(DuplicateError):
            tc.add_container("container", ContainerType.DOCKER, "image", ["mount"])

    def test_add_invlaid_container(self):
        tc = TransformationCatalog()
        with pytest.raises(ValueError):
            tc.add_container("container", "docker", "image", ["mount"])

    def test_has_container(self):
        tc = TransformationCatalog()
        tc.add_container("container1", ContainerType.DOCKER, "image", ["mount"])
        tc.add_container("container2", ContainerType.DOCKER, "image", ["mount"])

        assert tc.has_container("container1")
        assert tc.has_container("container2")

    def remove_container(self):
        tc = TransformationCatalog()
        tc.add_container("container", ContainerType.DOCKER, "image", ["mount"])

        assert tc.has_container("container")
        assert len(tc.containers) == 1

        tc.remove_container("container")
        assert not tc.has_container("container")
        assert len(tc.containers) == 0

    def remove_container_not_added(self):
        tc = TransformationCatalog()
        with pytest.raises(NotFoundError):
            tc.remove_container("container")

    def test_chaining(self):
        tc = TransformationCatalog()

        (
            tc.add_transformations(Transformation("t1"))
            .add_transformations(Transformation("t2"))
            .add_container(
                "container1", ContainerType.DOCKER, "image", ["mount1", "mount2"]
            )
            .add_container(
                "container2", ContainerType.DOCKER, "image", ["mount1", "mount2"]
            )
        )

        assert tc.has_transformation("t1")
        assert tc.has_transformation("t2")
        assert tc.has_container("container1")
        assert tc.has_container("container2")

        (tc.remove_container("container1").remove_container("container2"))

        assert len(tc.containers) == 0

    def test_tojson(self, convert_yaml_schemas_to_json, load_schema):
        tc = TransformationCatalog()
        (
            tc.add_transformations(
                Transformation("t1").add_site(
                    "local", "/pfn", TransformationType.INSTALLED
                )
            )
            .add_transformations(
                Transformation("t2").add_site(
                    "local", "/pfn", TransformationType.INSTALLED
                )
            )
            .add_container("container1", ContainerType.DOCKER, "image", ["mount1"])
            .add_container("container2", ContainerType.DOCKER, "image", ["mount1"])
        )

        expected = {
            "pegasus": PEGASUS_VERSION,
            "transformations": [t.__json__() for _, t in tc.transformations.items()],
            "containers": [c.__json__() for _, c in tc.containers.items()],
        }

        expected["transformations"] = sorted(
            expected["transformations"], key=lambda t: t["name"]
        )
        expected["containers"] = sorted(expected["containers"], key=lambda c: c["name"])

        result = tc.__json__()

        result["transformations"] = sorted(
            result["transformations"], key=lambda t: t["name"]
        )
        result["containers"] = sorted(result["containers"], key=lambda c: c["name"])

        tc_schema = load_schema("tc-5.0.json")
        validate(instance=result, schema=tc_schema)

        assert result == expected

    def test_tojson_no_containers(self, convert_yaml_schemas_to_json, load_schema):
        tc = TransformationCatalog()
        (
            tc.add_transformations(Transformation("t1")
                .add_site("local", "/pfn", TransformationType.INSTALLED)).add_transformations(
                Transformation("t2").add_site("local2", "/pfn", TransformationType.STAGEABLE)
            )
        )

        expected = {
            "pegasus": PEGASUS_VERSION,
            "transformations": [t.__json__() for _, t in tc.transformations.items()],
        }

        expected["transformations"] = sorted(
            expected["transformations"], key=lambda t: t["name"]
        )

        result = tc.__json__()

        result["transformations"] = sorted(
            result["transformations"], key=lambda t: t["name"]
        )

        tc_schema = load_schema("tc-5.0.json")
        validate(instance=result, schema=tc_schema)

        assert expected == result

    def test_write(self):
        tc = TransformationCatalog()
        (
            tc.add_transformations(Transformation("t1")).add_transformations(
                Transformation("t2")
            )
        )

        expected = {
            "pegasus": PEGASUS_VERSION,
            "transformations": [t.__json__() for _, t in tc.transformations.items()],
        }

        expected["transformations"] = sorted(
            expected["transformations"], key=lambda t: t["name"]
        )

        test_output_filename = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "TransformationCatalogTestOutput.json",
        )

        tc.write(non_default_filepath=test_output_filename, file_format=FileFormat.JSON)

        with open(test_output_filename, "r") as f:
            result = json.load(f)
            result["transformations"] = sorted(
                result["transformations"], key=lambda t: t["name"]
            )

        assert result == expected

        # cleanup
        os.remove(test_output_filename)

