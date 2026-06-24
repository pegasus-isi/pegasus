import json
import re
from pathlib import Path

import pytest
import yaml

from Pegasus.api.writable import _CustomEncoder


def _tojson(obj):
    """Returns dict representation of obj using writable._CustomEncoder"""
    return json.loads(json.dumps(obj, cls=_CustomEncoder))


@pytest.fixture(scope="module")
def convert_yaml_schemas_to_json():
    """
    Convert all the yaml schemas into json files.
    These files will be used whenever schema validation is
    needed for the tests. The json files will be cleaned up
    at the end of the test module.
    """
    # get the path of the schema file with the given name
    path = Path(__file__).resolve().parents[4] / "share/pegasus/schema/yaml"

    json_schemas = {
        path / filename: path / filename.replace(".yml", ".json")
        for filename in [
            "common.yml",
            "rc-5.0.yml",
            "tc-5.0.yml",
            "sc-5.0.yml",
            "wf-5.0.yml",
        ]
    }

    # convert each of the yml schemas to json
    for yml_filename, json_filename in json_schemas.items():
        with yml_filename.open() as yml_file, json_filename.open("w") as json_file:
            json_str = json.dumps(yaml.safe_load(yml_file))

            # for references pointing to '*.yml' files, convert them to point
            # to '.json' files instead
            json_str = re.sub(
                r"([a-z0-9\-\.]+)(.yml)",
                "file://" + str(path / r"\1.json"),
                json_str,
            )

            json_obj = json.loads(json_str)
            json_obj["$id"] = "file://" + str(json_filename)

            json.dump(json_obj, json_file, indent=4)

    yield

    # cleanup
    for _, json_schema in json_schemas.items():
        json_schema.unlink()


@pytest.fixture(scope="function")
def load_schema():
    """
    Load the given schema into memory.
    """

    def _load_schema(name):
        # get the path of the schema file with the given name
        path = Path(__file__).resolve().parents[4] / "share/pegasus/schema/yaml" / name

        with path.open() as f:
            return json.load(f)

    return _load_schema
