import json
import os
import re

import pytest
import yaml


@pytest.fixture(scope="module")
def convert_yaml_schemas_to_json():
    """
    Convert all the yaml schemas into json files.
    These files will be used whenever schema validation is 
    needed for the tests. The json files will be cleaned up 
    at the end of the test module.
    """
    # get the path of the schema file with the given name
    path = os.path.dirname(os.path.realpath(__file__))
    path = path.replace("packages/pegasus-api/test/api", "share/pegasus/schema/yaml")

    json_schemas = {
        os.path.join(path, filename): os.path.join(
            path, filename.replace(".yml", ".json")
        )
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
        with open(yml_filename) as yml_file, open(json_filename, "w") as json_file:

            json_str = json.dumps(yaml.safe_load(yml_file))

            # for references pointing to '*.yml' files, convert them to point
            # to '.json' files instead
            json_str = re.sub(
                r"([a-z0-9\-\.]+)(.yml)",
                os.path.join("file://" + path, r"\1.json"),
                json_str,
            )

            json_obj = json.loads(json_str)
            json_obj["$id"] = "file://" + json_filename

            json.dump(json_obj, json_file, indent=4)

    yield

    # cleanup
    for _, json_schema in json_schemas.items():
        os.remove(json_schema)


@pytest.fixture(scope="function")
def load_schema():
    """
    Load the given schema into memory.
    """

    def _load_schema(name):
        # get the path of the schema file with the given name
        path = os.path.dirname(os.path.realpath(__file__))
        path = path.replace(
            "packages/pegasus-api/test/api", "share/pegasus/schema/yaml"
        )
        path = os.path.join(path, name)

        with open(path) as f:
            return json.load(f)

    return _load_schema
