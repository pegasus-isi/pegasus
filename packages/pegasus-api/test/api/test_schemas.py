import pytest
from jsonschema import Draft7Validator

# Validating the actual format of the schemas against the Draft 7 meta-schema


@pytest.mark.parametrize(
    "schema_file",
    [
        ("common.json"),
        ("wf-5.0.json"),
        ("rc-5.0.json"),
        ("tc-5.0.json"),
        ("sc-5.0.json"),
    ],
)
def test_schema(schema_file, convert_yaml_schemas_to_json, load_schema):
    schema = load_schema(schema_file)
    Draft7Validator.check_schema(schema)

    # everything defined under $defs is not automatically validated unless we
    # explicitly do so
    for _, _def in schema["$defs"].items():
        Draft7Validator.check_schema(_def)
