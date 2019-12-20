import json
from enum import Enum

import yaml

# TODO: if we need to deserialize a class
def todict(_dict, cls):
    # https://pypi.org/project/dataclasses-fromdict/
    raise NotImplementedError()


class FileFormat(Enum):
    """Supported file types we can write"""

    JSON = "json"
    YAML = "yml"


class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        # TODO: handle instance of Date and Path
        """
        if isinstance(obj, Date):
			return "whatever spec we come up with for Date such as ISO8601"
        elif isinstance(obj, Path):
            return obj.resolve
        """

        if hasattr(obj, "__json__"):
            if callable(obj.__json__):
                return obj.__json__()
            else:
                raise TypeError("__json__ is not callable for {}".format(obj))

        return json.JSONEncoder.default(self, obj)


def filter_out_nones(_dict):
    """Helper function to remove keys where their values are set to None to avoid cluttering yaml/json files
    
    :param _dict: object represented as a dict
    :type _dict: dict
    :raises ValueError: _dict must be of type dict
    :return: new dictionary with 'None' values removed 
    :rtype: dict
    """
    if not isinstance(_dict, dict):
        raise ValueError(
            "a dict must be passed to this function, not {}".format(type(_dict))
        )

    return {key: value for key, value in _dict.items() if value is not None}


class Writable:
    """Derived class can serialized to a json or yaml file"""

    def write(self, non_default_filepath="", file_format=FileFormat.YAML):
        """Write this object, formatted in YAML, to a file
        
        :param non_default_filepath: path to which this catalog will be written, defaults to '<ClassName>.yml' if non_default_filepath is "" or None
        :type non_default_filepath: str, optional
        :param file_format: class can be serialized as either YAML or JSON, defaults to YAML
        :type file_format: FileFormat
        """
        if not isinstance(file_format, FileFormat):
            raise ValueError("invalid file format {}".format(file_format))

        default_filepath = type(self).__name__
        if non_default_filepath != "":
            path = non_default_filepath
        else:
            if file_format == FileFormat.YAML:
                path = ".".join([default_filepath, FileFormat.YAML.value])
            elif file_format == FileFormat.JSON:
                path = ".".join([default_filepath, FileFormat.JSON.value])

        with open(path, "w") as file:
            if file_format == FileFormat.YAML:
                yaml.dump(CustomEncoder().default(self), file, sort_keys=False)
            elif file_format == FileFormat.JSON:
                json.dump(self, file, cls=CustomEncoder, indent=4)
