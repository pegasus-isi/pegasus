import json
from enum import Enum


def todict(_dict, cls):
    # https://pypi.org/project/dataclasses-fromdict/
    raise NotImplementedError()


class FileFormat(Enum):
    JSON = "json"
    YAML = "yml"


# TODO: look at source of json.dump and json.dumps so that we can figure out
# how to encode yaml
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
    if not isinstance(_dict, dict):
        raise ValueError(
            "a dict must be passed to this function, not {}".format(type(_dict))
        )

    return {key: value for key, value in _dict.items() if value is not None}

