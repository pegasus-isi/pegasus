"""
Abstract :mod:`yaml` with Pegasus specific defaults.

.. moduleauthor:: Rajiv Mayani <mayani@isi.edu>
"""
import io
from collections import OrderedDict
from functools import partial
from pathlib import Path
from typing import Dict

import yaml as _yaml
import yaml.constructor

try:
    from yaml import CSafeDumper as _Dumper
    from yaml import CSafeLoader as _Loader
except ImportError:
    from yaml import SafeDumper as _Dumper
    from yaml import SafeLoader as _Loader

__all__ = (
    "load",
    "loads",
    "load_all",
    "dump",
    "dumps",
    "dump_all",
)


# Loader extras
def _construct_bool(self, node):
    """
    Ensure :mod:`yaml` handles booleans in the expected fashion.

    Ensure :mod:`yaml` does not translate on, yes to True, and off, no to False.

    .. notes::

        In YAML 1.2 the usage of off/on/yes/no was dropped.
    """
    bool_values = {
        "true": True,
        "false": False,
    }
    v = self.construct_scalar(node)
    return bool_values.get(v.lower(), v)


# Disable deserializing yes/no to boolean True/False
_Loader.add_constructor("tag:yaml.org,2002:bool", _construct_bool)


# Disable deserializing date, datetime strings to date, datetime object
_Loader.add_constructor(
    "tag:yaml.org,2002:timestamp", _Loader.yaml_constructors["tag:yaml.org,2002:str"],
)


# Dumper extras
def _represent_path(self, data: Path):
    """
    Serialize a `Path` object to a string.

    .. warning::
        Path("./aaa") serializes to "aaa"

    :param data: [description]
    :type data: pathlib.Path
    """
    return self.represent_scalar("tag:yaml.org,2002:str", str(data))


def _represent_ordered_dict(self, data):
    """
    Serialize OrderedDict to a yaml map

    :param data: [description]
    :type data: collections.OrderedDict
    """
    return self.represent_mapping("tag:yaml.org,2002:map", data.items())


# Serializing Python `Path` objects to `str`
# NOTE: Path("./aaa") serializes to "aaa"
_Dumper.add_multi_representer(Path, _represent_path)

_Dumper.add_representer(OrderedDict, _represent_ordered_dict)

load = partial(_yaml.load, Loader=_Loader)


def loads(s: str, *args, **kwargs) -> Dict:
    """
    Deserialize ``s`` (a ``str``, ``bytes`` or ``bytearray`` instance containing a YAML document) to a Python dictionary.

    [extended_summary]

    :param s: [description]
    :type s: str
    :return: [description]
    :rtype: Dict
    """
    return load(io.StringIO(s), *args, **kwargs)


load_all = partial(_yaml.load_all, Loader=_Loader)


dump = partial(_yaml.dump, Dumper=_Dumper)


def dumps(obj: Dict, Dumper=_Dumper, *args, **kwargs) -> str:
    """
    Serialize ``obj`` to a YAML formatted ``str``.

    [extended_summary]

    :param obj: [description]
    :type obj: Dict
    :return: [description]
    :rtype: str
    """
    return dump(obj, *args, **kwargs)


dump_all = partial(_yaml.dump_all, Dumper=_Dumper)
