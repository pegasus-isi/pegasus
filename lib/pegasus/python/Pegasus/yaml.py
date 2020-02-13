# -*- coding: utf-8 -*-
"""
Abstract :mod:`yaml` with Pegasus specific defaults.

.. moduleauthor:: Rajiv Mayani <mayani@isi.edu>
"""

import io
from functools import partial
from typing import Dict

import yaml as _yaml
import yaml.constructor

try:
    from yaml import CSafeLoader as _Loader, CSafeDumper as _Dumper
except ImportError:
    from yaml import SafeLoader as _Loader, SafeDumper as _Dumper

__all__ = (
    "load",
    "loads",
    "load_all",
    "dump",
    "dumps",
    "dump_all",
)

load = partial(_yaml.load, Loader=_Loader)

load_all = partial(_yaml.load_all, Loader=_Loader)

dump = partial(_yaml.dump, Dumper=_Dumper)

dump_all = partial(_yaml.dump_all, Dumper=_Dumper)

yaml.constructor.SafeConstructor.yaml_constructors[
    "tag:yaml.org,2002:timestamp"
] = yaml.constructor.SafeConstructor.yaml_constructors["tag:yaml.org,2002:str"]


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
