"""
Abstract :mod:`json` with Pegasus specific defaults.

.. moduleauthor:: Rajiv Mayani <mayani@isi.edu>
"""

import io
import json as _json
import logging
import uuid
from enum import Enum
from functools import partial
from pathlib import Path
from typing import Iterator, List, Optional

__all__ = (
    "load",
    "loads",
    "load_all",
    "dump",
    "dumps",
    "dump_all",
)


class _CustomJSONEncoder(_json.JSONEncoder):
    """JSON encoder extended with Pegasus-specific type handling.

    Supports serialization of :class:`uuid.UUID`, :class:`enum.Enum`,
    :class:`pathlib.Path`, SQLAlchemy model instances, and objects that
    implement ``__html__()``, ``__json__()``, or ``__table__`` (SQLAlchemy).
    """

    def default(self, o):
        if isinstance(o, uuid.UUID):
            return str(o)
        elif isinstance(o, Enum):
            return o.name
        elif isinstance(o, Path):
            # Serializing Python `Path` objects to `str`
            # NOTE: Path("./aaa") serializes to "aaa"
            return str(o)
        elif hasattr(o, "__html__"):
            return o.__html__()
        elif hasattr(o, "__json__"):
            return o.__json__()
        elif hasattr(o, "__table__"):
            return {k: getattr(o, k) for k in o.__table__.columns.keys()}
        else:
            logging.getLogger(__name__).warning(
                "Don't know how to handle type %s" % type(o)
            )

        return _json.JSONEncoder.default(self, o)


load = _json.load


loads = _json.loads


def load_all(s, *args, **kwargs) -> Iterator:
    """
    Deserialize newline-delimited JSON (NDJSON) from ``s`` to an iterator of Python objects.

    Each non-empty line of ``s`` is parsed as a separate JSON document.

    :param s: NDJSON string or open text file object to deserialize
    :type s: str or file-like
    :return: iterator of deserialized Python objects, one per JSON line
    :rtype: Iterator
    :raises TypeError: if ``s`` is neither a string nor a file-like object
    """
    if isinstance(s, str):
        fp = io.StringIO(s)
    elif hasattr(s, "read"):
        fp = s
    else:
        raise TypeError("s must either be a string or an open text file")

    for d in fp.readlines():
        yield loads(d.strip(), *args, **kwargs)


dump = partial(_json.dump, cls=_CustomJSONEncoder)


dumps = partial(_json.dumps, cls=_CustomJSONEncoder)


def dump_all(objs: List, fp=None, *args, **kwargs) -> Optional[str]:
    """
    Serialize a list of objects to newline-delimited JSON (NDJSON).

    Each object is serialized as a compact JSON line (no pretty-printing).
    If ``fp`` is ``None``, the NDJSON string is returned; otherwise the
    output is written to ``fp`` and ``None`` is returned.

    :param objs: list of objects to serialize
    :type objs: List
    :param fp: open writable file object, or ``None`` to return a string, defaults to None
    :type fp: file-like, optional
    :return: NDJSON string if ``fp`` is None, otherwise None
    :rtype: Optional[str]
    :raises TypeError: if ``fp`` is not None and does not have a ``write`` method
    """
    if fp is None:
        fp = io.StringIO()
    elif hasattr(fp, "write"):
        fp = fp
    else:
        raise TypeError("s must either be None or an open text file")

    # Disables pretty printing, when :meth:`dump_all` is called; to support ndjson.
    kwargs.update({"indent": None, "separators": None})

    for d in objs:
        fp.write(dumps(d, *args, **kwargs) + "\n")

    return fp.getvalue() if isinstance(fp, io.StringIO) else None
