import getpass
import json
from collections import OrderedDict
from datetime import datetime
from pathlib import Path
from typing import Optional, TextIO, Union

from .errors import PegasusError

from Pegasus import yaml

__all__ = ["Writable"]


class _CustomEncoder(json.JSONEncoder):
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


def _filter_out_nones(_dict):
    """Helper function to remove keys where their values are set to None to avoid cluttering yaml/json files

    :param _dict: object represented as a dict
    :type _dict: dict
    :raises ValueError: _dict must be of type dict
    :return: new dictionary with 'None' values removed
    :rtype: dict
    """
    if not isinstance(_dict, dict):
        raise TypeError(
            "invalid _dict: {}; _dict must be of type {}".format(_dict, type(dict))
        )

    return OrderedDict([(k, v) for k, v in _dict.items() if v is not None])


class Writable:
    """Derived class can be serialized to a json or yaml file"""

    _FORMATS = {"yml", "yaml", "json"}

    def __init__(self):
        self._path = None

    @property
    def path(self) -> Path:
        """
        Retrieve the path to which this object has been written to.

        :raises PegasusError: object has not yet been written to a file
        :return: resolved path to which this object has been written
        :rtype: Path
        """
        if self._path == None:
            raise PegasusError(
                "{}.write(filename) must be called before trying to retrieve path".format(
                    self.__class__.__name__
                )
            )

        return Path(self._path)

    def _write(self, file, _format):
        """Internal function to dump to file in either yaml or json formats

        :param file: file object to write to
        :type file: file
        :param _format: file format that can be "yml", "yaml", or "json
        :type _ext: str
        :raises ValueError: _format must be one of "yml", "yaml" or "json"
        """
        if _format.lower() not in Writable._FORMATS:
            raise ValueError(
                "invalid _ext: {_format}, extension must be one of {formats}".format(
                    _format=_format, formats=Writable._FORMATS
                )
            )

        # add file info
        self_as_dict = OrderedDict(
            [
                (
                    "x-pegasus",
                    {
                        "createdBy": getpass.getuser(),
                        "createdOn": datetime.now().strftime(r"%m-%d-%yT%H:%M:%SZ"),
                        "apiLang": "python",
                    },
                )
            ]
        )

        self_as_dict.update(
            json.loads(
                json.dumps(self, cls=_CustomEncoder), object_pairs_hook=OrderedDict
            )
        )

        if _format == "yml" or _format == "yaml":
            # TODO: figure out how to get yaml.dump to recurse down into nested objects
            # yaml.dump(_CustomEncoder().default(self), file, sort_keys=False)
            yaml.dump(self_as_dict, file, allow_unicode=True)
        else:
            json.dump(
                self_as_dict, file, cls=_CustomEncoder, indent=4, ensure_ascii=False
            )

    def write(self, file: Optional[Union[str, TextIO]] = None, _format: str = "yml"):
        """Serialize this class as either yaml or json and write to the given
        file. If file==None, this class will be written to a default file. The
        following classes have these defaults:

        .. table:: Default Files
            :widths: auto

            =====================  ===================
            Class                  Default Filename
            =====================  ===================
            SiteCatalog            sites.yml
            ReplicaCatalog         replicas.yml
            TransformationCatalog  transformations.yml
            Workflow               workflow.yml
            =====================  ===================

        :param file: path or file object (opened in "w" mode) to write to, defaults to None
        :type file: Optional[Union[str, TextIO]]
        :param _format: can be either "yml", "yaml" or "json", defaults to "yml"
        :type _format: str, optional
        :raises ValueError: _format must be one of "yml", "yaml" or "json"
        :raises TypeError: file must be a str or file object
        """
        if _format.lower() not in Writable._FORMATS:
            raise ValueError(
                "invalid file format: {_format}, format should be one of 'yml', 'yaml', or 'json'"
            )

        # default file name
        if file is None:
            file = self._DEFAULT_FILENAME

        # do the write
        if isinstance(file, str):
            path = Path(file)
            ext = path.suffix[1:].lower()

            with open(file, "w") as f:
                if ext in Writable._FORMATS:
                    self._write(f, ext)
                else:
                    self._write(f, _format)

            self._path = str(path.resolve())

        elif hasattr(file, "read"):
            try:
                f = Path(str(file.name))
                ext = f.suffix[1:]
            except AttributeError:
                # writing to a stream such as StringIO with
                # no attr "name"
                self._write(file, _format)
            else:
                if ext in Writable._FORMATS:
                    self._write(file, ext)
                else:
                    self._write(file, _format)

                if isinstance(file.name, str):
                    self._path = str(Path(file.name).resolve())

        else:
            raise TypeError(
                "{file} must be of type str or file object".format(file=file)
            )
