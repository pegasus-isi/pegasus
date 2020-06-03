__author__ = "Rajiv Mayani"

import logging
import uuid
from decimal import Decimal
from enum import Enum

from flask.json import JSONEncoder
from sqlalchemy import inspect

from Pegasus.service.base import ErrorResponse, OrderedDict, PagedResponse

log = logging.getLogger(__name__)


class PegasusJsonEncoder(JSONEncoder):
    """JSON Encoder for Pegasus Service API Resources."""

    def default(self, o):
        """."""

        if isinstance(o, uuid.UUID):
            return str(o)

        elif isinstance(o, Decimal):
            return float(o)

        elif isinstance(o, Enum):
            return o.name

        elif isinstance(o, PagedResponse):
            json_record = OrderedDict([("records", o.records)])

            if o.total_records or o.total_filtered:
                meta = OrderedDict()

                if o.total_records is not None:
                    meta["records_total"] = o.total_records

                if o.total_filtered is not None:
                    meta["records_filtered"] = o.total_filtered

                json_record["_meta"] = meta

            return json_record

        elif isinstance(o, ErrorResponse):
            json_record = OrderedDict([("code", o.code), ("message", o.message)])

            if o.errors:
                json_record["errors"] = [{"field": f, "errors": e} for f, e in o.errors]

            return json_record

        elif hasattr(o, "__json__"):
            return o.__json__()

        elif hasattr(o, "__table__"):
            unloaded = inspect(o).unloaded
            _v = {
                k: getattr(o, k)
                for k in o.__mapper__.column_attrs.keys()
                if k not in unloaded
            }

            for k in getattr(o, "__includes__", {}):
                _v[k] = getattr(o, k)

            for k in getattr(o, "__excludes__", {}):
                del _v[k]

            return _v

        return JSONEncoder.default(self, o)
