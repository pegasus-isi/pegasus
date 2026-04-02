import logging

from flask import make_response
from sqlalchemy.orm.exc import NoResultFound

from Pegasus.service._query import InvalidQueryError
from Pegasus.service._serialize import jsonify
from Pegasus.service._sort import InvalidSortError
from Pegasus.service.base import ErrorResponse, InvalidJSONError
from Pegasus.service.monitoring import monitoring

log = logging.getLogger(__name__)

JSON_HEADER = {"Content-Type": "application/json"}
"""
Error

{
    "code"     : <string:code>,
    "message"  : <string:message>,
    "errors"   : [
        {
            "field"  : <string:field>,
            "errors" : [
                <string:errors>,
                ..
            ]
        },
        ..
    ]
}
"""


@monitoring.errorhandler(NoResultFound)
def no_result_found(error):
    e = ErrorResponse("NOT_FOUND", str(error))
    response_json = jsonify(e)

    return make_response(response_json, 404, JSON_HEADER)


@monitoring.errorhandler(InvalidQueryError)
def invalid_query_error(error):
    e = ErrorResponse("INVALID_QUERY", str(error))
    response_json = jsonify(e)

    return make_response(response_json, 400, JSON_HEADER)


@monitoring.errorhandler(InvalidSortError)
def invalid_order_error(error):
    e = ErrorResponse("INVALID_ORDER", str(error))
    response_json = jsonify(e)

    return make_response(response_json, 400, JSON_HEADER)


@monitoring.errorhandler(InvalidJSONError)
def invalid_json_error(error):
    e = ErrorResponse("INVALID_JSON", str(error))
    response_json = jsonify(e)

    return make_response(response_json, 400, JSON_HEADER)


@monitoring.errorhandler(Exception)
def catch_all(error):
    log.exception(error)

    app_code, http_code = error.codes if hasattr(error, "codes") else ("UNKNOWN", 500)

    e = ErrorResponse(app_code, str(error))
    response_json = jsonify(e)

    return make_response(response_json, http_code, JSON_HEADER)
