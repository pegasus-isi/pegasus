#  Copyright 2007-2014 University Of Southern California
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

__author__ = 'Rajiv Mayani'

import logging

from flask import make_response
from Pegasus.service.base import (
    ErrorResponse, InvalidJSONError, InvalidOrderError, InvalidQueryError
)
from Pegasus.service.monitoring import monitoring_routes
from Pegasus.service.monitoring.utils import jsonify
from sqlalchemy.orm.exc import NoResultFound

log = logging.getLogger(__name__)

JSON_HEADER = {'Content-Type': 'application/json'}
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


@monitoring_routes.errorhandler(NoResultFound)
def no_result_found(error):
    e = ErrorResponse('NOT_FOUND', error.message)
    response_json = jsonify(e)

    return make_response(response_json, 404, JSON_HEADER)


@monitoring_routes.errorhandler(InvalidQueryError)
def invalid_query_error(error):
    e = ErrorResponse('INVALID_QUERY', error.message)
    response_json = jsonify(e)

    return make_response(response_json, 400, JSON_HEADER)


@monitoring_routes.errorhandler(InvalidOrderError)
def invalid_order_error(error):
    e = ErrorResponse('INVALID_ORDER', error.message)
    response_json = jsonify(e)

    return make_response(response_json, 400, JSON_HEADER)


@monitoring_routes.errorhandler(InvalidJSONError)
def invalid_json_error(error):
    e = ErrorResponse('INVALID_JSON', error.message)
    response_json = jsonify(e)

    return make_response(response_json, 400, JSON_HEADER)


@monitoring_routes.errorhandler(Exception)
def catch_all(error):
    log.exception(error)

    app_code, http_code = error.codes if hasattr(error, 'codes'
                                                 ) else ('UNKNOWN', 500)

    e = ErrorResponse(app_code, error.message)
    response_json = jsonify(e)

    return make_response(response_json, http_code, JSON_HEADER)
