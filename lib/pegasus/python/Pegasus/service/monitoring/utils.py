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

import json

import logging

from flask import g

from Pegasus.service.monitoring.serializer import PegasusServiceJSONEncoder

log = logging.getLogger(__name__)


def jsonify(obj, indent=5, separators=(',', ': '), cls=PegasusServiceJSONEncoder, **kwargs):
    if g.query_args.get('pretty_print', False):
        response_json = json.dumps(obj, indent=indent, separators=separators, cls=cls, **kwargs)
    else:
        response_json = json.dumps(obj, cls=cls, **kwargs)

    return response_json
