#!/usr/bin/env python

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

from Pegasus.service.base import BaseQueryParser, InvalidQueryError


class RootWorkflowQueryParser(BaseQueryParser):
    FIELDS = set([
        'r.wf_id',
        'r.wf_uuid',
        'r.submit_hostname',
        'r.submit_dir',
        'r.planner_arguments',
        'r.planner_version',
        'r.user',
        'r.grid_dn',
        'r.dax_label',
        'r.dax_version',
        'r.dax_file',
        'r.dag_file_name',
        'r.timestamp'
    ])

    def __init__(self, expression):
        super(RootWorkflowQueryParser, self).__init__(expression)

    def identifier_handler(self, text):
        if text not in RootWorkflowQueryParser.FIELDS:
            raise InvalidQueryError('Invalid field %r' % text)

        super(RootWorkflowQueryParser, self).identifier_handler(text)

    #
    # Override Method Handler for Identifiers
    #
    BaseQueryParser.mapper[BaseQueryParser.IDENTIFIER] = identifier_handler


def main():
    constraint = RootWorkflowQueryParser("""
        id=1 OR lfn='a\\'b \\' d \\''
        AND ( pfn='b' or pfn ='cc' )
        AND site != NULL
        AND ( attr_name='x' AND attr_value likE 'y')
    """)

    print constraint


if __name__ == '__main__':
    main()
