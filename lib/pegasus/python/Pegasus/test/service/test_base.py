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
import unittest

from Pegasus.service.base import BaseOrderParser, InvalidOrderError


class OrderParserTestCase(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.ERROR)

    def test_valid_prefix(self):
        expressions = ['a.a', 'a1.a', 'a_.a']

        for e in expressions:
            r = BaseOrderParser(e).get_sort_order()
            self.assertEqual(r[0][0], e)

    def test_invalid_prefix(self):
        expressions = ['.', '.a', '1.a', '1a.a', '_.a', '_a.a', 'a-.a']

        for e in expressions:
            self.assertRaises(InvalidOrderError, BaseOrderParser, e)

    def test_valid_identifier(self):
        expressions = ['a', 'a1', 'a_', 'a.a', 'a.a1', 'a.a_']

        for e in expressions:
            r = BaseOrderParser(e).get_sort_order()
            self.assertEqual(r[0][0], e)

    def test_invalid_identifier(self):
        expressions = ['a.', 'a.0', 'a.0a', 'a._', 'a._a', 'a.a-']

        for e in expressions:
            self.assertRaises(InvalidOrderError, BaseOrderParser, e)
