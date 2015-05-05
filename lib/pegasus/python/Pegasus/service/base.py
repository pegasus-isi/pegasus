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

import re

import StringIO

from decimal import Decimal

from plex import Range, Lexicon, Rep, Rep1, Str, Any, IGNORE, Scanner, AnyBut, NoCase, Opt
from plex.errors import UnrecognizedInput


class BaseSerializer(object):
    """
    Base Serializer class provides a template used to serialize objects to/from JSON
    """

    def __init__(self, fields, pretty_print=False):
        self._fields = fields

        self._pretty_print = pretty_print

        if self._pretty_print is True:
            self._pretty_print_opts = {
                'indent': 4,
                'separators': (',', ': ')
            }
        else:
            self._pretty_print_opts = {}

    def encode_collection(self, records, records_total, records_filtered):
        raise NotImplementedError('Method not implemented')

    def encode_record(self, record):
        raise NotImplementedError('Method not implemented')

    def decode_collection(self, records):
        raise NotImplementedError('Method not implemented')

    def decode_record(self, record):
        raise NotImplementedError('Method not implemented')

    @staticmethod
    def _links(self, record):
        raise NotImplementedError('Method not implemented')

    @staticmethod
    def _get_field_value(record, field):
        value = getattr(record, field)
        return float(value) if isinstance(value, Decimal) else value


class ServiceError(Exception):
    pass


class InvalidQueryError(ServiceError):
    pass


class InvalidOrderError(ServiceError):
    pass


class BaseQueryParser(object):
    """
    Base Query Parser class provides a basic implementation to parse the `query` argument
    i.e. Basic WHERE clause as used in SQL.

    Note: The base class only provides a partial implementation of the SQL where clause.
    """

    def __init__(self, expression=None):
        self.expression = expression

        self._state = 0
        self._parenthesis_count = 0

        self._operator_stack = []
        self._postfix_result = []
        self._condition = [0, 0, 0]
        self._in_collection = None
        self._identifiers = set()

        self._scanner = None
        self._lexicon = None
        self._mapper = None

        self._init()

        self.parse_expression(expression)

    def _init(self):
        comma = Str(',')
        whitespace = Rep1(Any(' \t\n'))
        open_parenthesis = Str('(')
        close_parenthesis = Str(')')

        letter = Range('AZaz')
        digit = Range('09')
        prefix = letter + Rep(letter) + Str('.')

        identifier = Opt(prefix) + letter + Rep(letter | digit | Str('_'))
        comparators = NoCase(Str('=', '!=', '<', '<=', '>', '>=', 'like', 'in'))
        string_literal = Str('\'') + Rep(AnyBut('\'') | Str(' ') | Str("\\'")) + Str('\'')
        integer_literal = Opt(Str('+', '-')) + Rep1(digit)
        float_literal = Opt(Str('+', '-')) + Rep1(digit) + Str('.') + Rep1(digit)

        operands = NoCase(Str('AND', 'OR'))
        null = Str('NULL')

        COMMA = 1
        OPEN_PARENTHESIS = 2
        CLOSE_PARENTHESIS = 3
        NULL = 4
        OPERAND = 5
        COMPARATOR = 6
        STRING_LITERAL = 7
        INTEGER_LITERAL = 8
        FLOAT_LITERAL = 9
        IDENTIFIER = 10

        self._lexicon = Lexicon([
            (whitespace, IGNORE),
            (comma, COMMA),
            (open_parenthesis, OPEN_PARENTHESIS),
            (close_parenthesis, CLOSE_PARENTHESIS),
            (null, NULL),
            (operands, OPERAND),
            (comparators, COMPARATOR),
            (string_literal, STRING_LITERAL),
            (integer_literal, INTEGER_LITERAL),
            (float_literal, FLOAT_LITERAL),
            (identifier, IDENTIFIER)
        ])

        self._mapper = {
            COMMA: self.comma_handler,
            OPEN_PARENTHESIS: self.open_parenthesis_handler,
            CLOSE_PARENTHESIS: self.close_parenthesis_handler,
            NULL: self.null_handler,
            OPERAND: self.operand_handler,
            COMPARATOR: self.comparator_handler,
            STRING_LITERAL: self.string_literal_handler,
            INTEGER_LITERAL: self.integer_literal_handler,
            FLOAT_LITERAL: self.float_literal_handler,
            IDENTIFIER: self.identifier_handler,
        }

    @property
    def identifiers(self):
        return self._identifiers

    def parse_expression(self, expression):
        """
        Uses postfix evaluation of filter string.
        http://www.sunshine2k.de/coding/java/SimpleParser/SimpleParser.html
        """
        try:
            f = StringIO.StringIO(expression)
            self._scanner = Scanner(self._lexicon, f, 'query')

            while 1:
                token = self._scanner.read()

                if token[0] in self._mapper:
                    self._mapper[token[0]](token[1])

                elif token[0] is None:
                    break

            if self._parenthesis_count != 0:
                raise InvalidQueryError('Invalid parenthesis count')

            while len(self._operator_stack) > 0:
                self._postfix_result.append(self._operator_stack.pop())

        except UnrecognizedInput as e:
            raise InvalidQueryError(str(e))

        finally:
            f.close()

    def comma_handler(self, text):
        if self._state == 3:
            return

        file, line, char_pos = self._scanner.position()
        msg = 'Invalid token: Line: %d Char: %d' % (line, char_pos)
        raise InvalidQueryError(msg)

    def open_parenthesis_handler(self, text):
        if self._state == 3:
            self._in_collection = []
            return

        self._parenthesis_count += 1
        self._operator_stack.append('(')

    def close_parenthesis_handler(self, text):
        if self._state == 3:
            self._condition[2] = self._in_collection
            self._postfix_result.append(tuple(self._condition))
            self._state = 0
            return

        if self._parenthesis_count <= 0:
            file, line, char_pos = self._scanner.position()
            msg = 'Invalid parenthesis order: Line: %d Char: %d' % (line, char_pos)
            raise InvalidQueryError(msg)
        else:
            self._parenthesis_count -= 1

        while len(self._operator_stack) > 0:
            operator = self._operator_stack.pop()

            if operator == '(':
                break
            else:
                self._postfix_result.append(operator)

    def null_handler(self, text):
        if self._state == 2:
            self._condition[2] = None
            self._postfix_result.append(tuple(self._condition))
            self._state = 0
        else:
            file, line, char_pos = self._scanner.position()
            msg = 'NULL found out of order: Line: %d Char: %d' % (line, char_pos)
            raise InvalidQueryError(msg)

    def operand_handler(self, text):
        self._operator_stack.append(text.upper())

    def comparator_handler(self, text):
        if self._state == 1:
            self._condition[1] = text.upper()
            self._state = 3 if self._condition[1] == 'IN' else 2
        else:
            file, line, char_pos = self._scanner.position()
            msg = 'Comparator %r found out of order: Line: %d Char: %d' % (text, line, char_pos)
            raise InvalidQueryError(msg)

    def string_literal_handler(self, text):
        if self._state == 2:
            self._condition[2] = text.strip("'")
            self._postfix_result.append(tuple(self._condition))
            self._state = 0
        elif self._state == 3:
            self._in_collection.append(text.strip("'"))
        else:
            file, line, char_pos = self._scanner.position()
            msg = 'String literal %r found out of order: Line: %d Char: %d' % (text, line, char_pos)
            raise InvalidQueryError(msg)

    def integer_literal_handler(self, text):
        if self._state == 2:
            self._condition[2] = text.strip()
            self._postfix_result.append(tuple(self._condition))
            self._state = 0
        elif self._state == 3:
            self._in_collection.append(text.strip())
        else:
            file, line, char_pos = self._scanner.position()
            msg = 'Integer literal %s found out of order: Line: %d Char: %d' % (text, line, char_pos)
            raise InvalidQueryError(msg)

    def float_literal_handler(self, text):
        if self._state == 2:
            self._condition[2] = text.strip()
            self._postfix_result.append(tuple(self._condition))
            self._state = 0
        elif self._state == 3:
            self._in_collection.append(text.strip())
        else:
            file, line, char_pos = self._scanner.position()
            msg = 'Integer literal %d found out of order: Line: %d Char: %d' % (text, line, char_pos)
            raise InvalidQueryError(msg)

    def identifier_handler(self, text):
        if self._state == 0:
            self._condition[0] = text
            self._state = 1
            self._identifiers.add(text)
        else:
            file, line, char_pos = self._scanner.position()
            msg = 'Field %r found out of order: Line: %d Char: %d' % (text, line, char_pos)
            raise InvalidQueryError(msg)

    def evaluate(self):
        return self._postfix_result

    def clear(self):
        self._state = 0
        self._parenthesis_count = 0

        self._operator_stack = []
        self._postfix_result = []
        self._condition = [0, 0, 0]
        self._identifiers = set()

        self._scanner = None

    def __str__(self):
        comparator = {
            '=': '__eq__',
            '!=': '__ne__',
            '<': '__lt__',
            '<=': '__le__',
            '>': '__gt__',
            '>=': '__ge__',
            'LIKE': 'like',
            'IN': 'in_'
        }

        operators = {
            'AND': 'and_',
            'OR': 'or_'
        }

        operands = []

        def condition_expansion(expr, field):
            operands.append('%s.%s ( %s )' % (field, comparator[expr[1]], expr[2]))

        for token in self._postfix_result:
            if isinstance(token, tuple):
                identifier = token[0]
                condition_expansion(token, identifier)

            elif isinstance(token, str) or isinstance(token, unicode):
                operand_2 = operands.pop()
                operand_1 = operands.pop()

                if token in operators:
                    operands.append('%s ( %s, %s )' % (operators[token], operand_1, operand_2))

        return operands.pop()


class BaseOrderParser(object):
    """
    Base Order Parser class provides a basic implementation to parse the `order` argument
    i.e. ORDER clause as used in SQL.
    """
    #
    # Order Clause Identifier Rules:
    #   Prefix + Identifier
    #
    # Prefix Rules:
    #   Is optional
    #   Must start with a-z OR A-Z
    #   Can contain a-z OR A-Z OR 0-9 OR _
    #   Must end with .
    #
    # Identifier Rules:
    #   Is mandatory
    #   Must start with a-z OR A-Z
    #   Can contain a-z OR A-Z OR 0-9 OR _
    #
    IDENTIFIER_PATTERN = re.compile('^([a-zA-Z][a-zA-Z0-9_]*\.)?([a-zA-Z][a-zA-Z0-9_]*)$')

    def __init__(self, expression):
        self.expression = expression
        self._sort_order = []

        try:
            self._parse_expression()
        except UnrecognizedInput, e:
            raise InvalidOrderError(str(e))

    def _parse_expression(self):
        tokens = self.expression.replace('\n\t', ' ').split(',')
        tokens = [token.split() for token in tokens if len(token) > 0]
        tokens = [token for token in tokens if len(token) > 0]

        for token in tokens:
            length = len(token)

            if length == 0 or length > 2:
                raise InvalidOrderError('Invalid ORDER clause %r' % ' '.join(token))

            self.identifier_handler(token[0])

            if length == 2:
                self.order_handler(token[1])

        self._sort_order = [tuple(order_clause) for order_clause in self._sort_order]

    def identifier_handler(self, identifier):
        match = BaseOrderParser.IDENTIFIER_PATTERN.match(identifier)

        if match:
            self._sort_order.append([identifier, 'ASC'])
        else:
            raise InvalidOrderError('Invalid identifier %r' % identifier)

    def order_handler(self, order):
        _order = order.upper()

        if _order in set(('ASC', 'DESC')):
            self._sort_order[len(self._sort_order) - 1][1] = order.upper()
        else:
            raise InvalidOrderError('Invalid sorting order %r' % order)

    def get_sort_order(self):
        return self._sort_order

    def __str__(self):
        s = StringIO.StringIO()

        for i in self._sort_order:
            s.write(str(i))
            s.write(' ')

        out = s.getvalue()
        s.close()

        return out


class BaseResource(object):
    """
    Purpose of Resource is to centralize field definitions in one place, and to aid in Query, Order Parsing and
    Query, Order evaluation
    """

    def __init__(self, alias=None):
        self._prefix = None
        self._resource = alias if alias else None
        self._fields = None
        self._prefixed_fields = None

    @property
    def prefix(self):
        return self._prefix

    @property
    def fields(self):
        return self._fields

    @property
    def prefixed_fields(self):
        if self._prefixed_fields is None:
            self._prefixed_fields = set([field for field in self.fields])
            self._prefixed_fields |= set(['%s.%s' % (self.prefix, field) for field in self.fields])

        return self._prefixed_fields

    def mapped_fields(self, alias=None):
        mapped_fields = {}
        for field in self.prefixed_fields:
            mapped_fields[field] = self.get_mapped_field(field, alias)

        return mapped_fields

    def get_mapped_field(self, field, alias=None):
        resource = alias if alias else self._resource
        suffix = self._get_suffix(field)

        return getattr(resource, suffix)

    def is_field_valid(self, field):
        return field in self.prefixed_fields

    @staticmethod
    def _split_identifier(identifier):
        return identifier.split('.', 1)

    @staticmethod
    def _get_prefix(field):
        return BaseResource._split_identifier(field)[0]

    @staticmethod
    def _get_suffix(field):
        splits = BaseResource._split_identifier(field)
        return splits[0] if len(splits) == 1 else splits[1]
