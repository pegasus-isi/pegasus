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

from collections import OrderedDict

from werkzeug.routing import BaseConverter


class PagedResponse:
    def __init__(self, records, total, filtered):
        self._records = records
        self._total = total
        self._filtered = filtered

    @property
    def records(self):
        return self._records

    @property
    def total_records(self):
        return self._total

    @property
    def total_filtered(self):
        return self._filtered


class ErrorResponse:
    def __init__(self, code, message, errors=None):
        self._code = code
        self._message = message
        self._errors = errors

    @property
    def code(self):
        return self._code

    @code.setter
    def code(self, code):
        self._code = code

    @property
    def message(self):
        return self._message

    @message.setter
    def message(self, message):
        self._message = message

    @property
    def errors(self):
        return self._errors

    @errors.setter
    def errors(self, errors):
        self._errors = errors


class ServiceError(Exception):
    pass


class InvalidJSONError(Exception):
    pass


class BaseResource:
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
            self._prefixed_fields = {field for field in self.fields}
            self._prefixed_fields |= {
                "{}.{}".format(self.prefix, field) for field in self.fields
            }

        return self._prefixed_fields

    def mapped_fields(self, alias=None):
        mapped_fields = {}
        for field in self.prefixed_fields:
            mapped_fields[field] = self.get_mapped_field(field, alias)

        return mapped_fields

    def get_mapped_field(self, field, alias=None, ignore_prefix=False):
        resource = alias if alias else self._resource
        suffix = self._split_identifier(field)
        if len(suffix) == 2:
            suffix = (
                field
                if ignore_prefix is False and suffix[0] != self.prefix
                else suffix[1]
            )

        else:
            suffix = suffix[0]

        return getattr(resource, suffix)

    def is_field_valid(self, field):
        return field in self.prefixed_fields

    @staticmethod
    def _split_identifier(identifier):
        return identifier.split(".", 1)

    @staticmethod
    def _get_prefix(field):
        return BaseResource._split_identifier(field)[0]

    @staticmethod
    def _get_suffix(field):
        splits = BaseResource._split_identifier(field)
        return splits[0] if len(splits) == 1 else splits[1]


class BooleanConverter(BaseConverter):
    def to_python(self, value):
        value = value.strip().lower()

        if value in {"1", "0", "true", "false"}:
            return bool(value)

        else:
            raise ServiceError("Expecting boolean found %s" % value)

    def to_url(self, value):
        return "true" if value else "false"


class OrderedSet(set):
    def __init__(self, *args):
        self.__data = OrderedDict()

        if args:
            for arg in args:
                self.add(arg)

    def add(self, element):
        """
        Add an element to a set.

        This has no effect if the element is already present.
        """
        self.__data[element] = True

    def clear(self):
        """ Remove all elements from this set. """
        self.__data.clear()

    def values(self):
        "od.items() -> list of (key, value) pairs in od"
        return [key for key in self.__data]

    def remove(self, element):
        """
        Remove an element from a set; it must be a member.

        If the element is not a member, raise a KeyError.
        """
        del self.__data[element]

    def __contains__(self, element):
        """ x.__contains__(y) <==> y in x. """
        return element in self.__data

    def __iter__(self):
        """ x.__iter__() <==> iter(x) """
        yield from self.__data

    def __len__(self):
        """ x.__len__() <==> len(x) """
        return len(self.__data)
