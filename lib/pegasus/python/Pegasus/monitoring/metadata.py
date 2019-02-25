"""
This file implements the metadata related classes for pegasus-monitord.
"""
from __future__ import print_function

##
#  Copyright 2007-2012 University Of Southern California
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
##

# Import Python modules
import json
import logging
import tempfile

import os
from cStringIO import StringIO

__author__ = "Karan Vahi <vahi@isi.edu>"

logger = logging.getLogger(__name__)


class Metadata( object ):
    """
    Base class for for different types of metadata
    """

    def __init__(self):
        self._id = None
        self._type = None
        self._attributes = {} #dictionary indexed by attribute name and values



    def get_id(self):
        return self._id

    def set_id(self, id):
        self._id = id


    def add_attribute(self, key, value):
        self._attributes[key] = value

    def get_type(self):
        return self._type

    def get_attribute_value(self, key):
        return self._attributes.get(key)

    def get_attribute_keys(self):
        return self._attributes.keys()

    @staticmethod
    def write_to_jsonfile( metadata_list, directory  , name, prefix="pegasus-monitord"):
        try:
            temp_file = tempfile.NamedTemporaryFile( dir=directory, prefix=prefix , suffix=".meta", delete=False)
            jsonify(metadata_list, temp_file)
            logger.debug( "Written out metadata to %s", temp_file.name )
            os.chmod(temp_file.name, 0o644)
            # rename the file to the name to assure atomicity
            os.rename( temp_file.name, os.path.join(directory,name))

        except Exception as e:
            # Error sending this event... disable the sink from now on...
            logger.warning(" unable to write out  metadata to directory %s with name %s ", directory, name)
            logger.exception(e)
            return False

        return True


class FileMetadata( Metadata ):
    """
    Represents a single mkdir request
    """

    def __init__(self):
        super(FileMetadata, self).__init__()
        self._type = "file"

    def convert_to_rce(self):
        """
        Converts to a file based RC description
        :return:
        """

        rce = StringIO()
        rce.write(  self.get_id() )
        pfn = self.get_attribute_value( "pfn")
        if pfn is None:
            pfn = "@@PFN@@"
        if( pfn ):
            rce.write(" ")
            rce.write(pfn)

        for key in self.get_attribute_keys():
            rce.write(" ")
            rce.write(key)
            rce.write("=")
            rce.write("\"")
            rce.write(self.get_attribute_value(key))
            rce.write("\"")
        return rce.getvalue()



class MetadataCustomEncoder(json.JSONEncoder):

    def default( self, o ):
        if isinstance(o, Metadata):
            return o.__dict__
        return json.JSONEncoder.default(self, o)


def jsonify( *args , **kwargs):
    json.dump( *args, cls=MetadataCustomEncoder, indent=2, **kwargs )

def main():
    a = FileMetadata()
    a._id = "f.a"
    a.add_attribute( "size", "100")
    a.add_attribute( "checksum", "XXXXXX")

    b = FileMetadata()
    b._id = "f.b"
    b.add_attribute( "size", "10")
    b.add_attribute( "checksum", "XXXXXX")

    print(json.dumps(a.__dict__))

    l = []
    l.append( a )
    l.append( b )
    l.append( 1 )

    print(json.dumps(l, cls=MetadataCustomEncoder, indent=2))

    Metadata.write_to_jsonfile(l, "/tmp", "test")

if __name__ == "__main__":
    main()