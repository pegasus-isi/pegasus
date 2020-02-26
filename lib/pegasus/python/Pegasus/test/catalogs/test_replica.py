#!/usr/bin/env python
#
#  Copyright 2017 University Of Southern California
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
#
__author__ = "Rafael Ferreira da Silva"

import unittest

from Pegasus.catalogs.replica_catalog import *
from Pegasus.DAX3 import *


class TestReplicaCatalog(unittest.TestCase):
    def test_rc(self):
        rc = ReplicaCatalog("/home/test")
        self.assertEqual("rc.txt", rc.filename)

        rc = ReplicaCatalog("/home/test", "rc")
        self.assertEqual("rc", rc.filename)

    def test_rc_add(self):
        rc = ReplicaCatalog("/home/test")
        self.assertRaises(Exception, rc.add)
        self.assertRaises(Exception, rc.add, "name")

        self.assertEqual(len(rc._replicas), 0)

        name = "myfile"
        path = "file://mypath"
        rc.add(name, path)
        self.assertEqual(len(rc._replicas), 1)
        self.assertEqual(len(rc._replicas[name][path]), 0)

        rc.add(name, path, site="site-A")
        self.assertEqual(len(rc._replicas), 1)
        self.assertEqual(len(rc._replicas[name][path]), 1)
        self.assertEqual(rc._replicas[name][path][0], ("site", "site-A"))

        meta_set = set()
        meta_set.add(Metadata("meta-name", "meta-content"))

        rc.add(name, path, site="site-A", metadata=meta_set)
        self.assertEqual(len(rc._replicas), 1)
        self.assertEqual(len(rc._replicas[name][path]), 2)
        self.assertEqual(rc._replicas[name][path][0], ("site", "site-A"))
        self.assertEqual(rc._replicas[name][path][1], ("meta-name", "meta-content"))

        rc.add(name, path, site="site-A", metadata=meta_set)
        self.assertEqual(len(rc._replicas[name][path]), 2)


if __name__ == "__main__":
    unittest.main()
