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
__author__ = 'Rafael Ferreira da Silva'

import unittest

from Pegasus.catalogs.transformation_catalog import *
from Pegasus.DAX3 import *


class TestTransformationCatalog(unittest.TestCase):
    def test_tc(self):
        tc = TransformationCatalog('/home/test')
        self.assertEqual('tc.txt', tc.filename)

        tc = TransformationCatalog('/home/test', 'tc')
        self.assertEqual('tc', tc.filename)

    def test_tc_add_executable(self):
        tc = TransformationCatalog('/home/test')
        self.assertRaises(Exception, tc.add)
        self.assertEqual(len(tc._executables), 0)

        e = Executable('my-exec')
        tc.add(e)
        self.assertEqual(len(tc._executables), 1)
        self.assertEqual(tc._executables[0].name, e.name)

        tc.add(e)
        self.assertEqual(len(tc._executables), 1)

    def test_tc_add_container(self):
        tc = TransformationCatalog('/home/test')
        self.assertRaises(Exception, tc.add)
        self.assertEqual(len(tc._containers), 0)

        c = Container('cont-pegasus', ContainerType.DOCKER, 'docker:///rynge/montage:latest')
        tc.add_container(c)
        self.assertEqual(len(tc._containers), 1)
        self.assertEqual(tc._containers[0].name, c.name)

        tc.add_container(c)
        self.assertEqual(len(tc._containers), 1)


if __name__ == '__main__':
    unittest.main()
