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
from __future__ import print_function

import os

__author__ = 'Rafael Ferreira da Silva'


class ReplicaCatalog:
    def __init__(self, workflow_dir, filename='rc.txt'):
        """
        Create a Pegasus replica catalog.
        :param workflow_dir: Path to the workflow directory
        :param filename: catalog filename (default: rc.txt)
        """
        self.workflow_dir = workflow_dir
        self.filename = filename
        self._replicas = {}

    def add(self, name, path, site=None, metadata=None):
        """
        Add a replica to the catalog. 
        :param name: Replica file name
        :param path: Replica file path
        :param site: Site name where replica is available
        :param metadata: Additional metadata provided as a set (optional)
        """
        if not name or not path:
            raise Exception('A replica name and path should be provided.')

        if name not in self._replicas:
            self._replicas[name] = {path: []}

        if site:
            for m in self._replicas[name][path]:
                if m[0] == 'site':
                    self._replicas[name][path].remove(m)
                    break
            self._replicas[name][path].append(('site', site))

        if metadata:
            for md in metadata:
                for m in self._replicas[name][path]:
                    if m[0] == md.key:
                        self._replicas[name][path].remove(m)
                        break
                self._replicas[name][path].append((md.key, md.value))

    def write(self, force=False):
        """
        Write the catalog to a file.
        :param force: whether to overwrite the catalog file
        """
        catalog_file = self.workflow_dir + '/' + self.filename

        if not os.path.isfile(catalog_file) or force:
            with open(catalog_file, 'w') as ppf:
                for name in self._replicas:
                    for path in self._replicas[name]:
                        ppf.write('%s %s ' % (name, path))
                        for m in self._replicas[name][path]:
                            ppf.write('%s="%s" ' % (m[0], m[1]))
                        ppf.write('\n')

        else:
            print(
                '\x1b[0;35mWARNING: Replica Catalog (%s) already exists. Use "force=True" '
                'to overwrite it.\n\x1b[0m' % catalog_file
            )
