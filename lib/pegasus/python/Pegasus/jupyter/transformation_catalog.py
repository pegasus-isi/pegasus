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

import os


class Type:
    INSTALLED = 'INSTALLED'
    STAGEABLE = 'STAGEABLE'


class TransformationCatalog:
    def __init__(self, workflow_dir, filename='tc.txt'):
        """
        Create a Pegasus transformation catalog.
        :param workflow_dir: Path to the workflow directory
        :param filename: catalog filename (default: rc.txt)
        """
        self.workflow_dir = workflow_dir
        self.filename = filename
        self._executables = []

    def add(self, executable):
        """
        Add an executable to the transformation catalog.
        :param executable: A DAX3 Executable object
        """
        if not executable:
            raise Exception('An executable should be provided.')

        self._executables.append(executable)

    def write(self, force=False):
        """
        Write the catalog to a file.
        :param force: whether to overwrite the catalog file
        """
        catalog_file = self.workflow_dir + '/' + self.filename

        if not os.path.isfile(catalog_file) or force:
            with open(catalog_file, 'w') as ppf:
                for e in self._executables:
                    # executable name
                    name = e.name
                    if e.namespace:
                        name = e.namespace + '::' + name
                    if e.version:
                        name = name + ':' + e.version
                    ppf.write('tr %s {\n' % name)

                    # profiles
                    for p in e.profiles:
                        ppf.write('\tprofile %s "%s" "%s"\n' % (p.namespace, p.key, p.value))

                    # pfns
                    installed = 'INSTALLED'
                    if not e.installed:
                        installed = 'STAGEABLE'

                    for pfn in e.pfns:
                        ppf.write('\tsite %s {\n' % pfn.site)
                        # profiles
                        for p in pfn.profiles:
                            ppf.write('\t\tprofile %s "%s" "%s"\n' % (p.namespace, p.key, p.value))

                        ppf.write('\t\tpfn "%s"\n' % pfn.url)
                        if e.arch:
                            ppf.write('\t\tarch "%s"\n' % e.arch)
                        if e.os:
                            ppf.write('\t\tos "%s"\n' % e.os)
                        if e.osrelease:
                            ppf.write('\t\tosrelease "%s"\n' % e.osrelease)
                        if e.osversion:
                            ppf.write('\t\tosversion "%s"\n' % e.osversion)
                        ppf.write('\t\ttype "%s"\n' % installed)
                        ppf.write('\t}\n')

                    ppf.write('}\n')

        else:
            print('\x1b[0;35mWARNING: Transformation Catalog (%s) already exists. Use "force=True" '
                  'to overwrite it.\n\x1b[0m' % catalog_file)
