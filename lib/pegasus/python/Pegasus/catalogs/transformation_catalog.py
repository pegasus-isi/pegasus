#!/usr/bin/env python
#
#  Copyright 2017-2018 University Of Southern California
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
        self._containers = []

    def add(self, executable):
        """
        Add an executable to the transformation catalog.
        :param executable: A DAX3 Executable object
        """
        if not executable:
            raise Exception('An executable should be provided.')

        if executable not in self._executables:
            self._executables.append(executable)

    def add_container(self, container):
        """
        Add a container to the transformation catalog.
        :param container: A DAX3 Container object
        """
        if not container:
            raise Exception('A container should be provided.')

        if container not in self._containers:
            self._containers.append(container)

    def write(self, force=False):
        """
        Write the catalog to a file.
        :param force: whether to overwrite the catalog file
        """
        catalog_file = self.workflow_dir + '/' + self.filename

        if not os.path.isfile(catalog_file) or force:
            with open(catalog_file, 'w') as ppf:
                # executables
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
                        ppf.write(
                            '\tprofile %s "%s" "%s"\n' %
                            (p.namespace, p.key, p.value)
                        )

                    # pfns
                    installed = 'INSTALLED'
                    if not e.installed:
                        installed = 'STAGEABLE'

                    for pfn in e.pfns:
                        ppf.write('\tsite %s {\n' % pfn.site)
                        # profiles
                        for p in pfn.profiles:
                            ppf.write(
                                '\t\tprofile %s "%s" "%s"\n' %
                                (p.namespace, p.key, p.value)
                            )

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

                        # reference to container
                        if e.container:
                            ppf.write('\t\tcontainer "%s"\n' %
                                      e.container.name
                            )

                        ppf.write('\t}\n')

                    ppf.write('}\n\n')

                # containers
                for c in self._containers:
                    ppf.write('cont %s {\n' % c.name)
                    ppf.write('\ttype "%s"\n' % c.type)
                    ppf.write('\timage "%s"\n' % c.image)

                    if c.imagesite:
                        ppf.write('\timage_site "%s"\n' % c.imagesite)

                    if c.dockerfile:
                        ppf.write('\tdockerfile "%s"\n' % c.dockerfile)

                    # mount
                    for m in c.mount:
                        ppf.write('\tmount "%s"\n' % m)

                    # profiles
                    for p in c.profiles:
                        ppf.write(
                            '\tprofile %s "%s" "%s"\n' %
                            (p.namespace, p.key, p.value)
                        )

                    ppf.write('}\n\n')

        else:
            print(
                '\x1b[0;35mWARNING: Transformation Catalog (%s) already exists. Use "force=True" '
                'to overwrite it.\n\x1b[0m' % catalog_file
            )
