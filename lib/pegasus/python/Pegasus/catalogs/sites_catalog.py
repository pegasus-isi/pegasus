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
import platform

from Pegasus.DAX3 import *

__author__ = 'Rafael Ferreira da Silva'


class OSType:
    LINUX = 'LINUX'
    SUNOS = 'SUNOS'
    AIX = 'AIX'
    MACOSX = 'MACOSX'
    WINDOWS = 'WINDOWS'


class DirectoryType:
    SHARED_SCRATCH = 'shared-scratch'
    SHARED_STORAGE = 'shared-storage'
    LOCAL_SCRATCH = 'local-scratch'
    LOCAL_STORAGE = 'local-storage'


class JobType:
    COMPUTE = 'compute'
    AUXILLARY = 'auxillary'
    TRANSFER = 'transfer'
    REGISTER = 'register'
    CLEANUP = 'cleanup'


class GridType:
    GT2 = 'gt2'
    GT4 = 'gt4'
    GT5 = 'gt5'
    CONDOR = 'condor'
    CREAM = 'cream'
    BATCH = 'batch'
    PBS = 'pbs'
    LSF = 'lsf'
    SGE = 'sge'
    NORDUGRID = 'nordugrid'
    UNICORE = 'unicore'
    EC2 = 'ec2'
    DELTACLOUD = 'deltacloud'


class SchedulerType:
    FORK = 'Fork'
    PBS = 'PBS'
    LSF = 'LSF'
    CONDOR = 'Condor'
    SGE = 'SGE'
    UNKNOWN = 'unknown'


class SitesCatalog:
    def __init__(self, workflow_dir, filename='sites.xml'):
        """
        Create a Pegasus site catalog.
        :param workflow_dir: Path to the workflow directory
        :param filename: sites catalog filename (default: sites.xml)
        """
        self.workflow_dir = workflow_dir
        self.filename = filename
        self._create_local_site()

    def add_site(self, handle, arch=Arch.X86_64, os=OSType.LINUX):
        """
        Add a site to the sites catalog
        :param handle: Site name
        :param arch: Site architecture (default: x86_64)
        :param os: Site OS (default: LINUX)
        """
        if not handle:
            raise Exception('A site handle should be provided.')
        if handle in self._sites:
            raise Exception('Site "%s" already exists.' % handle)

        self._sites.update(self._create_site(handle, arch, os))

    def add_site_profile(self, handle, namespace, key, value=''):
        """
        Add a profile to a specific site.
        :param handle: Site name
        :param namespace: Namespace values recognized by Pegasus
        :param key: Profile key
        :param value: Profile value (default: '')
        """
        if not handle or not namespace or not key:
            raise Exception(
                'A site handle, a namespace, and a key should be provided.'
            )

        if handle not in self._sites:
            raise Exception('There are no entries for site "%s".' % handle)

        profile = {'namespace': namespace, 'key': key, 'value': value}
        self._sites[handle]['profiles'].append(profile)

    def add_job_manager(self, handle, type, contact, scheduler, jobtype=None):
        """
        Add a job manager to a specific site.
        :param handle: Site name
        :param type: The universe name is actually the primary key for the jobmanager identification
        :param contact: The contact string is the secondary key for any job manager
        :param scheduler: Grid scheduler
        :param jobtype: Type of Jobs in the executable workflow the grid supports
        """
        if not handle or not type or not contact or not scheduler:
            raise Exception(
                'A site handle, and a jobmanager type, contact, and scheduler should be provided.'
            )

        if handle not in self._sites:
            raise Exception('There are no entries for site "%s".' % handle)

        grid = {'type': type, 'contact': contact, 'scheduler': scheduler}

        if jobtype:
            grid['jobtype'] = jobtype

        self._sites[handle]['grids'].append(grid)

    def write(self, force=False):
        """
        Write the sites catalog to a file.
        :param force: whether to overwrite the catalog file
        """
        sites_catalog_file = self.workflow_dir + '/' + self.filename

        if not os.path.isfile(sites_catalog_file) or force:
            with open(sites_catalog_file, 'w') as ppf:
                ppf.write('<?xml version="1.0" encoding="UTF-8"?>\n')
                ppf.write(
                    '<sitecatalog xmlns="http://pegasus.isi.edu/schema/sitecatalog" '
                    'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
                    'xsi:schemaLocation="http://pegasus.isi.edu/schema/sitecatalog '
                    'http://pegasus.isi.edu/schema/sc-4.1.xsd" version="4.1">\n'
                )

                # writing sites
                for handle in self._sites:
                    ppf.write(
                        '\t<site handle="%s" arch="%s" os="%s">\n' % (
                            handle, self._sites[handle]['arch'],
                            self._sites[handle]['os']
                        )
                    )

                    # directories
                    dirs = self._sites[handle]['directories']
                    for dir in dirs:
                        ppf.write(
                            '\t\t<directory type="%s" path="%s">\n' %
                            (dir, dirs[dir]['path'])
                        )
                        ppf.write(
                            '\t\t\t<file-server operation="all" url="file://%s"/>\n'
                            % dirs[dir]['path']
                        )
                        ppf.write('\t\t</directory>\n')

                    # grids
                    for grid in self._sites[handle]['grids']:
                        ppf.write(
                            '\t\t<grid type="%s" contact="%s" scheduler="%s" '
                            %
                            (grid['type'], grid['contact'], grid['scheduler'])
                        )

                        if 'jobtype' in grid:
                            ppf.write('jobtype="%s" ' % grid['jobtype'])
                        ppf.write('/>\n')

                    # site profiles
                    for p in self._sites[handle]['profiles']:
                        ppf.write(
                            '\t\t<profile namespace="%s" key="%s">%s</profile>\n'
                            % (p['namespace'], p['key'], p['value'])
                        )

                    ppf.write('\t</site>\n')
                ppf.write('</sitecatalog>\n')

        else:
            print(
                '\x1b[0;35mWARNING: Sites Catalog (%s) already exists. Use "force=True" '
                'to overwrite it.\n\x1b[0m' % sites_catalog_file
            )

    def _create_local_site(self):
        """
        Create a local site for the workflow
        """
        os = platform.system()
        if os.lower() == 'linux':
            os = OSType.LINUX
        elif os.lower() == 'windows':
            os = OSType.WINDOWS
        else:
            os = OSType.MACOSX

        # create local site
        self._sites = self._create_site('local', platform.machine(), os)
        self._sites['local']['directories'] = {
            DirectoryType.SHARED_SCRATCH:
                {
                    'path': self.workflow_dir + '/scratch'
                },
            DirectoryType.SHARED_STORAGE:
                {
                    'path': self.workflow_dir + '/output'
                }
        }

    def _create_site(self, handle, arch, os):
        """
        Create a general site.
        :param handle: Site name
        :param arch: Site architecture
        :param os: Site operational system
        :return: The dictionary object of the site
        """
        return {
            handle:
                {
                    'arch': arch,
                    'os': os,
                    'directories': {},
                    'grids': [],
                    'profiles': []
                }
        }
