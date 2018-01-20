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

from Pegasus.catalogs.sites_catalog import *
from Pegasus.DAX3 import *


class TestSiteCatalog(unittest.TestCase):
    def test_sc(self):
        sc = SitesCatalog('/home/test')
        self.assertEqual('sites.xml', sc.filename)

        sc = SitesCatalog('/home/test', 'sc')
        self.assertEqual('sc', sc.filename)

    def test_sc_add_site(self):
        sc = SitesCatalog('/home/test')
        self.assertRaises(Exception, sc.add_site, None)
        self.assertEqual(len(sc._sites), 1)  # local site

        sc.add_site('my-site')
        self.assertRaises(Exception, sc.add_site, 'my-site')
        self.assertEqual(len(sc._sites), 2)

    def test_sc_add_site_profile(self):
        sc = SitesCatalog('/home/test')
        self.assertRaises(Exception, sc.add_site_profile, None, None, None)
        self.assertRaises(
            Exception, sc.add_site_profile, 'my-site', None, None
        )
        self.assertRaises(
            Exception, sc.add_site_profile, 'my-site', Namespace.ENV, None
        )
        self.assertRaises(
            Exception, sc.add_site_profile, 'my-site', Namespace.ENV, 'my-key'
        )

        sc.add_site('my-site')
        sc.add_site_profile('my-site', Namespace.ENV, 'my-key')
        self.assertEqual(len(sc._sites['my-site']['profiles']), 1)

    def test_sc_add_job_manager(self):
        sc = SitesCatalog('/home/test')
        self.assertRaises(
            Exception, sc.add_job_manager, None, None, None, None
        )
        self.assertRaises(
            Exception, sc.add_job_manager, 'my-site', None, None, None
        )
        self.assertRaises(
            Exception, sc.add_job_manager, 'my-site', GridType.GT2, None, None
        )
        self.assertRaises(
            Exception, sc.add_job_manager, 'my-site', GridType.GT2,
            'iz-login.isi.edu/jobmanager-pbs', None
        )
        self.assertRaises(
            Exception, sc.add_job_manager, 'my-site', GridType.GT2,
            'iz-login.isi.edu/jobmanager-pbs', SchedulerType.PBS
        )
        self.assertRaises(
            Exception, sc.add_job_manager, 'my-site', GridType.GT2,
            'iz-login.isi.edu/jobmanager-pbs', SchedulerType.PBS,
            JobType.COMPUTE
        )

        sc.add_site('my-site')
        sc.add_job_manager(
            'my-site', GridType.GT2, 'iz-login.isi.edu/jobmanager-pbs',
            SchedulerType.PBS, JobType.COMPUTE
        )

        self.assertEqual(len(sc._sites['my-site']['grids']), 1)


if __name__ == '__main__':
    unittest.main()
