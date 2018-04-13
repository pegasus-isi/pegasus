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
import subprocess
import sys
import time
from datetime import datetime

from Pegasus.catalogs.replica_catalog import *
from Pegasus.catalogs.sites_catalog import *
from Pegasus.catalogs.transformation_catalog import *
from Pegasus.DAX3 import *
from Pegasus.init import *

from future import standard_library
standard_library.install_aliases()

__author__ = 'Rafael Ferreira da Silva'


class Cleanup:
    """
    """
    LEAF = 'leaf'
    INPLACE = 'inplace'
    CONSTRAINT = 'constraint'


class Instance:
    def __init__(
        self,
        dax=None,
        sites_catalog=None,
        replica_catalog=None,
        transformation_catalog=None,
        workflow_dir=None,
        input_dir=None
    ):
        """
        Create an object of the Instance class to run a Pegasus workflow.
        :param dax: A Pegasus DAX3 object
        :param sites_catalog: A Pegasus sites catalog
        :param replica_catalog: A Pegasus replica catalog
        :param transformation_catalog: A Pegasus transformation catalog
        :param workflow_dir: A path to the workflow directory
        :param input_dir: A path to the inputs directory
        """
        self.dax = dax
        self.base_dir = workflow_dir
        self.input_dir = input_dir
        self.submit_dir = None
        self.output_dir = None
        self.wf_image_abs = None
        self.wf_image_exe = None

        # private members
        self._is_tutorial = False
        self._submit = False
        self._sites_catalog = sites_catalog
        self._replica_catalog = replica_catalog
        self._transformation_catalog = transformation_catalog
        self._properties = {
            # basic pegasus properties
            'pegasus.data.configuration': 'condorio'
        }

    def tutorial(
        self,
        env=TutorialEnv.LOCAL_MACHINE,
        example=TutorialExample.SPLIT,
        workflow_dir=None
    ):
        """
        Generate a Pegasus tutorial workflow.
        :param env: Execution environment (e.g., TutorialEnv.LOCAL_MACHINE)
        :param example: Example tutorial worklfow (e.g., TutorialExample.SPLIT)
        :param workflow_dir: Name of the folder where the workflow will be generated 
        """
        if not env:
            raise Exception(
                'An environment option should be provided (e.g., TutorialEnv.LOCAL_MACHINE).'
            )
        if not example:
            raise Exception(
                'A tutorial workflow should be provided (e.g., TutorialExample.SPLIT).'
            )

        shared_dir = None
        try:
            out = subprocess.getoutput('pegasus-config --python-dump')
            for line in out.split('\n'):
                if 'pegasus_share_dir' in line:
                    pegasus_shared_dir = line.split('=')[1].strip()[1:-1]
                    shared_dir = os.path.join(pegasus_shared_dir, 'init')
                    break

        except subprocess.CalledProcessError as grepexc:
            print("error code", grepexc.returncode, grepexc.output)

        # generate workflow folder
        if not workflow_dir:
            d = datetime.now()
            workflow_dir = '-'.join(
                [example[1], env[1],
                 d.replace(microsecond=0).isoformat()]
            )
        workflow_dir = os.path.abspath(workflow_dir)

        self.base_dir = workflow_dir
        self.workflow = Workflow(workflow_dir, shared_dir)
        self.workflow.config = "tutorial"
        self.workflow.daxgen = "tutorial"
        self.workflow.tutorial_setup = env[1]
        self.workflow.tutorial = example[1]
        self.workflow.generate_tutorial = True

        # checks
        if example == TutorialExample.DIAMOND and env != TutorialEnv.OSG_FROM_ISI:
            raise Exception(
                'The "diamond" workflow can only run on OSG sites.'
            )
        if example == TutorialExample.MPI and env != TutorialEnv.BLUEWATERS_GLITE:
            raise Exception(
                'The "MPI Hello World" workflow can only run on Bluewaters.'
            )

        # setup tutorial and generate folder
        self.workflow.setup_tutorial()
        self.workflow.generate()

        # generate DAX
        out, err = subprocess.Popen(
            './generate_dax.sh %s.dax' % self.workflow.tutorial,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True,
            cwd=self.base_dir
        ).communicate()
        if err:
            raise Exception(err)

        self.dax = ADAG(self.workflow.tutorial)
        self._is_tutorial = True

    def set_property(self, key, value):
        """
        Add a property to the Pegasus properties file.
        :param key: Property key
        :param value: Property value
        """
        if not key:
            raise Exception('A key should be provided.')
        if not value:
            raise Exception('A value should be provided.')
        self._properties[key] = value

    def run(
        self, submit=True, cleanup=Cleanup.INPLACE, site='local', force=False
    ):
        """
        The main method, which is used to run a Pegasus workflow.
        :param submit: Plan and submit the executable workflow generated (default: True)
        :param cleanup: The cleanup strategy to use (default: Cleanup.INPLACE)
        :param site: The sitename of the workflow 
        :param force: Skip reduction of the workflow, resulting in build style dag (default: False)
        """
        if not self._is_tutorial and (
            not self.dax or not isinstance(self.dax, ADAG)
        ):
            raise Exception('Invalid DAX object')

        if not self.base_dir:
            self.base_dir = os.path.abspath('./' + self.dax.name)
        if not os.path.exists(self.base_dir):
            os.makedirs(self.base_dir)
        if not self.input_dir:
            self.input_dir = self.base_dir + '/input'
            if not os.path.exists(self.input_dir):
                os.makedirs(self.input_dir)

        self._submit = submit
        properties_file = self.base_dir + '/pegasus.properties'
        submit_dir = self.base_dir + '/submit'
        self.output_dir = self.base_dir + '/output'
        dax_name = self.base_dir + '/' + self.dax.name + '.dax'

        if not self._is_tutorial:
            # write the sites catalog
            if not self._sites_catalog:
                self._sites_catalog = SitesCatalog(self.base_dir)
            self._sites_catalog.write(force=force)

            # write the replica catalog
            if not self._replica_catalog:
                self._replica_catalog = ReplicaCatalog(self.base_dir)
            self._replica_catalog.write(force=force)

            # write the transformation catalog
            if not self._transformation_catalog:
                self._transformation_catalog = TransformationCatalog(
                    self.base_dir
                )
            self._transformation_catalog.write(force=force)

            # write properties file
            self.set_property(
                'pegasus.catalog.site.file', self._sites_catalog.filename
            )
            self.set_property('pegasus.catalog.replica', 'File')
            self.set_property(
                'pegasus.catalog.replica.file', self._replica_catalog.filename
            )
            self.set_property('pegasus.catalog.transformation', 'Text')
            self.set_property(
                'pegasus.catalog.transformation.file',
                self._transformation_catalog.filename
            )
            self.set_property('pegasus.metrics.app', self.dax.name)

            with open(properties_file, 'w') as ppf:
                for key in self._properties:
                    ppf.write('%s=%s\n' % (key, self._properties[key]))

            # write DAX file
            f = open(dax_name, 'w')
            self.dax.writeXML(f)
            f.close()

        # prepare for submission
        cmd = [
            'pegasus-plan', '--conf', properties_file, '--dax', dax_name,
            '--dir', submit_dir, '--input-dir', self.input_dir, '--output-dir',
            self.output_dir, '--sites', site
        ]

        if cleanup:
            cmd.append('--cleanup %s' % cleanup)
        else:
            cmd.append('--nocleanup')
        if force:
            cmd.append('--force')
        if submit:
            cmd.append('--submit')

        # plan the workflow
        out, err = subprocess.Popen(
            ' '.join(cmd),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True,
            cwd=self.base_dir
        ).communicate()
        if err:
            raise Exception(err)

        for line in out.decode('utf8').split('\n'):
            if 'pegasus-run' in line:
                self.submit_dir = line.split('pegasus-run')[1].strip()
                print('The pegasus workflow has been successfully planned.\n' \
                      'Please, use the ```submit()``` method to start the workflow execution.\n\n'
                      '\x1b[1;34mPegasus submit dir: %s\x1b[0m' % self.submit_dir)

                break
            elif 'pegasus-status -l' in line:
                self.submit_dir = line.split('pegasus-status -l')[1
                                                                  ].strip()
                print('The pegasus workflow has been successfully planned and started to run.\n' \
                      'Please, use the status() method to follow the progress of the workflow execution.\n\n'
                      '\x1b[1;34mPegasus submit dir: %s\x1b[0m' % self.submit_dir)
                break

    def submit(self):
        """
        Run the workflow in case it has only been planned. 
        """
        if self._submit:
            raise Exception('The workfow execution has already been started.')

        out, err = subprocess.Popen(
            'pegasus-run %s' % self.submit_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True,
            cwd=self.base_dir
        ).communicate()
        if err:
            raise Exception(err)

        self._submit = True
        print(
            'The pegasus workflow has started its execution.\n'
            'Please, use the status() method to follow the progress of the workflow execution.'
        )

    def status(self, loop=False, delay=10):
        """
        Monitor the workflow status.
        :param loop: Whether to query the workflow status within a loop until it is completed or failed (default: False)
        :param delay: Delay in seconds to query the workflow status (default: 10 seconds)
        """
        if not self._submit:
            raise Exception(
                'The workfow has not started its execution yet.\n'
                'Please, check if the workflow is planned and submitted for execution.'
            )
        seq = False

        while True:
            out, err = subprocess.Popen(
                'pegasus-status -l %s' % self.submit_dir,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                shell=True,
                cwd=self.base_dir
            ).communicate()
            if err:
                raise Exception(err)

            for line in out.decode('utf8').split('\n'):
                if 'UNRDY' in line:
                    seq = True
                elif seq:
                    seq = False
                    v = line.split()

                    state = v[8]
                    if state == 'Success':
                        state = '\x1b[1;32m' + state + '\x1b[0m'
                    elif state == 'Failure':
                        state = '\x1b[1;31m' + state + '\x1b[0m'

                    progress = '\x1b[1;34m' + 'Progress: ' + v[
                        7
                    ] + '%\x1b[0m (' + state + ')'
                    completed = '\x1b[1;32mCompleted: ' + v[5] + '\x1b[0m'
                    queued = '\x1b[1;33mQueued: ' + v[1] + '\x1b[0m'
                    running = '\x1b[1;36mRunning: ' + v[3] + '\x1b[0m'
                    fail = '\x1b[1;31mFailed: ' + v[6] + '\x1b[0m'

                    st = progress + '\t(' + completed + ', ' + queued + ', ' + running + ', ' + fail + ')'
                    print('%s\r' % st, end='')
                    break

            if not loop or 'Success' in out.decode(
                'utf8'
            ) or 'Failure' in out.decode('utf8'):
                break
            time.sleep(delay)

    def statistics(
        self, workflow=False, jobs=False, breakdown=False, time=False
    ):
        """
        Print the workflow statistics.
        :param workflow:
        :param jobs:
        :param breakdown:
        :param time:
        """
        if not self._submit:
            raise Exception(
                'The workfow has not started its execution yet.\n'
                'Please, check if the workflow is planned and submitted for execution.'
            )

        out, err = subprocess.Popen(
            'pegasus-statistics -s all %s' % self.submit_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True,
            cwd=self.base_dir
        ).communicate()

        if err:
            raise Exception(err)

        for line in out.decode('utf8').split('\n'):
            if line.startswith('Workflow wall time'):
                v = line.split(':')
                print(
                    'Workflow Wall Time: \x1b[1;34m' + v[1].strip() + '\x1b[0m'
                )
                break

    def outputs(self):
        """
        Print a list of output files.
        """
        if not self.output_dir:
            raise Exception('No output directory is configured.')

        outputs = [
            os.path.join(root, name)
            for root, dirs, files in os.walk(self.output_dir) for name in files
        ]

        for f in outputs:
            print(f.replace(self.output_dir + '/', ''))

    def inspect(self, path):
        """
        Inspect an output file.
        :param path: Path to output file.
        """
        if not path:
            raise Exception(
                'A path to an output file should be provided. '
                'Use outputs() to obtain a list of output files.'
            )

        with open(self.output_dir + '/' + path) as file:
            for line in file:
                print(line)

    def view(self, abstract=True, force=False):
        """
        Generate a PNG image of the worklfow.
        :param abstract: Whether to generate the abstract or executable workflow
        :param force: Force image generation (even though it has been generated before)
        :return: Path to the workflow image
        """
        if abstract and self.wf_image_abs and not force:
            return self.wf_image_abs

        if not abstract and self.wf_image_exe and not force:
            return self.wf_image_exe

        if not self._is_tutorial and (
            not self.dax or not isinstance(self.dax, ADAG)
        ):
            raise Exception('Invalid DAX object')

        if abstract:
            basename = self.base_dir + '/' + self.dax.name + '.dax'
        else:
            if not self.submit_dir:
                raise Exception(
                    'The workfow has not started its execution yet.\n'
                    'Please, check if the workflow is planned and submitted for execution.'
                )
            basename = self.submit_dir + '/' + self.dax.name + '-0.dag'

        sp = subprocess.Popen(
            'pegasus-graphviz -o %s.dot %s' % (basename, basename),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True,
            cwd=self.base_dir
        )
        out, err = sp.communicate()

        if sp.returncode != 0 and err:
            raise Exception(err)

        out, err = subprocess.Popen(
            'dot -Tpng %s.dot -o %s.png' % (basename, basename),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True,
            cwd=self.base_dir
        ).communicate()
        if err:
            raise Exception(err)

        if abstract:
            self.wf_image_abs = '%s.png' % basename
            return self.wf_image_abs
        else:
            self.wf_image_exe = '%s.png' % basename
            return self.wf_image_exe
