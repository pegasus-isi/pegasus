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

import os
from setuptools import setup, find_packages

# Utility function to read the README file.
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

def find_package_data(dirname):
    def find_paths(dirname):
        items = []
        for fname in os.listdir(dirname):
            path = os.path.join(dirname, fname)
            if os.path.isdir(path):
                items += find_paths(path)
            elif not path.endswith(".py") and not path.endswith(".pyc"):
                items.append(path)
        return items
    items = find_paths(dirname)
    return [os.path.relpath(path, dirname) for path in items]

setup(
    name = "pegasus-service",
    version = "0.1",
    author = "Pegasus Team",
    author_email = "pegasus@isi.edu",
    description = "Pegasus as a Service",
    long_description = read("README.md"),
    license = "Apache2",
    url = "https://github.com/pegasus-isi/pegasus-service",
    classifiers = [
        "Development Status :: 3 - Alpha",
        "Topic :: Utilities",
        "License :: OSI Approved :: Apache Software License",
    ],
    packages = find_packages(),
    package_data = {"Pegasus.service" : find_package_data("Pegasus/service") },
    include_package_data = True,
    zip_safe = False,
    entry_points = {
        'console_scripts': [
            'pegasus-service-server = Pegasus.service.server:main',
            'pegasus-service-admin = Pegasus.service.admin:main',
            'pegasus-service-catalogs = Pegasus.service.catalogs:main',
            'pegasus-service-ensemble = Pegasus.service.ensembles:main'
        ]
    },
    install_requires = [
        "werkzeug==0.9.3",
        "Flask==0.10",
        "Jinja2==2.7",
        "Flask-SQLAlchemy==0.16",
        "Flask-Cache==0.13.1",
        "SQLAlchemy==0.8.0",
        "WTForms==1.0.3",
        "requests==1.2.3",
        "passlib==1.6.1",
        "MarkupSafe==0.18"
    ],
    test_suite = "Pegasus.service.tests"
)

