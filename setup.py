import os
import sys
from setuptools import setup, find_packages

# Utility function to read the README file.
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

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
    package_data = {"" : ["templates/*", "static/*"] },
    include_package_data = True,
    zip_safe = False,
    entry_points = {
        'console_scripts': [
            'pegasus-service-server = pegasus.service.server:main',
            'pegasus-service-admin = pegasus.service.admin:main',
        ]
    },
    install_requires = [
        "pegasus-wms>=4.3.0",
        "werkzeug==0.9.3",
        "Flask==0.10",
        "Jinja2==2.7",
        "Flask-SQLAlchemy==0.16",
        "SQLAlchemy==0.8.0",
        "MySQL-python==1.2.4c1",
        "WTForms==1.0.3",
        "requests==1.1.0",
        "passlib==1.6.1",
        "MarkupSafe==0.18"
    ],
    test_suite = "pegasus.service.tests"
)

