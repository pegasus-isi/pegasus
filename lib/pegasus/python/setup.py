import os
import subprocess
import sys

from setuptools import find_packages, setup

src_dir = os.path.dirname(__file__)
home_dir = os.path.abspath(os.path.join(src_dir, "../../.."))

install_requires = [
    "PyYAML",
    "Werkzeug==0.14.1",
    "Jinja2==2.8.1",
    "Flask-SQLAlchemy==2.3.2",
    "MarkupSafe==1.0",
    "itsdangerous==0.24",
    "boto==2.48.0",
    "pamela==1.0.0",
    "globus-sdk==1.4.1",
    "pika==1.1.0",
    "pika==1.1.0",
    # TODO: Remove pyOpenSSL?
    # "pyOpenSSL==17.5.0",
    # Python 2.6
    'Flask==0.12.4;python_version<="2.6"',
    'Flask-Cache==0.13.1;python_version<="2.6"',
    'requests==2.18.4;python_version<="2.6"',
    'ordereddict==1.1;python_version<="2.6"',
    'argparse==1.4.0;python_version<="2.6"',
    'sqlalchemy==1.1.15;python_version<="2.6"',
    # Python 2.7+
    'Flask==1.0.2;python_version>"2.6"',
    'Flask-Caching;python_version>"2.6"',
    'requests==2.21.0;python_version>"2.6"',
    'sqlalchemy==1.2.1;python_version>"2.6"',
    # Python 3 Backport
    'pathlib2;python_version<"3.0"',
    'functools32;python_version<"3.0"',
    # 'dataclasses;python_version=="3.6"',
]


excludes = ["Pegasus.test*"]


#
# Create Manifest file to exclude tests, and service files
#
def create_manifest_file():
    global excludes

    f = None
    try:
        f = open("MANIFEST.in", "w")
        f.write("recursive-exclude Pegasus/test *\n")

        if sys.version_info[1] <= 4:
            f.write("recursive-exclude Pegasus/service *\n")
            excludes.append("Pegasus.service*")

    finally:
        if f:
            f.close()


#
# Install conditional dependencies
#
def setup_installer_dependencies():
    global install_requires

    # if sys.version_info >= (3, 0):
    #    install_requires.append('future==0.16.0')

    # if sys.version_info[1] < 7:
    #    install_requires.append('ordereddict==1.1')
    #    install_requires.append('argparse==1.4.0')

    # if sys.version_info[1] <= 4:
    #    install_requires.append('SQLAlchemy==0.7.6')
    #    install_requires.append('pysqlite==2.6.0')

    # else:
    #    install_requires.append('SQLAlchemy==0.8.0')

    # if subprocess.call(["which", "pg_config"]) == 0:
    #    install_requires.append('psycopg2==2.6')

    if subprocess.call(["which", "mysql_config"]) == 0:
        install_requires.append('MySQL-Python;python_version<="2.6"')
        install_requires.append('mysqlclient;python_version>"2.6"')


#
# Utility function to read the pegasus Version.in file
#
def read_version():
    return (
        subprocess.Popen(
            "%s/release-tools/getversion" % home_dir, stdout=subprocess.PIPE, shell=True
        )
        .communicate()[0]
        .decode()
        .strip()
    )


#
# Utility function to read the README file.
#
def read(fname):
    return open(os.path.join(src_dir, fname)).read()


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
    return [path.replace(dirname, "") for path in items]


create_manifest_file()
setup_installer_dependencies()

setup(
    name="pegasus-wms",
    version=read_version(),
    author="Pegasus Team",
    author_email="pegasus@isi.edu",
    description="Pegasus Workflow Management System Python API",
    long_description=read("README"),
    license="Apache2",
    url="http://pegasus.isi.edu",
    python_requires=">=2.6,!=3.0.*,!=3.1.*,!=3.2.*,!=3.3.*",
    keywords=["scientific workflows"],
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Operating System :: Unix",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Topic :: Scientific/Engineering",
        "Topic :: Utilities",
        "License :: OSI Approved :: Apache Software License",
    ],
    packages=find_packages(exclude=excludes),
    package_data={"Pegasus.service": find_package_data("Pegasus/service/")},
    include_package_data=True,
    zip_safe=False,
    install_requires=install_requires,
    extras_require={"postgresql": ["psycopg2"], "mysql": []},
)
