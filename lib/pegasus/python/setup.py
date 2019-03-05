import os
import sys
import subprocess
from setuptools import setup, find_packages

src_dir = os.path.dirname(__file__)
home_dir = os.path.abspath(os.path.join(src_dir, "../../.."))

install_requires = [
    "Werkzeug==0.14.1",
    "Flask==0.12.4",
    "Jinja2==2.8.1",
    "Flask-SQLAlchemy==0.16",
    "Flask-Cache==0.13.1",
    "requests==2.21.0",
    "MarkupSafe==1.0",
    "itsdangerous==0.24",
    "boto==2.48.0",
    "pam==0.1.4",
    "pyOpenSSL==17.5.0",
    "plex==2.0.0dev"
]

excludes = ['Pegasus.test*']


#
# Create Manifest file to exclude tests, and service files
#
def create_manifest_file():
    global excludes

    f = None
    try:
        f = open('MANIFEST.in', 'w')
        f.write('recursive-exclude Pegasus/test *\n')

        if sys.version_info[1] <= 4:
            f.write('recursive-exclude Pegasus/service *\n')
            excludes.append('Pegasus.service*')

    finally:
        if f:
            f.close()


#
# Install conditional dependencies
#
def setup_installer_dependencies():
    global install_requires

    if sys.version_info >= (3, 0):
        install_requires.append('future==0.16.0')

    if sys.version_info[1] < 7:
        install_requires.append('ordereddict==1.1')
        install_requires.append('argparse==1.4.0')

    if sys.version_info[1] <= 4:
        install_requires.append('SQLAlchemy==0.7.6')
        install_requires.append('pysqlite==2.6.0')

    else:
        install_requires.append('SQLAlchemy==0.8.0')

    if subprocess.call(["which", "pg_config"]) == 0:
        install_requires.append('psycopg2==2.6')

    if subprocess.call(["which", "mysql_config"]) == 0:
        install_requires.append('MySQL-python==1.2.5')


#
# Utility function to read the pegasus Version.in file
#
def read_version():
    return subprocess.Popen("%s/release-tools/getversion" % home_dir,
                            stdout=subprocess.PIPE, shell=True).communicate()[0].strip()


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
    keywords=["scientific workflows"],
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Operating System :: Unix",
        "Programming Language :: Python",
        "Topic :: Scientific/Engineering",
        "Topic :: Utilities",
        "License :: OSI Approved :: Apache Software License",
    ],
    packages=find_packages(exclude=excludes),
    package_data={"Pegasus.service": find_package_data("Pegasus/service/")},
    include_package_data=True,
    zip_safe=False,
    install_requires=install_requires
)

