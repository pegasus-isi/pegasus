import os
import subprocess

# need the externals for the correct version of setuptools
import sys
sys.path.insert(0, '../../lib/pegasus/externals/python')

from setuptools import find_packages, setup

src_dir = os.path.dirname(__file__)
home_dir = os.path.abspath(os.path.join(src_dir, "../.."))

install_requires = [
    "six>=1.9.0",
]


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


setup(
    name="pegasus-wms.dax",
    version=read_version(),
    author="Pegasus Team",
    author_email="pegasus@isi.edu",
    description="Pegasus Workflow Management System Python API",
    long_description=read("README.md"),
    long_description_content_type="text/markdown",
    license="Apache2",
    url="http://pegasus.isi.edu",
    python_requires=">=2.6,!=3.0.*,!=3.1.*,!=3.2.*,!=3.3.*,!=3.4.*",
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
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Topic :: Scientific/Engineering",
        "Topic :: Utilities",
        "License :: OSI Approved :: Apache Software License",
    ],
    namespace_packages=["Pegasus"],
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    include_package_data=True,
    zip_safe=False,
    install_requires=install_requires,
)
