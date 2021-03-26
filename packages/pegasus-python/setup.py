import os
import subprocess

from setuptools import setup

src_dir = os.path.dirname(__file__)
home_dir = os.path.abspath(os.path.join(src_dir, "../.."))

install_requires = [
    # Utils
    # DAX/Workflow
    "PyYAML>5.3,<5.5",
    # pegasus-init
    "GitPython>1.0,<3.2",
    "pamela>=1.0,<1.1",
    "pika==1.1.0",
    "Flask>1.1,<1.2",
    "Flask-Caching>1.8,<1.11",
    "requests>2.23,<2.26",
    "sqlalchemy>1.3,<1.4",
    "pegasus-wms.api>=5.0,<5.1",
    "pegasus-wms.dax>=5.0,<5.1",
    "pegasus-wms.common>=5.0,<5.1",
    "pegasus-wms.worker>=5.0,<5.1",
]

#
# Install conditional dependencies
#
def setup_installer_dependencies():
    global install_requires

    if subprocess.call(["which", "mysql_config"]) == 0:
        install_requires.append("mysqlclient")


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


# TODO: Someday remove this method and replace with setuptools.find_namespace_packages
def find_namespace_packages(where):
    pkgs = []
    for root, dirs, _ in os.walk(where):
        root = root[len(where) + 1 :]
        for pkg in dirs:
            if pkg == where or pkg.endswith(".egg-info") or pkg == "__pycache__":
                continue

            pkgs.append(os.path.join(root, pkg).replace("/", "."))

    return pkgs


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


setup_installer_dependencies()

setup(
    name="pegasus-wms",
    version=read_version(),
    author="Pegasus Team",
    author_email="pegasus@isi.edu",
    description="Pegasus Workflow Management System Python Codebase",
    long_description=read("README.md"),
    long_description_content_type="text/markdown",
    license="Apache2",
    url="http://pegasus.isi.edu",
    python_requires=">=3.5",
    keywords=["scientific workflows"],
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Operating System :: Unix",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Topic :: Scientific/Engineering",
        "Topic :: Utilities",
        "License :: OSI Approved :: Apache Software License",
    ],
    package_dir={"": "src"},
    packages=find_namespace_packages(where="src"),
    package_data={"Pegasus.service": find_package_data("src/Pegasus/service/")},
    include_package_data=True,
    zip_safe=False,
    install_requires=install_requires,
    use_2to3=False,
    convert_2to3_doctests=[],
    extras_require={
        "postgresql": ["psycopg2"],
        "mysql": ["mysqlclient"],
        "cwl": ["cwl-utils==0.3", "jsonschema==3.2.0"],
    },
)
