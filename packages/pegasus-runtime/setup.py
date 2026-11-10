import os
from setuptools import setup

src_dir = os.path.dirname(__file__)


def find_namespace_packages(where):
    pkgs = []
    for root, dirs, _ in os.walk(where):
        root = root[len(where) + 1:]
        for pkg in dirs:
            if pkg == where or pkg.endswith(".egg-info") or pkg == "__pycache__":
                continue
            pkgs.append(os.path.join(root, pkg).replace("/", "."))
    return pkgs


setup(
    name="pegasus-wms.runtime",
    version="5.2.0-dev.0",
    author="Pegasus Team",
    author_email="pegasus@isi.edu",
    description="Pegasus WMS Runtime Prediction — ML model and inference engine",
    license="Apache-2.0",
    url="http://pegasus.isi.edu",
    python_requires=">=3.6",
    package_dir={"": "src"},
    packages=find_namespace_packages(where="src"),
    package_data={
        "Pegasus.runtime.models": [".gitkeep", "*.pkl"],
    },
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        "torch>=1.13",
        "scikit-learn>=1.0",
        "numpy>=1.21",
        "pandas>=1.3",
        "PyYAML",
        "pegasus-wms.api",
    ],
)
