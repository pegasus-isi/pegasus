"""
Installation script for Pegasus Python library

Author: Dan Gunter <dkgunter@lbl.gov>
"""
try:
    from setuptools import setup
except:
    from distutils.core import setup
from glob import glob
import os
import sys

VERSION = os.environ.get('PEGASUS_VERSION','trunk')

# Main function
# -------------

setup(name = "Pegasus",
      version=VERSION,
      packages = ["Pegasus",
          "Pegasus.monitoring",
          "Pegasus.plots_stats",
          "Pegasus.plots_stats.plots",
          "Pegasus.plots_stats.stats",
          "Pegasus.test",
          "Pegasus.tools"],
      ext_modules = [],
      package_data = {},
      scripts = [ ],
      install_requires=[ ],
      # metadata for upload to PyPI
      author = "Pegasus Team",
      author_email = "deelman@isi.edu",
      maintainer = "Karan Vahi",
      maintainer_email = "vahi@isi.edu",
      description = "Pegasus Python library",
      long_description = "",
      license = "LBNL Open-Source",
      keywords = "workflow",
      url = "https://confluence.pegasus.isi.edu/display/pegasus/Home",
      classifiers = [
        "Development Status :: 5 - Production/Stable",
        "Environment :: No Input/Output (Daemon)",
        "Intended Audience :: Science/Research",
        "Intended Audience :: System Administrators",
        "License :: Other/Proprietary License",
        "Natural Language :: English",
        "Operating System :: POSIX",
        "Programming Language :: Python",
        "Topic :: Database",
        "Topic :: Workflow",
        "Topic :: System :: Monitoring",
        ],
      )
