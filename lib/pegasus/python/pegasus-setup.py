"""
Installation script for Pegasus Python library

Author: Dan Gunter <dkgunter@lbl.gov>
"""
try:
    from setuptools import setup
except:
    from distutils.core import setup
import os

VERSION = os.environ.get('PEGASUS_VERSION','trunk')

setup(name = "Pegasus",
      version=VERSION,
      packages = [
          "Pegasus",
          "Pegasus.monitoring",
          "Pegasus.dashboard",
          "Pegasus.plots_stats",
          "Pegasus.plots_stats.plots",
          "Pegasus.plots_stats.stats",
          "Pegasus.test",
          "Pegasus.tools"
      ],
      ext_modules = [],
      package_data = {},
      scripts = [],
      install_requires=[ ],
      author = "Pegasus Team",
      author_email = "pegasus-support@isi.edu",
      maintainer = "Karan Vahi",
      maintainer_email = "vahi@isi.edu",
      description = "Pegasus Python library",
      long_description = "",
      license = "Apache 2.0",
      keywords = "workflow",
      url = "http://pegasus.isi.edu",
      classifiers = [
        "Development Status :: 5 - Production/Stable",
        "Environment :: No Input/Output (Daemon)",
        "Intended Audience :: Science/Research",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: Apache Software License",
        "Natural Language :: English",
        "Operating System :: POSIX",
        "Programming Language :: Python",
        "Topic :: Database",
        "Topic :: Workflow",
        "Topic :: System :: Monitoring",
        ],
      )
