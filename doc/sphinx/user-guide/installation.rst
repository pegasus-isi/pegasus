.. _installation:

============
Installation
============

The preferred way to install Pegasus is with native (RPM/DEB) packages.
It is recommended that you also install HTCondor (formerly Condor)
(`yum <http://research.cs.wisc.edu/htcondor/yum/>`__,
`debian <http://research.cs.wisc.edu/htcondor/debian/>`__) from native
packages.

.. _prereqs:

Prerequisites
=============

Pegasus has a few dependencies:

-  **JDK 17 or higher**. Check with:

   ::

      # java -version
      openjdk 17.0.14

-  **Python 3.6 or higher**. Check with:

   ::

      # python3 --version
      Python 3.6.9

   Python3 package ``yaml`` is also required, but it
   will be installed automatically if you use the RPM/DEB packages.

-  **HTCondor 23.X or higher**. See
   http://www.cs.wisc.edu/htcondor/ for more information. You should be
   able to run ``condor_q`` and get queue information.

.. _optional:

Optional Software
=================

-  **mysqlclient**. Python module for MySQL access. Only needed if you
   want to store the runtime database in MySQL (default is SQLite).

-  **psycopg2**. Python module for PostgreSQL access. Only needed if you
   want to store the runtime database in PostgreSQL (default is SQLite)

.. _env:

Environment
===========

To use Pegasus, you need to have the pegasus-\* tools in your PATH. If
you have installed Pegasus from RPM/DEB packages. the tools will be in
the default PATH, in /usr/bin. If you have installed Pegasus from binary
tarballs or source, add the bin/ directory to your PATH.

Example for bourne shells:

::

   $ export PATH=/some/install/pegasus-1.X/bin:$PATH

..

If you want to use the API to generate workflows (:ref:`api-reference`), you might also need to set your PYTHONPATH, PERL5LIB, or CLASSPATH. The right setting can be found by using pegasus-config:

::

   $ export PYTHONPATH=`pegasus-config --python`
   $ export CLASSPATH=`pegasus-config --classpath`

.. _rhel:

RHEL / CentOS / Rocky / Alma / SL
=================================

Binary packages provided for: RHEL 8, RHEL 9 and RHEL 10 (including OSes
derived from RHEL: CentOS, Rocky, AlmaLinux, SL)

.. tabs::

   .. code-tab:: bash EL 10

      curl --output /etc/yum.repos.d/pegasus.repo \
            https://download.pegasus.isi.edu/pegasus/rhel/10/pegasus.repo
      dnf install epel-release
      dnf install --enablerepo devel pegasus

   .. code-tab:: bash EL 9

      curl --output /etc/yum.repos.d/pegasus.repo \
            https://download.pegasus.isi.edu/pegasus/rhel/9/pegasus.repo
      dnf install epel-release
      dnf install --enablerepo devel pegasus

   .. code-tab:: bash EL 8

      curl --output /etc/yum.repos.d/pegasus.repo \
            https://download.pegasus.isi.edu/pegasus/rhel/8/pegasus.repo
      dnf install epel-release
      dnf install --enablerepo powertools pegasus


Ubuntu
======

.. tabs::

   .. code-tab:: bash 26.04 LTS (Resolute Raccoon)

      curl https://download.pegasus.isi.edu/pegasus/gpg.txt | apt-key add -
      echo 'deb https://download.pegasus.isi.edu/pegasus/ubuntu resolute main' >/etc/apt/sources.list.d/pegasus.list
      apt-get update
      apt-get install pegasus

   .. code-tab:: bash 24.04 LTS (Noble Numbat)

      curl https://download.pegasus.isi.edu/pegasus/gpg.txt | apt-key add -
      echo 'deb https://download.pegasus.isi.edu/pegasus/ubuntu noble main' >/etc/apt/sources.list.d/pegasus.list
      apt-get update
      apt-get install pegasus

   .. code-tab:: bash 22.04 LTS (Jammy Jellyfish)

      curl https://download.pegasus.isi.edu/pegasus/gpg.txt | apt-key add -
      echo 'deb https://download.pegasus.isi.edu/pegasus/ubuntu jammy main' >/etc/apt/sources.list.d/pegasus.list
      apt-get update
      apt-get install pegasus


Debian
======

.. tabs::

   .. code-tab:: bash Debian 13 (Trixie)

      wget -O - https://download.pegasus.isi.edu/pegasus/gpg.txt | apt-key add -
      echo 'deb https://download.pegasus.isi.edu/pegasus/debian trixie main' >/etc/apt/sources.list.d/pegasus.list
      apt-get update
      apt-get install pegasus

   .. code-tab:: bash Debian 12 (Bookworm)

      wget -O - https://download.pegasus.isi.edu/pegasus/gpg.txt | apt-key add -
      echo 'deb https://download.pegasus.isi.edu/pegasus/debian bookworm main' >/etc/apt/sources.list.d/pegasus.list
      apt-get update
      apt-get install pegasus

   .. code-tab:: bash Debian 11 (Bullseye)

      wget -O - https://download.pegasus.isi.edu/pegasus/gpg.txt | apt-key add -
      echo 'deb https://download.pegasus.isi.edu/pegasus/debian bullseye main' >/etc/apt/sources.list.d/pegasus.list
      apt-get update
      apt-get install pegasus



.. _macosx:

macOS
=====

The easiest way to install Pegasus on Mac OS is to use Homebrew. You
will need to install XCode and the XCode command-line tools, as well as
Homebrew. Then you just need to tap the Pegasus tools repository and
install Pegasus and HTCondor like this:

::

   $ brew tap pegasus-isi/tools
   $ brew install pegasus htcondor


Once the installation is complete, you need to start the HTCondor
service. The easiest way to do that is to use the Homebrew services tap:

::

   $ brew tap homebrew/services
   $ brew services list
   $ brew services start htcondor

You can also stop HTCondor like this:

::

   $ brew services stop htcondor

And you can uninstall Pegasus and HTCondor using ``brew rm`` like this:

::

   $ brew rm pegasus htcondor

..

.. note::

   It is also possible to install the latest development versions of
   Pegasus using the ``--HEAD`` arguments to
   ``brew install``, like this: ``$ brew install --HEAD pegasus``

.. _tarballs:

Pegasus from Tarballs
=====================

The Pegasus prebuild tarballs can be downloaded from the `Pegasus
Download Page <https://pegasus.isi.edu/downloads>`__.

Use these tarballs if you already have HTCondor installed or prefer to
keep the HTCondor installation separate from the Pegasus installation.

-  Untar the tarball

   ::

      $ tar zxf pegasus-*.tar.gz

-  include the Pegasus bin directory in your PATH

   ::

      $ export PATH=/path/to/pegasus-install/bin:$PATH

-  If you do not already have the Python3 package ``yaml``,
   and ``GitPython``, you can create a virtual environment.
   For example:

   ::

      $ python3 -m venv ~/pegasus-env
      $ . ~/pegasus-env/bin/activate
      $ python3 -m pip install pyyaml GitPython


.. _pypi-packages:

Pegasus Python Packages for PyPi
================================

- To install the new Pegasus API.

   ::

      $ pip install pegasus-wms.api

Mixing Environments (system/venv/conda/...)
===========================================

If you need to mix a Pegasus install with other environments, such as using
the Pegasus command line tools from a system install, but use the Python
install and libraries from Conda, you can tell Pegasus to leave the
environment alone. Note that by doing this, you will need to supply the
requirements in your own environment. Set the environemnt variable:

   ::

      $ export PEGASUS_UPDATE_PYTHONPATH=0

Then install the following packages:


   ::

      boto3
      certifi
      GitPython
      pyjwt
      pyyaml
      s3transfer
      six
      urllib3

You should now be able to use the Pegasus command line tools.
