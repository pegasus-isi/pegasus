.. _installtaion:

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

-  **Java 1.8 or higher**. Check with:

   ::

      # java -version
      java version "1.8.0"

-  **Python 2.6 or higher**. Check with:

   ::

      # python -v
      Python 2.6.2

   **Non-standard Python installation:** Pegasus will use the system
   Python by default. If you want to override this behavior, please set
   the **PEGASUS_PYTHON** environment variable during the build. This
   environment variable is only for build time configuration. Once
   built, Pegasus will continue to use the build time specified Python
   install.

-  **HTCondor (formerly Condor) 8.6 or higher**. See
   http://www.cs.wisc.edu/htcondor/ for more information. You should be
   able to run ``condor_q`` and ``condor_status``.

.. _optional:

Optional Software
=================

-  **Globus 5.0 or higher**. Globus is only needed if you want to run
   against grid sites or use GridFTP for data transfers. See
   http://www.globus.org/ for more information.

-  **psycopg2**. Python module for PostgreSQL access. Only needed if you
   want to store the runtime database in PostgreSQL (default is SQLite)

-  **python3-pika**. Python module for sending workflow events to
   RabbitMQ. This is optional, and has to be enabled in the Pegasus
   workflow configuration.

.. _env:

Environment
===========

To use Pegasus, you need to have the pegasus-\* tools in your PATH. If
you have installed Pegasus from RPM/DEB packages. the tools will be in
the default PATH, in /usr/bin. If you have installed Pegasus from binary
tarballs or source, add the bin/ directory to your PATH.

Example for bourne shells:

::


       $ export PATH=/some/install/pegasus-4.8/bin:$PATH

..

   **Note**

   Pegasus 4.x is different from previous versions of Pegasus in that it
   does not require PEGASUS_HOME to be set or sourcing of any
   environment setup scripts.

If you want to use the :ref:`dax-generator-api`, you might also need to set
your PYTHONPATH, PERL5LIB, or CLASSPATH. The right setting can be found
by using pegasus-config:

::

   $ export PYTHONPATH=`pegasus-config --python`
   $ export PERL5LIB=`pegasus-config --perl`
   $ export CLASSPATH=`pegasus-config --classpath`

.. _rhel:

RHEL / CentOS / Scientific Linux
================================

.. tabs::

   .. tab:: CentOS 6

      .. code-block:: bash

         curl --output /etc/yum.repos.d/pegasus.repo \
              https://download.pegasus.isi.edu/wms/download/rhel/6/pegasus.repo

         yum install pegasus

   .. tab:: CentOS 7

      .. code-block:: bash

         curl --output /etc/yum.repos.d/pegasus.repo \
              https://download.pegasus.isi.edu/wms/download/rhel/7/pegasus.repo

         yum install pegasus

   .. tab:: CentOS 8

      .. code-block:: bash

         curl --output /etc/yum.repos.d/pegasus.repo \
              https://download.pegasus.isi.edu/wms/download/rhel/8/pegasus.repo

         yum install pegasus

Binary packages provided for: RHEL 6 x86_64, RHEL 7 x86_64 (and OSes
derived from RHEL: CentOS, SL)

Add the Pegasus repository to yum downloading the Pegasus repos file and
adding it to\ **``/etc/yum.repos.d/``**. For RHEL 7 based systemes:

::

   # wget -O /etc/yum.repos.d/pegasus.repo https://download.pegasus.isi.edu/pegasus/rhel/7/pegasus.repo

For RHEL 6 based systems:

::

   # wget -O /etc/yum.repos.d/pegasus.repo https://download.pegasus.isi.edu/pegasus/rhel/6/pegasus.repo

Search for, and install Pegasus:

::

   # yum search pegasus
   pegasus.x86_64 : Workflow management system for Condor, grids, and clouds
   # yum install pegasus
   Running Transaction
   Installing     : pegasus

   Installed:
   pegasus

   Complete!

Ubuntu
======

Binary packages provided for: 17.04 (Zesty Zapus) x86_64, 16.04 (Xenial
Xerus) x86_64

**For 17.04 (Zesty Zapus) based systems:**

To be able to install and upgrade from the Pegasus apt repository, you
will have to trust the repository key. You only need to add the
repository key once:

::

   # wget -O - https://download.pegasus.isi.edu/pegasus/gpg.txt | apt-key add -

Create repository file, update and install Pegasus:

::

   # echo 'deb https://download.pegasus.isi.edu/pegasus/ubuntu zesty main' >/etc/apt/sources.list.d/pegasus.list
   # apt-get update
   # apt-get install pegasus

**For 16.04 (Xenial Xerus) based systems:**

To be able to install and upgrade from the Pegasus apt repository, you
will have to trust the repository key. You only need to add the
repository key once:

::

   # wget -O - https://download.pegasus.isi.edu/pegasus/gpg.txt | apt-key add -

Create repository file, update and install Pegasus:

::

   # echo 'deb https://download.pegasus.isi.edu/pegasus/ubuntu xenial main' >/etc/apt/sources.list.d/pegasus.list
   # apt-get update
   # apt-get install pegasus

Debian
======

Binary packages provided for: 9 (Stretch) x86_64, 10 (Buster) x86_64

To be able to install and upgrade from the Pegasus apt repository, you
will have to trust the repository key. You only need to add the
repository key once:

::

   # wget -O - https://download.pegasus.isi.edu/pegasus/gpg.txt | apt-key add -

Create repository file, update and install Pegasus (currently available
releases are stretch (9) and buster (10) - replace the *strecth* part):

::

   # echo 'deb https://download.pegasus.isi.edu/pegasus/debian stretch main' >/etc/apt/sources.list.d/pegasus.list
   # apt-get update
   # apt-get install pegasus

.. _macosx:

Mac OS X
========

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

   **Note**

   It is also possible to install the latest development versions of
   Pegasus using the ``--devel`` and ``--HEAD`` arguments to
   ``brew install``, like this: ``$ brew install --devel pegasus``

.. _tarballs:

Pegasus from Tarballs
=====================

The Pegasus prebuild tarballs can be downloaded from the `Pegasus
Download Page <https://pegasus.isi.edu/downloads>`__.

Use these tarballs if you already have HTCondor installed or prefer to
keep the HTCondor installation separate from the Pegasus installation.

-  Untar the tarball

   ::

      # tar zxf pegasus-*.tar.gz

-  include the Pegasus bin directory in your PATH

   ::

      # export PATH=/path/to/pegasus-install/bin:$PATH
