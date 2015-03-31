![Pegasus](doc/docbook/images/pegasusfront-black-reduced.png)

Pegasus Workflow Management System
----------------------------------

Before you try to run anything, you might want to make sure that your
environment works. We depend on a number of packages that you need to have
installed: Condor 7.4+, Java 1.6+, Python 2.4+, Perl 5.6+, and optionally
Globus Toolkit 4.2+.

Please refer to the RELEASE_NOTES for important changes. For instance, it is no
longer necessary to set the PEGASUS_HOME environment variable. However, in
order to find all tools, you must include Pegasus's "bin" directory in your
PATH environment variable.

Please refer to the user guide for instructions on the packages and their
installation. You can find [online documentation](http://pegasus.isi.edu/documentation)
on the Pegasus webpage and in the distributed "doc" directory. 

Installation
------------

The easiest way to install Pegasus is to use one of the binary packages
available on the [Pegasus downloads page](http://pegasus.isi.edu/downloads).
Consult [Chapter 3 of the Pegasus User Guide](http://pegasus.isi.edu/wms/docs/latest/installation.php)
for more information about installing Pegasus from binary packages.

Pegasus requires the following software to be installed on your system:

* Java 1.6 or later
* Python 2.4 or later (2.5 or later preferred)
* Condor 8.0 or later
* Perl 5
* Globus 5 (optional, required for GRAM and GridFTP)

Building from Source
--------------------

Pegasus can be compiled on any recent Linux or Mac OS X system.

### Source Dependencies

In order to build Pegasus from source, make sure you have the following
packages installed:

#### Debian systems (Debian, Ubuntu, etc.)

Install the following packages using apt-get:

* default-jdk
* ant
* gcc
* g++
* make
* python-setuptools
* asciidoc (optional, required to build documentation)
* fop (optional, required to build documentation)
* lintian (optional, required to build DEB package)
* debhelper (optional, required to build DEB package)
* asciidoc (optional, required to build documentation)
* fop (optional, required to build documentation)
* libmysqlclient-dev (optional, required to access MySQL databases)
* libpq-dev (optional, required to access PostgreSQL databases)

#### Red Hat systems (RHEL, CentOS, Scientific Linux, Fedora, etc.)

Install the following packages using yum:

* java
* java-devel
* ant
* ant-junit
* gcc
* gcc-c++
* make
* python-devel
* rpm-build (optional, required to build RPM package)
* mysql-devel (optional, required to access MySQL databases)
* postgresql-devel (optional, required to access PostgreSQL databases)

#### Mac OS X

Install Xcode and the Xcode command-line tools.

Install homebrew and the following homebrew packages:

* mysql (optional, required to access MySQL databases)
* postgresql (optional, required to access PostgreSQL databases)

### Compiling

Ant is used to compile Pegasus.

To build a binary tarball (excluding documentation), run:

 $ ant dist

To build the release tarball (including documentation), run:

 $ ant dist-release

Getting Started
---------------

You can find more information about Pegasus on the [Pegasus Website](http://pegasus.isi.edu).

Pegasus has an extensive [User Guide](http://pegasus.isi.edu/wms/docs/latest/)
that documents how to create, plan, and monitor workflows.

We recommend you start by completing the Pegasus Tutorial from [Chapter 2 of the
Pegasus User Guide](http://pegasus.isi.edu/wms/docs/latest/tutorial.php).

There is documentation on the Pegasus website for the
[Python](http://pegasus.isi.edu/wms/docs/latest/python/),
[Java](http://pegasus.isi.edu/wms/docs/latest/javadoc/) and
[Perl](http://pegasus.isi.edu/wms/docs/latest/perl/) APIs used to construct DAXes.

There are [several examples](http://pegasus.isi.edu/examples) of how to
construct workflows on the Pegasus website and in the [Pegasus Git repository
](https://github.com/pegasus-isi/pegasus/tree/master/share/pegasus/examples).

There are also examples of how to [configure Pegasus for different execution
environments](http://pegasus.isi.edu/wms/docs/latest/execution_environments.php)
in the Pegasus User Guide.

If you need help using Pegasus, please contact us. See the [support page]
(http://pegasus.isi.edu/support) on the Pegasus website for contact information.

