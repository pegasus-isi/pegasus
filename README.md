![Pegasus](doc/sphinx/images/pegasusfront-black-reduced.png)

Pegasus Workflow Management System
----------------------------------

Pegasus WMS is a configurable system for mapping and executing scientific
workflows over a wide range of computational infrastructures including laptops,
campus clusters, supercomputers, grids, and commercial and academic clouds.
Pegasus has been used to run workflows with up to 1 million tasks that process
tens of terabytes of data at a time.

Pegasus WMS bridges the scientific domain and the execution environment by
automatically mapping high-level workflow descriptions onto distributed
resources. It automatically locates the necessary input data and computational
resources required by a workflow, and plans out all of the required data
transfer and job submission operations required to execute the workflow.
Pegasus enables scientists to construct workflows in abstract terms without
worrying about the details of the underlying execution environment or the
particulars of the low-level specifications required by the middleware (Condor,
Globus, Amazon EC2, etc.). In the process, Pegasus can plan and optimize the
workflow to enable efficient, high-performance execution of large
workflows on complex, distributed infrastructures.

Pegasus has a number of features that contribute to its usability and
effectiveness:

* Portability / Reuse – User created workflows can easily be run in different
environments without alteration. Pegasus currently runs workflows on top of
Condor pools, Grid infrastrucutures such as Open Science Grid and XSEDE,
Amazon EC2, Google Cloud, and HPC clusters. The same workflow can run on a
single system or across a heterogeneous set of resources.
* Performance – The Pegasus mapper can reorder, group, and prioritize tasks in
order to increase overall workflow performance.
* Scalability – Pegasus can easily scale both the size of the workflow, and
the resources that the workflow is distributed over. Pegasus runs workflows
ranging from just a few computational tasks up to 1 million. The number of
resources involved in executing a workflow can scale as needed without any
impediments to performance.
* Provenance – By default, all jobs in Pegasus are launched using the
Kickstart wrapper that captures runtime provenance of the job and helps in
debugging. Provenance data is collected in a database, and the data can be
queried with tools such as pegasus-statistics, pegasus-plots, or directly
using SQL.
* Data Management – Pegasus handles replica selection, data transfers and
output registration in data catalogs. These tasks are added to a workflow as
auxilliary jobs by the Pegasus planner.
* Reliability – Jobs and data transfers are automatically retried in case of
failures. Debugging tools such as pegasus-analyzer help the user to debug the
workflow in case of non-recoverable failures.
* Error Recovery – When errors occur, Pegasus tries to recover when possible
by retrying tasks, by retrying the entire workflow, by providing workflow-level
checkpointing, by re-mapping portions of the workflow, by trying alternative
data sources for staging data, and, when all else fails, by providing a rescue
workflow containing a description of only the work that remains to be done.
It cleans up storage as the workflow is executed so that data-intensive
workflows have enough space to execute on storage-constrained resources.
Pegasus keeps track of what has been done (provenance) including the locations
of data used and produced, and which software was used with which parameters.


Getting Started
---------------

You can find more information about Pegasus on the [Pegasus Website](http://pegasus.isi.edu).

Pegasus has an extensive [User Guide](http://pegasus.isi.edu/documentation/)
that documents how to create, plan, and monitor workflows.

We recommend you start by completing the Pegasus Tutorial from [Chapter 2 of the
Pegasus User Guide](http://pegasus.isi.edu/documentation/tutorial.php).

The easiest way to install Pegasus is to use one of the binary packages
available on the [Pegasus downloads page](http://pegasus.isi.edu/downloads).
Consult [Chapter 3 of the Pegasus User Guide](http://pegasus.isi.edu/wms/docs/latest/installation.php)
for more information about installing Pegasus from binary packages.

There is documentation on the Pegasus website for the Python, Java and Perl
[DAX generator APIs](https://pegasus.isi.edu/documentation/dax_generator_api.php).

There are [several examples](http://pegasus.isi.edu/documentation/examples/) of
how to construct workflows on the Pegasus website and in the [Pegasus Git
repository](https://github.com/pegasus-isi/pegasus/tree/master/share/pegasus/examples).

There are also examples of how to [configure Pegasus for different execution
environments](http://pegasus.isi.edu/documentation/execution_environments.php)
in the Pegasus User Guide.

If you need help using Pegasus, please contact us. See the [contact page]
(http://pegasus.isi.edu/contact) on the Pegasus website for more information.


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
* R (optional, required to build R DAX api)
* asciidoc (optional, required to build documentation)
* fop (optional, required to build documentation)
* lintian (optional, required to build DEB package)
* debhelper (optional, required to build DEB package)
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
* openssl-devel
* R (optional, required to build R DAX api)
* rpm-build (optional, required to build RPM package)
* mysql-devel (optional, required to access MySQL databases)
* postgresql-devel (optional, required to access PostgreSQL databases)

In addition, RHEL 5 systems will require Python 2.6, which can be
installed from EPEL. You will also need to install the right setuptools
for Python 2.6, which can be installed from the Python Package Index using:

    $ wget http://pypi.python.org/packages/2.6/s/setuptools/setuptools-0.6c9-py2.6.egg#md5=ca37b1ff16fa2ede6e19383e7b59245a
    $ sudo /bin/sh setuptools-0.6c9-py2.6.egg

or, if you don't have root access:

    $ /bin/sh setuptools-0.6c9-py2.6.egg -d ~/.local/lib/python2.6/site-packages

#### Mac OS X

Install Xcode and the Xcode command-line tools.

Install homebrew and the following homebrew packages:

* ant
* openssl
* R (optional, required to build R DAX api)
* asciidoc (optional, required for manpages and docs)
* fop (optional, required for docs)
* mysql (optional, required to access MySQL databases)
* postgresql (optional, required to access PostgreSQL databases)

#### SUSE (openSUSE, SLES)

Install the following packages:

* git-core
* ant
* ant-nodeps
* make
* gcc
* gcc-c++
* sqlite3
* python
* python-setuptools
* python-devel
* libopenssl-devel
* R (optional, required to build R DAX api)
* asciidoc (optional, required for documentation)
* libmysqlclient-devel (optional, required for MySQL support)

Other packages may be required to run unit tests, and build MPI tools.

### Compiling

Ant is used to compile Pegasus.

To get a list of build targets run:

    $ ant -p

The targets that begin with "dist" are what you want to use.

To build a basic binary tarball (excluding documentation), run:

    $ ant dist

To build the release tarball (including documentation), run:

    $ ant dist-release

The resulting packages will be created in the `dist` subdirectory.

