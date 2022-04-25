<p align="center">
      <img src="doc/sphinx/images/pegasusfront-black-reduced.png" width="200" alt="Pegasus WMS" />
</p>

Pegasus Workflow Management System
----------------------------------
<p align="left">
    <img src="https://img.shields.io/github/license/pegasus-isi/pegasus?color=blue&label=Licence"/>
    <img src="https://img.shields.io/github/v/tag/pegasus-isi/pegasus?label=Latest"/>
    <img src="https://img.shields.io/pypi/dm/pegasus-wms?color=green&label=PyPI%20Downloads"/>
    <img src="https://img.shields.io/github/contributors-anon/pegasus-isi/pegasus?color=green&label=Contributors"/>
</p>

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
Condor pools, Grid infrastructures such as Open Science Grid and XSEDE,
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

We recommend you start by completing the Pegasus Tutorial from [Chapter 3 of the
Pegasus User Guide](https://pegasus.isi.edu/documentation/user-guide/tutorial.html).

The easiest way to install Pegasus is to use one of the binary packages
available on the [Pegasus downloads page](http://pegasus.isi.edu/downloads).
Consult [Chapter 2 of the Pegasus User Guide](https://pegasus.isi.edu/documentation/user-guide/installation.html)
for more information about installing Pegasus from binary packages.

There is documentation on the Pegasus website for the Python, Java and R
[Abstract Workflow Generator APIs](https://pegasus.isi.edu/documentation/reference-guide/api-reference.html).
We strongly recommend using the Python API which is feature complete, and also
allows you to invoke all the pegasus command line tools.

You can use *pegasus-init* command line tool to run several examples
on your local machine. Consult [Chapter 4 of the Pegasus
User Guide](https://pegasus.isi.edu/documentation/user-guide/example-workflows.html)
for more information.

There are also examples of how to [Configure Pegasus for Different Execution
Environments](https://pegasus.isi.edu/documentation/user-guide/execution-environments.html)
in the Pegasus User Guide.

If you need help using Pegasus, please contact us. See the [contact page]
(http://pegasus.isi.edu/contact) on the Pegasus website for more information.


Building from Source
--------------------

Pegasus can be compiled on any recent Linux or Mac OS X system.

### Source Dependencies

In order to build Pegasus from source, make sure you have the following installed:

* Git
* Java 8 or higher
* Python 3.5 or higher
* R
* Ant
* gcc
* g++
* make
* tox 3.14.5 or higher
* mysql (optional, required to access MySQL databases)
* postgresql (optional, required to access PostgreSQL databases)
* Python pyyaml
* Python GitPython

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
