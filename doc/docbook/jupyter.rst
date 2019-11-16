.. _jupyter:

=================
Jupyter Notebooks
=================

.. _jupyter-introduction:

Introduction
============

The `Jupyter Notebook <http://jupyter.org/>`__ is an open-source web
application that allows you to create and share documents that contain
live code, equations, visualizations and explanatory text. Its flexible
and portable format resulted in a rapidly adoption by the research
community to share and interact with experiments. Jupyter Notebooks has
a strong potential to reduce the gap between researchers and the complex
knowledge required to run large-scale scientific workflows via a
programmatic high-level interface to access/manage workflow
capabilities.

The Pegasus-Jupyter integration aims to facilitate the usage of Pegasus
via Jupyter notebooks. In addition to easiness of usage, notebooks
foster reproducibility (all the information to run an experiment is in a
unique place) and reuse (notebooks are portable if running in equivalent
environments). Since `Pegasus 4.8.0 </downloads>`__, a Python API to
declare and manage Pegasus workflows via Jupyter has been provided. The
user can create a notebook and declare a workflow using the `Pegasus DAX
API <#dax_generator_api>`__, and then create an instance of the workflow
for execution. This API encapsulates most of Pegasus commands (e.g.,
plan, run, statistics, among others), and also allows workflow creation,
execution, and monitoring. The API also provides mechanisms to define
Pegasus catalogs (sites, replica, and transformation), as well as to
generate tutorial example workflows.

.. _jupyter-requirements:

Requirements
============

In order to run Pegasus workflows via Jupyter the following software
packages are required:

1. Python 2.7 or superior (Jupyter requires version 2.7+)

2. Java 1.6 or superior

3. Pegasus 4.8.0 or superior (submit node)

4. Jupyter (see `installation
   notes <http://jupyter.org/install.html>`__)

.. _jupyter-api:

The Pegasus DAX and Jupyter Python APIs
=======================================

The first step to enable Jupyter to use the Pegasus API is to import the
Python Pegasus Jupyter API. The instance module will automatically load
the Pegasus DAX3 API and the catalogs APIs.

::

   from Pegasus.jupyter.instance import *

By default, the API automatically creates a folder in the user's $HOME
directory based on the workflow name. However, a predefined path for the
workflow files can also be defined as follows:

::

   workflow_dir = '/home/pegasus/wf-split-tutorial'

.. _jupyter-api-dax:

Creating an Abstract Workflow
-----------------------------

Workflow creation within Jupyter follows the same steps to generate a
DAX with the `DAX Generator API <#dax_generator_api>`__.

.. _jupyter-api-catalogs:

Creating the Catalogs
---------------------

The `Replica Catalog <#replica>`__ (RC) tells Pegasus where to find each
of the input files for the workflow. We provide a Python API for
creating the RC programmatically. For detailed information on how the RC
works and its semantics can be found `here <#replica>`__, and the
auto-generated python documentation for this API can be found
`here <python/replica_catalog.html>`__.

::

   rc = ReplicaCatalog(workflow_dir)
   rc.add('pegasus.html', 'file:///home/pegasus/pegasus.html', site='local')

The `Transformation Catalog <#transformation>`__ (TC) describes all of
the executables (called "transformations") used by the workflow. The
Python Jupyter API also provides methods to manage this catalog. A
detailed description of the TC properties can be found
`here <#transformation>`__, and the auto-generated python documentation
for this API can be found `here <python/transformation_catalog.html>`__.

::

   e_split = Executable('split', arch=Arch.X86_64, os=OSType.LINUX, installed=True)
   e_split.addPFN(PFN('file:///usr/bin/split', 'condorpool'))

   e_wc = Executable('wc', arch=Arch.X86_64, os=OSType.LINUX, installed=True)
   e_wc.addPFN(PFN('file:///usr/bin/wc', 'condorpool'))

   tc = TransformationCatalog(workflow_dir)
   tc.add(e_split)
   tc.add(e_wc)

The `Site Catalog <#site>`__ (SC) describes the sites where the workflow
jobs are to be executed. A detailed description of the SC properties and
handlers can be found `here <#transformation>`__, and the auto-generated
python documentation for this API can be found
`here <python/sites_catalog.html>`__.

::

   sc = SitesCatalog(workflow_dir)
   sc.add_site('condorpool', arch=Arch.X86_64, os=OSType.LINUX)
   sc.add_site_profile('condorpool', namespace=Namespace.PEGASUS, key='style', value='condor')
   sc.add_site_profile('condorpool', namespace=Namespace.CONDOR, key='universe', value='vanilla')

.. _jupyter-api-exec:

Workflow Execution
------------------

Workflow execution and management are performed using an *Instance*
object. An instance receives a DAX object (created with the `DAX
Generator API <#dax_generator_api>`__), and the catalogs objects
(replica, transformation, and site). A path to the workflow directory
can also be provided:

::

   instance = Instance(dax, replica_catalog=rc, transformation_catalog=tc, sites_catalog=sc, workflow_dir=workflow_dir)

An instance object represents a workflow run, from where the workflow
execution can be launched, monitored, and managed. The *run* method
starts the workflow execution.

::

   instance.run(site='condorpool')

After the workflow has been submitted you can monitor it using the
*status()* method. This method takes two arguments:

1. *loop*: whether the status command should be invoked once or
   continuously until the workflow is completed or a failure is
   detected.

2. *delay*: The delay (in seconds) the status will be refreshed. Default
   value is 10s.

::

   instance.status(loop=True, delay=5)

JupyterHub
==========

The Pegasus Jupyter API can also be used with
`JupyterHub <https://jupyterhub.readthedocs.io>`__ portals. Due to the
strict requirement of Python 3 for running the multi-user hub, our API
requires the Python `future <https://pypi.python.org/pypi/future>`__
package in order to be compatible with Python 3.

.. _jupyter-api-reference:

API Reference
=============

Refer to the auto-generated python documentation explaining the `Jupyter
API (instance) <python/instance.html>`__, and for the catalogs
(`sites <python/sites_catalog.html>`__,
`replica <python/replica_catalog.html>`__, and
`transformation <python/transformation_catalog.html>`__).

.. _jupyter-example:

Tutorial Example Notebook
=========================

The Pegasus `Tutorial VM <#tutorial_vm>`__ is configured with Jupyter
and the example Pegasus Tutorial Jupyter Notebook. To start Jupyter, use
the following command in the VM terminal:

::

   $ jupyter-notebook --browser=firefox

This command will open the browser with a tab to the Jupyter dashboard,
which will show your $HOME directory list of files. The Pegasus Tutorial
Notebook can be found into the **'jupyter'** folder.
