.. _introduction:

============
Introduction
============

.. _overview:

Overview and Features
=====================

`Pegasus WMS <http://pegasus.isi.edu>`__ is a configurable system for
mapping and executing abstract application workflows over a wide range
of execution environments including a laptop, a campus cluster, a Grid,
or a commercial or academic cloud. Today, Pegasus runs workflows on
Amazon EC2, Nimbus, Open Science Grid, the TeraGrid, and many campus
clusters. One workflow can run on a single system or across a
heterogeneous set of resources. Pegasus can run workflows ranging from
just a few computational tasks up to 1 million.

Pegasus WMS bridges the scientific domain and the execution environment
by automatically mapping high-level workflow descriptions onto
distributed resources. It automatically locates the necessary input data
and computational resources necessary for workflow execution. Pegasus
enables scientists to construct workflows in abstract terms without
worrying about the details of the underlying execution environment or
the particulars of the low-level specifications required by the
middleware (Condor, Globus, or Amazon EC2). Pegasus WMS also bridges the
current cyberinfrastructure by effectively coordinating multiple
distributed resources. The input to Pegasus is a description of the
abstract workflow in XML format.

Pegasus allows researchers to translate complex computational tasks into
workflows that link and manage ensembles of dependent tasks and related
data files. Pegasus automatically chains dependent tasks together, so
that a single scientist can complete complex computations that once
required many different people. New users are encouraged to explore the
:doc:`tutorial` to become familiar with how to operate
Pegasus for their own workflows. Users create and run a sample project
to demonstrate Pegasus capabilities.

Pegasus has a number of features that contribute to its useability and
effectiveness.

-  **Portability / Reuse**

   User created workflows can easily be run in different environments
   without alteration. Pegasus currently runs workflows on top of
   Condor, Grid infrastrucutures such as Open Science Grid and TeraGrid,
   Amazon EC2, Nimbus, and many campus clusters. The same workflow can
   run on a single system or across a heterogeneous set of resources.

-  **Performance**

   The Pegasus mapper can reorder, group, and prioritize tasks in order
   to increase the overall workflow performance.

-  **Scalability**

   Pegasus can easily scale both the size of the workflow, and the
   resources that the workflow is distributed over. Pegasus runs
   workflows ranging from just a few computational tasks up to 1
   million. The number of resources involved in executing a workflow can
   scale as needed without any impediments to performance.

-  **Provenance**

   By default, all jobs in Pegasus are launched via the **kickstart**
   process that captures runtime provenance of the job and helps in
   debugging. The provenance data is collected in a database, and the
   data can be summarised with tools such as **pegasus-statistics**,
   **pegasus-plots**, or directly with SQL queries.

-  **Data Management**

   Pegasus handles replica selection, data transfers and output
   registrations in data catalogs. These tasks are added to a workflow
   as auxilliary jobs by the Pegasus planner.

-  **Reliability**

   Jobs and data transfers are automatically retried in case of
   failures. Debugging tools such as **pegasus-analyzer** helps the user
   to debug the workflow in case of non-recoverable failures.

-  **Error Recovery**

   When errors occur, Pegasus tries to recover when possible by retrying
   tasks, by retrying the entire workflow, by providing workflow-level
   checkpointing, by re-mapping portions of the workflow, by trying
   alternative data sources for staging data, and, when all else fails,
   by providing a rescue workflow containing a description of only the
   work that remains to be done. It cleans up storage as the workflow is
   executed so that data-intensive workflows have enough space to
   execute on storage-constrained resource. Pegasus keeps track of what
   has been done (provenance) including the locations of data used and
   produced, and which software was used with which parameters.

-  **Operating Environments**

   Pegasus workflows can be deployed across a variety of environments:

   -  *Local Execution*

      Pegasus can run a workflow on a single computer with Internet
      access. Running in a local environment is quicker to deploy as the
      user does not need to gain access to muliple resources in order to
      execute a workfow.

   -  *Condor Pools and Glideins*

      Condor is a specialized workload management system for
      compute-intensive jobs. Condor queues workflows, schedules, and
      monitors the execution of each workflow. Condor Pools and Glideins
      are tools for submitting and executing the Condor daemons on a
      Globus resource. As long as the daemons continue to run, the
      remote machine running them appears as part of your Condor pool.
      For a more complete description of Condor, see the `Condor Project
      Pages <http://www.cs.wisc.edu/condor/description.html>`__

   -  *Grids*

      Pegasus WMS is entirely compatible with Grid computing. Grid
      computing relies on the concept of distributed computations.
      Pegasus apportions pieces of a workflow to run on distributed
      resources.

   -  *Clouds*

      Cloud computing uses a network as a means to connect a Pegasus end
      user to distributed resources that are based in the cloud.

.. _workflow-gallery:

Workflow Gallery
================

Pegasus is curently being used in a broad range of applications. To
review example workflows, see the :doc:`example-workflows` chapter.
To see additional details about the workflows of the applications
see the `Gallery of Workflows <http://pegasus.isi.edu/workflow_gallery/>`__.

We are always looking for new applications willing to leverage our
workflow technologies. If you are interested please contact us at
pegasus at isi dot edu.
