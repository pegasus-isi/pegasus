.. _running-workflows:

=================
Running Workflows
=================

.. _executable-workflows:

Executable Workflows (DAG)
==========================

The DAG is an executable (concrete) workflow that can be executed over a
variety of resources. When the workflow tasks are mapped to your
target resources, explicit nodes are added to the workflow for
orchestrating data transfer between the tasks; performing data cleanups
and registration of outputs.

When you take the Abstract Workflow created in
:ref:`Creating Workflows <creating-workflows>`, and plan it for a
single remote grid execution, in this case for a site with handle **hpcc**,
and plan the workflow without clean-up nodes, the following executable
workflow is built:

Planning augments the original abstract workflow with ancillary tasks to
facilitate the proper execution of the workflow. These tasks include:

-  the creation of remote working directories. These directories
   typically have name that seeks to avoid conflicts with other
   simultaneously running similar workflows. Such tasks use a job prefix
   of ``create_dir``.

-  the stage-in of input files before any task which requires these
   files. Any file consumed by a task needs to be staged to the task, if
   it does not already exist on that site. Such tasks use a job prefix
   of ``stage_in``.If multiple files from various sources need to be
   transferred, multiple stage-in jobs will be created. Additional
   advanced options permit to control the size and number of these jobs,
   and whether multiple compute tasks can share stage-in jobs.

-  the original compute task is concretized into a compute job in the DAG.
   Compute jobs are a concatination of the job's **name** and **id**
   attribute from the input Abstract Workflow file.

-  the stage-out of data products to one or more output sites. Data
   products with their **stageOut** flag set to ``false`` will not
   be staged to the output sites. However, they may still be eligible
   for staging to other, dependent tasks. Stage-out tasks use a
   job prefix of ``stage_out``.

-  If compute jobs run at different sites, an intermediary staging task
   with prefix ``stage_inter`` is inserted between the compute jobs in
   the workflow, ensuring that the data products of the parent are
   available to the child job.

-  the registration of data products in an output replica catalog. Data
   products with their **register** flag set to ``false`` will not
   be registered.

-  the clean-up of transient files and working directories. These steps
   can be omitted with the ``--no-cleanup`` option to the planner.

The :ref:`Data Management <data-management>` chapter details more about
when and how staging nodes are inserted into the workflow.

The DAG will be found in file ``diamond-0.dag``, constructed from the
**name** and **index** attributes found in the root element of the
Abstract Workflow file.

::

   ######################################################################
   # PEGASUS WMS GENERATED DAG FILE
   # DAG diamond
   # Index = 0, Count = 1
   ######################################################################

   JOB create_dir_diamond_0_hpcc create_dir_diamond_0_hpcc.sub
   SCRIPT POST create_dir_diamond_0_hpcc /opt/pegasus/default/bin/pegasus-exitcode create_dir_diamond_0_hpcc.out

   JOB stage_in_local_hpcc_0 stage_in_local_hpcc_0.sub
   SCRIPT POST stage_in_local_hpcc_0 /opt/pegasus/default/bin/pegasus-exitcode stage_in_local_hpcc_0.out

   JOB preprocess_ID000001 preprocess_ID000001.sub
   SCRIPT POST preprocess_ID000001 /opt/pegasus/default/bin/pegasus-exitcode preprocess_ID000001.out

   JOB findrange_ID000002 findrange_ID000002.sub
   SCRIPT POST findrange_ID000002 /opt/pegasus/default/bin/pegasus-exitcode findrange_ID000002.out

   JOB findrange_ID000003 findrange_ID000003.sub
   SCRIPT POST findrange_ID000003 /opt/pegasus/default/bin/pegasus-exitcode findrange_ID000003.out

   JOB analyze_ID000004 analyze_ID000004.sub
   SCRIPT POST analyze_ID000004 /opt/pegasus/default/bin/pegasus-exitcode analyze_ID000004.out

   JOB stage_out_local_hpcc_2_0 stage_out_local_hpcc_2_0.sub
   SCRIPT POST stage_out_local_hpcc_2_0 /opt/pegasus/default/bin/pegasus-exitcode stage_out_local_hpcc_2_0.out

   PARENT findrange_ID000002 CHILD analyze_ID000004
   PARENT findrange_ID000003 CHILD analyze_ID000004
   PARENT preprocess_ID000001 CHILD findrange_ID000002
   PARENT preprocess_ID000001 CHILD findrange_ID000003
   PARENT analyze_ID000004 CHILD stage_out_local_hpcc_2_0
   PARENT stage_in_local_hpcc_0 CHILD preprocess_ID000001
   PARENT create_dir_diamond_0_hpcc CHILD findrange_ID000002
   PARENT create_dir_diamond_0_hpcc CHILD findrange_ID000003
   PARENT create_dir_diamond_0_hpcc CHILD preprocess_ID000001
   PARENT create_dir_diamond_0_hpcc CHILD analyze_ID000004
   PARENT create_dir_diamond_0_hpcc CHILD stage_in_local_hpcc_0
   ######################################################################
   # End of DAG
   ######################################################################

The DAG file declares all jobs and links them to a Condor submit file
that describes the planned, concrete job. In the same directory as the
DAG file are all Condor submit files for the jobs from the picture plus
a number of additional helper files.

The various instructions that can be put into a DAG file are described
in `Condor's DAGMAN
documentation <https://htcondor.readthedocs.io/en/latest/users-manual/dagman-workflows.html>`__.
The constituents of the submit directory are described in the \ `"Submit
Directory Details" <#submit-directory>`__\ section

.. _data-staging-configuration:

Data Staging Configuration
==========================

Pegasus can be broadly setup to run workflows in the following
configurations

-  **Condor Pool Without a shared filesystem**

   This setup applies to a condor pool where the worker nodes making up
   a condor pool don't share a filesystem. All data IO is achieved using
   Condor File IO. This is a special case of the non shared filesystem
   setup, where instead of using pegasus-transfer to transfer input and
   output data, Condor File IO is used. This is the **default** data
   staging configuration in Pegasus.

-  **NonShared FileSystem**

   This setup applies to where the head node and the worker nodes of a
   cluster don't share a filesystem. Compute jobs in the workflow run in
   a local directory on the worker node

-  **Shared File System**

   This setup applies to where the head node and the worker nodes of a
   cluster share a filesystem. Compute jobs in the workflow run in a
   directory on the shared filesystem.

.. note::

   The default data staging configuration was changed from **sharedfs**
   (Shared File System) to **condorio** (Condor Pool Without a shared
   filesystem) starting with **Pegasus 5.0 release**.

For the purposes of data configuration various sites, and directories
are defined below.

1. **Submit Host**

   The host from where the workflows are submitted . This is where
   Pegasus and Condor DAGMan are installed. This is referred to as the
   **"local"** site in the site catalog .

2. **Compute Site**

   The site where the jobs mentioned in the DAX are executed. There
   needs to be an entry in the Site Catalog for every compute site. The
   compute site is passed to pegasus-plan using **--sites** option

3. **Staging Site**

   A site to which the separate transfer jobs in the executable workflow
   ( jobs with stage_in , stage_out and stage_inter prefixes that
   Pegasus adds using the transfer refiners) stage the input data to and
   the output data from to transfer to the final output site. Currently,
   the staging site is always the compute site where the jobs execute.

4. **Output Site**

   The output site is the final storage site where the users want the
   output data from jobs to go to. The output site is passed to
   pegasus-plan using the **--output** option. The stageout jobs in the
   workflow stage the data from the staging site to the final storage
   site.

5. **Input Site**

   The site where the input data is stored. The locations of the input
   data are catalogued in the Replica Catalog, and the pool attribute of
   the locations gives us the site handle for the input site.

6. **Workflow Execution Directory**

   This is the directory created by the create dir jobs in the
   executable workflow on the Staging Site. This is a directory per
   workflow per staging site. Currently, the Staging site is always the
   Compute Site.

7. **Worker Node Directory**

   This is the directory created on the worker nodes per job usually by
   the job wrapper that launches the job.

You can specifiy the data configuration to use either in

1. properties - Specify the global property
   `pegasus.data.configuration <#data_conf_props>`__ .

2. site catalog - Starting 4.5.0 release, you can specify pegasus
   profile key named data.configuration and associate that with your
   compute sites in the site catalog.

.. _condorio:

Condor Pool Without a Shared Filesystem
---------------------------------------

By default, Pegasus is setup to do your data transfers in this mode.
This setup applies to a condor pool where the worker nodes making up a
condor pool don't share a filesystem. All data IO is achieved using
Condor File IO. This is a special case of the non shared filesystem
setup, where instead of using pegasus-transfer to transfer input and
output data, Condor File IO is used.

**Setup**

-  Submit Host and staging site are same

-  head node and worker nodes of compute site don't share a filesystem

-  Input Data is staged from remote sites.

-  Remote Output Site i.e site other than compute site. Can be submit
   host.

.. figure:: ../images/data-configuration-condorio.png
   :alt: Condor Pool Without a Shared Filesystem

   Condor Pool Without a Shared Filesystem

The data flow is as follows in this case

1. Stagein Job executes on the submit host to stage in input data from
   Input Sites ( 1---n) to a workflow specific execution directory on
   the submit host

2. Compute Job starts on a worker node in a local execution directory.
   Before the compute job starts, Condor transfers the input data for
   the job from the workflow execution directory on thesubmit host to
   the local execution directory on the worker node.

3. The compute job executes in the worker node, and executes on the
   worker node.

4. The compute Job writes out output data to the local directory on the
   worker node using Posix IO

5. When the compute job finishes, Condor transfers the output data for
   the job from the local execution directory on the worker node to the
   workflow execution directory on the submit host.

6. Stageout Job executes ( either on Submit Host or staging site ) to
   stage out output data from the workflow specific execution directory
   to a directory on the final output site.

In this case, the compute jobs are wrapped as
:ref:`PegasusLite <pegasuslite>` instances.

This mode is especially useful for running in the cloud environments
where you don't want to setup a shared filesystem between the worker
nodes. Running in that mode is explained in detail
:ref:`here. <amazon-aws>`

.. tip::

   Set **pegasus.data.configuration** to **condorio** to run in this
   configuration. In this mode, the staging site is automatically set to
   site **local**

In this setup, Pegasus always stages the input files through the submit
host i.e the stage-in job stages in data from the input site to the
submit host (local site). The input data is then transferred to remote
worker nodes from the submit host using Condor file transfers. In the
case, where the input data is locally accessible at the submit host i.e
the input site and the submit host are the same, then it is possible to
bypass the creation of separate stage in jobs that copy the data to the
workflow specific directory on the submit host. Instead, Condor file
transfers can be setup to transfer the input files directly from the
locally accessible input locations ( file URL's with "*site*" attribute
set to local) specified in the replica catalog. More details can be
found at :ref:`bypass-input-staging`.

In some cases, it might be useful to setup the PegasusLite jobs to
pull input data directly from the input site without going through the
staging server.


.. _non-shared-fs:

Non Shared Filesystem
---------------------

In this setup , Pegasus runs workflows on local file-systems of worker
nodes with the the worker nodes not sharing a filesystem. The data
transfers happen between the worker node and a staging / data
coordination site. The staging site server can be a file server on the
head node of a cluster or can be on a separate machine.

**Setup**

-  compute and staging site are the different

-  head node and worker nodes of compute site don't share a filesystem

-  Input Data is staged from remote sites.

-  Remote Output Site i.e site other than compute site. Can be submit
   host.

.. figure:: ../images/data-configuration-nonsharedfs.png
   :alt: Non Shared Filesystem Setup

   Non Shared Filesystem Setup

The data flow is as follows in this case

1. Stagein Job executes ( either on Submit Host or on staging site ) to
   stage in input data from Input Sites ( 1---n) to a workflow specific
   execution directory on the staging site.

2. Compute Job starts on a worker node in a local execution directory.
   Accesses the input data using pegasus transfer to transfer the data
   from the staging site to a local directory on the worker node

3. The compute job executes in the worker node, and executes on the
   worker node.

4. The compute Job writes out output data to the local directory on the
   worker node using Posix IO

5. Output Data is pushed out to the staging site from the worker node
   using pegasus-transfer.

6. Stageout Job executes ( either on Submit Host or staging site ) to
   stage out output data from the workflow specific execution directory
   to a directory on the final output site.

In this case, the compute jobs are wrapped as
:ref:`PegasusLite <pegasuslite>` instances.

This mode is especially useful for running in the cloud environments
where you don't want to setup a shared filesystem between the worker
nodes. Running in that mode is explained in detail
:ref:`here. <amazon-aws>`

.. tip::

   Set  **pegasus.data.configuration** to **nonsharedfs** to run in this
   configuration. The staging site can be specified using the
   **--staging-site** option to pegasus-plan.

In this setup, Pegasus always stages the input files through the staging
site i.e the stage-in job stages in data from the input site to the
staging site. The PegasusLite jobs that start up on the worker nodes,
then pull the input data from the staging site for each job. In some
cases, it might be useful to setup the PegasusLite jobs to pull input
data directly from the input site without going through the staging
server. More details can be found at :ref:`bypass-input-staging`.

.. _shared-fs:

Shared File System
------------------

In this setup, Pegasus runs workflows in the shared file system
setup, where the worker nodes and the head node of a cluster share a
filesystem.

.. figure:: ../images/data-configuration-sharedfs.png
   :alt: Shared File System Setup
   :width: 100.0%

   Shared File System Setup

The data flow is as follows in this case

1. Stagein Job executes ( either on Submit Host or Head Node ) to stage
   in input data from Input Sites ( 1---n) to a workflow specific
   execution directory on the shared filesystem.

2. Compute Job starts on a worker node in the workflow execution
   directory. Accesses the input data using Posix IO

3. Compute Job executes on the worker node and writes out output data to
   workflow execution directory using Posix IO

4. Stageout Job executes ( either on Submit Host or Head Node ) to stage
   out output data from the workflow specific execution directory to a
   directory on the final output site.

..

.. tip::

   Set **pegasus.data.configuration** to **sharedfs** to run in this
   configuration.



Pegasus-Plan
============

pegasus-plan is the main executable that takes in the abstract workflow
( DAX ) and generates an executable workflow ( usually a Condor DAG ) by
querying various catalogs and performing several refinement steps.
Before users can run pegasus plan the following needs to be done:

1. Populate the various catalogs

   1. **Replica Catalog**

      The Replica Catalog needs to be catalogued with the locations of
      the input files required by the workflows. This can be done by
      using pegasus-rc-client (See the Replica section of :ref:`Creating
      Workflows <replica>`).

      By default Pegasus picks up a file named **replicas.yml** in the
      current working directory ( from where pegasus-plan is invoked) as
      the Replica Catalog for planning.

   2. **Transformation Catalog**

      The Transformation Catalog needs to be catalogued with the
      locations of the executables that the workflows will use. This can
      be done by using pegasus-tc-client (See the Transformation section
      of :ref:`Creating Workflows <transformation>`).

      By default Pegasus picks up a file named **transformations.yml** in the
      current working directory ( from where pegasus-plan is invoked) as
      the Transformation Catalog for planning.

   3. **Site Catalog**

      The Site Catalog needs to be catalogued with the site layout of
      the various sites that the workflows can execute on. A site
      catalog can be generated for OSG by using the client
      pegasus-sc-client (See the Site section of the :ref:`Creating
      Workflows <site>`).

      By default Pegasus picks up a file named **sites.yml** in the
      current working directory ( from where pegasus-plan is invoked) as
      the Site Catalog for planning.

2. Configure Properties

   After the catalogs have been configured, the user properties file
   need to be updated with the types and locations of the catalogs to
   use. These properties are described in the **basic.properties** files
   in the **etc** sub directory (see the Properties section of
   the :ref:`Configuration <props>` chapter.

   The basic properties that you may need to be set if using non default
   types and locations are for various catalogs are listed below:

   .. table:: Basic Properties that you may need to set

      ====================================== ===============================
      pegasus.catalog.replica                type of replica catalog backend
      pegasus.catalog.replica.file           path to replica catalog file
      pegasus.catalog.transformation         type of transformation catalog
      pegasus.catalog.transformation.file    path to transformation file
      pegasus.catalog.site.file              path to site catalog file
      pegasus.data.configuration             the data configuration mode for
                                             data staging.
      ====================================== ===============================


To execute pegasus-plan user usually requires to specify the following
options:


1. **--dir** the base directory where the executable workflow is
   generated

2. **--sites** comma separated list of execution sites. By default,
   Pegasus assumes a site named **condorpool** as your execution
   site.

3. **--output** the output site where to transfer the materialized
   output files.

4. **--submit** boolean value whether to submit the planned workflow for
   execution after planning is done.

5. the path to the DAX file that needs to be mapped.



.. include:: _basic-properties.rst
