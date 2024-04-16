.. _pegasuslite:

===========
PegasusLite
===========

Starting Pegasus 4.0 , all compute jobs ( single or clustered jobs) that
are executed in a non shared filesystem setup, are executed using
lightweight job wrapper called PegasusLite.

.. figure:: ../images/data-configuration-pegasuslite.png
   :alt: Workflow Running in NonShared Filesystem Setup with PegasusLite launching compute jobs
   :width: 100.0%

   Workflow Running in NonShared Filesystem Setup with PegasusLite
   launching compute jobs

When PegasusLite starts on a remote worker node to run a compute job ,
it performs the following actions:

1. Discovers the best run-time directory based on space requirements and
   create the directory on the local filesystem of the worker node to
   execute the job.

2. Prepare the node for executing the unit of work. This involves
   discovering whether the pegasus worker tools are already installed on
   the node or need to be brought in.

3. Use pegasus-transfer to stage in the input data to the runtime
   directory (created in step 1) on the remote worker node.

4. If enabled, do integrity checking on the input files transferred
   to the remote worker node. This is done by computing a new
   checksum on the file staged and matching it with the one in the
   job description.

5. Launch the compute job.

6. Use pegasus-transfer to stage out the output data to the data
   coordination site.

7. Remove the directory created in Step 1.

.. note::

   If you are using containers for your workflow, then Steps 3-6
   will occur inside the container.

.. _source-env-in-pegasuslite:

Setting the environment in PegasusLite for your job
===================================================

In addition, to the usual environment profiles that you can associate
with your job in the various catalogs, PegasusLite allows you to specify
a user provided environment setup script file that is sourced early in the
generated PegasusLite wrapper. The purpose of this is to allow users
to specify to do things such as module load to load appropriate libraries
required by their jobs; when they run on nodes on a cluster.

In order to specify this setup script, you can specify it in the Site Catalog
either as a

#. Pegasus profile named **pegasus_lite_env_source** associated with site
   *local* that indicates a path to a setup script residing on the
   submit host that needs to be sourced in PegasusLite when running the job.
   This file is then transferred using Condor file transfer from the submit
   host to the compute node where the job executes.

#. If the setup script already is present on the compute nodes on the cluster;
   path to it can be set as an env profile named **PEGASUS_LITE_ENV_SOURCE**
   with the compute site.

.. _separate-compute-jobs-dtn:

Specify Compute Job in PegasusLite to run on different node
===========================================================

When running workflows on systems such as OLCF summit, data staging can be tricky
for PegasusLite jobs. The data staging needs to happen on the cluster Service nodes,
while the compute job need to be launched using the *jsrun* command to execute
on the compute nodes.

For example; an invocation of a compute job in PegasusLite would need to
look like this

..

    jsrun -n 1 -a 1 -c 42 -g 0 /path/to/kickstart user-executable args

The above cannot be achieved by specifying a job wrapper, as mentioning
the wrapper as the executable path in TC, as in that case
*pegasus-kickstart* will run on the Service node, and invoke the jsrun command.

To get this behavior you can specify the following Pegasus Profile keys
with your job

#. **gridstart.launcher** : Specifies the launcher executable to use to
   launch the GridStart(*pegasus-kickstart*). In the above example value
   for this would be jsrun.

#. **gridstart.launcher.arguments**: Specifies the arguments to pass to
   the launcher. In the above example, value for this would be
   -n 1 -a 1 -c 42 -g 0 .

.. _separate-containterized-compute-job:

Specify Wrapper to Launch a Containerized Compute Job in PegasusLite
=====================================================================

When running workflows with containerized jobs such as `Tensor Flow`, `PyTorch`
or even Open MPI jobs via PegasusLite, one may encounter a situation where the actual
*docker* or *singularity* invocation has to be prefixed with srun for example,
to allow for a job to detect and use the multiple cores on a node.

For example; an invocation of a containerized compute job in PegasusLite would
need to look like this

..

    srun --kill-on-bad-exit $singularity_exec exec --no-home --bind $PWD:/srv --bind /scratch image.sif /srv/job-cont.sh


The above cannot be achieved by specifying a gridstart launcher, as mentioning
the launcher for the job will wrap *pegasus-kickstart* invocation that happens
inside the container as part of the /srv/job-cont.sh. In this scenario, we want the wrapper
(srun) to run on the HOST OS.

To get this behavior you can specify the following Pegasus Profile keys
with your job

#. **container.launcher** : Specifies the launcher executable to use to
   launch the singularity|docker invocation. In the above example value
   for this would be srun.

#. **container.launcher.arguments**: Specifies the arguments to pass to
   the launcher. In the above example, value for this would be
   --kill-on-bad-exit .