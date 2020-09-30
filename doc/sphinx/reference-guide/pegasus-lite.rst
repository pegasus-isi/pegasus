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

