.. _cli-pegasus-halt:

============
pegasus-halt
============

stops a workflow gracefully, current jobs will finish

   ::

      pegasus-halt [rundir]



Description
===========

**pegasus-halt** stops a workflow gracefully by allowing the jobs
already running to finish on their own. No new jobs will be submitted.
Once all jobs have finished, the workflow will stop. A stopped workflow
can be restarted with the pegasus-run command.

Another way to remove a workflow is with the pegasus-remove command. The
difference is that pegasus-remove will stop running jobs.



Options
=======

**rundir**
   The run directory of the workflow you want to stop



Authors
=======

Pegasus Team http://pegasus.isi.edu
