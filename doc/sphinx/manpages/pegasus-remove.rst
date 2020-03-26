==============
pegasus-remove
==============

1
pegasus-remove
removes a workflow that has been planned and submitted using
pegasus-plan and pegasus-run
   ::

      pegasus-remove [-d dagid] [-v] [rundir]

.. __description:

Description
===========

The pegasus-remove command remove a submitted/running workflow that has
been planned and submitted using **pegasus-plan** and **pegasus-run**.
The command can be invoked either in the planned directory with no
options and arguments or just the full path to the run directory.

Another way to remove a workflow is with the pegasus-halt command. The
difference is that pegasus-halt will allow current jobs to finish
gracefully before stopping the workflow.

.. __options:

Options
=======

By default pegasus-remove does not require any options or arguments if
invoked from within the planned workflow directory. If running the
command outside the workflow directory then a full path to the workflow
directory needs to be specified or the *dagid* of the workflow to be
removed.

**pegasus-remove** takes the following options:

**-d** *dagid*; \ **--dagid** *dagid*
   The workflow dagid to remove

**-v**; \ **--verbose**
   Raises debug level. Each invocation increase the level by 1.

*rundir*
   Is the full qualified path to the base directory containing the
   planned workflow DAG and submit files. This is optional if
   pegasus-remove command is invoked from within the run directory.

.. __return_value:

Return Value
============

If the workflow is removed successfully pegasus-remove returns with an
exit code of 0. However, in case of error, a non-zero exit code
indicates problems. An error message clearly marks the cause.

.. __files:

Files
=====

The following files are opened:

**braindump**
   This file is located in the rundir. pegasus-remove uses this file to
   find out paths to several other files.

.. __environment_variables:

Environment Variables
=====================

**PATH**
   The path variable is used to locate binary for **condor_rm**.

.. __see_also:

See Also
========

pegasus-plan(1), pegasus-run(1)

.. __authors:

Authors
=======

Gaurang Mehta <gmehta at isi dot edu>

Jens-S. Vöckler <voeckler at isi dot edu>

Pegasus Team http://pegasus.isi.edu