===========
pegasus-run
===========

1
pegasus-run
executes a workflow that has been planned using \*pegasus-plan*.
   ::

      pegasus-run [-Dproperty=value…][-c propsfile][-d level]
                  [-v][--grid*][rundir]

.. __description:

Description
===========

The **pegasus-run** command executes a workflow that has been planned
using **pegasus-plan**. By default pegasus-run can be invoked either in
the planned directory with no options and arguments or just the full
path to the run directory. **pegasus-run** also can be used to resubmit
a failed workflow by running the same command again.

.. __options:

Options
=======

By default **pegasus-run** does not require any options or arguments if
invoked from within the planned workflow directory. If running the
command outside the workflow directory then a full path to the workflow
directory needs to be specified.

**pegasus-run** takes the following options

**-D**\ *property=value*
   The **-D** option allows an advanced user to override certain
   properties which influence **pegasus-run**. One may set several CLI
   properties by giving this option multiple times.

   The **-D** option(s) must be the first option on the command line.
   CLI properties take precedence over the file-based properties of the
   same key.

   See the `PROPERTIES <#PROPERTIES>`__ section below.

**-c** *propsfile*; \ **--conf** *propsfile*
   Provide a property file to override the default Pegasus properties
   file from the planning directory. Ordinary users do not need to use
   this option unless the specifically want to override several
   properties

**-d** *level*; \ **--debug** *level*
   Set the debug level for the client. Default is 0.

**-v**; \ **--verbose**
   Raises debug level. Each invocation increase the level by 1.

**--grid**
   Enable grid checks to see if your submit machine is GRID enabled.

*rundir*
   Is the full qualified path to the base directory containing the
   planned workflow DAG and submit files. This is optional if the
   **pegasus-run** command is invoked from within the run directory.

.. __return_value:

Return Value
============

If the workflow is submitted for execution **pegasus-run** returns with
an exit code of 0. However, in case of error, a non-zero return value
indicates problems. An error message clearly marks the cause.

.. __files:

Files
=====

The following files are created, opened or written to:

**braindump**
   This file is located in the rundir. pegasus-run uses this file to
   find out paths to several other files, properties configurations etc.

**pegasus.?????????.properties**
   This file is located in the rundir. pegasus-run uses this properties
   file by default to configure its internal settings.

**workflowname.dag**
   pegasus-run uses the workflowname.dag or workflowname.sh file and
   submits it either to condor for execution or runs it locally in a
   shell environment

.. _PROPERTIES:

Properties
==========

pegasus-run reads its properties from several locations.

**RUNDIR/pegasus.??????????.properties**
   The default location for pegasus-run to read the properties from

**--conf propfile**
   properties file provided in the conf option replaces the default
   properties file used.

**$HOME/.pegasusrc**
   will be used if neither default rundir properties or --conf
   propertiesfile are found.

   Additionally properties can be provided individually using the
   **-Dpropkey**\ =\ *propvalue* option on the command line before all
   other options. These properties will override properties provided
   using either **--conf** or *RUNDIR/pegasus.???????.properties* or the
   *$HOME/.pegasusrc*

   The merge logic is CONF PROPERTIES \|\| DEFAULT RUNDIR PROPERTIES
   \|\| PEGASUSRC overriden by Command line properties

.. __environment_variables:

Environment Variables
=====================

**PATH**
   The path variable is used to locate binaries for condor-submit-dag,
   condor-dagman, condor-submit,pegasus-submit-dag, pegasus-dagman and
   pegasus-monitord

.. __see_also:

See Also
========

pegasus-plan(1)

.. __authors:

Authors
=======

Gaurang Mehta ``<gmehta at isi dot edu>``

Jens-S. Vöckler ``<voeckler at isi dot edu>``

Pegasus Team http://pegasus.isi.edu
