==========
pegasus-em
==========

Submit and monitor ensembles of workflows

   ::

      pegasus-em COMMAND [options] [ARGUMENT…]


Commands
========

**server** [-d]
   Start the ensemble manager server.

**ensembles**
   List ensembles.

**create** *ENSEMBLE* [-R *MAX_RUNNING*] [-P *MAX_PLANNING*]
   Create an ensemble.

**pause** *ENSEMBLE*
   Pause ensemble.

**activate** *ENSEMBLE*
   Activate a paused ensemble.

**config** *ENSEMBLE* [-R *MAX_RUNNING*] \| [-P *MAX_PLANNING*]
   Configure an ensemble.

**submit** *ENSEMBLE.WORKFLOW* *plan_command* [ARGUMENT…]
   Submit a workflow. The command is either **pegasus-plan**, or a shell
   script that calls **pegasus-plan**. The output of *plan_command* must
   contain the output of **pegasus-plan**.

**workflows** *ENSEMBLE* [-l]
   List the workflows in an ensemble.

**replan** *ENSEMBLE.WORKFLOW*
   Replan a failed workflow.

**rerun** *ENSEMBLE.WORKFLOW*
   Rerun a failed workflow.

**status** *ENSEMBLE.WORKFLOW*
   Display the status of a workflow.

**analyze** *ENSEMBLE.WORKFLOW*
   Analyze the current state of a workflow.

**priority** *ENSEMBLE.WORKFLOW* -p *PRIORITY*
   Alter the priority of a workflow.



Common Options
==============

**-h**; \ **--help**
   Print help message

**-d**; \ **--debug**
   Enable debugging



Create and Config Options
=========================

**-R** *N*; \ **--max-running** *N*
   Maximum number of concurrently running workflows.

**-P** *N*; \ **--max-planning** *N*
   Maximum number of workflows being planned simultaneously.



Workflows Options
=================

**-l**; \ **--long**
   Use long listing format.



Authors
=======

Pegasus Team ``<pegasus@isi.edu>``
