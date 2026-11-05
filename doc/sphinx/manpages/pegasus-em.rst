.. _cli-pegasus-em:

==========
pegasus-em
==========

Submit and monitor ensembles of workflows

   ::

      pegasus-em COMMAND [options] [ARGUMENT…]


Commands
========

**server** [-d] [-p *PORT*]
   Start the ensemble manager server. It listens on 127.0.0.1 only.

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

**cron-trigger** *ENSEMBLE* *TRIGGER* *INTERVAL* *WORKFLOW_SCRIPT* [-t TIMEOUT] [--args ARG1 [ARG2 ...]]
   Start a timed workflow trigger.

**file-pattern-trigger** *ENSEMBLE* *TRIGGER* *INTERVAL* *WORKFLOW_SCRIPT* *FILE_PATTERN* [FILE_PATTERN ...] [-t TIMEOUT] [--args ARG1 [ARG2 ...]]
   Start a timed, file pattern workflow trigger.

**triggers** *ENSEMBLE* [-l]
   List the triggers in an ensemble.

Common Options
==============

**-h**; \ **--help**
   Print help message

**-d**; \ **--debug**
   Enable debugging



Server Options
==============

**-p** *PORT*; \ **--port** *PORT*
   Port to listen on (1024-65535). Overrides **EM_PORT** in the configuration
   file. The default port is derived from your UID.



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



Triggers Options
================

**-l**; \ **--long**
   Use long listing format.



Configuration
=============

No configuration file is required. Settings can optionally be placed in
``~/.pegasus/service.json``; keys must be UPPERCASE, e.g.

   ::

      {
          "EM_PORT": 8000,
          "EM_INTERVAL": 60
      }

**EM_PORT**
   Port the server listens on and the client connects to. **server -p**
   takes precedence.

**AUTHENTICATION**
   Defaults to **LocalTokenAuthentication**: on startup the server writes its
   port and a random access token to ``~/.pegasus/em.server.json`` (mode
   0600), and the client reads that file automatically. Other users on the
   same machine cannot read the token and therefore cannot use your ensemble
   manager. A new token is created each time the server starts.

   Set it to **NoAuthentication** to disable authentication. This lets any
   user who can connect to 127.0.0.1 run commands as you, so do not use it on
   shared machines such as cluster login nodes. Set it to
   **PAMAuthentication** to authenticate with **USERNAME** and **PASSWORD**
   from the configuration file.
