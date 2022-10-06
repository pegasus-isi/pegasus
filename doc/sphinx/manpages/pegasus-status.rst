.. _cli-pegasus-status:

==============
pegasus-status
==============

1
pegasus-status
Pegasus workflow- and run-time status

   ::

      pegasus-status [-h|--help]
                     [-l|--long]
                     [-d|--debug]
                     [-w|--watch <sec>]
                     [-L|--legend]
                     [--noqueue]
                     [-j|--jsonrv]
                     [rundir]



Description
===========

**pegasus-status** shows the current state of the Condor Q and a
workflow, depending on settings. It requires a valid run directory to
to show the status of a workflow. If no valid run directory could be
determined, it throws an error with the specified reason for it. If a run 
directory was specified, status about the workflow will be displayed.

Many options will modify the behavior of this program, such as : watch mode,
the presence of jobs in the queue, progress in the workflow directory,
etc. It's important to note that **-j** option takes precedence over all
other options and will result in returning the status in a json structure
format.



Options
=======

**-h**; \ **--help**
   Prints a concise help and exits.

**-V**; \ **--version**
   Prints the version information and exits.

**-w** [*sec*]; \ **--watch** [*sec*]
   This option enables the *watch mode*. In watch mode, the program
   repeatedly polls the status sources and shows them in an updating
   window. This option requires an argument *sec* to determine how
   often these sources are polled.

   We *strongly* recommend to set this interval not too low, as frequent
   polling will degrade the scheduler performance and increase the host
   load. The *sec* argument recommended to be entered is 60 seconds.

   Watch mode is disabled by default.

**-L**; \ **--legend**
   This option shows a legend explaining the columns in the output

   By default, legends are turned off to save terminal real estate.

**-Q**; \ **--noqueue**
   This option turns off the output from parsing Condor Q.

   By default, Condor Q will be parsed for jobs belonging to the workflow
   run directory specified as an arguemnt.

**-d**; \ **--debug**
   This is an internal debugging tool and should not be used outside the
   development team. In debugging mode, it will show the condor Q expression
   used to retrieve the jobs in Q belonging to the workflow run directory
   entered. It also shows the traversal of all the DAG sub-directories'
   used to retrieve the progress of the DAG (and sub-DAGs, if any).

   By default, debug mode is off.

**-l**; \ **--long**
   This option will show one line per sub-DAG, including one line for
   the workflow. In case of hierarchical workflows, now each sub-dag
   is shown along with indented branch. This option now shows more columns
   for jobs in condor Q.

   By default, only DAG totals (sums) are shown.

**-D**; \ **--dirs**
   This option can be used along with **--long** option in case of hierarchical
   workflows, to show relative directories under *dagnames* per sub-DAG. It's
   main usage comes for hierarichal workflows.

   By default, only dag names are shown.

**-j**
   This option returns the status of the workflow in a JSON serializable data
   structure (Python dict). Sample of this structure is shown below, where the
   keys are - 
+ *totals* : contains the overall progress of the workflow
+ *dags* : contains progress regarding each workflow, in case of hierarchical workflows each sub-DAG with it's name as corresponding key
+ *condor_jobs*: contains all the jobs in Q belonging to a specific workflow, with it's unique *wf_uuid* as corresponding key. Furthermore, each workflow has *DAG_NAME* key and *DAG_CONDOR_JOBS* key with a list of condor Q jobs of the corresponding DAG
| If there are no jobs of the workflow in the condor Q, *condor_jobs* is absent from the returned structure. By default, **-j** option is off.
   
.. code-block:: json

    {
  "totals": {
    "unready": 13,
    "ready": 0,
    "pre": 0,
    "queued": 1,
    "post": 1,
    "succeeded": 14,
    "failed": 0,
    "percent_done": 48.28,
    "total": 29
  },
  "dags": {
    "root": {
      "unready": 4,
      "ready": 0,
      "pre": 0,
      "queued": 1,
      "post": 0,
      "succeeded": 8,
      "failed": 0,
      "percent_done": 61.54,
      "total": 13,
      "dagname": "workflow-0.dag",
      "state": "Running"
    }
  },
  "condor_jobs": {
    "f436c93a-5ef5-4d9f-815a-0ccee5e9de67": {
      "DAG_NAME": "root",
      "DAG_CONDOR_JOBS": [
        {
          "ClusterId": 2457,
          "Cmd": "/usr/bin/pegasus-dagman",
          "EnteredCurrentStatus": 1664819625,
          "Iwd": "workflow/run/directory/run0001",
          "JobPrio": 0,
          "JobStatus": "Run",
          "pegasus_site": "local",
          "pegasus_wf_name": "hierarchical-workflow-0",
          "pegasus_wf_xformation": "pegasus::dagman",
          "UserLog": "run/directory/run0001/workflow-0.dag.dagman.log"
        },
        {
          "ClusterId": 2465,
          "Cmd": "/usr/bin/condor_dagman",
          "EnteredCurrentStatus": 1664819681,
          "Iwd": "workflow/run/directory/run0001/00/00/./inner",
          "JobPrio": 30,
          "JobStatus": "Run",
          "pegasus_site": "local",
          "pegasus_wf_dag_job_id": "pegasus-plan_diamond_subworkflow",
          "pegasus_wf_dax_job_id": "diamond_subworkflow",
          "pegasus_wf_name": "hierarchical-workflow-0",
          "pegasus_wf_xformation": "condor::dagman",
          "UserLog": "run/directory/run0001/workflow-0.log"
        }
      ]
     }
   }
 }

*rundir*
   This option shows statistics about the given DAG that runs in
   *rundir*. To gather proper statistics, **pegasus-status** needs to
   traverse the directory and all sub-directories. This can become an
   expensive operation on shared filesystems.

   If the current directory is not a valid *rundir*, no DAG statistics
   will be shown.



Return Value
============

**pegasus-status** will typically return success in regular mode, and
the termination signal in watch mode. Abnormal behavior will result in a
non-zero exit code.



Example
=======

**pegasus-status rundir**
   This invocation will parse the Condor Q for the current user and show
   all her jobs. Additionally, if the current directory is a valid
   Pegasus workflow directory, totals about the DAG in that directory
   are displayed.

**pegasus-status -l rundir**
   As above, but providing a specific Pegasus workflow directory in
   argument *rundir* and requesting to itemize sub-DAGs.

**pegasus-status -w 300 -Ll**
   This invocation will parse the queue,show legends, 
   itemize DAG statistics of the current working
   directory, and redraw the terminal every five minutes with updated
   statistics.


Restrictions
============

Currently only supports a single run directory. If you want
to watch multiple run directories, we suggest to open multiple terminals
and watch them separately. If that is not an option, or deemed too
expensive, you can ask *pegasus-support at isi dot edu* to extend the
program.



See Also
========

condor_q(1), pegasus-statistics(1)
