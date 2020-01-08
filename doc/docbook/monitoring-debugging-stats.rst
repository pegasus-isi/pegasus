.. _monitoring-debugging-stats:

====================================
Monitoring, Debugging and Statistics
====================================

Pegasus comes bundled with useful tools that help users debug workflows
and generate useful statistics and plots about their workflow runs. Most
of the tools query a runtime workflow database ( usually a sqllite in
the workflow submit directory ) populated at runtime by
pegasus-monitord. With the exception of pegasus-monitord (see below),
all tools take in the submit directory as an argument. Users can invoke
the tools listed in this chapter as follows:

::

   $ pegasus-[toolname]   <path to the submit directory>

.. _workflow-status:

Workflow Status
===============

As the number of jobs and tasks in workflows increase, the ability to
track the progress and quickly debug a workflow becomes more and more
important. Pegasus comes with a series of utilities that can be used to
monitor and debug workflows both in real-time as well as after execution
is already completed.

.. _monitoring-pegasus-status:

pegasus-status
--------------

To monitor the execution of the workflow run the ``pegasus-status``
command as suggested by the output of the ``pegasus-run`` command.
``pegasus-status`` shows the current status of the Condor Q as
pertaining to the master workflow from the workflow directory you are
pointing it to. In a second section, it will show a summary of the state
of all jobs in the workflow and all of its sub-workflows.

The details of ``pegasus-status`` are described in its respective
`manual page <#cli-pegasus-status>`__. There are many options to help
you gather the most out of this tool, including a watch-mode to
repeatedly draw information, various modes to add more information, and
legends if you are new to it, or need to present it.

::

   $ pegasus-status /Workflow/dags/directory
   STAT  IN_STATE  JOB
   Run      05:08  level-3-0
   Run      04:32   |-sleep_ID000005
   Run      04:27   \_subdax_level-2_ID000004
   Run      03:51      |-sleep_ID000003
   Run      03:46      \_subdax_level-1_ID000002
   Run      03:10         \_sleep_ID000001
   Summary: 6 Condor jobs total (R:6)

   UNREADY   READY     PRE  QUEUED    POST SUCCESS FAILURE %DONE
         0       0       0       6       0       3       0  33.3
   Summary: 3 DAGs total (Running:3)

Without the ``-l`` option, the only a summary of the workflow statistics
is shown under the current queue status. However, with the ``-l``
option, it will show each sub-workflow separately:

::

   $ pegasus-status -l /Workflow/dags/directory
   STAT  IN_STATE  JOB
   Run      07:01  level-3-0
   Run      06:25   |-sleep_ID000005
   Run      06:20   \_subdax_level-2_ID000004
   Run      05:44      |-sleep_ID000003
   Run      05:39      \_subdax_level-1_ID000002
   Run      05:03         \_sleep_ID000001
   Summary: 6 Condor jobs total (R:6)

   UNRDY READY   PRE  IN_Q  POST  DONE  FAIL %DONE STATE   DAGNAME
       0     0     0     1     0     1     0  50.0 Running level-2_ID000004/level-1_ID000002/level-1-0.dag
       0     0     0     2     0     1     0  33.3 Running level-2_ID000004/level-2-0.dag
       0     0     0     3     0     1     0  25.0 Running *level-3-0.dag
       0     0     0     6     0     3     0  33.3         TOTALS (9 jobs)
   Summary: 3 DAGs total (Running:3)

The following output shows a successful workflow of workflow summary
after it has finished.

::

   $ pegasus-status work/2011080514
   (no matching jobs found in Condor Q)
   UNREADY   READY     PRE  QUEUED    POST SUCCESS FAILURE %DONE
         0       0       0       0       0   7,137       0 100.0
   Summary: 44 DAGs total (Success:44)

..

   **Warning**

   For large workflows with many jobs, please note that
   ``pegasus-status`` will take time to compile state from all workflow
   files. This typically affects the initial run, and sub-sequent runs
   are faster due to the file system's buffer cache. However, on a
   low-RAM machine, thrashing is a possibility.

The following output show a failed workflow after no more jobs from it
exist. Please note how no active jobs are shown, and the failure status
of the total workflow.

::

   $ pegasus-status work/submit
   (no matching jobs found in Condor Q)
   UNREADY   READY     PRE  QUEUED    POST SUCCESS FAILURE %DONE
        20       0       0       0       0       0       2   0.0
   Summary: 1 DAG total (Failure:1)

.. _monitoring-pegasus-analyzer:

pegasus-analyzer
----------------

Pegasus-analyzer is a command-line utility for parsing several files in
the workflow directory and summarizing useful information to the user.
It should be used after the workflow has already finished execution.
pegasus-analyzer quickly goes through the jobstate.log file, and
isolates jobs that did not complete successfully. It then parses their
submit, and kickstart output files, printing to the user detailed
information for helping the user debug what happened to his/her
workflow.

The simplest way to invoke pegasus-analyzer is to simply give it a
workflow run directory, like in the example below:

::

   $ pegasus-analyzer  /home/user/run0004
   pegasus-analyzer: initializing...

   ************************************Summary*************************************

    Total jobs         :     26 (100.00%)
    # jobs succeeded   :     25 (96.15%)
    # jobs failed      :      1 (3.84%)
    # jobs held        :      1 (3.84%)
    # jobs unsubmitted :      0 (0.00%)

   *******************************Held jobs' details*******************************

   ================================sleep_ID0000001=================================

          submit file            : sleep_ID0000001.sub
          last_job_instance_id   : 7
          reason                 :  Error from slot1@corbusier.isi.edu:
                                    STARTER at 128.9.64.188 failed to
                                    send file(s) to
                                    <128.9.64.188:62639>: error reading from
                                    /opt/condor/8.4.8/local.corbusier/execute/dir_76205/f.out:
                                    (errno 2) No such file or directory;
                                   SHADOW failed to receive file(s) from <128.9.64.188:62653>

   ******************************Failed jobs' details******************************

   ============================register_viz_glidein_7_0============================

    last state: POST_SCRIPT_FAILURE
          site: local
   submit file: /home/user/run0004/register_viz_glidein_7_0.sub
   output file: /home/user/run0004/register_viz_glidein_7_0.out.002
    error file: /home/user/run0004/register_viz_glidein_7_0.err.002

   -------------------------------Task #1 - Summary--------------------------------

   site        : local
   executable  : /lfs1/software/install/pegasus/default/bin/rc-client
   arguments   : -Dpegasus.user.properties=/lfs1/work/pegasus/run0004/pegasus.15181.properties \
   -Dpegasus.catalog.replica.url=rlsn://smarty.isi.edu --insert register_viz_glidein_7_0.in
   exitcode    : 1
   working dir : /lfs1/work/pegasus/run0004

   ---------Task #1 - pegasus::rc-client - pegasus::rc-client:1.0 - stdout---------

   2009-02-20 16:25:13.467 ERROR [root] You need to specify the pegasus.catalog.replica property
   2009-02-20 16:25:13.468 WARN  [root] non-zero exit-code 1

In the case above, pegasus-analyzer's output contains a brief summary
section, showing how many jobs have succeeded and how many have failed.
If there are any held jobs, pegasus-analyzer will report the name of the
job that was held, and the reason why , as determined from the
dagman.out file for the workflow. The last_job_instance_id is the
database id for the job in the job instance table of the monitoring
database. After that, pegasus-analyzer will print information about each
job that failed, showing its last known state, along with the location
of its submit, output, and error files. pegasus-analyzer will also
display any stdout and stderr from the job, as recorded in its kickstart
record. Please consult pegasus-analyzer's man page for more examples and
a detailed description of its various command-line options.

   **Note**

   Starting with 4.0 release, by default pegasus analyzer queries the
   database to debug the workflow. If you want it to use files in the
   submit directory , use the **--files** option.

.. _monitoring-pegasus-remove:

pegasus-remove
--------------

If you want to abort your workflow for any reason you can use the
pegasus-remove command listed in the output of pegasus-run invocation or
by specifying the Dag directory for the workflow you want to terminate.

::

   $ pegasus-remove /PATH/To/WORKFLOW DIRECTORY

Resubmitting failed workflows
-----------------------------

Pegasus will remove the DAGMan and all the jobs related to the DAGMan
from the condor queue. A rescue DAG will be generated in case you want
to resubmit the same workflow and continue execution from where it last
stopped. A rescue DAG only skips jobs that have completely finished. It
does not continue a partially running job unless the executable supports
checkpointing.

To resubmit an aborted or failed workflow with the same submit files and
rescue Dag just rerun the pegasus-run command

::

   $ pegasus-run /Path/To/Workflow/Directory

.. _plotting-statistics:

Plotting and Statistics
=======================

Pegasus plotting and statistics tools queries the Stampede database
created by pegasus-monitord for generating the output.The stampede
scheme can be found `here <#stampede_schema_overview>`__.

The statistics and plotting tools use the following terminology for
defining tasks, jobs etc. Pegasus takes in a DAX which is composed of
tasks. Pegasus plans it into a Condor DAG / Executable workflow that
consists of Jobs. In case of Clustering, multiple tasks in the DAX can
be captured into a single job in the Executable workflow. When DAGMan
executes a job, a job instance is populated . Job instances capture
information as seen by DAGMan. In case DAGMan retires a job on detecting
a failure , a new job instance is populated. When DAGMan finds a job
instance has finished , an invocation is associated with job instance.
In case of clustered job, multiple invocations will be associated with a
single job instance. If a Pre script or Post Script is associated with a
job instance, then invocations are populated in the database for the
corresponding job instance.

pegasus-statistics
------------------

Pegasus statistics can compute statistics over one or more than one
workflow run.

Command to generate statistics over a single run is as shown below.

::

   $ pegasus-statistics /scratch/grid-setup/run0001/ -s all

   #
   # Pegasus Workflow Management System - http://pegasus.isi.edu
   #
   # Workflow summary:
   #   Summary of the workflow execution. It shows total
   #   tasks/jobs/sub workflows run, how many succeeded/failed etc.
   #   In case of hierarchical workflow the calculation shows the
   #   statistics across all the sub workflows.It shows the following
   #   statistics about tasks, jobs and sub workflows.
   #     * Succeeded - total count of succeeded tasks/jobs/sub workflows.
   #     * Failed - total count of failed tasks/jobs/sub workflows.
   #     * Incomplete - total count of tasks/jobs/sub workflows that are
   #       not in succeeded or failed state. This includes all the jobs
   #       that are not submitted, submitted but not completed etc. This
   #       is calculated as  difference between 'total' count and sum of
   #       'succeeded' and 'failed' count.
   #     * Total - total count of tasks/jobs/sub workflows.
   #     * Retries - total retry count of tasks/jobs/sub workflows.
   #     * Total+Retries - total count of tasks/jobs/sub workflows executed
   #       during workflow run. This is the cumulative of retries,
   #       succeeded and failed count.
   # Workflow wall time:
   #   The wall time from the start of the workflow execution to the end as
   #   reported by the DAGMAN.In case of rescue dag the value is the
   #   cumulative of all retries.
   # Cumulative job wall time:
   #   The sum of the wall time of all jobs as reported by kickstart.
   #   In case of job retries the value is the cumulative of all retries.
   #   For workflows having sub workflow jobs (i.e SUBDAG and SUBDAX jobs),
   #   the wall time value includes jobs from the sub workflows as well.
   # Cumulative job wall time as seen from submit side:
   #   The sum of the wall time of all jobs as reported by DAGMan.
   #   This is similar to the regular cumulative job wall time, but includes
   #   job management overhead and delays. In case of job retries the value
   #   is the cumulative of all retries. For workflows having sub workflow
   #   jobs (i.e SUBDAG and SUBDAX jobs), the wall time value includes jobs
   #   from the sub workflows as well.
   # Cumulative job badput wall time:
   #   The sum of the wall time of all failed jobs as reported by kickstart.
   #   In case of job retries the value is the cumulative of all retries.
   #   For workflows having sub workflow jobs (i.e SUBDAG and SUBDAX jobs),
   #   the wall time value includes jobs from the sub workflows as well.
   # Cumulative job badput wall time as seen from submit side:
   #   The sum of the wall time of all failed jobs as reported by DAGMan.
   #   This is similar to the regular cumulative job badput wall time, but includes
   #   job management overhead and delays. In case of job retries the value
   #   is the cumulative of all retries. For workflows having sub workflow
   #   jobs (i.e SUBDAG and SUBDAX jobs), the wall time value includes jobs
   #   from the sub workflows as well.
   ------------------------------------------------------------------------------
   Type           Succeeded Failed  Incomplete  Total     Retries   Total+Retries
   Tasks          4         0       0           4         0         4
   Jobs           20        0       0           20        0         20
   Sub-Workflows  0         0       0           0         0         0
   ------------------------------------------------------------------------------

   Workflow wall time                                       : 6 mins, 55 secs
   Cumulative job wall time                                 : 4 mins, 58 secs
   Cumulative job wall time as seen from submit side        : 5 mins, 11 secs
   Cumulative job badput wall time                          : 0.0 secs
   Cumulative job badput wall time as seen from submit side : 0.0 secs

   Integrity Metrics
   5 files checksums compared with total duration of 0.439 secs
   8 files checksums generated with total duration of 1.031 secs

   Summary                       : ./statistics/summary.txt
   Workflow execution statistics : ./statistics/workflow.txt
   Job instance statistics       : ./statistics/jobs.txt
   Transformation statistics     : ./statistics/breakdown.txt
   Integrity statistics          : ./statistics/integrity.txt
   Time statistics               : ./statistics/time.txt

By default the output gets generated to a statistics folder inside the
submit directory. The output that is generated by pegasus-statistics is
based on the value set for command line option 's'(statistics_level). In
the sample run the command line option 's' is set to 'all' to generate
all the statistics information for the workflow run. Please consult the
pegasus-statistics man page to find a detailed description of various
command line options.

   **Note**

   In case of hierarchal workflows, the metrics that are displayed on
   stdout take into account all the jobs/tasks/sub workflows that make
   up the workflow by recursively iterating through each sub workflow.

Command to generate statistics over all workflow runs populated in a
single database is as shown below.

::

   $ pegasus-statistics -Dpegasus.monitord.output='mysql://s_user:s_user123@127.0.0.1:3306/stampede' -o /scratch/workflow_1_2/statistics -s all --multiple-wf


   #
   # Pegasus Workflow Management System - http://pegasus.isi.edu
   #
   # Workflow summary:
   #   Summary of the workflow execution. It shows total
   #   tasks/jobs/sub workflows run, how many succeeded/failed etc.
   #   In case of hierarchical workflow the calculation shows the
   #   statistics across all the sub workflows.It shows the following
   #   statistics about tasks, jobs and sub workflows.
   #     * Succeeded - total count of succeeded tasks/jobs/sub workflows.
   #     * Failed - total count of failed tasks/jobs/sub workflows.
   #     * Incomplete - total count of tasks/jobs/sub workflows that are
   #       not in succeeded or failed state. This includes all the jobs
   #       that are not submitted, submitted but not completed etc. This
   #       is calculated as  difference between 'total' count and sum of
   #       'succeeded' and 'failed' count.
   #     * Total - total count of tasks/jobs/sub workflows.
   #     * Retries - total retry count of tasks/jobs/sub workflows.
   #     * Total+Retries - total count of tasks/jobs/sub workflows executed
   #       during workflow run. This is the cumulative of retries,
   #       succeeded and failed count.
   # Workflow wall time:
   #   The wall time from the start of the workflow execution to the end as
   #   reported by the DAGMAN.In case of rescue dag the value is the
   #   cumulative of all retries.
   # Workflow cumulative job wall time:
   #   The sum of the wall time of all jobs as reported by kickstart.
   #   In case of job retries the value is the cumulative of all retries.
   #   For workflows having sub workflow jobs (i.e SUBDAG and SUBDAX jobs),
   #   the wall time value includes jobs from the sub workflows as well.
   # Cumulative job wall time as seen from submit side:
   #   The sum of the wall time of all jobs as reported by DAGMan.
   #   This is similar to the regular cumulative job wall time, but includes
   #   job management overhead and delays. In case of job retries the value
   #   is the cumulative of all retries. For workflows having sub workflow
   #   jobs (i.e SUBDAG and SUBDAX jobs), the wall time value includes jobs
   #   from the sub workflows as well.
   # Workflow cumulative job badput wall time:
   #   The sum of the wall time of all failed jobs as reported by kickstart.
   #   In case of job retries the value is the cumulative of all retries.
   #   For workflows having sub workflow jobs (i.e SUBDAG and SUBDAX jobs),
   #   the wall time value includes jobs from the sub workflows as well.
   # Cumulative job badput wall time as seen from submit side:
   #   The sum of the wall time of all failed jobs as reported by DAGMan.
   #   This is similar to the regular cumulative job badput wall time, but includes
   #   job management overhead and delays. In case of job retries the value
   #   is the cumulative of all retries. For workflows having sub workflow
   #   jobs (i.e SUBDAG and SUBDAX jobs), the wall time value includes jobs
   #   from the sub workflows as well.

   ------------------------------------------------------------------------------
   Type           Succeeded Failed  Incomplete  Total     Retries   Total+Retries
   Tasks          8         0       0           8         0         8
   Jobs           34        0       0           34        0         34
   Sub-Workflows  0         0       0           0         0         0
   ------------------------------------------------------------------------------

   Workflow cumulative job wall time                        : 8 mins, 5 secs
   Cumulative job wall time as seen from submit side        : 8 mins, 35 secs
   Workflow cumulative job badput wall time                 : 0
   Cumulative job badput wall time as seen from submit side : 0

..

   **Note**

   When computing statistics over multiple workflows, please note,

   1. All workflow run information should be populated in a single
      STAMPEDE database.

   2. The --output argument must be specified.

   3. Job statistics information is not computed.

   4. Workflow wall time information is not computed.

Pegasus statistics can also compute statistics over a few specified
workflow runs, by specifying the either the submit directories, or the
workflow UUIDs.

::

   pegasus-statistics -Dpegasus.monitord.output='<DB_URL>' -o <OUTPUT_DIR> <SUBMIT_DIR_1> <SUBMIT_DIR_2> .. <SUBMIT_DIR_n>

   OR

   pegasus-statistics -Dpegasus.monitord.output='<DB_URL>' -o <OUTPUT_DIR> --isuuid <UUID_1> <UUID_2> .. <UUID_n>

pegasus-statistics generates the following statistics files based on the
command line options set.

.. _pegasus-statistics-summary:

Summary Statistics File [summary.txt]
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The summary statistics are listed on the stdout by default, and can be
written out to a file by providing the -s summary option.

-  **Workflow summary** - Summary of the workflow execution. In case of
   hierarchical workflow the calculation shows the statistics across all
   the sub workflows.It shows the following statistics about tasks, jobs
   and sub workflows.

   -  **Succeeded** - total count of succeeded tasks/jobs/sub workflows.

   -  **Failed** - total count of failed tasks/jobs/sub workflows.

   -  **Incomplete** - total count of tasks/jobs/sub workflows that are
      not in succeeded or failed state. This includes all the jobs that
      are not submitted, submitted but not completed etc. This is
      calculated as difference between 'total' count and sum of
      'succeeded' and 'failed' count.

   -  **Total** - total count of tasks/jobs/sub workflows.

   -  **Retries** - total retry count of tasks/jobs/sub workflows.

   -  **Total Run** - total count of tasks/jobs/sub workflows executed
      during workflow run. This is the cumulative of total retries,
      succeeded and failed count.

-  **Workflow wall time** - The wall time from the start of the workflow
   execution to the end as reported by the DAGMAN.In case of rescue dag
   the value is the cumulative of all retries.

-  **Workflow cummulate job wall time** - The sum of the wall time of
   all jobs as reported by kickstart. In case of job retries the value
   is the cumulative of all retries. For workflows having sub workflow
   jobs (i.e SUBDAG and SUBDAX jobs), the wall time value includes jobs
   from the sub workflows as well. This value is multiplied by the
   multiplier_factor in the job instance table.

-  **Cumulative job wall time as seen from submit side** - The sum of
   the wall time of all jobs as reported by DAGMan. This is similar to
   the regular cumulative job wall time, but includes job management
   overhead and delays. In case of job retries the value is the
   cumulative of all retries. For workflows having sub workflow jobs
   (i.e SUBDAG and SUBDAX jobs), the wall time value includes jobs from
   the sub workflows. This value is multiplied by the multiplier_factor
   in the job instance table.

-  **Integrity Metrics**

   -  Number of files for which the checksum was compared against a
      previously computed or provided checksum and total duration in
      seconds spent in doing it.

   -  Number of files for which the checksum was generated during
      workflow execution and total duration in seconds spent in doing
      it.

.. _pegasus-statistics-workflow:

Workflow statistics file per workflow [workflow.txt]
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Workflow statistics file per workflow contains the following information
about each workflow run. In case of hierarchal workflows, the file
contains a table for each sub workflow. The file also contains a 'Total'
table at the bottom which is the cumulative of all the individual
statistics details.

A sample table is shown below. It shows the following statistics about
tasks, jobs and sub workflows.

-  **Workflow retries** - number of times a workflow was retried.

-  **Succeeded** - total count of succeeded tasks/jobs/sub workflows.

-  **Failed** - total count of failed tasks/jobs/sub workflows.

-  **Incomplete** - total count of tasks/jobs/sub workflows that are not
   in succeeded or failed state. This includes all the jobs that are not
   submitted, submitted but not completed etc. This is calculated as
   difference between 'total' count and sum of 'succeeded' and 'failed'
   count.

-  **Total** - total count of tasks/jobs/sub workflows.

-  **Retries** - total retry count of tasks/jobs/sub workflows.

-  **Total Run** - total count of tasks/jobs/sub workflows executed
   during workflow run. This is the cumulative of total retries,
   succeeded and failed count.

.. table:: Workflow Statistics

   ==================================== ============= ========= ====== ========== ===== ======= ========= ================
   #                                    Type          Succeeded Failed Incomplete Total Retries Total Run Workflow Retries
   ==================================== ============= ========= ====== ========== ===== ======= ========= ================
   2a6df11b-9972-4ba0-b4ba-4fd39c357af4                                                                   0
   \                                    Tasks         4         0      0          4     0       4
   \                                    Jobs          13        0      0          13    0       13
   \                                    Sub Workflows 0         0      0          0     0       0
   ==================================== ============= ========= ====== ========== ===== ======= ========= ================

Job statistics file per workflow [jobs.txt]
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Job statistics file per workflow contains the following details about
the job instances in each workflow. A sample file is shown below.

-  **Job** - the name of the job instance

-  **Try** - the number representing the job instance run count.

-  **Site** - the site where the job instance ran.

-  **Kickstart(sec.)** - the actual duration of the job instance in
   seconds on the remote compute node.

-  **Mult** - multiplier factor from the job instance table for the job.

-  **Kickstart_Mult** - value of the Kickstart column multiplied by
   Mult.

-  **CPU-Time** - remote CPU time computed as the stime + utime (when
   Kickstart is not used, this is empty).

-  **Post(sec.)** - the postscript time as reported by DAGMan.

-  **CondorQTime(sec.)** - the time between submission by DAGMan and the
   remote Grid submission. It is an estimate of the time spent in the
   condor q on the submit node .

-  **Resource(sec.)** - the time between the remote Grid submission and
   start of remote execution . It is an estimate of the time job
   instance spent in the remote queue .

-  **Runtime(sec.)** - the time spent on the resource as seen by Condor
   DAGMan . Is always >=kickstart .

-  **Seqexec(sec.)** - the time taken for the completion of a clustered
   job instance .

-  **Seqexec-Delay(sec.)** - the time difference between the time for
   the completion of a clustered job instance and sum of all the
   individual tasks kickstart time .

.. table:: Job statistics

   ========================== === ===== ========= ==== ============== ======== ==== =========== ======== ======= ======= =============
   Job                        Try Site  Kickstart Mult Kickstart_Mult CPU-Time Post CondorQTime Resource Runtime Seqexec Seqexec-Delay
   ========================== === ===== ========= ==== ============== ======== ==== =========== ======== ======= ======= =============
   analyze_ID0000004          1   local 60.002    1    60.002         59.843   5.0  0.0         -        62.0    -       -
   create_dir_diamond_0_local 1   local 0.027     1    0.027          0.003    5.0  5.0         -        0.0     -       -
   findrange_ID0000002        1   local 60.001    10   600.01         59.921   5.0  0.0         -        60.0    -       -
   findrange_ID0000003        1   local 60.002    10   600.02         59.912   5.0  10.0        -        61.0    -       -
   preprocess_ID0000001       1   local 60.002    1    60.002         59.898   5.0  5.0         -        60.0    -       -
   register_local_1_0         1   local 0.459     1    0.459          0.432    6.0  5.0         -        0.0     -       -
   register_local_1_1         1   local 0.338     1    0.338          0.331    5.0  5.0         -        0.0     -       -
   register_local_2_0         1   local 0.348     1    0.348          0.342    5.0  5.0         -        0.0     -       -
   stage_in_local_local_0     1   local 0.39      1    0.39           0.032    5.0  5.0         -        0.0     -       -
   stage_out_local_local_0_0  1   local 0.165     1    0.165          0.108    5.0  10.0        -        0.0     -       -
   stage_out_local_local_1_0  1   local 0.147     1    0.147          0.098    7.0  5.0         -        0.0     -       -
   stage_out_local_local_1_1  1   local 0.139     1    0.139          0.089    5.0  6.0         -        0.0     -       -
   stage_out_local_local_2_0  1   local 0.145     1    0.145          0.101    5.0  5.0         -        0.0     -       -
   ========================== === ===== ========= ==== ============== ======== ==== =========== ======== ======= ======= =============

.. _pegasus-statistics-transformation:

Transformation statistics file per workflow [breakdown.txt]
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Transformation statistics file per workflow contains information about
the invocations in each workflow grouped by transformation name. A
sample file is shown below.

-  **Transformation** - name of the transformation.

-  **Count** - the number of times invocations with a given
   transformation name was executed.

-  **Succeeded** - the count of succeeded invocations with a given
   logical transformation name .

-  **Failed** - the count of failed invocations with a given logical
   transformation name .

-  **Min (sec.)** - the minimum runtime value of invocations with a
   given logical transformation name times the multipler_factor.

-  **Max (sec.)** - the minimum runtime value of invocations with a
   given logical transformation name times the multiplier_factor.

-  **Mean (sec.)** - the mean of the invocation runtimes with a given
   logical transformation name times the multiplier_factor.

-  **Total (sec.)** - the cumulative of runtime value of invocations
   with a given logical transformation name times the multiplier_factor.

.. table:: Transformation Statistics

   ========================= ===== ========= ====== ====== ====== ====== =======
   Transformation            Count Succeeded Failed Min    Max    Mean   Total
   ========================= ===== ========= ====== ====== ====== ====== =======
   dagman::post              13    13        0      5.0    7.0    5.231  68.0
   diamond::analyze          1     1         0      60.002 60.002 60.002 60.002
   diamond::findrange        2     2         0      600.01 600.02 600.02 1200.03
   diamond::preprocess       1     1         0      60.002 60.002 60.002 60.002
   pegasus::dirmanager       1     1         0      0.027  0.027  0.027  0.027
   pegasus::pegasus-transfer 5     5         0      0.139  0.39   0.197  0.986
   pegasus::rc-client        3     3         0      0.338  0.459  0.382  1.145
   ========================= ===== ========= ====== ====== ====== ====== =======

.. _pegasus-statistics-time:

Time statistics file [time.txt]
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Time statistics file contains job instance and invocation statistics
information grouped by time and host. The time grouping can be on
day/hour. The file contains the following tables Job instance statistics
per day/hour, Invocation statistics per day/hour, Job instance
statistics by host per day/hour and Invocation by host per day/hour. A
sample Invocation statistics by host per day table is shown below.

-  **Job instance statistics per day/hour** - the number of job
   instances run, total runtime sorted by day/hour.

-  **Invocation statistics per day/hour** - the number of invocations ,
   total runtime sorted by day/hour.

-  **Job instance statistics by host per day/hour** - the number of job
   instances run, total runtime on each host sorted by day/hour.

-  **Invocation statistics by host per day/hour** - the number of
   invocations , total runtime on each host sorted by day/hour.

.. table:: Invocation statistics by host per day

   ================= ================= ===== ==============
   Date [YYYY-MM-DD] Host              Count Runtime (Sec.)
   ================= ================= ===== ==============
   2011-07-15        butterfly.isi.edu 54    625.094
   ================= ================= ===== ==============

Integrity statistics file per workflow [integrity.txt]
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Integrity statistics file contains integrity metrics grouped by file
type (input or output) and integrity type (check or compute). A sample
table is shown below. It shows the following statistics about integrity
checks.

-  **Type** - the type of integrity metric. Check means checksum was
   compared for a file, and compute means a checksum was generated for a
   file.

-  **File type** - the type of file: input or output from a job
   perspective.

-  **Count** - the number of times type, file type integrity check was
   performed.

-  **Total duration** - sum of duration in seconds for the 'count'
   number of records matching the particular type, file-type combo.

.. table:: Integrity Statistics

   ==================================== ======= ========= ===== ==============
   #                                    Type    File Type Count Total Duration
   ==================================== ======= ========= ===== ==============
   4555392d-1b37-407c-98d3-60fb86cb9d57
   \                                    check   input     5     0.164
   \                                    check   output    5     1.456
   \                                    compute input     5     0.693
   \                                    compute output    5     0.758
   ==================================== ======= ========= ===== ==============

pegasus-plots
-------------

Pegasus-plots generates graphs and charts to visualize workflow
execution. To generate graphs and charts run the command as shown below.

::

   $ pegasus-plots  -p all  /scratch/grid-setup/run0001/


   ...

   ******************************************** SUMMARY ********************************************

   Graphs and charts generated by pegasus-plots can be viewed by opening the generated html file in the web browser  :
   /scratch/grid-setup/run0001/plots/index.html

   **************************************************************************************************

By default the output gets generated to plots folder inside the submit
directory. The output that is generated by pegasus-plots is based on the
value set for command line option 'p'(plotting_level).In the sample run
the command line option 'p' is set to 'all' to generate all the charts
and graphs for the workflow run. Please consult the pegasus-plots man
page to find a detailed description of various command line options.
pegasus-plots generates an index.html file which provides links to all
the generated charts and plots. A sample index.html page is shown below.

.. figure:: images/pegasus_plots_index.png
   :alt: pegasus-plot index page
   :width: 100.0%

   pegasus-plot index page

pegasus-plots generates the following plots and charts.

**Dax Graph**

Graph representation of the DAX file. A sample page is shown below.

.. figure:: images/dax_page.png
   :alt: DAX Graph
   :width: 100.0%

   DAX Graph

**Dag Graph**

Graph representation of the DAG file. A sample page is shown below.

.. figure:: images/dag_page.png
   :alt: DAG Graph
   :width: 100.0%

   DAG Graph

**Gantt workflow execution chart**

Gantt chart of the workflow execution run. A sample page is shown below.

.. figure:: images/gantt_chart_page.png
   :alt: Gantt Chart
   :width: 100.0%

   Gantt Chart

The toolbar at the top provides zoom in/out , pan left/right/top/bottom
and show/hide job name functionality.The toolbar at the bottom can be
used to show/hide job states. Failed job instances are shown in red
border in the chart. Clicking on a sub workflow job instance will take
you to the corresponding sub workflow chart.

**Host over time chart**

Host over time chart of the workflow execution run. A sample page is
shown below.

.. figure:: images/host_chart_page.png
   :alt: Host over time chart
   :width: 100.0%

   Host over time chart

The toolbar at the top provides zoom in/out , pan left/right/top/bottom
and show/hide host name functionality.The toolbar at the bottom can be
used to show/hide job states. Failed job instances are shown in red
border in the chart. Clicking on a sub workflow job instance will take
you to the corresponding sub workflow chart.

**Time chart**

Time chart shows job instance/invocation count and runtime of the
workflow run over time. A sample page is shown below.

.. figure:: images/time_chart_page.png
   :alt: Time chart
   :width: 100.0%

   Time chart

The toolbar at the top provides zoom in/out and pan
left/right/top/bottom functionality. The toolbar at the bottom can be
used to switch between job instances/ invocations and day/hour
filtering.

**Breakdown chart**

Breakdown chart shows invocation count and runtime of the workflow run
grouped by transformation name. A sample page is shown below.

.. figure:: images/breakdown_chart_page.png
   :alt: Breakdown chart
   :width: 100.0%

   Breakdown chart

The toolbar at the bottom can be used to switch between invocation count
and runtime filtering. Legends can be clicked to get more details.

Dashboard
=========

As the number of jobs and tasks in workflows increase, the ability to
track the progress and quickly debug a workflow becomes more and more
important. The dashboard provides users with a tool to monitor and debug
workflows both in real-time as well as after execution is already
completed, through a browser.

Workflow Dashboard
------------------

Pegasus Workflow Dashboard is bundled with Pegasus. The pegasus-service
is developed in Python and uses the Flask framework to implement the web
interface.The users can then connect to this server using a browser to
monitor/debug workflows.

   **Note**

   the workflow dashboard can only monitor workflows which have been
   executed using Pegasus 4.2.0 and above.

To start the Pegasus Dashboard execute the following command

::

   $ pegasus-service --host 127.0.0.1 --port 5000

   SSL is not configured: Using self-signed certificate
   2015-04-13 16:14:23,074:Pegasus.service.server:79: WARNING: SSL is not configured: Using self-signed certificate
   Service not running as root: Will not be able to switch users
   2015-04-13 16:14:23,074:Pegasus.service.server:86: WARNING: Service not running as root: Will not be able to switch users

By default, the server is configured to listen only on
localhost/127.0.0.1 on port 5000. A user can view the dashboard on
**https://localhost:5000/**

To make the Pegasus Dashboard listen on all network interfaces OR on a
different port, users can pass different values to the --host and/or
--port options.

By default, the dashboard server can only monitor workflows run by the
current user i.e. the user who is running the pegasus-service.

The Dashboard's home page lists all workflows, which have been run by
the current-user. The home page shows the status of each of the workflow
i.e. Running/Successful/Failed/Failing. The home page lists only the top
level workflows (Pegasus supports hierarchical workflows i.e. workflows
within a workflow). The rows in the table are color coded

-  **Green**: indicates workflow finished successfully.

-  **Red**: indicates workflow finished with a failure.

-  **Blue**: indicates a workflow is currently running.

-  **Gray**: indicates a workflow that was archived.

.. figure:: images/dashboard_home.png
   :alt: Dashboard Home Page
   :width: 100.0%

   Dashboard Home Page

To view details specific to a workflow, the user can click on
corresponding workflow label. The workflow details page lists workflow
specific information like workflow label, workflow status, location of
the submit directory, files, and metadata associated with the workflow
etc. The details page also displays pie charts showing the distribution
of jobs based on status.

In addition, the details page displays a tab listing all sub-workflows
and their statuses. Additional tabs exist which list information for all
running, failed, successful, and failing jobs.

   **Note**

   Failing jobs are currently running jobs (visible in Running tab),
   which have failed in previous attempts to execute them.

The information displayed for a job depends on it's status. For example,
the failed jobs tab displays the job name, exit code, links to available
standard output, and standard error contents.

.. figure:: images/dashboard_workflow_details.png
   :alt: Dashboard Workflow Page
   :width: 100.0%

   Dashboard Workflow Page

To view details specific to a job the user can click on the
corresponding job's job label. The job details page lists information
relevant to a specific job. For example, the page lists information like
job name, exit code, run time, etc.

The job instance section of the job details page lists all attempts made
to run the job i.e. if a job failed in its first attempt due to
transient errors, but ran successfully when retried, the job instance
section shows two entries; one for each attempt to run the job.

The job details page also shows tab's for failed, and successful task
invocations (Pegasus allows users to group multiple smaller task's into
a single job i.e. a job may consist of one or more tasks)

.. figure:: images/dashboard_job_details.png
   :alt: Dashboard Job Description Page
   :width: 100.0%

   Dashboard Job Description Page

The task invocation details page provides task specific information like
task name, exit code, duration, metadata associated with the task, etc.
Task details differ from job details, as they are more granular in
nature.

.. figure:: images/dashboard_invocation_details.png
   :alt: Dashboard Invocation Page
   :width: 100.0%

   Dashboard Invocation Page

The dashboard also has web pages for workflow statistics and workflow
charts, which graphically renders information provided by the
pegasus-statistics and pegasus-plots command respectively.

The Statistics page shows the following statistics.

1. Workflow level statistics

2. Job breakdown statistics

3. Job specific statistics

4. Integrity statistics

.. figure:: images/dashboard_statistics.png
   :alt: Dashboard Statistics Page
   :width: 100.0%

   Dashboard Statistics Page

The Charts page shows the following charts.

1. Job Distribution by Count/Time

2. Time Chart by Job/Invocation

3. Workflow Execution Gantt Chart

The chart below shows the invocation distribution by count or time.

.. figure:: images/dashboard_plots_job_dist.png
   :alt: Dashboard Plots - Job Distribution
   :width: 100.0%

   Dashboard Plots - Job Distribution

The time chart shown below shows the number of jobs/invocations in the
workflow and their total runtime

.. figure:: images/dashboard_plots_time_charts.png
   :alt: Dashboard Plots - Time Chart
   :width: 100.0%

   Dashboard Plots - Time Chart

The workflow gantt chart lays out the execution of the jobs in the
workflow over time.

.. figure:: images/dashboard_plots_wf_gantt.png
   :alt: Dashboard Plots - Workflow Gantt Chart
   :width: 100.0%

   Dashboard Plots - Workflow Gantt Chart

Notifications
=============

The Pegasus Workflow Mapper now supports job and workflow level
notifications. You can specify in the DAX with the job or the workflow

-  the event when the notification needs to be sent

-  the executable that needs to be invoked.

The notifications are issued from the submit host by the
pegasus-monitord daemon that monitors the Condor logs for the workflow.
When a notification is issued, pegasus-monitord while invoking the
notifying executable sets certain environment variables that contain
information about the job and workflow state.

The Pegasus release comes with default notification clients that send
notifications via email or jabber.

Specifying Notifications in the DAX
-----------------------------------

Currently, you can specify notifications for the jobs and the workflow
by the use of invoke elements.

Invoke elements can be sub elements for the following elements in the
DAX schema.

-  job - to associate notifications with a compute job in the DAX.

-  dax - to associate notifications with a dax job in the DAX.

-  dag - to associate notifications with a dag job in the DAX.

-  executable - to associate notifications with a job that uses a
   particular notification

The invoke element can be specified at the root element level of the DAX
to indicate workflow level notifications.

The invoke element may be specified multiple times, as needed. It has a
mandatory **when** attribute with the following value set

.. table:: Invoke Element attributes and meaning.

   ======================================== =====================================================================================================
   Enumeration of Values for when attribute Meaning
   ======================================== =====================================================================================================
   never                                    (default). Never notify of anything. This is useful to temporarily disable an existing notifications.
   start                                    create a notification when the job is submitted.
   on_error                                 after a job finishes with failure (exitcode != 0).
   on_success                               after a job finishes with success (exitcode == 0).
   at_end                                   after a job finishes, regardless of exitcode.
   all                                      like start and at_end combined.
   ======================================== =====================================================================================================

You can specify multiple invoke elements corresponding to same when
attribute value in the DAX. This will allow you to have multiple
notifications for the same event.

Here is an example that illustrates that.

::

   <job id="ID000001" namespace="example" name="mDiffFit" version="1.0"
          node-label="preprocess" >
       <argument>-a top -T 6  -i <file name="f.a"/>  -o <file name="f.b1"/></argument>

       <!-- profiles are optional -->
       <profile namespace="execution" key="site">isi_viz</profile>
       <profile namespace="condor" key="getenv">true</profile>

       <uses name="f.a" link="input"  register="false" transfer="true" type="data" />
       <uses name="f.b" link="output" register="false" transfer="true" type="data" />

       <!-- 'WHEN' enumeration: never, start, on_error, on_success, at_end, all -->
       <invoke when="start">/path/to/notify1 arg1 arg2</invoke>
       <invoke when="start">/path/to/notify1 arg3 arg4</invoke>
       <invoke when="on_success">/path/to/notify2 arg3 arg4</invoke>
     </job>

In the above example the executable notify1 will be invoked twice when a
job is submitted ( when="start" ), once with arguments arg1 and arg2 and
second time with arguments arg3 and arg4.

The DAX Generator API `chapter <#dax_generator_api>`__ has information
about how to add notifications to the DAX using the DAX api's.

.. _pegasus-notify-file:

Notify File created by Pegasus in the submit directory
------------------------------------------------------

Pegasus while planning a workflow writes out a notify file in the submit
directory that contains all the notifications that need to be sent for
the workflow. pegasus-monitord picks up this notifications file to
determine what notifications need to be sent and when.

1. ENTITY_TYPE ID NOTIFICATION_CONDITION ACTION

   -  ENTITY_TYPE can be either of the following keywords

      -  WORKFLOW - indicates workflow level notification

      -  JOB - indicates notifications for a job in the executable
         workflow

      -  DAXJOB - indicates notifications for a DAX Job in the
         executable workflow

      -  DAGJOB - indicates notifications for a DAG Job in the
         executable workflow

   -  ID indicates the identifier for the entity. It has different
      meaning depending on the entity type - -

      -  workflow - ID is wf_uuid

      -  JOB|DAXJOB|DAGJOB - ID is the job identifier in the executable
         workflow ( DAG ).

   -  NOTIFICATION_CONDITION is the condition when the notification
      needs to be sent. The notification conditions are enumerated in
      `this table <#notification_conditions_table>`__

   -  ACTION is what needs to happen when condition is satisfied. It is
      executable + arguments

2. INVOCATION JOB_IDENTIFIER INV.ID NOTIFICATION_CONDITION ACTION

   The INVOCATION lines are only generated for clustered jobs, to
   specifiy the finer grained notifications for each constitutent
   job/invocation .

   -  JOB IDENTIFIER is the job identifier in the executable workflow (
      DAG ).

   -  INV.ID indicates the index of the task in the clustered job for
      which the notification needs to be sent.

   -  NOTIFICATION_CONDITION is the condition when the notification
      needs to be sent. The notification conditions are enumerated in
      `Table 1 <#notification_conditions_table>`__

   -  ACTION is what needs to happen when condition is satisfied. It is
      executable + arguments

A sample notifications file generated is listed below.

::

   WORKFLOW d2c4f79c-8d5b-4577-8c46-5031f4d704e8 on_error /bin/date1

   INVOCATION merge_vahi-preprocess-1.0_PID1_ID1 1 on_success /bin/date_executable
   INVOCATION merge_vahi-preprocess-1.0_PID1_ID1 1 on_success /bin/date_executable
   INVOCATION merge_vahi-preprocess-1.0_PID1_ID1 1 on_error /bin/date_executable

   INVOCATION merge_vahi-preprocess-1.0_PID1_ID1 2 on_success /bin/date_executable
   INVOCATION merge_vahi-preprocess-1.0_PID1_ID1 2 on_error /bin/date_executable

   DAXJOB subdax_black_ID000003 on_error /bin/date13
   JOB    analyze_ID00004    on_success /bin/date

Configuring pegasus-monitord for notifications
----------------------------------------------

Whenever pegasus-monitord enters a workflow (or sub-workflow) directory,
it will read the notifications file generated by Pegasus.
Pegasus-monitord will match events in the running workflow against the
notifications specified in the notifications file and will initiate the
script specified in a notification when that notification matches an
event in the workflow. It is important to note that there will be a
delay between a certain event happening in the workflow, and
pegasus-monitord processing the log file and executing the corresponding
notification script.

The following command line options (and properties) can change how
pegasus-monitord handles notifications:

-  --no-notifications (pegasus.monitord.notifications=False): Will
   disable notifications completely.

-  --notifications-max=nn (pegasus.monitord.notifications.max=nn): Will
   limit the number of concurrent notification scripts to nn. Once
   pegasus-monitord reaches this number, it will wait until one
   notification script finishes before starting a new one. Notifications
   happening during this time will be queued by the system. The default
   number of concurrent notification scripts for pegasus-monitord is 10.

-  --notifications-timeout=nn
   (pegasus.monitord.notifications.timeout=nn): This setting is used to
   change how long will pegasus-monitord wait for a notification script
   to finish. By default pegasus-monitord will wait for as long as it
   takes (possibly indefinitely) until a notification script ends. With
   this option, pegasus-monitord will wait for at most nn seconds before
   killing the notification script.

It is also important to understand that pegasus-monitord will not issue
any notifications when it is executed in replay mode.

Environment set for the notification scripts
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Whenever a notification in the notifications file matches an event in
the running workflow, pegasus-monitord will run the corresponding script
specified in the ACTION field of the notifications file.
Pegasus-monitord will set the following environment variables for each
notification script is starts:

-  PEGASUS_EVENT: The NOTIFICATION_CONDITION that caused the
   notification. In the case of the "all" condition, pegasus-monitord
   will substitute it for the actual event that caused the match (e.g.
   "start" or "at_end").

-  PEGASUS_EVENT_TIMESTAMP: Timestamp in EPOCH format for the event
   (better for automated processing).

-  PEGASUS_EVENT_TIMESTAMP_ISO: Same as above, but in ISO format (better
   for human readability).

-  PEGASUS_SUBMIT_DIR: The submit directory for the workflow (usually
   the value from "submit_dir" in the braindump.txt file)

-  PEGASUS_STDOUT: For workflow notifications, this will correspond to
   the dagman.out file for that workflow. For job and invocation
   notifications, this field will contain the output file (stdout) for
   that particular job instance.

-  PEGASUS_STDERR: For job and invocation notifications, this field will
   contain the error file (stderr) for the particular executable job
   instance. This field does not exist in case of workflow
   notifications.

-  PEGASUS_WFID: Contains the workflow id for this notification in the
   form of DAX_LABEL + DAX_INDEX (from the braindump.txt file).

-  PEGASUS_JOBID: For workflow notifications, this contains the
   worfkflow wf_uuid (from the braindump.txt file). For job and
   invocation notifications, this field contains the job identifier in
   the executable workflow ( DAG ) for the particular notification.

-  PEGASUS_INVID: Contains the index of the task in the clustered job
   for the notification.

-  PEGASUS_STATUS: For workflow notifications, this contains DAGMan's
   exit code. For job and invocation notifications, this field contains
   the exit code for the particular job/task. Please note that this
   field is not present for 'start' notification events.

Default Notification Scripts
----------------------------

Pegasus ships with two reference notification scripts. These can be used
as starting point when creating your own notification scripts, or if the
default one is all you need, you can use them directly in your
workflows. The scripts are:

-  **libexec/notification/email** - sends email, including the output
   from ``pegasus-status`` (default) or ``pegasus-analyzer``.

   ::

      $ ./libexec/notification/email --help
      Usage: email [options]

      Options:
        -h, --help            show this help message and exit
        -t TO_ADDRESS, --to=TO_ADDRESS
                              The To: email address. Defines the recipient for the
                              notification.
        -f FROM_ADDRESS, --from=FROM_ADDRESS
                              The From: email address. Defaults to the required To:
                              address.
        -r REPORT, --report=REPORT
                              Include workflow report. Valid values are: none
                              pegasus-analyzer pegasus-status (default)

-  **libexec/notification/jabber**- sends simple notifications to
   Jabber/GTalk. This can be useful for job failures.

   ::

      $ ./libexec/notification/jabber --help
      Usage: jabber [options]

      Options:
        -h, --help            show this help message and exit
        -i JABBER_ID, --jabberid=JABBER_ID
                              Your jabber id. Example: user@jabberhost.com
        -p PASSWORD, --password=PASSWORD
                              Your jabber password
        -s HOST, --host=HOST  Jabber host, if different from the host in your jabber
                              id. For Google talk, set this to talk.google.com
        -r RECIPIENT, --recipient=RECIPIENT
                              Jabber id of the recipient. Not necessary if you want
                              to send to your own jabber id

For example, if the DAX generator is written in Python and you want
notifications on 'at_end' events (successful or failed):

::

   # job level notifications - in this case for at_end events
   job.invoke('at_end', pegasus_home + "/libexec/notifications/email --to me@somewhere.edu")

Please see the `notifications example <#notifications_example>`__ to see
a full workflow using notifications.

.. _monitoring:

Monitoring Database
===================

Pegasus launches a monitoring daemon called pegasus-monitord per
workflow ( a single daemon is launched if a user submits a hierarchal
workflow ) . pegasus-monitord parses the workflow and job logs in the
submit directory and populates to a database. This chapter gives an
overview of the pegasus-monitord and describes the schema of the runtime
database.

.. _monitoring-pegasus-monitord:

pegasus-monitord
----------------

**Pegasus-monitord** is used to follow workflows, parsing the output of
DAGMan's dagman.out file. In addition to generating the jobstate.log
file, which contains the various states that a job goes through during
the workflow execution, **pegasus-monitord** can also be used to mine
information from jobs' submit and output files, and either populate a
database, or write a file with NetLogger events containing this
information. **Pegasus-monitord** can also send notifications to users
in real-time as it parses the workflow execution logs.

**Pegasus-monitord** is automatically invoked by **pegasus-run**, and
tracks workflows in real-time. By default, it produces the jobstate.log
file, and a SQLite database, which contains all the information listed
in the `Stampede schema <#stampede_schema_overview>`__. When a workflow
fails, and is re-submitted with a rescue DAG, **pegasus-monitord** will
automatically pick up from where it left previously and continue to
write the jobstate.log file and populate the database.

If, after the workflow has already finished, users need to re-create the
jobstate.log file, or re-populate the database from scratch,
**pegasus-monitord**'s **--replay** option should be used when running
it manually.

Populating to different backend databases
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In addition to SQLite, **pegasus-monitord** supports other types of
databases, such as MySQL and Postgres. Users will need to install the
low-level database drivers, and can use the **--dest** command-line
option, or the **pegasus.monitord.output** property to select where the
logs should go.

As an example, the command:

::

   $ pegasus-monitord -r diamond-0.dag.dagman.out

will launch **pegasus-monitord** in replay mode. In this case, if a
jobstate.log file already exists, it will be rotated and a new file will
be created. It will also create/use a SQLite database in the workflow's
run directory, with the name of diamond-0.stampede.db. If the database
already exists, it will make sure to remove any references to the
current workflow before it populates the database. In this case,
**pegasus-monitord** will process the workflow information from start to
finish, including any restarts that may have happened.

Users can specify an alternative database for the events, as illustrated
by the following examples:

::

   $ pegasus-monitord -r -d mysql://username:userpass@hostname/database_name diamond-0.dag.dagman.out

::

   $ pegasus-monitord -r -d sqlite:////tmp/diamond-0.db diamond-0.dag.dagman.out

In the first example, **pegasus-monitord** will send the data to the
**database_name** database located at server **hostname**, using the
**username** and **userpass** provided. In the second example,
**pegasus-monitord** will store the data in the /tmp/diamond-0.db SQLite
database.

   **Note**

   For absolute paths four slashes are required when specifying an
   alternative database path in SQLite.

Users should also be aware that in all cases, with the exception of
SQLite, the database should exist before **pegasus-monitord** is run (as
it creates all needed tables but does not create the database itself).

Finally, the following example:

::

   $ pegasus-monitord -r --dest diamond-0.bp diamond-0.dag.dagman.out

sends events to the diamond-0.bp file. (please note that in replay mode,
any data on the file will be overwritten).

One important detail is that while processing a workflow,
**pegasus-monitord** will automatically detect if/when sub-workflows are
initiated, and will automatically track those sub-workflows as well. In
this case, although **pegasus-monitord** will create a separate
jobstate.log file in each workflow directory, the database at the
top-level workflow will contain the information from not only the main
workflow, but also from all sub-workflows.

.. _monitoring-files:

Monitoring related files in the workflow directory
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Pegasus-monitord** generates a number of files in each workflow
directory:

-  **jobstate.log**: contains a summary of workflow and job execution.

-  **monitord.log**: contains any log messages generated by
   **pegasus-monitord**. It is not overwritten when it restarts. This
   file is not generated in replay mode, as all log messages from
   **pegasus-monitord** are output to the console. Also, when
   sub-workflows are involved, only the top-level workflow will have
   this log file. Starting with release 4.0 and 3.1.1, monitord.log file
   is rotated if it exists already.

-  **monitord.started**: contains a timestamp indicating when
   **pegasus-monitord** was started. This file get overwritten every
   time **pegasus-monitord** starts.

-  **monitord.done**: contains a timestamp indicating when
   **pegasus-monitord** finished. This file is overwritten every time
   **pegasus-monitord** starts.

-  **monitord.info**: contains **pegasus-monitord** state information,
   which allows it to resume processing if a workflow does not finish
   properly and a rescue dag is submitted. This file is erased when
   **pegasus-monitord** is executed in replay mode.

-  **monitord.recover**: contains **pegasus-monitord** state information
   that allows it to detect that a previous instance of
   **pegasus-monitord** failed (or was killed) midway through parsing a
   workflow's execution logs. This file is only present while
   **pegasus-monitord** is running, as it is deleted when it ends and
   the **monitord.info** file is generated.

-  **monitord.subwf.db**: contains information that aids
   **pegasus-monitord** to track when sub-workflows fail and are
   re-planned/re-tried. It is overwritten when **pegasus-monitord** is
   started in replay mode.

-  **monitord-notifications.log**: contains the log file for
   notification-related messages. Normally, this file only includes logs
   for failed notifications, but can be populated with all notification
   information when **pegasus-monitord** is run in verbose mode via the
   **-v** command-line option.

Multiple End points
~~~~~~~~~~~~~~~~~~~

pegasus-monitord can be used to publish `events <#stampede_wf_events>`__
to different backends at the same time. The configuration of this is
managed through properties matching
**pegasus.catalog.workflow.<variable-name>.url** .

For example, to enable populating to an AMQP end point and a file format
in addition to default sqlite you can configure as follows

::

   pegasus.catalog.workflow.amqp.url amqp://vahi:XXXXX@amqp.isi.edu:5672/panorama/monitoring
   pegasus.catalog.workflow.file.url file:///lfs1/work/monitord/amqp/nl.bp

If you want to only override the default sqlite population , then you
can specify pegasus.catalog.workflow.url property .

.. _stampede-schema-overview:

Overview of the Workflow Database Schema.
-----------------------------------------

Pegasus takes in a DAX which is composed of tasks. Pegasus plans it into
a Condor DAG / Executable workflow that consists of Jobs. In case of
Clustering, multiple tasks in the DAX can be captured into a single job
in the Executable workflow. When DAGMan executes a job, a job instance
is populated . Job instances capture information as seen by DAGMan. In
case DAGMan retires a job on detecting a failure , a new job instance is
populated. When DAGMan finds a job instance has finished , an invocation
is associated with job instance. In case of clustered job, multiple
invocations will be associated with a single job instance. If a Pre
script or Post Script is associated with a job instance, then
invocations are populated in the database for the corresponding job
instance.

The current schema version is **4.0** that is stored in the schema_info
table.

.. figure:: images/stampede_schema_overview-small.png
   :alt: Workflow Database Schema
   :name: stampede_schema_overview_figure

   Workflow Database Schema

.. _schema-upgrade-tool:

Stampede Schema Upgrade Tool
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Starting Pegasus 4.x the monitoring and statistics database schema has
changed. If you want to use the pegasus-statistics, pegasus-analyzer and
pegasus-plots against a 3.x database you will need to upgrade the schema
first using the schema upgrade tool
/usr/share/pegasus/sql/schema_tool.py or
/path/to/pegasus-4.x/share/pegasus/sql/schema_tool.py

Upgrading the schema is required for people using the MySQL database for
storing their monitoring information if it was setup with 3.x monitoring
tools.

If your setup uses the default SQLite database then the new databases
run with Pegasus 4.x are automatically created with the correct schema.
In this case you only need to upgrade the SQLite database from older
runs if you wish to query them with the newer clients.

To upgrade the database

::

   For SQLite Database

   cd /to/the/workflow/directory/with/3.x.monitord.db

   Check the db version

   /usr/share/pegasus/sql/schema_tool.py -c connString=sqlite:////to/the/workflow/directory/with/workflow.stampede.db
   2012-02-29T01:29:43.330476Z INFO   netlogger.analysis.schema.schema_check.SchemaCheck.init |
   2012-02-29T01:29:43.330708Z INFO   netlogger.analysis.schema.schema_check.SchemaCheck.check_schema.start |
   2012-02-29T01:29:43.348995Z INFO   netlogger.analysis.schema.schema_check.SchemaCheck.check_schema
                                      | Current version set to: 3.1.
   2012-02-29T01:29:43.349133Z ERROR  netlogger.analysis.schema.schema_check.SchemaCheck.check_schema
                                      | Schema version 3.1 found - expecting 4.0 - database admin will need to run upgrade tool.


   Convert the Database to be version 4.x compliant

   /usr/share/pegasus/sql/schema_tool.py -u connString=sqlite:////to/the/workflow/directory/with/workflow.stampede.db
   2012-02-29T01:35:35.046317Z INFO   netlogger.analysis.schema.schema_check.SchemaCheck.init |
   2012-02-29T01:35:35.046554Z INFO   netlogger.analysis.schema.schema_check.SchemaCheck.check_schema.start |
   2012-02-29T01:35:35.064762Z INFO   netlogger.analysis.schema.schema_check.SchemaCheck.check_schema
                                     | Current version set to: 3.1.
   2012-02-29T01:35:35.064902Z ERROR  netlogger.analysis.schema.schema_check.SchemaCheck.check_schema
                                     | Schema version 3.1 found - expecting 4.0 - database admin will need to run upgrade tool.
   2012-02-29T01:35:35.065001Z INFO   netlogger.analysis.schema.schema_check.SchemaCheck.upgrade_to_4_0
                                     | Upgrading to schema version 4.0.

   Verify if the database has been converted to Version 4.x

   /usr/share/pegasus/sql/schema_tool.py -c connString=sqlite:////to/the/workflow/directory/with/workflow.stampede.db
   2012-02-29T01:39:17.218902Z INFO   netlogger.analysis.schema.schema_check.SchemaCheck.init |
   2012-02-29T01:39:17.219141Z INFO   netlogger.analysis.schema.schema_check.SchemaCheck.check_schema.start |
   2012-02-29T01:39:17.237492Z INFO   netlogger.analysis.schema.schema_check.SchemaCheck.check_schema | Current version set to: 4.0.
   2012-02-29T01:39:17.237624Z INFO   netlogger.analysis.schema.schema_check.SchemaCheck.check_schema | Schema up to date.

   For upgrading a MySQL database the steps remain the same. The only thing that changes is the connection String to the database
   E.g.

   /usr/share/pegasus/sql/schema_tool.py -u connString=mysql://username:password@server:port/dbname

After the database has been upgraded you can use either 3.x or 4.x
clients to query the database with **pegasus-statistics**, as well as
**pegasus-plots**\ and **pegasus-analyzer.**

Storing of Exitcode in the database
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Kickstart records capture raw status in addition to the exitcode . The
exitcode is derived from the raw status. Starting with Pegasus 4.0
release, all exitcode columns ( i.e invocation and job instance table
columns ) are stored with the raw status by pegasus-monitord. If an
exitcode is encountered while parsing the dagman log files , the value
is converted to the corresponding raw status before it is stored. All
user tools, pegasus-analyzer and pegasus-statistics then convert the raw
status to exitcode when retrieving from the database.

Multiplier Factor
~~~~~~~~~~~~~~~~~

Starting with the 4.0 release, there is a multiplier factor associated
with the jobs in the job_instance table. It defaults to one, unless the
user associates a Pegasus profile key named **cores** with the job in
the DAX. The factor can be used for getting more accurate statistics for
jobs that run on multiple processors/cores or mpi jobs.

The multiplier factor is used for computing the following metrics by
pegasus statistics.

-  In the summary, the workflow cumulative job wall time

-  In the summary, the cumulative job wall time as seen from the submit
   side

-  In the jobs file, the multiplier factor is listed along-with the
   multiplied kickstart time.

-  In the breakdown file, where statistics are listed per transformation
   the mean, min , max and average values take into account the
   multiplier factor.

.. _stampede-wf-events:

Stampede Workflow Events
========================

All the events generated by the system ( Pegasus planner and monitoring
daemon) are formatted as Netlogger BP events. The netlogger events that
Pegasus generates are described in Yang schema file that can be found in
the share/pegasus/schema/ directory. The stampede yang schema is
described below.

::

Typedefs
--------

The following typedefs are used in the yang schema to describe the
certain event attributes.

-  distinguished-name

   ::

      typedef distinguished-name {
         type string;
      }

-  uuid

   ::

      typedef uuid {
         type string {
             length "36";
             pattern
                '[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}';
         }
      }

-  intbool

   ::

      typedef intbool {
         type uint8 {
             range "0 .. 1";
         }
      }

-  nl_ts

   ::

      typedef nl_ts {
          type string {
              pattern
                '(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[\+\-]\d{2}:\d{2}))|(\d{1,9}(\.\d+)?)';
           }
      }

-  peg_inttype

   ::

      typedef peg_inttype {
          type uint8 {
              range "0 .. 11";
          }
      }

-  peg_strtype

   ::

      typedef peg_strtype {
          type enumeration {
              enum "unknown" {
                value 0;
              }
              enum "compute" {
                value 1;
              }
              enum "stage-in-tx" {
                value 2;
              }
              enum "stage-out-tx" {
                value 3;
              }
              enum "registration" {
                value 4;
              }
              enum "inter-site-tx" {
                value 5;
              }
              enum "create-dir" {
                value 6;
              }
              enum "staged-compute" {
                value 7;
              }
              enum "cleanup" {
                value 8;
              }
              enum "chmod" {
                value 9;
              }
              enum "dax" {
                value 10;
              }
              enum "dag" {
                value 11;
              }
         }
      }

-  condor_jobstates

   ::

      typedef condor_jobstates {
          type enumeration {
              enum "PRE_SCRIPT_STARTED" {
                value 0;
              }
              enum "PRE_SCRIPT_TERMINATED" {
                value 1;
              }
              enum "PRE_SCRIPT_SUCCESS" {
                value 2;
              }
              enum "PRE_SCRIPT_FAILED" {
                value 3;
              }
              enum "SUBMIT" {
                value 4;
              }
              enum "GRID_SUBMIT" {
                value 5;
              }
              enum "GLOBUS_SUBMIT" {
                value 6;
              }
              enum "SUBMIT_FAILED" {
                value 7;
              }
              enum "EXECUTE" {
                value 8;
              }
              enum "REMOTE_ERROR" {
                value 9;
              }
              enum "IMAGE_SIZE" {
                value 10;
              }
              enum "JOB_TERMINATED" {
                value 11;
              }
              enum "JOB_SUCCESS" {
                value 12;
              }
              enum "JOB_FAILURE" {
                value 13;
              }
              enum "JOB_HELD" {
                value 14;
              }
              enum "JOB_EVICTED" {
                value 15;
              }
              enum "JOB_RELEASED" {
                value 16;
              }
              enum "POST_SCRIPT_STARTED" {
                value 17;
              }
              enum "POST_SCRIPT_TERMINATED" {
                value 18;
              }
              enum "POST_SCRIPT_SUCCESS" {
                value 19;
              }
              enum "POST_SCRIPT_FAILED" {
                value 20;
              }
          }
      }

-  condor_wfstates

   ::

      typedef condor_wfstates {
          type enumeration {
              enum "WORKFLOW_STARTED" {
                value 0;
              }
              enum "WORKFLOW_TERMINATED" {
                value 1;
              }
          }
      }

Groupings
---------

Groupings are groups of common attributes that different type of events
refer to. The following groupings are defined.

-  **base-event** - Common components in all events

   -  ts - Timestamp, ISO8601 or numeric seconds since 1/1/1970"

   -  level - Severity level of event. Subset of NetLogger BP levels.
      For '*.end' events, if status is non-zero then level should be
      Error."

   -  xwf.id - DAG workflow UUID

   ::

      grouping base-event {
            description
              "Common components in all events";
            leaf ts {
              type nl_ts;
              mandatory true;
              description
                "Timestamp, ISO8601 or numeric seconds since 1/1/1970";
            }

            leaf level {
              type enumeration {
                enum "Info" {
                  value 0;
                }
                enum "Error" {
                  value 1;
                }
              }
              description
                "Severity level of event. "
                  + "Subset of NetLogger BP levels. "
                  + "For '*.end' events, if status is non-zero then level should be Error.";
            }

            leaf xwf.id {
              type uuid;
              description "DAG workflow id";
            }
       }  // grouping base-event

-  **base-job-inst** - Common components for all job instance events

   -  all attributes from base-event

   -  job_inst.id - Job instance identifier i.e the submit sequence
      generated by monitord.

   -  js.id - Jobstate identifier

   -  job.id - Identifier for corresponding job in the DAG

   ::

      grouping base-job-inst {
            description
              "Common components for all job instance events";
            uses base-event;

            leaf job_inst.id {
              type int32;
              mandatory true;
              description
                "Job instance identifier i.e the submit sequence generated by monitord";
            }

            leaf js.id {
              type int32;
              description "Jobstate identifier";
            }

            leaf job.id {
              type string;
              mandatory true;
              description
                "Identifier for corresponding job in the DAG";
            }
          }

-  **sched-job-inst** - Scheduled job instance.

   -  all attributes from base-job-inst

   -  sched.id - Identifier for job in scheduler

   ::

      grouping sched-job-inst {
            description "Scheduled job instance";
            uses base-job-inst;

            leaf sched.id {
              type string;
              mandatory true;
              description
                "Identifier for job in scheduler";
            }
      }

-  **base-metadata**

   -  uses

   -  key

   -  value

   ::

      grouping base-metadata {
            description
              "Common components for all metadata events that describe metadata for an entity.";
            uses base-event;

            leaf key {
              type string;
              mandatory true;
              description
                "Key for the metadata tuple";
            }

            leaf value {
              type string;
              description
                "Corresponding value of the key";
            }
      }  // grouping base-metadata

Events
------

The system generates following types of events, that are described
below.

-  `stampede.wf.plan <#stampede_wf_plan_event>`__

-  `stampede.static.start <#stampede_static_start_event>`__

-  `stampede.static.end <#stampede_static_end_event>`__

-  `stampede.xwf.start <#stampede_xwf_start_event>`__

-  `stampede.xwf.end <#stampede_xwf_end_event>`__

-  `stampede.task.info <#stampede_task_info_event>`__

-  `stampede.task.edge <#stampede_task_edge_event>`__

-  `stampede.wf.map.task_job <#stampede_wf_map_task_job_event>`__

-  `stampede.xwf.map.subwf_job <#stampede_xwf_map_subwf_job_event>`__

-  `stampede.int.metric <#stampede_int_metric>`__

-  `stampede.job.info <#stampede_job_info_event>`__

-  `stampede.job.edge <#stampede_job_edge_event>`__

-  `stampede.job_inst.pre.start <#stampede_job_inst_pre_start_event>`__

-  `stampede.job_inst.pre.term <#stampede_job_inst_pre_term_event>`__

-  `stampede.job_inst.pre.end <#stampede_job_inst_pre_end_event>`__

-  `stampede.job_inst.submit.start <#stampede_job_inst_submit_start_event>`__

-  `stampede.job_inst.submit.end <#stampede_job_inst_submit_end_event>`__

-  `stampede.job_inst.held.start <#stampede_job_inst_held_start_event>`__

-  `stampede.job_inst.held.end <#stampede_job_inst_held_end_event>`__

-  `stampede.job_inst.main.start <#stampede_job_inst_main_start_event>`__

-  `stampede.job_inst.main.term <#stampede_job_inst_main_term_event>`__

-  `stampede.job_inst.main.end <#stampede_job_inst_main_end_event>`__

-  `stampede.job_inst.composite <#stampede_job_inst_composite_event>`__

-  `stampede.job_inst.post.start <#stampede_job_inst_post_start_event>`__

-  `stampede.job_inst.post.term <#stampede_job_inst_post_term_event>`__

-  `stampede.job_inst.post.end <#stampede_job_inst_post_end_event>`__

-  `stampede.job_inst.host.info <#stampede_job_inst_host_info_event>`__

-  `stampede.job_inst.image.info <#stampede_job_inst_image_info_event>`__

-  `stampede.job_inst.tag <#stampede_job_inst_tag_event>`__

-  `stampede.inv.start <#stampede_inv_start_event>`__

-  `stampede.inv.end <#stampede_inv_end_event>`__

-  `stampede.static.meta.start <#stampede_static_meta_start_event>`__

-  `stampede.static.meta.end <#stampede_static_meta_end_event>`__

-  `stampede.xwf.meta <#stampede_xwf_meta_event>`__

-  `stampede.task.meta <#stampede_task_meta_event>`__

-  `stampede.task.monitoring <#stampede_task_monitoring>`__

-  `stampede.rc.meta <#stampede_rc_meta_event>`__

-  `stampede.wf.map.file <#stampede_wf_map_file_event>`__

The events are described in detail below

-  **stampede.wf.plan**

   ::

      container stampede.wf.plan {
                  uses base-event;

                  leaf submit.hostname {
                    type inet:host;
                    mandatory true;
                    description
                      "The hostname of the Pegasus submit host";
                  }

                  leaf dax.label {
                    type string;
                    default "workflow";
                    description
                      "Label for abstract workflow specification";
                  }

                  leaf dax.index {
                    type string;
                    default "workflow";
                    description
                      "Index for the DAX";
                  }

                  leaf dax.version {
                    type string;
                    mandatory true;
                    description
                      "Version number for DAX";
                  }

                  leaf dax.file {
                    type string;
                    mandatory true;
                    description
                      "Filename for for the DAX";
                  }

                  leaf dag.file.name {
                    type string;
                    mandatory true;
                    description
                      "Filename for the DAG";
                  }

                  leaf planner.version {
                    type string;
                    mandatory true;
                    description
                      "Version string for Pegasus planner, e.g. 3.0.0cvs";
                  }

                  leaf grid_dn {
                    type distinguished-name;
                    description
                      "Grid DN of submitter";
                  }

                  leaf user {
                    type string;
                    description
                      "User name of submitter";
                  }

                  leaf submit.dir {
                    type string;
                    mandatory true;
                    description
                      "Directory path from which workflow was submitted";
                  }

                  leaf argv {
                    type string;
                    description
                      "All arguments given to planner on command-line";
                  }

                  leaf parent.xwf.id {
                    type uuid;
                    description
                      "Parent workflow in DAG, if any";
                  }

                  leaf root.xwf.id {
                    type string;
                    mandatory true;
                    description
                      "Root of workflow hierarchy, in DAG. "
                        + "Use this workflow's UUID if it is the root";
                  }
      }  // container stampede.wf.plan

-  **stampede.static.start**

   ::

      container stampede.static.start {
           uses base-event;
      }

-  **stampede.static.end**

   ::

      container stampede.static.end {
          uses base-event;
      }  //

-  **stampede.xwf.start**

   ::

      container stampede.xwf.start {
                  uses base-event;

                  leaf restart_count {
                    type uint32;
                    mandatory true;
                    description
                      "Number of times workflow was restarted (due to failures)";
                  }
      }  // container stampede.xwf.start

-  **stampede.xwf.end**

   ::

      container stampede.xwf.end {
                  uses base-event;

                  leaf restart_count {
                    type uint32;
                    mandatory true;
                    description
                      "Number of times workflow was restarted (due to failures)";
                  }

                  leaf status {
                    type int16;
                    mandatory true;
                    description
                      "Status of workflow. 0=success, -1=failure";
                  }
      }  // container stampede.xwf.end

-  **stampede.task.info**

   ::

      container stampede.task.info {
                  description
                    "Information about task in DAX";
                  uses base-event;

                  leaf transformation {
                    type string;
                    mandatory true;
                    description
                      "Logical name of the underlying executable";
                  }

                  leaf argv {
                    type string;
                    description
                      "All arguments given to transformation on command-line";
                  }

                  leaf type {
                    type peg_inttype;
                    mandatory true;
                    description "Type of task";
                  }

                  leaf type_desc {
                    type peg_strtype;
                    mandatory true;
                    description
                      "String description of task type";
                  }

                  leaf task.id {
                    type string;
                    mandatory true;
                    description
                      "Identifier for this task in the DAX";
                  }
                }  // container stampede.task.info

-  **stampede.task.edge**

   ::

      container stampede.task.edge {
                  description
                    "Represents child/parent relationship between two tasks in DAX";
                  uses base-event;

                  leaf parent.task.id {
                    type string;
                    mandatory true;
                    description "Parent task";
                  }

                  leaf child.task.id {
                    type string;
                    mandatory true;
                    description "Child task";
                  }
      }  // container stampede.task.edge

-  **stampede.wf.map.task_job**

   ::

      container stampede.wf.map.task_job {

                  description
                    "Relates a DAX task to a DAG job.";
                  uses base-event;

                  leaf task.id {
                    type string;
                    mandatory true;
                    description
                      "Identifier for the task in the DAX";
                  }

                  leaf job.id {
                    type string;
                    mandatory true;
                    description
                      "Identifier for corresponding job in the DAG";
                  }
      }  // container stampede.wf.map.task_job

-  **stampede.xwf.map.subwf_job**

   ::

      container stampede.xwf.map.subwf_job {

                  description
                    "Relates a sub workflow to the corresponding job instance";
                  uses base-event;

                  leaf subwf.id {
                    type string;
                    mandatory true;
                    description
                      "Sub Workflow Identified / UUID";
                  }

                  leaf job.id {
                    type string;
                    mandatory true;
                    description
                      "Identifier for corresponding job in the DAG";
                  }

                  leaf job_inst.id {
                    type int32;
                    mandatory true;
                    description
                      "Job instance identifier i.e the submit sequence generated by monitord";
                  }
      }  // container stampede.xwf.map.subwf_job

-  **stampede.job.info**

   ::

      container stampede.job.info {

                  description
                    "A description of a job in the DAG";
                  uses base-event;

                  leaf job.id {
                    type string;
                    mandatory true;
                    description
                      "Identifier for job in the DAG";
                  }

                  leaf submit_file {
                    type string;
                    mandatory true;
                    description
                      "Name of file being submitted to the scheduler";
                  }

                  leaf type {
                    type peg_inttype;
                    mandatory true;
                    description "Type of task";
                  }

                  leaf type_desc {
                    type peg_strtype;
                    mandatory true;
                    description
                      "String description of task type";
                  }

                  leaf clustered {
                    type intbool;
                    mandatory true;
                    description
                      "Whether job is clustered or not";
                  }

                  leaf max_retries {
                    type uint32;
                    mandatory true;
                    description
                      "How many retries are allowed for this job before giving up";
                  }

                  leaf task_count {
                    type uint32;
                    mandatory true;
                    description
                      "Number of DAX tasks for this job. "
                        + "Auxiliary jobs without a task in the DAX will have the value '0'";
                  }

                  leaf executable {
                    type string;
                    mandatory true;
                    description
                      "Program to execute";
                  }

                  leaf argv {
                    type string;
                    description
                      "All arguments given to executable (on command-line)";
                  }
      }  // container stampede.job.info

-  **stampede.job.edge**

   ::

      container stampede.job.edge {

                  description
                    "Parent/child relationship between two jobs in the DAG";
                  uses base-event;

                  leaf parent.job.id {
                    type string;
                    mandatory true;
                    description "Parent job";
                  }

                  leaf child.job.id {
                    type string;
                    mandatory true;
                    description "Child job";
                  }
      }  // container stampede.job.edge

-  **stampede.job_inst.pre.start**

   ::

      container stampede.job_inst.pre.start {

                  description
                    "Start of a prescript for a job instance";
                  uses base-job-inst;
      }  // container stampede.job_inst.pre.start

-  **stampede.job_inst.pre.term**

   ::

      container stampede.job_inst.pre.term {
                  description
                    "Job prescript is terminated (success or failure not yet known)";
      }  // container stampede.job_inst.pre.term

-  **stampede.job_inst.pre.end**

   ::

      container stampede.job_inst.pre.end {
                  description
                    "End of a prescript for a job instance";
                  uses base-job-inst;

                  leaf status {
                    type int32;
                    mandatory true;
                    description
                      "Status of prescript. 0 is success, -1 is error";
                  }

                  leaf exitcode {
                    type int32;
                    mandatory true;
                    description
                      "the exitcode with which the prescript exited";
                  }
      }  // container stampede.job_inst.pre.end

-  **stampede.job_inst.submit.start**

   ::

      container stampede.job_inst.submit.start {
                  description
                    "When job instance is going to be submitted. "
                      + "Scheduler job id is not yet known";
                  uses sched-job-inst;
      }  // container stampede.job_inst.submit.start

-  **stampede.job_inst.submit.end**

   ::

      container stampede.job_inst.submit.end {
                  description
                    "When executable job is submitted";
                  uses sched-job-inst;

                  leaf status {
                    type int16;
                    mandatory true;
                    description
                      "Status of workflow. 0=success, -1=failure";
                  }
      }  // container stampede.job_inst.submit.end

-  **stampede.job_inst.held.start**

   ::

      container stampede.job_inst.held.start {
                  description
                    "When Condor holds the jobs";
                  uses sched-job-inst;
      }  // container stampede.job_inst.held.start

-  **stampede.job_inst.held.end**

   ::

      container stampede.job_inst.held.end {
                  description
                    "When the job is released after being held";
                  uses sched-job-inst;

                  leaf status {
                    type int16;
                    mandatory true;
                    description
                      "Status of workflow. 0=success, -1=failure";
                  }
      }  // container stampede.job_inst.held.end

-  **stampede.job_inst.main.start**

   ::

      container stampede.job_inst.main.start {
                  description
                    "Start of execution of a scheduler job";
                  uses sched-job-inst;

                  leaf stdin.file {
                    type string;
                    description
                      "Path to file containing standard input of job";
                  }

                  leaf stdout.file {
                    type string;
                    mandatory true;
                    description
                      "Path to file containing standard output of job";
                  }

                  leaf stderr.file {
                    type string;
                    mandatory true;
                    description
                      "Path to file containing standard error of job";
                  }
      }  // container stampede.job_inst.main.start

-  **stampede.job_inst.main.term**

   ::

      container stampede.job_inst.main.term {
                  description
                    "Job is terminated (success or failure not yet known)";
                  uses sched-job-inst;

                  leaf status {
                    type int32;
                    mandatory true;
                    description
                      "Execution status. 0=means job terminated, -1=job was evicted, not terminated";
                  }
      }  // container stampede.job_inst.main.term

-  **stampede.job_inst.main.end**

   ::

      container stampede.job_inst.main.end {
                  description
                    "End of main part of scheduler job";
                  uses sched-job-inst;

                  leaf stdin.file {
                    type string;
                    description
                      "Path to file containing standard input of job";
                  }

                  leaf stdout.file {
                    type string;
                    mandatory true;
                    description
                      "Path to file containing standard output of job";
                  }

                  leaf stdout.text {
                    type string;
                    description
                      "Text containing output of job";
                  }

                  leaf stderr.file {
                    type string;
                    mandatory true;
                    description
                      "Path to file containing standard error of job";
                  }

                  leaf stderr.text {
                    type string;
                    description
                      "Text containing standard error of job";
                  }

                  leaf user {
                    type string;
                    description
                      "Scheduler's name for user";
                  }

                  leaf site {
                    type string;
                    mandatory true;
                    description
                      "DAX name for the site at which the job ran";
                  }

                  leaf work_dir {
                    type string;
                    description
                      "Path to working directory";
                  }

                  leaf local.dur {
                    type decimal64 {
                      fraction-digits 6;
                    }
                    units "seconds";
                    description
                      "Duration as seen at the local node";
                  }

                  leaf status {
                    type int32;
                    mandatory true;
                    description
                      "Execution status. 0=success, -1=failure";
                  }

                  leaf exitcode {
                    type int32;
                    mandatory true;
                    description
                      "the exitcode with which the executable exited";
                  }

                  leaf multiplier_factor {
                    type int32;
                    mandatory true;
                    description
                      "the multiplier factor for use in statistics";
                  }

                  leaf cluster.start {
                    type nl_ts;
                    description
                      "When the enclosing cluster started";
                  }

                  leaf cluster.dur {
                    type decimal64 {
                      fraction-digits 6;
                    }
                    units "seconds";
                    description
                      "Duration of enclosing cluster";
                  }
      }  // container stampede.job_inst.main.end

-  **stampede.job_inst.post.start**

   ::

      container stampede.job_inst.post.start {
                  description
                    "Start of a postscript for a job instance";
                  uses sched-job-inst;
      }  // container stampede.job_inst.post.start

-  **stampede.job_inst.post.term**

   ::

      container stampede.job_inst.post.term {
                  description
                    "Job postscript is terminated (success or failure not yet known)";
                  uses sched-job-inst;
      }  // container stampede.job_inst.post.term

-  **stampede.job_inst.post.end**

   ::

      container stampede.job_inst.post.end {
                  description
                    "End of a postscript for a job instance";
                  uses sched-job-inst;

                  leaf status {
                    type int32;
                    mandatory true;
                    description
                      "Status of postscript. 0 is success, -1=failure";
                  }

                  leaf exitcode {
                    type int32;
                    mandatory true;
                    description
                      "the exitcode with which the postscript exited";
                  }
      }  // container stampede.job_inst.post.end

-  **stampede.job_inst.host.info**

   ::

      container stampede.job_inst.host.info {
                  description
                    "Host information associated with a job instance";
                  uses base-job-inst;

                  leaf site {
                    type string;
                    mandatory true;
                    description "Site name";
                  }

                  leaf hostname {
                    type inet:host;
                    mandatory true;
                    description "Host name";
                  }

                  leaf ip {
                    type inet:ip-address;
                    mandatory true;
                    description "IP address";
                  }

                  leaf total_memory {
                    type uint64;
                    description
                      "Total RAM on host";
                  }

                  leaf uname {
                    type string;
                    description
                      "Operating system name";
                  }
      }  // container stampede.job_inst.host.info

-  **stampede.job_inst.image.info**

   ::

      container stampede.job_inst.image.info {
                  description
                    "Image size associated with a job instance";
                  uses base-job-inst;

                  leaf size {
                    type uint64;
                    description "Image size";
                  }

                  leaf sched.id {
                    type string;
                    mandatory true;
                    description
                      "Identifier for job in scheduler";
                  }
      }  // container stampede.job_inst.image.info

-  **stampede.job_inst.tag**

   ::

      container stampede.job_inst.tag {
                  description
                    "A tag event to tag errors at a job_instance level";
                  uses base-job-inst;

                  leaf name {
                    type string;
                    description "Name of tagged event such as int.error";
                  }

                  leaf count {
                    type int32;
                    mandatory true;
                    description
                      "count of occurences of the events of type name for the job_instance";
                  }
      }  // container stampede.job_inst.tag

-  **stampede.job_inst.composite**

   ::

      container stampede.job_inst.composite{
                  description
                    "A de-normalized composite event at the job_instance level that captures all the job information. Useful when populating AMQP";
                  uses base-job-inst;

                 leaf jobtype {
                    type string;
                    description
                      "Type of job as classified by the planner.";
                  }

                 leaf stdin.file {
                    type string;
                    description
                      "Path to file containing standard input of job";
                  }

                  leaf stdout.file {
                    type string;
                    mandatory true;
                    description
                      "Path to file containing standard output of job";
                  }

                  leaf stdout.text {
                    type string;
                    description
                      "Text containing output of job";
                  }

                  leaf stderr.file {
                    type string;
                    mandatory true;
                    description
                      "Path to file containing standard error of job";
                  }

                  leaf stderr.text {
                    type string;
                    description
                      "Text containing standard error of job";
                  }

                  leaf user {
                    type string;
                    description
                      "Scheduler's name for user";
                  }

                  leaf site {
                    type string;
                    mandatory true;
                    description
                      "DAX name for the site at which the job ran";
                  }

                  leaf hostname {
                    type inet:host;
                    mandatory true;
                    description "Host name";
                  }

                  leaf  {
                    type string;
                    description
                      "Path to working directory";
                  }

                  leaf local.dur {
                    type decimal64 {
                      fraction-digits 6;
                    }
                    units "seconds";
                    description
                      "Duration as seen at the local node";
                  }

                  leaf status {
                    type int32;
                    mandatory true;
                    description
                      "Execution status. 0=success, -1=failure";
                  }

                  leaf exitcode {
                    type int32;
                    mandatory true;
                    description
                      "the exitcode with which the executable exited";
                  }

                  leaf multiplier_factor {
                    type int32;
                    mandatory true;
                    description
                      "the multiplier factor for use in statistics";
                  }

                  leaf cluster.start {
                    type nl_ts;
                    description
                      "When the enclosing cluster started";
                  }

                  leaf cluster.dur {
                    type decimal64 {
                      fraction-digits 6;
                    }
                    units "seconds";
                    description
                      "Duration of enclosing cluster";
                  }

                  leaf int_error_count {
                    type int32;
                    mandatory true;
                    description
                      "number of integrity errors encountered";
                  }
      }  // container stampede.job_inst.composite

-  **stampede.inv.start**

   ::

      container stampede.inv.start {
                  description
                    "Start of an invocation";
                  uses base-event;

                  leaf job_inst.id {
                    type int32;
                    mandatory true;
                    description
                      "Job instance identifier i.e the submit sequence generated by monitord";
                  }

                  leaf job.id {
                    type string;
                    mandatory true;
                    description
                      "Identifier for corresponding job in the DAG";
                  }

                  leaf inv.id {
                    type int32;
                    mandatory true;
                    description
                      "Identifier for invocation. "
                        + "Sequence number, with -1=prescript and -2=postscript";
                  }
      }  // container stampede.inv.start

-  **stampede.inv.end**

   ::

      container stampede.inv.end {
                  description
                    "End of an invocation";
                  uses base-event;

                  leaf job_inst.id {
                    type int32;
                    mandatory true;
                    description
                      "Job instance identifier i.e the submit sequence generated by monitord";
                  }

                  leaf inv.id {
                    type int32;
                    mandatory true;
                    description
                      "Identifier for invocation. "
                        + "Sequence number, with -1=prescript and -2=postscript";
                  }

                  leaf job.id {
                    type string;
                    mandatory true;
                    description
                      "Identifier for corresponding job in the DAG";
                  }

                  leaf start_time {
                    type nl_ts;
                    description
                      "The start time of the event";
                  }

                  leaf dur {
                    type decimal64 {
                      fraction-digits 6;
                    }
                    units "seconds";
                    description
                      "Duration of invocation";
                  }

                  leaf remote_cpu_time {
                    type decimal64 {
                      fraction-digits 6;
                    }
                    units "seconds";
                    description
                      "remote CPU time computed as the stime  + utime";
                  }

                  leaf exitcode {
                    type int32;
                    description
                      "the exitcode with which the executable exited";
                  }

                  leaf transformation {
                    type string;
                    mandatory true;
                    description
                      "Transformation associated with this invocation";
                  }

                  leaf executable {
                    type string;
                    mandatory true;
                    description
                      "Program executed for this invocation";
                  }

                  leaf argv {
                    type string;
                    description
                      "All arguments given to executable on command-line";
                  }

                  leaf task.id {
                    type string;
                    description
                      "Identifier for related task in the DAX";
                  }
      }  // container stampede.inv.end

-  **stampede.int.metric**

   ::

      container stampede.int.metric {
                  description
                    "additional task events picked up from the job stdout";
                  uses base-event;

                  leaf job_inst.id {
                    type int32;
                    mandatory true;
                    description
                       "Job instance identifier i.e the submit sequence generated by monitord";
                  }

                  leaf job.id {
                    type string;
                    mandatory true;
                    description
                       "Identifier for corresponding job in the DAG";
                  }

                  leaf type{
                    type string;
                    description
                      "enumerated type of metrics check|compute";
                  }

                  leaf file_type{
                    type string;
                    description
                      "enumerated type of file types input|output";
                  }

                  leaf count{
                    type int32;
                    description
                      "number of integrity events grouped by type , file_type ";
                  }

                  leaf duration{
                    type float;
                    description
                      "duration in seconds it took to perform these events ";
                  }
      }  // container stampede.int.metric

-  **stampede.static.meta.start**

   ::

      container stampede.static.meta.start {
                  uses base-event;
      }  // container stampede.static.meta.start

-  **stampede.static.meta.end**

   ::

      container stampede.static.meta.end {
                  uses base-event;
      } // container stampede.static.meta.end

-  **stampede.xwf.meta**

   ::

      container stampede.xwf.meta {
                  description
                    "Metadata associated with a workflow";
                  uses base-metadata;
      }  // container stampede.xwf.meta

-  **stampede.task.meta**

   ::

      container stampede.task.meta {
                  description
                    "Metadata associated with a task";
                  uses base-metadata;

                  leaf task.id {
                    type string;
                    description
                      "Identifier for related task in the DAX";
                  }
      }  // container stampede.task.meta

-  **stampede.task.monitoring**

   ::

      container stampede.task.monitoring {
                  description
                    "additional task events picked up from the job stdout";
                  uses base-event;

                  leaf job_inst.id {
                    type int32;
                    mandatory true;
                    description
                       "Job instance identifier i.e the submit sequence generated by monitord";
                  }

                  leaf job.id {
                    type string;
                    mandatory true;
                    description
                       "Identifier for corresponding job in the DAG";
                  }

                  leaf monitoring_event{
                    type string;
                    description
                      "the name of the monitoring event parsed from the job stdout";
                  }

                  leaf key{
                    type string;
                    description
                      "user defined keys in their payload in the event defined in the job stdout";
                  }
      }  // container stampede.task.meta

-  **stampede.rc.meta**

   ::

      container stampede.rc.meta {
                  description
                    "Metadata associated with a file in the replica catalog";
                  uses base-metadata;

                  leaf lfn.id {
                    type string;
                    description
                      "Logical File Identifier for the file";
                  }
      }  // container stampede.rc.meta

-  **stampede.wf.map.file**

   ::

      container stampede.wf.map.file {
                  description
                    "Event that captures what task generates or consumes a particular file";
                  uses base-event;

                  leaf lfn.id {
                    type string;
                    description
                      "Logical File Identifier for the file";
                  }

                  leaf task.id {
                    type string;
                    description
                      "Identifier for related task in the DAX";
                  }
      }  // container stampede.wf.map.file

.. _monitoring-amqp:

Publishing to AMQP Message Servers
==================================

The\ `workflow events <#stampede_wf_events>`__ generated by
*pegasus-monitord*\ can also be used to publish to an AMQP message
server such as RabbitMQ in addition to the stampede workflow database.

   **Note**

   A thing to keep in mind. The workflow events are documented as
   conforming to the netlogger requirements. When events are pushed to
   an AMQP endpoint, the . in the keys are replaced by \_ .

Configuration
-------------

In order to get *pegasus-monitord* to populate to a message queue, you
can set the following property

::

   pegasus.catalog.workflow.amqp.url amqp://[USERNAME:PASSWORD@]amqp.isi.edu[:port]/<exchange_name>

The routing key set for the messages matches the name of the stampede
workflow event being sent. By default, if you enable AMQP population
only the following events are sent to the server

-  stampede.job_inst.tag

-  stampede.inv.end

-  stampede.wf.plan

To configure additional events, you can specify a comma separated list
of events that need to be sent using the property
**pegasus.catalog.workflow.amqp.events**. For example

::

   pegasus.catalog.workflow.amqp.events = stampede.xwf.*,stampede.static.*

..

   **Note**

   To get all events you can just specify \* as the value to the
   property.

.. _amqp-rabbitmq-es:

Monitord, RabbitMQ, ElasticSearch Example
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The AMQP support in Monitord is still a work in progress, but even the
current functionality provides basic support for getting the monitoring
data into ElasticSearch. In our development environment, we use a
RabbitMQ instance with a simple exhange/queue. The configuration
required for Pegasus is:

::

   # help Pegasus developers collect data on integrity failures
   pegasus.monitord.encoding = json
   pegasus.catalog.workflow.amqp.url = amqp://friend:donatedata@msgs.pegasus.isi.edu:5672/prod/workflows


On the other side of the queue, Logstash is configured to receive the
messages and forward them to ElasticSearch. The Logstash pipeline looks
something like:

::

   input {
     rabbitmq {
       type => "workflow-events"
       host => "msg.pegasus.isi.edu"
       vhost => "prod"
       queue => "workflows-es"
       heartbeat => 30
       durable => true
       password => "XXXXXX"
       user => "prod-logstash"
     }
   }

   filter {
     if [type] == "workflow-events" {
       mutate {
         convert => {
           "dur" => "float"
           "remote_cpu_time" => "float"
         }
       }
       date {
         # set @timestamp from the ts of the actual event
         match => [ "ts", "UNIX" ]
       }
       date {
         match => [ "start_time", "UNIX" ]
         target => "start_time_human"
       }
       fingerprint {
         # create unique document ids
         source => "ts"
         concatenate_sources => true
         method => "SHA1"
         key => "Pegasus Event"
         target => "[@metadata][fingerprint]"
       }
     }
   }

   output {
     if [type] == "workflow-events" {
       elasticsearch {
         "hosts" => ["es1.isi.edu:9200", "es2.isi.edu:9200"]
         "sniffing" => false
         "document_type" => "workflow-events"
         "document_id" => "%{[@metadata][fingerprint]}"
         "index" => "workflow-events-%{+YYYY.MM.dd}"
         "template" => "/usr/share/logstash/templates/workflow-events.json"
         "template_name" => "workflow-events-*"
         "template_overwrite" => true
       }
     }

   }


Once the data is ElasticSearch, you can easily create for example
Grafana dashboard like:

.. figure:: images/grafana.png
   :alt: Grafana Dashboard
   :width: 100.0%

   Grafana Dashboard

A Pre-Configured Data Collection Pipeline
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In this `repository <https://github.com/pegasus-isi/dibbs-data-collection-setup>`_, 
we provide a containerized data-collection/visualization pipeline similar to 
what we use in production. The figure below illustrates the processes involved 
in the pipeline and how they are connected to one another. For more information 
regarding setup and usage, please visit the link referenced above.

.. figure:: images/data-collection-pipeline.svg
   :alt: Data Collection/Visualization Pipeline
   :width: 100.0%

   Data Collection/Visualization Pipeline