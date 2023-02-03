.. _cli-pegasus-analyzer:


================
pegasus-analyzer
================

pegasus-analyzer - debugs a workflow.
   ::

      pegasus-analyzer [--help|-h] [--quiet|-q] [--strict|-s]
                       [--summary|-S] [--monitord|-m|-t] [--verbose|-v]
                       [--output-dir|-o output_dir]
                       [--dag dag_filename] [--dir|-d|-i input_dir]
                       [--print|-p print_options] [--type workflow_type]
                       [--debug-job job][--debug-dir debug_dir]
                       [--local-executable local user executable]
                       [--conf|-c property_file] [--files]
                       [--top-dir dir_name] [--traverse-all|t] [--recurse|-r]
                       [--indent|-I indent_length]
                       [workflow_directory]



Description
===========

**pegasus-analyzer** is a command-line utility for parsing the
*jobstate.log* file and reporting successful and failed jobs. When
executed without any options, it will query the **SQLite** or **MySQL**
database and retrieve failed job information for the particular
workflow. When invoked with the **--files** option, it will retrieve
information from several log files, isolating jobs that did not complete
successfully, and printing their *stdout* and *stderr* so that users can
get detailed information about their workflow runs.



Options
=======

**-h**; \ **--help**
   Prints a usage summary with all the available command-line options.

**-q**; \ **--quiet**
   Only print the the output and error filenames instead of their
   contents.

**-s**; \ **--strict**
   Get jobs' output and error filenames from the job’s submit file.

**-S**; \ **--summary**
   Just print the summary about the jobs breakdown status and exit.

**-m**; \ **-t**; \ **--monitord**
   Invoke **pegasus-monitord** before analyzing the *jobstate.log* file.
   Although **pegasus-analyzer** can be executed during the workflow
   execution as well as after the workflow has already completed
   execution, **pegasus-monitord"** is always invoked with the
   **--replay** option. Since multiple instances of
   **pegasus-monitord"** should not be executed simultaneously in the
   same workflow directory, the user should ensure that no other
   instances of **pegasus-monitord** are running. If the *run_directory*
   is writable, **pegasus-analyzer** will create a *jobstate.log* file
   there, rotating an older log, if it is found. If the *run_directory*
   is not writable (e.g. when the user debugging the workflow is not the
   same user that ran the workflow), **pegasus-analyzer** will exit and
   ask the user to provide the **--output-dir** option, in order to
   provide an alternative location for **pegasus-monitord** log files.

**-v**; \ **--verbose**
   Sets the log level for **pegasus-analyzer**. If omitted, the default
   *level* will be set to *WARNING*. When this option is given, the log
   level is changed to *INFO*. If this option is repeated, the log level
   will be changed to *DEBUG*.

**-o** *output_dir*; \ **--output-dir** *output_dir*
   This option provides an alternative location for all monitoring log
   files for a particular workflow. It is mainly used when an user does
   not have write privileges to a workflow directory and needs to
   generate the log files needed by **pegasus-analyzer**. If this option
   is used in conjunction with the **--monitord** option, it will invoke
   **pegasus-monitord** using *output_dir* to store all output files.
   Because workflows can have sub-workflows, **pegasus-monitord** will
   create its files prepending the workflow *wf_uuid* to each filename.
   This way, multiple workflow files can be stored in the same
   directory. **pegasus-analyzer** has built-in logic to find the
   specific *jobstate.log* file by looking at the workflow
   *braindump.txt* file first and figuring out the corresponding
   *wf_uuid.* If *output_dir* does not exist, it will be created.

**--dag** 'dag_filename
   In this option, *dag_filename* specifies the path to the *DAG* file
   to use. **pegasus-analyzer** will get the directory information from
   the *dag_filename*. This option overrides the **--dir** option below.

**-d** *input_dir*; \ **-i** *input_dir*; \ **--dir** *input_dir*
   Makes **pegasus-analyzer** look for the *jobstate.log* file in the
   *input_dir* directory. If this option is omitted,
   **pegasus-analyzer** will look in the current directory.

**-p** *print_options*; \ **--print** *print_options*
   Tells **pegasus-analyzer** what extra information it should print for
   failed jobs. *print_options* is a comma-delimited list of options,
   that include *pre*, *invocation*, and/or *all*, which activates all
   printing options. With the *pre* option, **pegasus-analyzer** will
   print the *pre-script* information for failed jobs. For the
   *invocation* option, **pegasus-analyzer** will print the *invocation*
   command, so users can manually run the failed job.

**--debug-job** *job*
   When given this option, **pegasus-analyzer** turns on its
   *debug_mode*, when it can be used to debug a particular Pegasus Lite
   job. In this mode, **pegasus-analyzer** will create a shell script in
   the *debug_dir* (see below, for specifying it) and copy all necessary
   files to this local directory and then execute the job locally.

**--debug-dir** *debug_dir*
   When in *debug_mode*, **pegasus-analyzer** will create a temporary
   debug directory. Users can give this option in order to specify a
   particular *debug_dir* directory to be used instead.

**--local-executable** *local user executable*
   When in debug job mode for Pegasus Lite jobs, pegasus-analyzer
   creates a shell script to execute the Pegasus Lite job locally in a
   debug directory. The Pegasus Lite script refers to remote user
   executable path. This option can be used to pass the local path to
   the user executable on the submit host. If the path to the user
   executable in the Pegasus Lite job is same as the local installation.

**--type** *workflow_type*
   In this options, users specify what *workflow_type* they want to
   debug. At this moment, the only *workflow_type* available is
   **condor** and it is the default value if this option is not
   specified.

**-c** *property_file*; \ **--conf** *property_file*
   This option is used to specify an alternative property file, which
   may contain the path to the database to be used by
   **pegasus-analyzer**. If this option is not specified, the config
   file specified in the **braindump.txt** file will take precedence.

**--files**
   This option allows users to run **pegasus-analyzer** using the files
   in the workflow directory instead of the database as the source of
   information. **pegasus-analyzer** will output the same information,
   this option only changes where the data comes from.

**--top-dir** *dir_name*
   This option enables **pegasus-analyzer** to show information about
   sub-workflows when using the database mode. When debugging a
   top-level workflow with failures in sub-workflows, the analyzer will
   automatically print the command users should use to debug a failed
   sub-workflow. This allows the analyzer to find the database it needs
   to access.

**-T** ; \ **--traverse-all**
   This option set **pegasus-analyzer** to go through all the descendant
   workflows of the workflow running in the submit directory passed,
   irrespective of the fact whether the workflow has succeeded or failed.
   This option is useful when running **pegasus-analyzer** on a running
   hierarchical workflow, to detect failures in sub-workflows that are
   currently running.
   This option is mutually exclusive to the **--recurse** option, that
   recurses through only failed sub workflow jobs.

**-r**; \ **--recurse**
   This option sets **pegasus-analyzer** to automatically recurse into
   sub workflows in case of failure. By default, if a workflow has a sub
   workflow in it, and that sub workflow fails , **pegasus-analyzer**
   reports that the sub workflow node failed, and lists a command
   invocation that the user must execute to determine what jobs in the
   sub workflow failed. If this option is set, then the analyzer
   automatically issues the command invocation and in addition displays
   the failed jobs in the sub workflow.
   This option is mutually exclusive to the **--traverse-all** option,
   that traverses through all descendant workflows.

**-I**; \ **--indent**
   This option sets **indent** length to use when walking displaying
   results from invoking the command on a hierarchical workflow using the
   **-r|--recurse** option. This option dictates the number of white spaces
   to use when indenting the output of pegasus-analyzer of a sub workflow.


Environment Variables
=====================

**pegasus-analyzer** does not require that any environmental variables
be set. It locates its required Python modules based on its own
location, and therefore should not be moved outside of Pegasus' bin
directory.



Example
=======

The simplest way to use **pegasus-analyzer** is to go to the
*run_directory* and invoke the analyzer:

::

   $ pegasus-analyzer .

which will cause **pegasus-analyzer** to print information about the
workflow in the current directory.

**pegasus-analyzer** output contains a summary, followed by detailed
information about each job that either failed, or is in an unknown
state. Here is the summary section of the output:

::

   **************************Summary***************************

    Total jobs         :     75 (100.00%)
    # jobs succeeded   :     41 (54.67%)
    # jobs failed      :      0 (0.00%)
    # jobs held        :      1 (1.33%)
    # jobs unsubmitted :     33 (44.00%)
    # jobs unknown     :      1 (1.33%)

*jobs_succeeded* are jobs that have completed successfully.
*jobs_failed* are jobs that have finished, but that did not complete
successfully. *jobs_unsubmitted* are jobs that are listed in the
*dag_file*, but no information about them was found in the
*jobstate.log* file. *jobs_held* are jobs that were in HTCondor HELD
state on the last retry of the job. With default, pegasus added
periodic_remove expression with the jobs, a held job can eventually
fail. In that case, held job appears as a failed job also. Finally,
*jobs_unknown* are jobs that have started, but have not reached
completion.

After the summary section, **pegasus-analyzer** will display information
about each job in the *job_failed* and *job_unknown* categories.

::

   *******************************Held jobs' details*******************************

   ====================================sleep_j2====================================

           submit file            : sleep_j2.sub
           last_job_instance_id   : 7
           reason                 :  Error from slot1@corbusier.isi.edu:
                                     STARTER at 128.9.64.188 failed to
                                     send file(s) to
                                     <128.9.64.188:62639>: error reading from
                                     /opt/condor/8.4.8/local.corbusier/execute/dir_76205/f.out:
                                     (errno 2) No such file or directory;
                                    SHADOW failed to receive file(s) from <128.9.64.188:62653>

In the above example, the *sleep_j2* job was held, and the analyzer
displays the reason why it was held, as determined from the dagman.out
file for the workflow. The last_job_instance_id is the database id for
the job in the job instance table of the monitoring database.

::

   ******************Failed jobs' details**********************

   =======================findrange_j3=========================

     last state: POST_SCRIPT_FAILURE
           site: local
    submit file: /home/user/diamond-submit/findrange_j3.sub
    output file: /home/user/diamond-submit/findrange_j3.out.000
     error file: /home/user/diamond-submit/findrange_j3.err.000

   --------------------Task #1 - Summary-----------------------

    site        : local
    hostname    : server-machine.domain.com
    executable  : (null)
    arguments   : -a findrange -T 60 -i f.b2 -o f.c2
    error       : 2
    working dir :

In the example above, the *findrange_j3* job has failed, and the
analyzer displays information about the job, showing that the job
finished with a *POST_SCRIPT_FAILURE*, and lists the *submit*, *output*
and *error* files for this job. Whenever **pegasus-analyzer** detects
that the output file contains a kickstart record, it will display the
breakdown containing each task in the job (in this case we only have one
task). Because **pegasus-analyzer** was not invoked with the **--quiet**
flag, it will also display the contents of the *output* and *error*
files (or the stdout and stderr sections of the kickstart record), which
in this case are both empty.

In the case of *SUBDAG* and *subdax* jobs, **pegasus-analyzer** will
indicate it, and show the command needed for the user to debug that
sub-workflow. For example:

::

   =================subdax_black_ID000009=====================

     last state: JOB_FAILURE
           site: local
    submit file: /home/user/run1/subdax_black_ID000009.sub
    output file: /home/user/run1/subdax_black_ID000009.out
     error file: /home/user/run1/subdax_black_ID000009.err
     This job contains sub workflows!
     Please run the command below for more information:
     pegasus-analyzer -d /home/user/run1/blackdiamond_ID000009.000

   -----------------subdax_black_ID000009.out-----------------

   Executing condor dagman ...

   -----------------subdax_black_ID000009.err-----------------

tells the user the *subdax_black_ID000009* sub-workflow failed, and that
it can be debugged by using the indicated **pegasus-analyzer** command.



See Also
========

pegasus-status(1), pegasus-monitord(1), pegasus-statistics(1).


