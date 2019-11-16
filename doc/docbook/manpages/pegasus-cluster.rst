===============
pegasus-cluster
===============

1
pegasus-cluster
run a list of applications
   ::

      pegasus-cluster [-d] [-e | -f] [-S ec] [-s fn] [-R fn] [-n nr] [inputfile]

.. __description:

Description
===========

The **pegasus-cluster** tool executes a list of application in the order
specified (assuming sequential mode.) It is generally used to do
horizontal clustering of independent application, and does not care
about any application failures. Such failures should be caught by using
**pegasus-kickstart** to start application.

In vertical clustering mode, the *hard failure* mode is encouraged,
ending execution as soon as one application fails. When running a
complex workflow through **pegasus-cluster** , the order of applications
in the input file must be topologically sorted.

Applications are usually using **pegasus-kickstart** to execute. In the
**pegasus-kickstart** case, all invocations of **pegasus-kickstart**
except the first should add the **pegasus-kickstart** option *-H* to
supress repeating the XML preamble and certain other headers of no
interest when repeated.

**pegasus-cluster** permits shell-style quoting. One level of quoting is
removed from the arguments. Please note that **pegasus-kickstart** will
also remove one level of quoting.

.. __arguments:

Arguments
=========

**-d**
   This option increases the debug level. Debug message are generated on
   *stdout* . By default, debugging is minimal.

**-e**
   This flag turns on the old behavior of **pegasus-cluster** to always
   run everything *and* return success no matter what. The **-e** flag
   is mutually exclusive with the **-f** flag. By default, all
   applications are executed regardles of failures. Any detected
   application failure results in a non-zero exit status from
   **pegasus-cluster**.

**-f**
   In hard failure mode, as soon as one application fails, either
   through a non-zero exit code, or by dying on a signal, further
   execution is stopped. In parallel execution mode, one or more other
   applications later in the sequence file may have been started already
   by the time failure is detected. **Pegasus-cluster** will wait for
   the completion of these applications, but not start new ones. The
   **-f** flag is mutually exclusive with the **-e** flag. By default,
   all applications are executed regardless of failures. Any detected
   application failure results in a non-zero exit status from
   **pegasus-cluster**.

**-h**
   This option prints the help message and exits the program.

**-s fn**
   This option will send protocol message (for Mei) to the specified
   file. By default, all message are written to *stdout* .

**-R fn**
   The progress reporting feature, if turned on, will write one event
   record whenever an application is started, and one event record
   whenever an application finished. This is to enable tracking of jobs
   in progress. By default, track logs are not written, unless the
   environment variable *SEQEXEC_PROGRESS_REPORT* is set. If set,
   progress reports are appended to the file pointed to by the
   environment variable.

**-S ec**
   This option is a multi-option, which may be used multiple times. For
   each given non-zero exit-code of an application, mark it as a form of
   success. In **-f** mode, this means that **pegasus-cluster** will not
   fail when seeing this exit code from any application it runs. By
   default, all non-zero exit code constitute failure.

**-n nr**
   This option determines the amount of parallel execution. Typically,
   parallel execution is only recommended on multi-core systems, and
   must be deployed rather carefully, i.e. only completely independent
   jobs across of whole *inputfile* should ever be attempted to be run
   in parallel. The argument **nr** is the number of parallel jobs that
   should be used. In addition to a non-negative integer, the word
   *auto* is also understood. When *auto* is specified,
   **pegasus-cluster** will attempt to automatically determine the
   number of cores available in the system. Strictly sequential
   execution, as if *nr* was 1, is the default. If the environment
   variable *SEQEXEC_CPUS* is set, it will determine the default number
   of CPUs.

**inputfile**
   The input file specifies a list of application to run, one per line.
   Comments and empty lines are permitted. The comment character is the
   octothorpe (#), and extends to the end of line. By default,
   **pegasus-cluster** uses *stdin* to read the list of applications to
   execute.

.. __return_value:

Return Value
============

The **pegasus-cluster** tool returns 1, if an illegal option was used.
It returns 2, if the status file from option **-s** cannot be opened. It
returns 3, if the input file cannot be opened. It does *not* return any
failure for failed applications in old-exit **-e** mode. In *default*
and hard failure **-f** mode, it will return 5 for true failure. The
determination of failure is modified by the **-S** option.

All other internal errors being absent, **pegasus-cluster** will always
return 0 when run without **-f** . Unlike shell, it will *not* return
the last application’s exit code. In *default* mode, it will return 5,
if any application failed. Unlike shell, it will *not* return the last
application’s exit code. However, it will execute all applications. The
determination of failure is modified by the **-S** flag. In **-f** mode,
\*pegasus-cluster returns either 0 if all main sequence applications
succeeded, or 5 if one failed; or more than one in parallel execution
mode. It will run only as long as applications were successful. As
before, the \*-S flag determines what constitutes a failure.

The **pegasus-cluster** application will also create a small summary on
*stdout* for each job, and one for itself, about the success and
failure. The field **failed** reports any exit code that was not zero or
a signal of death termination. It does *not* include non-zero exit codes
that were marked as success using the **-S** option.

.. __task_summary:

Task Summary
============

Each task executed by **pegasus-cluster** generates a record bracketed
by square brackets like this (each entry is broken over two lines for
readability):

::

   [cluster-task id=1, start="2011-04-27T14:31:25.340-07:00", duration=0.521,
    status=0, line=1, pid=18543, app="/bin/usleep"]
   [cluster-task id=2, start="2011-04-27T14:31:25.342-07:00", duration=0.619,
    status=0, line=2, pid=18544, app="/bin/usleep"]
   [cluster-task id=3, start="2011-04-27T14:31:25.862-07:00", duration=0.619,
    status=0, line=3, pid=18549, app="/bin/usleep"]

Each record is introduced by the string *cluster-task* with the
following constituents, where strings are quoted:

**id**
   This is a numerical value for main sequence application, indicating
   the application’s place in the sequence file. The setup task uses the
   string *setup* , and the cleanup task uses the string *cleanup* .

**start**
   is the ISO 8601 time stamp, with millisecond resolution, when the
   application was started. This string is quoted.

**duration**
   is the application wall-time duration in seconds, with millisecond
   resolution.

**status**
   is the *raw* exit status as returned by the *wait* family of system
   calls. Typically, the exit code is found in the high byte, and the
   signal of death in the low byte. Typically, 0 indicates a successful
   execution, and any other value a problem. However, details could
   differ between systems, and exit codes are only meaningful on the
   same os and architecture.

**line**
   is the line number where the task was found in the main sequence
   file. Setup- and cleanup tasks don’t have this attribute.

**pid**
   is the process id under which the application had run.

**app**
   is the path to the application that was started. As with the progress
   record, any **pegasus-kickstart** will be parsed out so that you see
   the true application.

.. __pegasus_cluster_summary:

pegasus-cluster Summary
=======================

The final summary of counts is a record bracketed by square brackets
like this (broken over two lines for readability):

::

   [cluster-summary stat="ok", lines=3, tasks=3, succeeded=3, failed=0, extra=0,
    duration=1.143, start="2011-04-27T14:31:25.338-07:00", pid=18542, app="./seqexec"]

The record is introduced by the string *cluster-summary* with the
following constituents:

**stat**
   The string *fail* when **pegasus-cluster** would return with an exit
   status of 5. Concretely, this is any failure in *default* mode, and
   first failure in **-f** mode. Otherwise, it will always be the string
   *ok* , if the record is produced.

**lines**
   is the stopping line number of the input sequence file, indicating
   how far processing got. Up to the number of cores additional lines
   may have been parsed in case of **-f** mode.

**tasks**
   is the number of tasks processed.

**succeeded**
   is the number of main sequence jobs that succeeded.

**failed**
   is the number of main sequence jobs that failed. The failure
   condition depends on the **-S** settings, too.

**extra**
   is 0, 1 or 2, depending on the existence of setup- and cleanup jobs.

**duration**
   is the duration in seconds, with millisecond resolution, how long
   \*pegasus-cluster ran.

**start**
   is the start time of **pegasus-cluster** as ISO 8601 time stamp.

.. __see_also:

See Also
========

**pegasus-kickstart(1)**

.. __caveats:

Caveats
=======

The **-S** option sets success codes globally. It is not possible to
activate success codes only for one specific application, and doing so
would break the shell compatibility. Due to the global nature, use
success codes sparingly as last resort emergency handler. In better
plannable environments, you should use an application wrapper instead.

.. __example:

Example
=======

The following shows an example input file to **pegasus-cluster** making
use of **pegasus-kickstart** to track applications.

::

   #
   # mkdir
   /path/to/pegasus-kickstart -R HPC -n mkdir /bin/mkdir -m 2755 -p split-corpus split-ne-corpus
   #
   # drop-dian
   /path/to/pegasus-kickstart -H -R HPC -n drop-dian -o '^f-new.plain' /path/to/drop-dian /path/to/f-tok.plain /path/to/f-tok.NE
   #
   # split-corpus
   /path/to/pegasus-kickstart -H -R HPC -n split-corpus /path/to/split-seq-new.pl 23 f-new.plain split-corpus/corpus.
   #
   # split-corpus
   /path/to/pegasus-kickstart -H -R HPC -n split-corpus /path/to/split-seq-new.pl 23 /path/to/f-tok.NE split-ne-corpus/corpus.

.. __environment_variables:

Environment Variables
=====================

A number of environment variables permits to influence the behavior of
**pegasus-cluster** during run-time.

**SEQEXEC_PROGRESS_REPORT**
   If this variable is set, and points to a writable file location,
   progress report records are appended to the file. While care is taken
   to atomically append records to the log file, in case concurrent
   instances of **pegasus-cluster** are running, broken Linux NFS may
   still garble some content.

**SEQEXEC_CPUS**
   If this variable is set to a non-negative integer, that many CPUs are
   attempted to be used. The special value *auto* permits to auto-detect
   the number of CPUs available to **pegasus-cluster** on the system.

**SEQEXEC_SETUP**
   If this variable is set, and contains a single fully-qualified path
   to an executable and arguments, this executable will be run before
   any jobs are started. The exit code of this setup job will have no
   effect upon the main job sequence. Success or failure will not be
   counted towards the summary.

**SEQEXEC_CLEANUP**
   If this variable is set, and contains a single fully-qualified path
   to an executable and arguments, this executable will be before
   **pegasus-cluster** quits. Failure of any previous job will have no
   effect on the ability to run this job. The exit code of the cleanup
   job will have no effect on the overall success or failure state.
   Success or failure will not be counted towards the summary.

.. __history:

History
=======

As you may have noticed, **pegasus-cluster** had the name **seqexec** in
previous incantations. We are slowly moving to the new name to avoid
clashes in a larger OS installation setting. However, there is no
pertinent need to change the internal name, too, as no name clashes are
expected.

.. __authors:

Authors
=======

Jens-S. Vöckler <voeckler at isi dot edu>

Pegasus **http://pegasus.isi.edu/**
