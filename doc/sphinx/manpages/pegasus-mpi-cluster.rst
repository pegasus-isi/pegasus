===================
pegasus-mpi-cluster
===================

Enables running DAGs (Directed Acyclic Graphs) on clusters using MPI.

   ::

      pegasus-mpi-cluster [options] workflow.dag



Description
===========

**pegasus-mpi-cluster** is a tool used to run HTC (High Throughput
Computing) scientific workflows on systems designed for HPC (High
Performance Computing). Many HPC systems have custom architectures that
are optimized for tightly-coupled, parallel applications. These systems
commonly have exotic, low-latency networks that are designed for passing
short messages very quickly between compute nodes. Many of these
networks are so highly optimized that the compute nodes do not even
support a TCP/IP stack. This makes it impossible to run HTC applications
using software that was designed for commodity clusters, such as Condor.

**pegasus-mpi-cluster** was developed to enable loosely-coupled HTC
applications such as scientific workflows to take advantage of HPC
systems. In order to get around the network issues outlined above,
**pegasus-mpi-cluster** uses MPI (Message Passing Interface), a commonly
used API for writing SPMD (Single Process, Multiple Data) parallel
applications. Most HPC systems have an MPI implementation that works on
whatever exotic network architecture the system uses.

An **pegasus-mpi-cluster** job consists of a single master process (this
process is rank 0 in MPI parlance) and several worker processes. The
master process manages the workflow and assigns workflow tasks to
workers for execution. The workers execute the tasks and return the
results to the master. Any output written to stdout or stderr by the
tasks is captured (see `TASK STDIO <#TASK_STDIO>`__).

**pegasus-mpi-cluster** applications are expressed as DAGs (Directed
Acyclic Graphs) (see `DAG FILES <#DAG_FILES>`__). Each node in the graph
represents a task, and the edges represent dependencies between the
tasks that constrain the order in which the tasks are executed. Each
task is a program and a set of parameters that need to be run (i.e. a
command and some optional arguments). The dependencies typically
represent data flow dependencies in the application, where the output
files produced by one task are needed as inputs for another.

If an error occurs while executing a DAG that causes the workflow to
stop, it can be restarted using a rescue file, which records the
progress of the workflow (see `RESCUE FILES <#RESCUE_FILES>`__). This
enables **pegasus-mpi-cluster** to pick up running the workflow where it
stopped.

**pegasus-mpi-cluster** was designed to work either as a standalone tool
or as a complement to the Pegasus Workflow Managment System (WMS). For
more information about using PMC with Pegasus see the section on `PMC
AND PEGASUS <#PMC_AND_PEGASUS>`__.

**pegasus-mpi-cluster** allows applications expressed as a DAG to be
executed in parallel on a large number of compute nodes. It is designed
to be simple, lightweight and robust.



Options
=======

**-h**; \ **--help**
   Print help message

**-V**; \ **--version**
   Print version information

**-v**; \ **--verbose**
   Increase logging verbosity. Adding multiple **-v** increases the
   level more. The default log level is *INFO*. (see
   `LOGGING <#LOGGING>`__)

**-q**; \ **--quiet**
   Decrease logging verbosity. Adding multiple **-q** decreases the
   level more. The default log level is *INFO*. (see
   `LOGGING <#LOGGING>`__)

**-s**; \ **--skip-rescue**
   Ignore the rescue file for *workflow.dag* if it exists. Note that
   **pegasus-mpi-cluster** will still create a new rescue file for the
   current run. The default behavior is to use the rescue file if one is
   found. (see `RESCUE FILES <#RESCUE_FILES>`__)

**-o** *path*; \ **--stdout** *path*
   Path to file for task stdout. (see `TASK STDIO <#TASK_STDIO>`__ and
   **--per-task-stdio**)

**-e** *path*; \ **--stderr** *path*
   Path to file for task stderr. (see `TASK STDIO <#TASK_STDIO>`__ and
   **--per-task-stdio**)

**-m** *M*; \ **--max-failures** *M*
   Stop submitting new tasks after *M* tasks have failed. Once *M* has
   been reached, **pegasus-mpi-cluster** will finish running any tasks
   that have been started, but will not start any more tasks. This
   option is used to prevent **pegasus-mpi-cluster** from continuing to
   run a workflow that is suffering from a systematic error, such as a
   missing binary or an invalid path. The default for *M* is 0, which
   means unlimited failures are allowed.

**-t** *T*; \ **--tries** *T*
   Attempt to run each task *T* times before marking the task as failed.
   Note that the *T* tries do not count as failures for the purposes of
   the **-m** option. A task is only considered failed if it is tried
   *T* times and all *T* attempts result in a non-zero exitcode. The
   value of *T* must be at least 1. The default is 1.

**-n**; \ **--nolock**
   Do not lock DAGFILE. By default, **pegasus-mpi-cluster** will attempt
   to acquire an exclusive lock on DAGFILE to prevent multiple MPI jobs
   from running the same DAG at the same time. If this option is
   specified, then the lock will not be acquired.

**-r**; \ **--rescue** *path*
   Path to rescue log. If the file exists, and **-s** is not specified,
   then the log will be used to recover the state of the workflow. The
   file is truncated after it is read and a new rescue log is created in
   its place. The default is to append *.rescue* to the DAG file name.
   (see `RESCUE FILES <#RESCUE_FILES>`__)

**--host-script** *path*
   Path to a script or executable to launch on each unique host that
   **pegasus-mpi-cluster** is running on. This path can also be set
   using the PMC_HOST_SCRIPT environment variable. (see `HOST
   SCRIPTS <#HOST_SCRIPTS>`__)

**--host-memory** *size*
   Amount of memory available on each host in MB. The default is to
   determine the amount of physical RAM automatically. This value can
   also be set using the PMC_HOST_MEMORY environment variable. (see
   `RESOURCE-BASED SCHEDULING <#RESOURCE_SCHED>`__)

**--host-cpus** *cpus*
   Number of CPUs available on each host. The default is to determine
   the number of CPU cores automatically. This value can also be set
   using the PMC_HOST_CPUS environment variable. (see `RESOURCE-BASED
   SCHEDULING <#RESOURCE_SCHED>`__)

**--strict-limits**
   This enables strict memory usage limits for tasks. When this option
   is specified, and a task tries to allocate more memory than was
   requested in the DAG, the memory allocation operation will fail.

**--max-wall-time** *minutes*
   This is the maximum number of minutes that **pegasus-mpi-cluster**
   will allow the workflow to run. When this time expires
   **pegasus-mpi-cluster** will abort the workflow and merge all of the
   stdout/stderr files of the workers. The value is in minutes, and the
   default is unlimited wall time. This option was added so that the
   output of a workflow will be recorded even if the workflow exceeds
   the max wall time of its batch job. This value can also be set using
   the PMC_MAX_WALL_TIME environment variable.

**--per-task-stdio**
   This causes PMC to generate a .out.XXX and a .err.XXX file for each
   task instead of writing task stdout/stderr to **--stdout** and
   **--stderr**. The name of the files are "TASKNAME.out.XXX" and
   "TASKNAME.err.XXX", where "TASKNAME" is the name of the task from the
   DAG and "XXX" is a sequence number that is incremented each time the
   task is tried. This option overrides the values for **--stdout** and
   **--stderr**. This argument is used by Pegasus when workflows are
   planned in PMC-only mode to facilitate debugging and monitoring.

**--jobstate-log**
   This option causes PMC to generate a jobstate.log file for the
   workflow. The file is named "jobstate.log" and is placed in the same
   directory where the DAG file is located. If the file already exists,
   then PMC appends new lines to the existing file. This option is used
   by Pegasus when workflows are planned in PMC-only mode to facilitate
   monitoring.

**--monitord-hack**
   This option causes PMC to generate a .dagman.out file for the
   workflow. This file mimics the contents of the .dagman.out file
   generated by Condor DAGMan. The point of this option is to trick
   monitord into thinking that it is dealing with DAGMan so that it will
   generate the appropriate events to populate the STAMPEDE database for
   monitoring purposes. The file is named "DAG.dagman.out" where "DAG"
   is the path to the PMC DAG file.

**--no-resource-log**
   Do not generate a *workflow.dag.resource* file for the workflow.

**--no-sleep-on-recv**
   Do not use polling with sleep() to implement message receive. (see
   `Known Issues: CPU Usage <#CPU_USAGE_ISSUE>`__)

**--maxfds**
   Set the maximum number of file descriptors that can be left open by
   the master for I/O forwarding. By default this value is set
   automatically based on the value of getrlimit(RLIMIT_NOFILE). The
   value must be at least 1, and cannot be more than RLIMIT_NOFILE.

**--keep-affinity**
   By default PMC attempts to clear the CPU and memory affinity. This is
   to ensure that all available CPUs and memory can be used by PMC tasks
   on systems that are not configured properly. This flag tells PMC to
   keep the affinity settings inherited from its parent. Note that the
   memory policy can only be cleared if PMC was compiled with libnuma.
   CPU affinity is cleared using **sched_setaffinity()**, and memory
   policy is cleared with **set_mempolicy()**.

**--set-affinity**
   If this flag is set, then PMC will allocate CPUs to tasks and call
   **sched_setaffinity()** to bind the task to those CPUs. This only
   applies to multicore tasks (i.e. those tasks that specify -c N where
   N > 1). Single core tasks are not bound to a CPU to reduce the
   possibility of fragmentation. PMC does not currently have any
   mechanism to handle resource fragmentation that may occur if a
   workflow contains several tasks with different core counts. In the
   case that fragmentation would result in a task not being bound to a
   minimal number of sockets and cores, PMC will not bind the task to
   any CPUs. For example, if a 2 socket, 8 core machine without
   hyperthreading is being used to run 2, 4-core tasks, each task will
   be bound to a full socket. If the same machine is running 4, 2-core
   tasks, each task will get 2-cores on one socket. If 2 of the 2-core
   tasks finish, but they free up cores on two different sockets, and
   PMC wants to run a 4-core task, it will not bind the 4-core task to
   any CPUs, because that would result in the 4-core task being bound to
   two different sockets. Instead, PMC lets the 4-core task float, so
   that the scheduler can find a better placement when another one of
   the 2-core tasks finishes. In order to fix this issue we need to
   rearchitect PMC, which is on the roadmap.

.. _DAG_FILES:

DAG Files
=========

**pegasus-mpi-cluster** workflows are expressed using a simple
text-based format similar to that used by Condor DAGMan. There are only
two record types allowed in a DAG file: **TASK** and **EDGE**. Any blank
lines in the DAG (lines with all whitespace characters) are ignored, as
are any lines beginning with # (note that # can only appear at the
beginning of a line, not in the middle).

The format of a **TASK** record is:

::

   "TASK" id [options...] executable [arguments...]

Where *id* is the ID of the task, *options* is a list of task options,
*executable* is the path to the executable or script to run, and
*arguments…* is a space-separated list of arguments to pass to the task.
An example is:

::

   TASK t01 -m 10 -c 2 /bin/program -a -b

This example specifies a task *t01* that requires 10 MB memory and 2
CPUs to run */bin/program* with the arguments *-a* and *-b*. The
available task options are:

**-m** *M*; \ **--request-memory** *M*
   The amount of memory required by the task in MB. The default is 0,
   which means memory is not considered for this task. This option can
   be set for a job in the DAX by specifying the
   pegasus::pmc_request_memory profile. (see `RESOURCE-BASED
   SCHEDULING <#RESOURCE_SCHED>`__)

**-c** *N*; \ **--request-cpus** *N*
   The number of CPUs required by the task. The default is 1, which
   implies that the number of slots on a host should be less than or
   equal to the number of physical CPUs in order for all the slots to be
   used. This option can be set for a job in the DAX by specifying the
   pegasus::pmc_request_cpus profile. (see `RESOURCE-BASED
   SCHEDULING <#RESOURCE_SCHED>`__)

**-t** *T*; \ **--tries** *T*
   The number of times to try to execute the task before failing
   permanently. This is the task-level equivalent of the **--tries**
   command-line option.

**-p** *P*; \ **--priority** *P*
   The priority of the task. P should be an integer. Larger values have
   higher priority. The default is 0. Priorities are simply hints and
   are not strict—if a task cannot be matched to an available slot (e.g.
   due to resource availability), but a lower-priority task can, then
   the task will be deferred and the lower priority task will be
   executed. This option can be set for a job in the DAX by specifying
   the pegasus::pmc_priority profile.

**-f** *VAR=FILE*; \ **--pipe-forward** *VAR=FILE*
   Forward I/O to file *FILE* using pipes to communicate with the task.
   The environment variable *VAR* will be set to the value of a file
   descriptor for a pipe to which the task can write to get data into
   *FILE*. For example, if a task specifies: -f FOO=/tmp/foo then the
   environment variable FOO for the task will be set to a number (e.g.
   3) that represents the file /tmp/foo. In order to specify this
   argument in a Pegasus DAX you need to set the pegasus::pmc_arguments
   profile (note that the value of pmc_arguments must contain the "-f"
   part of the argument, so a valid value would be: <profile
   namespace="pegasus" key="pmc_arguments">-f A=/tmp/a </profile>). (see
   `I/O FORWARDING <#IO_FORWARDING>`__)

**-F** *SRC=DEST*; \ **--file-forward** *SRC=DEST*
   Forward I/O to the file *DEST* from the file *SRC*. When the task
   finishes, the worker will read the data from *SRC* and send it to the
   master where it will be written to the file *DEST*. After *SRC* is
   read it is deleted. In order to specify this argument in a Pegasus
   DAX you need to set the pegasus::pmc_arguments profile. (see `I/O
   FORWARDING <#IO_FORWARDING>`__)

The format of an **EDGE** record is:

::

   "EDGE" parent child

Where *parent* is the ID of the parent task, and *child* is the ID of
the child task. An example **EDGE** record is:

::

   EDGE t01 t02

A simple diamond-shaped workflow would look like this:

::

   # diamond.dag
   TASK A /bin/echo "I am A"
   TASK B /bin/echo "I am B"
   TASK C /bin/echo "I am C"
   TASK D /bin/echo "I am D"

   EDGE A B
   EDGE A C
   EDGE B D
   EDGE C D

.. _RESCUE_FILES:

Rescue Files
============

Many different types of errors can occur when running a DAG. One or more
of the tasks may fail, the MPI job may run out of wall time,
**pegasus-mpi-cluster** may segfault (we hope not), the system may
crash, etc. In order to ensure that the DAG does not need to be
restarted from the beginning after an error, **pegasus-mpi-cluster**
generates a rescue file for each workflow.

The rescue file is a simple text file that lists all of the tasks in the
workflow that have finished successfully. This file is updated each time
a task finishes, and is flushed periodically so that if the work- flow
fails and the user restarts it, **pegasus-mpi-cluster** can determine
which tasks still need to be executed. As such, the rescue file is a
sort-of transaction log for the workflow.

The rescue file contains zero or more DONE records. The format of these
records is:

::

   "DONE" *taskid*

Where *taskid* is the ID of the task that finished successfully.

By default, rescue files are named *DAGNAME.rescue* where *DAGNAME* is
the path to the input DAG file. The file name can be changed by
specifying the **-r** argument.

.. _PMC_AND_PEGASUS:

PMC and Pegasus
===============



Using PMC for Pegasus Task Clustering
-------------------------------------

PMC can be used as the wrapper for executing clustered jobs in Pegasus.
In this mode Pegasus groups several tasks together and submits them as a
single clustered job to a remote system. PMC then executes the
individual tasks in the cluster and returns the results.

PMC can be specified as the task manager for clustered jobs in Pegasus
in three ways:

1. Globally in the properties file

   The user can set a property in the properties file that results in
   all the clustered jobs of the workflow being executed by PMC. In the
   Pegasus properties file specify:

   ::

      #PEGASUS PROPERTIES FILE
      pegasus.clusterer.job.aggregator=mpiexec

   In the above example, all the clustered jobs on all remote sites will
   be launched via PMC as long as the property value is not overridden
   in the site catalog.

2. By setting the profile key "job.aggregator" in the site catalog:

   ::

      <site handle="siteX" arch="x86" os="LINUX">
          ...
          <profile namespace="pegasus" key="job.aggregator">mpiexec</profile>
      </site>

   In the above example, all the clustered jobs on a siteX are going to
   be executed via PMC as long as the value is not overridden in the
   transformation catalog.

3. By setting the profile key "job.aggregator" in the transformation
   catalog:

   ::

      tr B {
          site siteX {
              pfn "/path/to/mytask"
              arch "x86"
              os "linux"
              type "INSTALLED"
              profile pegasus "clusters.size" "3"
              profile pegasus "job.aggregator" "mpiexec"
          }
      }

   In the above example, all the clustered jobs for transformation B on
   siteX will be executed via PMC.

It is usually necessary to have a pegasus::mpiexec entry in your
transformation catalog that specifies a) the path to PMC on the remote
site and b) the relevant globus profiles such as xcount, host_xcount and
maxwalltime to control size of the MPI job. That entry would look like
this:

::

   tr pegasus::mpiexec {
       site siteX {
           pfn "/path/to/pegasus-mpi-cluster"
           arch "x86"
           os "linux"
           type "INSTALLED"
           profile globus "maxwalltime" "240"
           profile globus "host_xcount" "1"
           profile globus "xcount" "32"
       }
   }

If this transformation catalog entry is not specified, Pegasus will
attempt create a default path on the basis of the environment profile
PEGASUS_HOME specified in the site catalog for the remote site.

PMC can be used with both horizontal and label-based clustering in
Pegasus, but we recommend using label-based clustering so that entire
sub-graphs of a Pegasus DAX can be clustered into a single PMC job,
instead of only a single level of the workflow.



Pegasus Profiles for PMC
------------------------

There are several Pegasus profiles that map to PMC task options:

**pmc_request_memory**
   This profile is used to set the --request-memory task option and is
   usually specified in the DAX or transformation catalog.

**pmc_request_cpus**
   This key is used to set the --request-cpus task option and is usually
   specified in the DAX or transformation catalog.

**pmc_priority**
   This key is used to set the --priority task option and is usually
   specified in the DAX.

These profiles are used by Pegasus when generating PMC’s input DAG when
PMC is used as the task manager for clustered jobs in Pegasus.

The profiles can be specified in the DAX like this:

::

   <job id="ID0000001" name="mytask">
       <arguments>-a 1 -b 2 -c 3</arguments>
       ...
       <profile namespace="pegasus" key="pmc_request_memory">1024</profile>
       <profile namespace="pegasus" key="pmc_request_cpus">4</profile>
       <profile namespace="pegasus" key="pmc_priority">10</profile>
   </job>

This example specifies a PMC task that requires 1GB of memory and 4
cores, and has a priority of 10. It produces a task in the PMC DAG that
looks like this:

::

   TASK mytask_ID00000001 -m 1024 -c 4 -p 10 /path/to/mytask -a 1 -b 2 -c 3



Using PMC for the Entire Pegasus DAX
------------------------------------

Pegasus can also be configured to run the entire workflow as a single
PMC job. In this mode Pegasus will generate a single PMC DAG for the
entire workflow as well as a PBS script that can be used to submit the
workflow.

In contrast to using PMC as a task clustering tool, in this mode there
are no jobs in the workflow executed without PMC. The entire workflow,
including auxilliary jobs such as directory creation and file transfers,
is managed by PMC. If Pegasus is configured in this mode, then DAGMan
and Condor are not required.

To run in PMC-only mode, set the property "pegasus.code.generator" to
"PMC" in the Pegasus properties file:

::

   pegasus.code.generator=PMC

In order to submit the resulting PBS job you may need to make changes to
the .pbs file generated by Pegasus to get it to work with your cluster.
This mode is experimental and has not been used extensively.

.. _LOGGING:

Logging
=======

By default, all logging messages are printed to stderr. If you turn up
the logging using **-v** then you may end up with a lot of stderr being
forwarded from the workers to the master.

The log levels in order of severity are: FATAL, ERROR, WARN, INFO,
DEBUG, and TRACE.

The default logging level is INFO. The logging levels can be increased
with **-v** and decreased with **-q**.

.. _TASK_STDIO:

Task STDIO
==========

By default the stdout and stderr of tasks will be redirected to the
master’s stdout and stderr. You can change the path of these files with
the **-o** and **-e** arguments. You can also enable per-task stdio
files using the **--per-task-stdio** argument. Note that if per-task
stdio files are not used then the stdio of all workers will be merged
into one out and one err file by the master at the end, so I/O from
different workers will not be interleaved, but I/O from each worker will
appear in the order that it was generated. Also note that, if the job
fails for any reason, the outputs will not be merged, but instead there
will be one file for each worker named DAGFILE.out.X and DAGFILE.err.X,
where DAGFILE is the path to the input DAG, and *X* is the worker’s
rank.

.. _HOST_SCRIPTS:

Host Scripts
============

A host script is a shell script or executable that
**pegasus-mpi-cluster** launches on each unique host on which it is
running. They can be used to start auxilliary services, such as
memcached, that the tasks in a workflow require.

Host scripts are specified using either the **--host-script** argument
or the **PMC_HOST_SCRIPT** environment variable.

The host script is started when **pegasus-mpi-cluster** starts and must
exit with an exitcode of 0 before any tasks can be executed. If it the
host script returns a non-zero exitcode, then the workflow is aborted.
The host script is given 60 seconds to do any setup that is required. If
it doesn’t exit in 60 seconds then a SIGALRM signal is delivered to the
process, which, if not handled, will cause the process to terminate.

When the workflow finishes, **pegasus-mpi-cluster** will deliver a
SIGTERM signal to the host script’s process group. Any child processes
left running by the host script will receive this signal unless they
created their own process group. If there were any processes left to
receive this signal, then they will be given a few seconds to exit, then
they will be sent SIGKILL. This is the mechanism by which processes
started by the host script can be informed of the termination of the
workflow.

.. _RESOURCE_SCHED:

Resource-Based Scheduling
=========================

High-performance computing resources often have a low ratio of memory to
CPUs. At the same time, workflow tasks often have high memory
requirements. Often, the memory requirements of a workflow task exceed
the amount of memory available to each CPU on a given host. As a result,
it may be necessary to disable some CPUs in order to free up enough
memory to run the tasks. Similarly, many codes have support for
multicore hosts. In that case it is necessary for efficiency to ensure
that the number of cores required by the tasks running on a host do not
exceed the number of cores available on that host.

In order to make this process more efficient, **pegasus-mpi-cluster**
supports resource-based scheduling. In resource-based scheduling the
tasks in the workflow can specify how much memory and how many CPUs they
require, and **pegasus-mpi-cluster** will schedule them so that the
tasks running on a given host do not exceed the amount of physical
memory and CPUs available. This enables **pegasus-mpi-cluster** to take
advantage of all the CPUs available when the tasks' memory requirement
is low, but also disable some CPUs when the tasks' memory requirement is
higher. It also enables workflows with a mixture of single core and
multi-core tasks to be executed on a heterogenous pool.

If there are no hosts available that have enough memory and CPUs to
execute one of the tasks in a workflow, then the workflow is aborted.



Memory
------

Users can specify both the amount of memory required per task, and the
amount of memory available per host. If the amount of memory required by
any task exceeds the available memory of all the hosts, then the
workflow will be aborted. By default, the host memory is determined
automatically, however the user can specify **--host-memory** to "lie"
to **pegasus-mpi-cluster**. The amount of memory required for each task
is specified in the DAG using the **-m**/**--request-memory** argument
(see `DAG Files <#DAG_FILES>`__).



CPUs
----

Users can specify the number of CPUs required per task, and the total
number of CPUs available on each host. If the number of CPUs required by
a task exceeds the available CPUs on all hosts, then the workflow will
be aborted. By default, the number of CPUs on a host is determined
automatically, but the user can specify **--host-cpus** to over- or
under-subscribe the host. The number of CPUs required for each task is
specified in the DAG using the **-c**/**--request-cpus** argument (see
`DAG Files <#DAG_FILES>`__).

.. _IO_FORWARDING:

I/O Forwarding
==============

In workflows that have lots of small tasks it is common for the I/O
written by those tasks to be very small. For example, a workflow may
have 10,000 tasks that each write a few KB of data. Typically each task
writes to its own file, resulting in 10,000 files. This I/O pattern is
very inefficient on many parallel file systems because it requires the
file system to handle a large number of metadata operations, which are a
bottleneck in many parallel file systems.

One way to handle this problem is to have all 10,000 tasks write to a
single file. The problem with this approach is that it requires those
tasks to synchronize their access to the file using POSIX locks or some
other mutual exclusion mechanism. Otherwise, the writes from different
tasks may be interleaved in arbitrary order, resulting in unusable data.

In order to address this use case PMC implements a feature that we call
"I/O Forwarding". I/O forwarding enables each task in a PMC job to write
data to an arbitrary number of shared files in a safe way. It does this
by having PMC worker processes collect data written by the task and send
it over over the high-speed network using MPI messaging to the PMC
master process, where it is written to the output file. By having one
process (the PMC master process) write to the file all of the I/O from
many parallel tasks can be synchronized and written out to the files
safely.

There are two different ways to use I/O forwarding in PMC: pipes and
files. Pipes are more efficient, but files are easier to use.



I/O forwarding using pipes
--------------------------

I/O forwarding with pipes works by having PMC worker processes collect
data from each task using UNIX pipes. This approach is more efficient
than the file-based approach, but it requires the code of the task to be
changed so that the task writes to the pipe instead of a regular file.

In order to use I/O forwarding a PMC task just needs to specify the
**-f/--pipe-forward** argument to specify the name of the file to
forward data to, and the name of an environment variable through which
the PMC worker process can inform it of the file descriptor for the
pipe.

For example, if there is a task "mytask" that needs to forward data to
two files: "myfile.a" and "myfile.b", it would look like this:

::

   TASK mytask -f A=/tmp/myfile.a -f B=/tmp/myfile.b /bin/mytask

When the /bin/mytask process starts it will have two variables in its
environment: "A=3" and "B=4", for example. The value of these variables
is the file descriptor number of the corresponding files. In this case,
if the task wants to write to "/tmp/myfile.a", it gets the value of
environment variable "A", and calls write() on that descriptor number.
In C the code for that looks like this:

::

   char *A = getenv("A");
   int fd = atoi(A);
   char *message = "Hello, World\n";
   write(fd, message, strlen(message));

In some programming languages it is not possible to write to a file
descriptor directly. Fortran, for example, refers to files by unit
number instead of using file descriptors. In these languages you can
either link C I/O functions into your binary and call them from routines
written in the other language, or you can open a special file in the
Linux /proc file system to get another handle to the pipe you want to
access. For the latter, the file you should open is
"/proc/self/fd/NUMBER" where NUMBER is the file descriptor number you
got from the environment variable. For the example above, the pipe for
myfile.a (environment variable A) is "/proc/self/fd/3".

If you are using **pegasus-kickstart**, which is probably the case if
you are using PMC for a Pegasus workflow, then there’s a trick you can
do to avoid modifying your code. You use the /proc file system, as
described above, but you let pegasus-kickstart handle the path
construction. For example, if your application has an argument, -o, that
allows you to specify the output file then you can write your task like
this:

::

   TASK mytask -f A=/tmp/myfile.a /bin/pegasus-kickstart /bin/mytask -o /proc/self/fd/$A

In this case, pegasus-kickstart will replace the $A in your application
arguments with the file descriptor number you want. Your code can open
that path normally, write to it, and then close it as if it were a
regular file.



I/O forwarding using files
--------------------------

I/O forwarding with files works by having tasks write out data in files
on the local disk. The PMC worker process reads these files and forwards
the data to the master where it can be written to the desired output
file. This approach may be much less efficient than using pipes because
it involves the file system, which has more overhead than a pipe.

File forwarding can be enabled by giving the **-F/--file-forward**
argument to a task.

Here’s an example:

::

   TASK mytask -F /tmp/foo.0=/scratch/foo /bin/mytask -o /tmp/foo.0

In this case, the worker process will expect to find the file /tmp/foo.0
when mytask exits successfully. It reads the data from that file and
sends it to the master to be written to the end of /scratch/foo. After
/tmp/foo.0 is read it will be deleted by the worker process.

This approach works best on systems where the local disk is a RAM file
system such as Cray XT machines. Alternatively, the task can use
/dev/shm on a regular Linux cluster. It might also work relatively
efficiently on a local disk if the file system cache is able to absorb
all of the reads and writes.



I/O forwarding caveats
----------------------

When using I/O forwarding it is important to consider a few caveats.

First, if the PMC job fails for any reason (including when the workflow
is aborted for violating **--max-wall-time**), then the files containing
forwarded I/O may be corrupted. They can include **partial records**,
meaning that only part of the I/O from one or more tasks was written,
and they can include **duplicate records**, meaning that the I/O was
written, but the PMC job failed before the task could be marked as
successful, and the workflow was restarted later. We make no guarantees
about the contents of the data files in this case. It is up to the code
that reads the files to a) detect and b) recover from such problems. To
eliminate duplicates the records should include a unique identifier, and
to eliminate partials the records should include a checksum.

Second, you should not use I/O forwarding if your task is going to write
a lot of data to the file. Because the PMC worker is reading data off
the pipe/file into memory and sending it in an MPI message, if you write
too much, then the worker process will run the system out of memory.
Also, all the data needs to fit in a single MPI message. In pipe
forwarding there is no hard limit on the size, but in file forwarding
the limit is 1MB. We haven’t benchmarked the performance on large I/O,
but anything larger than about 1 MB is probably too much. At any rate,
if your data is larger than 1MB, then I/O forwarding probably won’t have
much of a performance benefit anyway.

Third, the I/O is not written to the file if the task returns a non-zero
exitcode. We assume that if the task failed that you don’t want the data
it produced.

Fourth, the data from different tasks is not interleaved. All of the
data written by a given task will appear sequentially in the output
file. Note that you can still get partial records, however, if any data
from a task appears it will never be split among non-adjacent ranges in
the output file. If you have 3 tasks that write: "I am a task" you can
get:

::

   I am a taskI am a taskI am a task

and:

::

   I am a taskI amI am a task

but not:

::

   I am a taskI amI am a task a task

Fifth, data from different tasks appears in arbitrary order in the
output file. It depends on what order the tasks were executed by PMC,
which may be arbitrary if there are no dependencies between the tasks.
The data that is written should contain enough information that you are
able to determine which task produced it if you require that. PMC does
not add any headers or trailers to the data.

Sixth, a task will only be marked as successful if all of its I/O was
successfully written. If the workflow completed successfully, then the
I/O is guaranteed to have been written.

Seventh, if the master is not able to write to the output file for any
reason (e.g. the master tries to write the I/O to the destination file,
but the write() call returns an error) then the task is marked as failed
even if the task produced a non-zero exitcode. In other words, you may
get a non-zero kickstart record even when PMC marks the task failed.

Eighth, the pipes are write-only. If you need to read and write data
from the file you should use file forwarding and not pipe forwarding.

Ninth, all files are opened by the master in append mode. This is so
that, if the workflow fails and has to be restarted, or if a task fails
and is retried, the data that was written previously is not lost. PMC
never truncates the files. This is one of the reasons why you can have
partial records and duplicate records in the output file.

Finally, in file forwarding the output file is removed when the task
exits. You cannot rely on the file to be there when the next task runs
even if you write it to a shared file system.



Misc
====



Resource Utilization
--------------------

At the end of the workflow run, the master will report the resource
utilization of the job. This is done by adding up the total runtimes of
all the tasks executed (including failed tasks) and dividing by the
total wall time of the job times N, where N is both the total number of
processes including the master, and the total number of workers. These
two resource utilization values are provided so that users can get an
idea about how efficiently they are making use of the resources they
allocated. Low resource utilization values suggest that the user should
use fewer cores, and longer wall time, on future runs, while high
resource utilization values suggest that the user could use more cores
for future runs and get a shorter wall time.



Known Issues
============



Cray Compiler Wrappers
----------------------

On Cray machines, the CC compiler wrapper for C++ code should be used to
compile PMC. That wrapper links in all the required MPI libraries.
**Cray compiler wrappers should not be used to compile tasks that run
under PMC.** If you use a Cray wrapper to compile a task that runs under
PMC, then the task will hang, or exit immediately with a 0 exit code
without doing anything. This appears to happen only when the application
binary is dynamically linked. It seems to be a problem with the
libraries that are linked into the code when it is compiled with a Cray
wrapper. To summarize: on Cray machines, compile PMC with the CC
wrapper, but compile code that runs under PMC without any wrappers.



fork() and exec()
-----------------

In order for the worker processes to start tasks on the compute node the
compute nodes must support the **fork()** and **exec()** system calls.
If your target machine runs a stripped-down OS on the compute nodes that
does not support these system calls, then **pegasus-mpi-cluster** will
not work.

.. _CPU_USAGE_ISSUE:

CPU Usage
---------

Many MPI implementations are optimized so that message sends and
receives do busy waiting (i.e. they spin/poll on a message send or
receive instead of sleeping). The reasoning is that sleeping adds
overhead and, since many HPC systems use space sharing on dedicated
hardware, there are no other processes competing, so spinning instead of
sleeping can produce better performance. On those implementations MPI
processes will run at 100% CPU usage even when they are just waiting for
a message. This is a big problem for multicore tasks in
**pegasus-mpi-cluster** because idle slots consume CPU resources. In
order to solve this problem **pegasus-mpi-cluster** processes sleep for
a short period between checks for waiting messages. This reduces the
load significantly, but causes a short delay in receiving messages. If
you are using an MPI implementation that sleeps on message send and
receive instead of doing busy waiting, then you can disable the sleep by
specifying the **--no-sleep-on-recv** option. Note that the master will
always sleep if **--max-wall-time** is specified because there is no way
to interrupt or otherwise timeout a blocking call in MPI (e.g. SIGALRM
does not cause MPI_Recv to return EINTR).



Task Environment
================

PMC sets a few environment variables when it launches a task. In
addition to the environment variables for pipe forwarding, it sets:

**PMC_TASK**
   The name of the task from the DAG file.

**PMC_MEMORY**
   The amount of memory requested by the task.

**PMC_CPUS**
   The number of CPUs requested by the task.

**PMC_RANK**
   The rank of the MPI worker that launched the task.

**PMC_HOST_RANK**
   The host rank of the MPI worker that launched the task.

In addition, if **--set-affinity** is specified, and PMC has allocated
some CPUs to the task, then it will export:

**PMC_AFFINITY**
   A comma-separated list of CPUs to which the task is/should be bound.



Environment Variables
=====================

The environment variables below are aliases for command-line options. If
the environment variable is present, then it is used as the default for
the associated option. If both are present, then the command-line option
is used.

**PMC_HOST_SCRIPT**
   Alias for the **--host-script** option.

**PMC_HOST_MEMORY**
   Alias for the **--host-memory** option.

**PMC_HOST_CPUS**
   Alias for the **--host-cpus** option.

**PMC_MAX_WALL_TIME**
   Alias for the **--max-wall-time** option.



Author
======

Gideon Juve ``<gideon@isi.edu>``

Mats Rynge ``<rynge@isi.edu>``
