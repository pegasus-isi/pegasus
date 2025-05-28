.. _reference-configuration:

=============
Configuration
=============

Pegasus has configuration options to configure

1. the behavior of an individual job via **profiles**

2. the behavior of the whole system via **properties**

For job level configuration ( such as what environment a job is set with
), the Pegasus Workflow Mapper uses the concept of profiles. Profiles
encapsulate configurations for various aspects of dealing with the Grid
infrastructure. They provide an abstract yet uniform interface to
specify configuration options for various layers from planner/mapper
behavior to remote environment settings. At various stages during the
mapping process, profiles may be added associated with the job. The
system supports five diffferent namespaces, with each namespace refers
to a different aspect of a job's runtime settings. A profile's
representation in the executable workflow (e.g. the Condor submit files)
depends on its namespace. Pegasus supports the following Namespaces for
profiles:

-  **env** permits remote environment variables to be set.

-  **globus** sets Globus RSL parameters.

-  **condor** sets Condor configuration parameters for the submit file.

-  **dagman** introduces Condor DAGMan configuration parameters.

-  **pegasus** configures the behaviour of various planner/mapper
   components.

-  **selector** allows to override site selection behavior of the planner.
   Can be specified only in the DAX.

.. note::

      The hints namespace is deprecated starting Pegasus 5.0. Use the
      selector namespace instead.

Properties are primarily used to configure the behavior of the Pegasus
WMS system at a global level. The properties file is actually a java
properties file and follows the same conventions as that to specify the
properties.

This chapter describes various types of profiles and properties, levels
of priorities for intersecting profiles, and how to specify profiles in
different contexts.

Differences between Profiles and Properties
===========================================

The main difference between properties and profiles is that profiles
eventually get associated at a per job level in the workflow. On the
other hand, properties are a way of configuring and controlling the
behavior of the whole system. While all profiles can be specified in the
properties file, not all properties can be used as profiles. This
section lists out the properties supported by Pegasus and if any can be
used as a profile, it is clearly indicated.

Profiles
========

Profile Structure Heading
-------------------------

All profiles are triples comprised of a namespace, a name or key, and a
value. The namespace is a simple identifier. The key has only meaning
within its namespace, and it's yet another identifier. There are no
constraints on the contents of a value

Profiles may be represented with different syntaxes in different
context. However, each syntax will describe the underlying triple.

Sources for Profiles
--------------------

Profiles may enter the job-processing stream at various stages.
Depending on the requirements and scope a profile is to apply, profiles
can be associated at

-  as user property settings.

-  workflow level

-  in the site catalog

-  in the transformation catalog

Unfortunately, a different syntax applies to each level and context.
This section shows the different profile sources and syntaxes. However,
at the foundation of each profile lies the triple of **namespace**, **key** and
**value**.

User Profiles in Properties
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Users can specify all profiles in the properties files where the
property name is **[namespace].key** and **value** of the property is
the value of the profile.

Namespace can be env|condor|globus|dagman|pegasus

Any profile specified as a property applies to the whole workflow i.e
(all jobs in the workflow) unless overridden at the job level , Site
Catalog , Transformation Catalog Level.

Some profiles that they can be set in the properties file are listed
below

::

   env.JAVA_HOME "/software/bin/java"

   condor.periodic_release 5
   condor.periodic_remove  my_own_expression
   condor.stream_error true
   condor.stream_output fa

   globus.maxwalltime  1000
   globus.maxtime      900
   globus.maxcputime   10
   globus.project      test_project
   globus.queue        main_queue

   dagman.post.arguments --test arguments
   dagman.retry  4
   dagman.post simple_exitcode
   dagman.post.path.simple_exitcode  /bin/exitcode/exitcode.sh
   dagman.post.scope all
   dagman.maxpre  12
   dagman.priority 13

   dagman.bigjobs.maxjobs 1


   pegasus.clusters.size 5

   pegasus.stagein.clusters 3

Profiles in the Workflow
~~~~~~~~~~~~~~~~~~~~~~~~

Examples of profiles used here include the maximum exected runtime of the
job and its maximum required amount of disk space in MB. For example, these can
be added to a job as follows:

.. code-block:: python
    :emphasize-lines: 3,4

    # Python API
    job = Job("abc")
    job.add_profiles(Namespace.PEGASUS, key="walltime", value=2)
    job.add_profiles(Namespace.PEGASUS, key="diskspace", value=1)


Profiles in Site Catalog
~~~~~~~~~~~~~~~~~~~~~~~~

If it becomes necessary to limit the scope of a profile to a single
site, these profiles should go into the site catalog. A profile in the
site catalog applies to all jobs and all application that run at the site.
Commonly, site catalog profiles set environment settings like the
``LD_LIBRARY_PATH``, or globus rsl parameters like queue and project names.
The following example illustrates the creation of a site called ``CCG``, which
has two profiles added to it at the end.

.. code-block:: python
    :emphasize-lines: 17,18

    # Python API
    ccg_site = Site(name="CCG", arch=Arch.X86_64, os_type=OS.LINUX)\
                .add_grids(
                    Grid(
                        grid_type=Grid.GT5,
                        contact="obelix.isi.edu/jobmanager-fork",
                        scheduler_type=Scheduler.FORK,
                        job_type=SupportedJobs.AUXILLARY
                    )
                )\
                .add_directories(
                    Directory(directory_type=Directory.SHARED_SCRATCH, path="/shared-scratch")
                        .add_file_servers(FileServer(url="gsiftp://headnode.isi.edu/shared-scratch", operation_type=Operation.ALL)),
                    Directory(directory_type=Directory.LOCAL_STORAGE, path="/local-storage")
                        .add_file_servers(FileServer(url="gsiftp://headnode.isi.edu/local-storage", operation_type=Operation.ALL))
                )\
                .add_profiles(Namespace.PEGASUS, key="clusters.num", value=1)\
                .add_profiles(Namespace.ENV, key="PEGASUS_HOME", value="/usr")

Profiles in Transformation Catalog
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Some profiles require a narrower scope than that of what the site catalog offers
as they may only apply to certain applications on certain sites.
Examples of this would be transformation specific profiles such as CPU related
variables or job clustering options. Such profiles are best specified in the \
transformation catalog.

Profiles can be associated to a specific transformation site or the transformation
itself (which would then be applied to all of its transformation sites).

In the following example, the transformation ``keg`` resides on two sites
(represented as TransformationSite objects). Profiles have been added to each
site specificly as well as to ``keg`` itself. The environment variable, ``APP_HOME``
would be applied to ``keg`` for both the ``isi`` and ``wind`` sites.

.. code-block:: python
    :emphasize-lines: 3,12,24

    # Python API
    keg = Transformation(name="keg")
    keg.add_profiles(Namespace.ENV, key="APP_HOME", value="/tmp/scratch")
    keg.add_sites(
            TransformationSite(
                name="isi",
                pfn="/path/to/keg",
                is_stageable=False,
                arch=Arch.X86_64,
                os_type=OS.LINUX
            )
            .add_profiles(Namespace.ENV, key="HELLO", value="WORLD")
        )

    keg.add_sites(
            TransformationSite(
                name="wind",
                pfn="/path/to/keg",
                is_stageable=False,
                arch=Arch.X86_64,
                os_type=OS.LINUX,
                is_stageable=True
            )
            .add_profiles(Namespace.ENV, key="CPATH", value="/usr/cpath")
        )


Profiles Conflict Resolution
----------------------------

Irrespective of where the profiles are specified, eventually the
profiles are associated with jobs. Multiple sources may specify the same
profile for the same job. For instance, a job may specify an environment
variable X. The site catalog may also specify an environment variable X
for the chosen site. The transformation catalog may specify an
environment variable X for the chosen site and application. When the job
is concretized, these three conflicts need to be resolved.

Pegasus defines a priority ordering of profiles. The higher priority
takes precedence (overwrites) a profile of a lower priority.

1. Transformation Catalog Profiles

2. Site Catalog Profiles

3. Job Profiles (defined in the Workflow)

4. Profiles in Properties

Details of Profile Handling
---------------------------

The previous sections omitted some of the finer details for the sake of
clarity. To understand some of the constraints that Pegasus imposes, it
is required to look at the way profiles affect jobs.

Details of env Profiles
~~~~~~~~~~~~~~~~~~~~~~~

Profiles in the env namespace are translated to a semicolon-separated
list of key-value pairs. The list becomes the argument for the Condor
environment command in the job's submit file.

::

   ######################################################################
   # Pegasus WMS  SUBMIT FILE GENERATOR
   # DAG : black-diamond, Index = 0, Count = 1
   # SUBMIT FILE NAME : findrange_ID000002.sub
   ######################################################################
   globusrsl = (jobtype=single)
   environment=GLOBUS_LOCATION=/shared/globus;LD_LIBRARY_PATH=/shared/globus/lib;
   executable = /shared/software/linux/pegasus/default/bin/kickstart
   globusscheduler = columbus.isi.edu/jobmanager-condor
   remote_initialdir = /shared/CONDOR/workdir/isi_hourglass
   universe = globus
   &mldr;
   queue
   ######################################################################
   # END OF SUBMIT FILE

Condor-G, in turn, will translate the *environment* command for any
remote job into Globus RSL environment settings, and append them to any
existing RSL syntax it generates. To permit proper mixing, all
*environment* setting should solely use the env profiles, and none of
the Condor nor Globus environment settings.

If *kickstart* starts a job, it may make use of environment variables in
its executable and arguments setting.

Details of globus Profiles
~~~~~~~~~~~~~~~~~~~~~~~~~~

Profiles in the *globus* Namespaces are translated into a list of
paranthesis-enclosed equal-separated key-value pairs. The list becomes
the value for the Condor *globusrsl* setting in the job's submit file:

::

   ######################################################################
   # Pegasus WMS SUBMIT FILE GENERATOR
   # DAG : black-diamond, Index = 0, Count = 1
   # SUBMIT FILE NAME : findrange_ID000002.sub
   ######################################################################
   globusrsl = (jobtype=single)(queue=fast)(project=nvo)
   executable = /shared/software/linux/pegasus/default/bin/kickstart
   globusscheduler = columbus.isi.edu/jobmanager-condor
   remote_initialdir = /shared/CONDOR/workdir/isi_hourglass
   universe = globus
   &mldr;
   queue
   ######################################################################
   # END OF SUBMIT FILE

For this reason, Pegasus prohibits the use of the *globusrsl* key in the
*condor* profile namespace.

.. _env-profiles:

The Env Profile Namespace
-------------------------

The *env* namespace allows users to specify environment variables of
remote jobs. Globus transports the environment variables, and ensure
that they are set before the job starts.

The key used in conjunction with an *env* profile denotes the name of
the environment variable. The value of the profile becomes the value of
the remote environment variable.

Grid jobs usually only set a minimum of environment variables by virtue
of Globus. You cannot compare the environment variables visible from an
interactive login with those visible to a grid job. Thus, it often
becomes necessary to set environment variables like LD_LIBRARY_PATH for
remote jobs.

If you use any of the Pegasus worker package tools like transfer or the
rc-client, it becomes necessary to set PEGASUS_HOME and GLOBUS_LOCATION
even for jobs that run locally

.. table:: Useful Environment Settings

    +------------------------------------+----------------------------------------------------------------+
    | Key Attributes                     | Description                                                    |
    +====================================+================================================================+
    || Property Key: env.PEGASUS_HOME    || Used by auxillary jobs created by Pegasus both on remote site |
    || Profile Key: PEGASUS_HOME         || and local site. Should be set usually set in the Site Catalog |
    || Scope : TC, SC, DAX, Properties   || for the sites                                                 |
    || Since : 2.0                       ||                                                               |
    || Type :String                      ||                                                               |
    +------------------------------------+----------------------------------------------------------------+
    || Property Key: env.GLOBUS_LOCATION || Used by auxillary jobs created by Pegasus both on remote site |
    || Profile Key: GLOBUS_LOCATION      || and local site. Should be set usually set in the Site Catalog |
    || Scope : TC, SC, DAX, Properties   || for the sites                                                 |
    || Since : 2.0                       ||                                                               |
    || Type :String                      ||                                                               |
    +------------------------------------+----------------------------------------------------------------+
    || Property Key: env.LD_LIBRARY_PATH || Point this to $GLOBUS_LOCATION/lib, except you cannot use the |
    || Profile Key: LD_LIBRARY_PATH      || dollar variable. You must use the full path. Applies to both, |
    || Scope : TC, SC, DAX, Properties   || local and remote jobs that use Globus components and should   |
    || Since : 2.0                       || be usually set in the site catalog for the sites              |
    || Type :String                      ||                                                               |
    +------------------------------------+----------------------------------------------------------------+

Even though Condor and Globus both permit environment variable settings
through their profiles, all remote environment variables must be set
through the means of *env* profiles.

The Globus Profile Namespace
----------------------------

The *globus* profile namespace encapsulates Globus resource
specification language (RSL) instructions. The RSL configures settings
and behavior of the remote scheduling system. Some systems require queue
name to schedule jobs, a project name for accounting purposes, or a
run-time estimate to schedule jobs. The Globus RSL addresses all these
issues.

A key in the *globus* namespace denotes the command name of an RSL
instruction. The profile value becomes the RSL value. Even though Globus
RSL is typically shown using parentheses around the instruction, the out
pair of parentheses is not necessary in globus profile specifications

The table below shows some commonly used RSL instructions. For an
authoritative list of all possible RSL instructions refer to the Globus
RSL specification.

.. table:: Useful Globus RSL Instructions

    +------------------------------------+---------------------------------------------------------------+
    | Property Key                       | Description                                                   |
    +====================================+===============================================================+
    | | Property Key: globus.count       | the number of times an executable is started.                 |
    | | Profile Key:count                |                                                               |
    | | Scope : TC, SC, DAX, Properties  |                                                               |
    | | Since : 2.0                      |                                                               |
    | | Type :Integer                    |                                                               |
    +------------------------------------+---------------------------------------------------------------+
    | | Property Key: globus.jobtype     | | specifies how the job manager should start the remote job.  |
    | | Profile Key: jobtype             | | While Pegasus defaults to single, use mpi when running      |
    | | Scope : TC, SC, DAX, Properties  | | MPI jobs.                                                   |
    | | Since : 2.0                      |                                                               |
    | | Type :String                     |                                                               |
    +------------------------------------+---------------------------------------------------------------+
    | | Property Key: globus.maxcputime  | the max CPU time in minutes for a single execution of a job.  |
    | | Profile Key: maxcputime          |                                                               |
    | | Scope : TC, SC, DAX, Properties  |                                                               |
    | | Since : 2.0                      |                                                               |
    | | Type :Integer                    |                                                               |
    +------------------------------------+---------------------------------------------------------------+
    | | Property Key: globus.maxmemory   | the maximum memory in MB required for the job                 |
    | | Profile Key: maxmemory           |                                                               |
    | | Scope : TC, SC, DAX, Properties  |                                                               |
    | | Since : 2.0                      |                                                               |
    | | Type :Integer                    |                                                               |
    +------------------------------------+---------------------------------------------------------------+
    | | Property Key: globus.maxtime     | | the maximum time or walltime in minutes for a single        |
    | | Profile Key:maxtime              | | execution of a job.                                         |
    | | Scope : TC, SC, DAX, Properties  |                                                               |
    | | Since : 2.0                      |                                                               |
    | | Type  : Integer                  |                                                               |
    +------------------------------------+---------------------------------------------------------------+
    | | Property Key: globus.maxwalltime | | the maximum walltime in minutes for a single execution      |
    | | Profile Key: maxwalltime         | | of a job.                                                   |
    | | Scope : TC, SC, DAX, Properties  |                                                               |
    | | Since : 2.0                      |                                                               |
    | | Type :Integer                    |                                                               |
    +------------------------------------+---------------------------------------------------------------+
    | | Property Key: globus.minmemory   | the minumum amount of memory required for this job            |
    | | Profile Key: minmemory           |                                                               |
    | | Scope : TC, SC, DAX, Properties  |                                                               |
    | | Since : 2.0                      |                                                               |
    | | Type :Integer                    |                                                               |
    +------------------------------------+---------------------------------------------------------------+
    | | Property Key: globus.totalmemory | the total amount of memory required for this job              |
    | | Profile Key: totalmemory         |                                                               |
    | | Scope : TC, SC, DAX, Properties  |                                                               |
    | | Since : 2.0                      |                                                               |
    | | Type :Integer                    |                                                               |
    +------------------------------------+---------------------------------------------------------------+
    | | Property Key: globus.project     | associates an account with a job at the remote end.           |
    | | Profile Key: project             |                                                               |
    | | Scope : TC, SC, DAX, Properties  |                                                               |
    | | Since : 2.0                      |                                                               |
    | | Type :String                     |                                                               |
    +------------------------------------+---------------------------------------------------------------+
    | | Property Key: globus.queue       | | the remote queue in which the job should be run. Used when  |
    | | Profile Key: queue               | | remote scheduler is a scheduler such PBS that supports      |
    | | Scope : TC, SC, DAX, Properties  | | queues.                                                     |
    | | Since : 2.0                      |                                                               |
    | | Type :String                     |                                                               |
    +------------------------------------+---------------------------------------------------------------+

Pegasus prevents the user from specifying certain RSL instructions as
globus profiles, because they are either automatically generated or can
be overridden through some different means. For instance, if you need to
specify remote environment settings, do not use the environment key in
the globus profiles. Use one or more env profiles instead.

.. table:: RSL Instructions that are not permissible

    +-------------+------------------------------------------------------------------------------+
    | Key         | Reason for Prohibition                                                       |
    +=============+==============================================================================+
    | arguments   | you specify arguments in the arguments section for a job in the DAX          |
    +-------------+------------------------------------------------------------------------------+
    | directory   | the site catalog and properties determine which directory a job will run in. |
    +-------------+------------------------------------------------------------------------------+
    | environment | use multiple env profiles instead                                            |
    +-------------+------------------------------------------------------------------------------+
    | executable  || the physical executable to be used is specified in the transformation       |
    |             || catalog and is also dependant on the gridstart module being used. If you    |
    |             || are launching jobs via kickstart then the executable created is the path    |
    |             || to kickstart and the application executable path appears in the arguments   |
    |             || for kickstart                                                               |
    +-------------+------------------------------------------------------------------------------+
    | stdin       | you specify in the abstract workflow for the job                             |
    +-------------+------------------------------------------------------------------------------+
    | stdout      | you specify in the abstract workflow for the job                             |
    +-------------+------------------------------------------------------------------------------+
    | stderr      | you specify in the abstract workflow for the job                             |
    +-------------+------------------------------------------------------------------------------+


.. _condor-profiles:

The Condor Profile Namespace
----------------------------

The Condor submit file controls every detail how and where a job is run.
The *condor* profiles permit to add or overwrite instructions in the
Condor submit file.

The *condor* namespace directly sets commands in the Condor submit file
for a job the profile applies to. Keys in the *condor* profile namespace
denote the name of the Condor command. The profile value becomes the
command's argument. All *condor* profiles are translated into key=value
lines in the Condor submit file

Some of the common condor commands that a user may need to specify are
listed below. For an authoritative list refer to the online condor
documentation. Note: Pegasus Workflow Planner/Mapper by default specify
a lot of condor commands in the submit files depending upon the job, and
where it is being run.

.. table:: Useful Condor Commands

    +---------------------------------------------+--------------------------------------------------------------------+
    | Property Key                                | Description                                                        |
    +=============================================+====================================================================+
    | | Property Key: condor.universe             | | Pegasus defaults to either globus or scheduler universes. Set to |
    | | Profile Key: universe                     | | grid for compute jobs that require grid universe. Set to         |
    | | Scope : TC, SC, Abstract WF, Properties   | | vanilla to run natively in a condor pool, or to run on           |
    | | Since : 2.0                               | | resources grabbed via condor glidein.                            |
    | | Type : String                             |                                                                    |
    +---------------------------------------------+--------------------------------------------------------------------+
    | | Property Key: condor.periodic_release     | | is the number of times job is released back to the queue if      |
    | | Profile Key: periodic_release             | | it goes to HOLD, e.g. due to Globus errors. Pegasus defaults     |
    | | Scope : TC, SC,  Abstract WF, Properties  | | to 3.                                                            |
    | | Since : 2.0                               |                                                                    |
    | | Type : String                             |                                                                    |
    +---------------------------------------------+--------------------------------------------------------------------+
    | | Property Key:condor.periodic_remove       | | is the number of times a job is allowed to get into HOLD         |
    | | Profile Key:periodic_remove               | | state before being removed from the queue.                       |
    | | Scope : TC, SC,Abstract WF, Properties    | | Pegasus defaults to 3.                                           |
    | | Since : 2.0                               |                                                                    |
    | | Type : String                             |                                                                    |
    +---------------------------------------------+--------------------------------------------------------------------+
    | | Property Key: condor.filesystemdomain     | Useful for Condor glide-ins to pin a job to a remote site.         |
    | | Profile Key: filesystemdomain             |                                                                    |
    | | Scope : TC, SC, Abstract WF, Properties   |                                                                    |
    | | Since : 2.0                               |                                                                    |
    | | Type :String                              |                                                                    |
    +---------------------------------------------+--------------------------------------------------------------------+
    | | Property Key: condor.stream_error         | | boolean to turn on the streaming of the stderr of the            |
    | | Profile Key: stream_error                 | | remote job back to submit host.                                  |
    | | Scope : TC, SC, Abstract WF, Properties   |                                                                    |
    | | Since : 2.0                               |                                                                    |
    | | Type :Boolean                             |                                                                    |
    +---------------------------------------------+--------------------------------------------------------------------+
    | | Property Key: condor.stream_output        | | boolean to turn on the streaming of the stdout of the            |
    | | Profile Key: stream_output                | | remote job back to submit host.                                  |
    | | Scope : TC, SC, Abstract WF, Properties   |                                                                    |
    | | Since : 2.0 Type :Boolean                 |                                                                    |
    +---------------------------------------------+--------------------------------------------------------------------+
    | | Property Key: condor.priority             | | integer value to assign the priority of a job. Higher            |
    | | Profile Key:priority                      | | value means higher priority. The priorities are only             |
    | | Scope : TC, SC, Abstract WF, Properties   | | applied for vanilla / standard/ local universe jobs.             |
    | | Since : 2.0                               | | Determines the order in which a users own jobs are executed.     |
    | | Type :String                              |                                                                    |
    +---------------------------------------------+--------------------------------------------------------------------+
    | | Property Key: condor.request_cpus         | New in Condor 7.8.0 . Number of CPU’s a job requires.              |
    | | Profile Key: request_cpus                 |                                                                    |
    | | Scope : TC, SC, Abstract WF, Properties   |                                                                    |
    | | Since : 2.0                               |                                                                    |
    | | Type :String                              |                                                                    |
    +---------------------------------------------+--------------------------------------------------------------------+
    | | Property Key: condor.request_gpus         | Number of GPU’s a job requires.                                    |
    | | Profile Key:request_gpus                  |                                                                    |
    | | Scope : TC, SC, Abstract WF, Properties   |                                                                    |
    | | Since : 4.6                               |                                                                    |
    | | Type :String                              |                                                                    |
    +---------------------------------------------+--------------------------------------------------------------------+
    | | Property Key: condor.request_memory       | New in Condor 7.8.0 . Amount of memory a job requires.             |
    | | Profile Key: request_memory               |                                                                    |
    | | Scope : TC, SC, Abstract WF, Properties   |                                                                    |
    | | Since : 2.0                               |                                                                    |
    | | Type :String                              |                                                                    |
    +---------------------------------------------+--------------------------------------------------------------------+
    | | Property Key: condor.request_disk         | New in Condor 7.8.0 . Amount of disk a job requires.               |
    | | Profile Key: request_disk                 |                                                                    |
    | | Scope : TC, SC, Abstract WF, Properties   |                                                                    |
    | | Since : 2.0                               |                                                                    |
    | | Type :String                              |                                                                    |
    +---------------------------------------------+--------------------------------------------------------------------+


Other useful condor keys, that advanced users may find useful and can be
set by profiles are

1. should_transfer_files

2. transfer_output

3. transfer_error

4. whentotransferoutput

5. requirements

6. rank

Pegasus prevents the user from specifying certain Condor commands in
condor profiles, because they are automatically generated or can be
overridden through some different means. The table below shows
prohibited Condor commands.

.. table:: Condor commands prohibited in condor profiles

    +-------------+------------------------------------------------------------------------------+
    | Key         | Reason for Prohibition                                                       |
    +=============+==============================================================================+
    | arguments   | you specify arguments in the arguments section for a job in the Abstract WF. |
    +-------------+------------------------------------------------------------------------------+
    | environment | use multiple env profiles instead                                            |
    +-------------+------------------------------------------------------------------------------+
    | executable  | | the physical executable to be used is specified in the transformation      |
    |             | | catalog and is also dependant on the gridstart module being used.          |
    |             | | If you are launching jobs via kickstart then the executable created        |
    |             | | is the path to kickstart and the application executable path appears       |
    |             | | in the arguments for kickstart                                             |
    +-------------+------------------------------------------------------------------------------+

.. _dagman-profiles:

The Dagman Profile Namespace
----------------------------

DAGMan is Condor's workflow manager. While planners generate most of
DAGMan's configuration, it is possible to tweak certain job-related
characteristics using dagman profiles. A dagman profile can be used to
specify a DAGMan pre- or post-script.

Pre- and post-scripts execute on the submit machine. Both inherit the
environment settings from the submit host when pegasus-submit-dag or
pegasus-run is invoked.

By default, kickstart launches all jobs except standard universe and MPI
jobs. Kickstart tracks the execution of the job, and returns usage
statistics for the job. A DAGMan post-script starts the Pegasus
application exitcode to determine, if the job succeeded. DAGMan receives
the success indication as exit status from exitcode.

If you need to run your own post-script, you have to take over the job
success parsing. The planner is set up to pass the file name of the
remote job's stdout, usually the output from kickstart, as sole argument
to the post-script.

The table below shows the keys in the dagman profile domain that are
understood by Pegasus and can be associated at a per job basis.

.. table:: Useful dagman Commands that can be associated at a per job basis

    +------------------------------------------------+-----------------------------------------------------------------+
    | Property Key                                   | Description                                                     |
    +================================================+=================================================================+
    | | Property Key: dagman.pre                     | | is the path to the pre-script. DAGMan executes the pre-script |
    | | Profile Key:PRE                              | | before it runs the job.                                       |
    | | Scope : TC, SC, Abstract WF, Properties      |                                                                 |
    | | Since : 2.0                                  |                                                                 |
    | | Type :String                                 |                                                                 |
    +------------------------------------------------+-----------------------------------------------------------------+
    | | Property Key: dagman.pre.arguments           | are command-line arguments for the pre-script, if any.          |
    | | Profile Key: PRE.ARGUMENTS                   |                                                                 |
    | | Scope : TC, SC, Abstract WF, Properties      |                                                                 |
    | | Since : 2.0                                  |                                                                 |
    | | Type :String                                 |                                                                 |
    +------------------------------------------------+-----------------------------------------------------------------+
    | | Property Key: dagman.post                    | | is the postscript type/mode that a user wants to associate    |
    | | Profile Key: POST                            | | with a job.                                                   |
    | | Scope : TC, SC, Abstract WF, Properties      | | - **pegasus-exitcode** - pegasus will by default              |
    | | Since : 2.0                                  | |    associate this postscript with all jobs launched via       |
    | | Type :String                                 | |    kickstart, as long the POST.SCOPE value is not set to      |
    |                                                | |    NONE.                                                      |
    |                                                | | - **none** -means that no postscript is generated for the     |
    |                                                | |    jobs. This is useful for MPI jobs that are not launched    |
    |                                                | |    via kickstart currently.                                   |
    |                                                | | - **any legal identifier** - Any other identifier of the      |
    |                                                | |    form ([_A-Za-z][_A-Za-z0-9]*), than one of the 2 reserved  |
    |                                                | |    keywords above, signifies a user postscript. This allows   |
    |                                                | |    the user to specify their own postscript for the jobs in   |
    |                                                | |    the workflow. The path to the postscript can be specified  |
    |                                                | |    by the dagman profile POST.PATH.[value] where [value] is   |
    |                                                | |    this legal identifier specified. The user postscript is    |
    |                                                | |    passed the name of the .out file of the job as the last    |
    |                                                | |    argument on the command line.                              |
    |                                                | |    For e.g. if the following dagman profiles were associated  |
    |                                                | |    with a job X                                               |
    |                                                | |    POST with value user_script /bin/user_postscript           |
    |                                                | |    POST.PATH.user_script with value /path/to/user/script      |
    |                                                | |    POST.ARGUMENTS with value -verbose                         |
    |                                                | |    then the following postscript will be associated with the  |
    |                                                | |    job X in the .dag file is                                  |
    |                                                | |    /path/to/user/script -verbose X.out where X.out contains   |
    |                                                | |    the stdout of the job X                                    |
    +------------------------------------------------+-----------------------------------------------------------------+
    | | Property Key:                                | the path to the post script on the submit host.                 |
    | |      dagman.post.path.[value of dagman.post] |                                                                 |
    | | Profile Key:post.path.[value of dagman.post] |                                                                 |
    | | Scope : TC, SC, Abstract WF, Properties      |                                                                 |
    | | Since : 2.0                                  |                                                                 |
    | | Type :String                                 |                                                                 |
    +------------------------------------------------+-----------------------------------------------------------------+
    | | Property Key: dagman.post.arguments          | are the command line arguments for the post script, if any.     |
    | | Profile Key:POST.ARGUMENTS                   |                                                                 |
    | | Scope : TC, SC, Abstract WF, Properties      |                                                                 |
    | | Since : 2.0                                  |                                                                 |
    | | Type :String                                 |                                                                 |
    +------------------------------------------------+-----------------------------------------------------------------+
    | | Property Key: dagman.retry                   | | is the number of times DAGMan retries the full job cycle      |
    | | Profile Key:RETRY                            | | from pre-script through post-script, if failure was           |
    | | Scope : TC, SC, Abstract WF, Properties      | | detected.                                                     |
    | | Since : 2.0                                  |                                                                 |
    | | Type :Integer                                |                                                                 |
    | | Default : 1                                  |                                                                 |
    +------------------------------------------------+-----------------------------------------------------------------+
    | | Property Key: dagman.category                | the DAGMan category the job belongs to.                         |
    | | Profile Key:CATEGORY                         |                                                                 |
    | | Scope : TC, SC, Abstract WF, Properties      |                                                                 |
    | | Since : 2.0                                  |                                                                 |
    | | Type :String                                 |                                                                 |
    +------------------------------------------------+-----------------------------------------------------------------+
    | | Property Key: dagman.priority                | | the priority to apply to a job. DAGMan uses this to select    |
    | | Profile Key: PRIORITY                        | | what jobs to release when MAXJOBS is enforced for the DAG.    |
    | | Scope : TC, SC, Abstract WF, Properties      |                                                                 |
    | | Since : 2.0                                  |                                                                 |
    | | Type :Integer                                |                                                                 |
    +------------------------------------------------+-----------------------------------------------------------------+
    | | Property Key: dagman.abort-dag-on            | | The ABORT-DAG-ON key word provides a way to abort the         |
    | | Profile Key:ABORT-DAG-ON                     | | entire DAG if a given node returns a specific exit code       |
    | | Scope : TC, Abstract WF,                     | | (AbortExitValue). The syntax for the value of the key is      |
    | | Since : 4.5                                  | | AbortExitValue [RETURN DAGReturnValue] . When a DAG aborts,   |
    | | Type :String                                 | | by default it exits with the node return value that caused    |
    |                                                | | the abort. This can be changed by using the optional          |
    |                                                | | RETURN key word along with specifying the desired             |
    |                                                | | DAGReturnValue                                                |
    +------------------------------------------------+-----------------------------------------------------------------+


The table below shows the keys in the dagman profile domain that are
understood by Pegasus and can be used to apply to the whole workflow.
These are used to control DAGMan's behavior at the workflow level, and
are recommended to be specified in the properties file.

.. table:: Useful dagman Commands that can be specified in the properties file.

    +---------------------------------------+-------------------------------------------------------------------+
    | Property Key                          | Description                                                       |
    +=======================================+===================================================================+
    | | Property Key: dagman.maxpre         | | sets the maximum number of PRE scripts within the DAG that may  |
    | | Profile Key: MAXPRE                 | | be running at one time                                          |
    | | Scope : Properties                  |                                                                   |
    | | Since : 2.0                         |                                                                   |
    | | Type :String                        |                                                                   |
    +---------------------------------------+-------------------------------------------------------------------+
    | | Property Key: dagman.maxpost        | | sets the maximum number of POST scripts within the DAG that     |
    | | Profile Key: MAXPOST                | | may be running at one time                                      |
    | | Scope : Properties                  |                                                                   |
    | | Since : 2.0                         |                                                                   |
    | | Type :String                        |                                                                   |
    +---------------------------------------+-------------------------------------------------------------------+
    | | Property Key: dagman.maxjobs        | | sets the maximum number of jobs within the DAG that will be     |
    | | Profile Key: MAXJOBS                | | submitted to Condor at one time.                                |
    | | Scope : Properties                  |                                                                   |
    | | Since : 2.0                         |                                                                   |
    | | Type :String                        |                                                                   |
    +---------------------------------------+-------------------------------------------------------------------+
    | | Property Key: dagman.maxidle        | | Sets the maximum number of idle jobs allowed before HTCondor    |
    | | Profile Key:MAXIDLE                 | | DAGMan stops submitting more jobs. Once idle jobs start to run, |
    | | Scope : Properties                  | | HTCondor DAGMan will resume submitting jobs. If the option      |
    | | Since : 2.0                         | | is omitted, the number of idle jobs is unlimited.               |
    | | Type :String                        |                                                                   |
    +---------------------------------------+-------------------------------------------------------------------+
    | | Property Key:                       | | is the value of maxjobs for a particular category. Users can    |
    | |      dagman.[CATEGORY-NAME].maxjobs | | associate different categories to the jobs at a per job basis.  |
    | | Profile Key:[CATEGORY-NAME].MAXJOBS | | However, the value of a dagman knob for a category can only     |
    | | Scope : Properties                  | | be specified at a per workflow basis in the properties.         |
    | | Since : 2.0                         |                                                                   |
    | | Type :String                        |                                                                   |
    +---------------------------------------+-------------------------------------------------------------------+
    | | Property Key: dagman.post.scope     | | scope for the postscripts.                                      |
    | | Profile Key:POST.SCOPE              | | - If set to all , means each job in the workflow will           |
    | | Scope : Properties                  | |   have a postscript associated with it.                         |
    | | Since : 2.0                         | | - If set to none , means no job has postscript associated       |
    | | Type :String                        | |   with it. None mode should be used if you are running vanilla  |
    |                                       | |   / standard/ local universe jobs, as in those cases Condor     |
    |                                       | |   traps the remote exitcode correctly. None scope is not        |
    |                                       | |   recommended for grid universe jobs.                           |
    |                                       | | - If set to essential, means only essential jobs have post      |
    |                                       | |   scripts associated with them. At present the only non         |
    |                                       | |   essential job is the replica registration job.                |
    +---------------------------------------+-------------------------------------------------------------------+

.. _pegasus-profiles:

The Pegasus Profile Namespace
-----------------------------

The *pegasus* profiles allow users to configure extra options to the
Pegasus Workflow Planner that can be applied selectively to a job or a
group of jobs. Site selectors may use a sub-set of *pegasus* profiles
for their decision-making.

The table below shows some of the useful configuration option Pegasus
understands.

.. table:: Useful Pegasus Profiles

    +--------------------------------------------+---------------------------------------------------------------------+
    | Property Key                               | Description                                                         |
    +============================================+=====================================================================+
    | | Property Key: pegasus.clusters.num       | | Please refer to the                                               |
    | | Profile Key: clusters.num                | | :ref:`Pegasus Clustering Guide <horizontal-clustering>`           |
    | | Scope : TC, SC, Abstract WF, Properties  | | for detailed description. This option determines the              |
    | | Since : 3.0                              | | total number of clusters per level. Jobs are evenly spread        |
    | | Type :Integer                            | | across clusters.                                                  |
    +--------------------------------------------+---------------------------------------------------------------------+
    | | Property Key: pegasus.clusters.size      | | Please refer to the                                               |
    | | Profile Key:clusters.size                | | :ref:`Pegasus Clustering Guide <horizontal-clustering>`           |
    | | Scope : TC, SC, Abstract WF, Properties  | | for detailed description. This profile determines the number of   |
    | | Since : 3.0                              | | jobs in each cluster. The number of clusters depends on the total |
    | | Type : Integer                           | | number of jobs on the level.                                      |
    +--------------------------------------------+---------------------------------------------------------------------+
    | | Property Key:                            | | Indicates the clustering executable that is used to run the       |
    | |  pegasus.clusterer.job.aggregator        | | clustered job on the remote site.                                 |
    | | Profile Key:job.aggregator               | |                                                                   |
    | | Scope : TC, SC, Abstract WF, Properties  |                                                                     |
    | | Since : 2.0                              |                                                                     |
    | | Type :Integer                            |                                                                     |
    +--------------------------------------------+---------------------------------------------------------------------+
    | | Property Key:                            | | The additional arguments with which a clustering                  |
    | | pegasus.clusterer.job.aggregator.\       | | executable should be invoked.                                     |
    | |                              arguments   | |                                                                   |
    | | Profile Key: job.aggregator.arguments    | |                                                                   |
    | | Scope : TC, SC, Abstract WF, Properties  |                                                                     |
    | | Since : 5.0.2                            |                                                                     |
    | | Default : None                           |                                                                     |
    | | See Also :                               |                                                                     |
    | |    pegasus.clusterer.job.aggregator      |                                                                     |
    +--------------------------------------------+---------------------------------------------------------------------+
    | | Property Key: pegasus.gridstart          | | Determines the executable for launching a job. This               |
    | | Profile Key: gridstart                   | | covers both tasks ( jobs specified by the user in the             |
    | | Scope : TC, SC, Abstract WF, Properties  | | Abstract Workflow) and additional jobs added by Pegasus           |
    | | Since : 2.0                              | | during the planning operation. Possible values are                |
    | | Type :String                             | | Kickstart | NoGridStart | PegasusLite | Distribute                |
    |                                            | | at the moment.                                                    |
    |                                            | | Note: This profile should only be set by users if you know        |
    |                                            | | what you are doing. Otherwise, let Pegasus do the right thing     |
    |                                            | | based on your configuration.                                      |
    |                                            | | - **Kickstart**                                                   |
    |                                            | |   By default, all jobs executed are launched using a lightweight  |
    |                                            | |   C executable called pegasus-kickstart. This generates valuable  |
    |                                            | |   runtime provenance information for the job as it is executed    |
    |                                            | |   on a remote node. This information serves as the basis for      |
    |                                            | |   the monitoring and debugging capabilities provided by Pegasus.  |
    |                                            | | - **NoGridStart**                                                 |
    |                                            | |   This explicity disables the wrapping of the jobs with           |
    |                                            | |   pegasus-kickstart. This is internally used by the planner to    |
    |                                            | |   launch jobs directly. If this is set, then the information      |
    |                                            | |   populated in the monitoring database is on the basis of what    |
    |                                            | |   is recorded in the DAGMan out file.                             |
    |                                            | | - **PegasusLite**                                                 |
    |                                            | |   This value is automatically associated by the Planner whenever  |
    |                                            | |   the job runs in either nonsharedfs or condorio mode. The        |
    |                                            | |   property pegasus.data.configuration decides whether a job is    |
    |                                            | |   launched via PegasusLite or not. PegasusLite is a lightweight   |
    |                                            | |   Pegasus wrapper generated for each job that allows a job to     |
    |                                            | |   run in a nonshared file system environment and is responsible   |
    |                                            | |   for staging in the input data and staging out the output data   |
    |                                            | |   back to a remote staging site for the job.                      |
    |                                            | | - **PegasusLite.None**                                            |
    |                                            | |   This value if set, forces PegasusLite to not launch the job via |
    |                                            | |   Kickstart.                                                      |
    |                                            | | - **Distribute**                                                  |
    |                                            | |   This wrapper is a HubZero specfiic wrapper that allows compute  |
    |                                            | |   jobs that are scheduled for a local PBS cluster to be run       |
    |                                            | |   locally on the submit host. The jobs are wrapped with a         |
    |                                            | |   distribute wrapper that is responsible for doing the qsub       |
    |                                            | |   and tracking of the status of the jobs in the PBS cluster.      |
    +--------------------------------------------+---------------------------------------------------------------------+
    | | Property Key: pegasus.gridstart.path     | | Sets the path to the gridstart . This profile is best set in      |
    | | Profile Key: gridstart.path              | | the Site Catalog.                                                 |
    | | Scope : TC, SC, Abstract WF, Properties  |                                                                     |
    | | Since : 2.0                              |                                                                     |
    | | Type :file path                          |                                                                     |
    +--------------------------------------------+---------------------------------------------------------------------+
    | | Property Key:                            | | Sets the arguments with which GridStart is used to launch a job   |
    | |    pegasus.gridstart.arguments           | | on the remote site.                                               |
    | | Profile Key:gridstart.arguments          |                                                                     |
    | | Scope : TC, SC, Abstract WF, Properties  |                                                                     |
    | | Since : 2.0                              |                                                                     |
    | | Type :String                             |                                                                     |
    +--------------------------------------------+---------------------------------------------------------------------+
    | | Property Key: pegasus.gridstart.launcher | | Specifies the launcher executable to use to launch the GridStart. |
    | | Profile Key: gridstart.launcher          | | Is useful, when we want the kickstart invocation for the compute  |
    | | Scope : TC, SC, Abstract WF, Properties  | | jobs to be submitted using jsrun, as in on sites where originally |
    | | Since : 4.9.4                            | | the job lands on DTN node like OLCF sites.                        |
    | | Type : file path                         |                                                                     |
    +--------------------------------------------+---------------------------------------------------------------------+
    | | Property Key:                            | | The arguments to the launcher executable for GridStart            |
    | |    pegasus.gridstart.launcher.arguments  |                                                                     |
    | | Profile Key:gridstart.launcher.arguments |                                                                     |
    | | Scope : TC, SC, Abstract WF, Properties  |                                                                     |
    | | Since : 2.0                              |                                                                     |
    | | Type :String                             |                                                                     |
    +--------------------------------------------+---------------------------------------------------------------------+
    | | Property Key: pegasus.stagein.clusters   | | This key determines the maximum number of stage-in jobs that      |
    | | Profile Key: stagein.clusters            | | are can executed locally or remotely per compute site per         |
    | | Scope : TC, SC, Abstract WF, Properties  | | workflow. This is used to configure the                           |
    | | Since : 4.0                              | | :ref:`BalancedCluster <transfer-refiner-balanced-cluster>`        |
    | | Type :Integer                            | | Transfer Refiner, which is the Default Refiner used in Pegasus.   |
    |                                            | | This profile is best set in the Site Catalog or in the            |
    |                                            | | Properties file                                                   |
    +--------------------------------------------+---------------------------------------------------------------------+
    | | Property Key:                            | | This key provides finer grained control in determining the        |
    | |           pegasus.stagein.local.clusters | | number of stage-in jobs that are executed locally and are         |
    | | Profile Key: stagein.local.clusters      | | responsible for staging data to a particular remote site.         |
    | | Scope : TC, SC, Abstract WF, Properties  | | This profile is best set in the Site Catalog or in the            |
    | | Since : 4.0                              | | Properties file                                                   |
    | | Type :Integer                            |                                                                     |
    +--------------------------------------------+---------------------------------------------------------------------+
    | | Property Key:                            | | This key provides finer grained control in determining the        |
    | |          pegasus.stagein.remote.clusters | | number of stage-in jobs that are executed remotely on the         |
    | | Profile Key:stagein.remote.clusters      | | remote site and are responsible for staging data to it.           |
    | | Scope : TC, SC, Abstract WF, Properties  | | This profile is best set in the Site Catalog or in the            |
    | | Since : 4.0                              | | Properties file                                                   |
    | | Type :Integer                            |                                                                     |
    +--------------------------------------------+---------------------------------------------------------------------+
    | | Property Key: pegasus.stageout.clusters  | | This key determines the maximum number of stage-out jobs          |
    | | Profile Key: stageout.clusters           | | that are can executed locally or remotely per compute site        |
    | | Scope : TC, SC, Abstract WF, Properties  | | per workflow. This is used to configure the                       |
    | | Since : 4.0                              | | :ref:`BalancedCluster <transfer-refiner-balanced-cluster>`        |
    | | Type : Integer                           | | Transfer Refiner, which is the Default Refiner used               |
    |                                            | | in Pegasus.                                                       |
    +--------------------------------------------+---------------------------------------------------------------------+
    | | Property Key:                            | | This key provides finer grained control in determining the        |
    | |       pegasus.stageout.local.clusters    | | number of stage-out jobs that are executed locally and are        |
    | | Profile Key: stageout.local.clusters     | | responsible for staging data from a particular remote site.       |
    | | Scope : TC, SC, Abstract WF, Properties  | | This profile is best set in the Site Catalog or in the            |
    | | Since : 4.0                              | | Properties file                                                   |
    | | Type :Integer                            |                                                                     |
    +--------------------------------------------+---------------------------------------------------------------------+
    | | Property Key:                            | | This key provides finer grained control in determining the        |
    | |        pegasus.stageout.remote.clusters  | | number of stage-out jobs that are executed remotely on the        |
    | | Profile Key: stageout.remote.clusters    | | remote site and are responsible for staging data from it.         |
    | | Scope : TC, SC, Abstract WF, Properties  | | This profile is best set in the Site Catalog or in the            |
    | | Since : 4.0                              | | Properties file                                                   |
    | | Type :Integer                            |                                                                     |
    +--------------------------------------------+---------------------------------------------------------------------+
    | | Property Key: pegasus.group              | | Tags a job with an arbitrary group identifier. The group          |
    | | Profile Key:group                        | | site selector makes use of the tag.                               |
    | | Scope : TC, SC, Abstract WF, Properties  |                                                                     |
    | | Since : 2.0                              |                                                                     |
    | | Type :String                             |                                                                     |
    +--------------------------------------------+---------------------------------------------------------------------+
    | | Property Key: pegasus.change.dir         | | If true, tells kickstart to change into the remote working        |
    | | Profile Key: change.dir                  | | directory. Kickstart itself is executed in whichever directory    |
    | | Scope : TC, SC, Abstract WF, Properties  | | the remote scheduling system chose for the job.                   |
    | | Since : 2.0                              |                                                                     |
    | | Type :Boolean                            |                                                                     |
    +--------------------------------------------+---------------------------------------------------------------------+
    | | Property Key: pegasus.create.dir         | | If true, tells kickstart to create the the remote working         |
    | | Profile Key: create.dir                  | | directory before changing into the remote working directory.      |
    | | Scope : TC, SC, Abstract WF, Properties  | | Kickstart itself is executed in whichever directory the           |
    | | Since : 2.0                              | | remote scheduling system chose for the job.                       |
    | | Type : Boolean                           |                                                                     |
    +--------------------------------------------+---------------------------------------------------------------------+
    | | Property Key: pegasus.transfer.proxy     | | If true, tells Pegasus to explicitly transfer the proxy for       |
    | | Profile Key:transfer.proxy               | | transfer jobs to the remote site. This is useful, when you        |
    | | Scope : TC, SC, Abstract WF, Properties  | | want to use a full proxy at the remote end, instead of the        |
    | | Since : 2.0                              | | limited proxy that is transferred by CondorG.                     |
    | | Type :Boolean                            |                                                                     |
    +--------------------------------------------+---------------------------------------------------------------------+
    | | Property Key: pegasus.style              | | Sets the condor submit file style. If set to globus, submit       |
    | | Profile Key:style                        | | file generated refers to CondorG job submissions. If set to       |
    | | Scope : TC, SC, Abstract WF, Properties  | | condor, submit file generated refers to direct Condor             |
    | | Since : 2.0                              | | submission to the local Condor pool. It applies for glidein,      |
    | | Type :String                             | | where nodes from remote grid sites are glided into the local      |
    |                                            | | condor pool. The default style that is applied is globus.         |
    +--------------------------------------------+---------------------------------------------------------------------+
    | | Property Key:                            | | This key is used to set the -m option for pegasus-mpi-cluster.    |
    | |    pegasus.pmc_request_memory            | | It specifies the amount of memory in MB that a job requires.      |
    | | Profile Key:pmc_request_memory           | | This profile is usually set in the Abstract WF for each job.      |
    | | Scope : TC, SC, Abstract WF, Properties  |                                                                     |
    | | Since : 4.2                              |                                                                     |
    | | Type :Integer                            |                                                                     |
    +--------------------------------------------+---------------------------------------------------------------------+
    | | Property Key: pegasus.pmc_request_cpus   | | This key is used to set the -c option for pegasus-mpi-cluster.    |
    | | Profile Key: pmc_request_cpus            | | It specifies the number of cpu’s that a job requires.             |
    | | Scope : TC, SC, Abstract WF, Properties  | | This profile is usually set in the Abstract WF for each job.      |
    | | Since : 4.2                              |                                                                     |
    | | Type :Integer                            |                                                                     |
    +--------------------------------------------+---------------------------------------------------------------------+
    | | Property Key: pegasus.pmc_priority       | | This key is used to set the -p option for pegasus-mpi-cluster.    |
    | | Profile Key: pmc_priority                | | It specifies the priority for a job . This profile is usually     |
    | | Scope : TC, SC, Abstract WF, Properties  | | set in the Abstract WF for each job. Negative values are          |
    | | Since : 4.2                              | | allowed for priorities.                                           |
    | | Type :Integer                            |                                                                     |
    +--------------------------------------------+---------------------------------------------------------------------+
    | | Property Key:                            | | The key is used to pass any extra arguments to the PMC task       |
    | |         pegasus.pmc_task_arguments       | | during the planning time. They are added to the very end          |
    | | Profile Key: pmc_task_arguments          | | of the argument string constructed for the task in the            |
    | | Scope : TC, SC, Abstract WF, Properties  | | PMC file. Hence, allows for overriding of any argument            |
    | | Since : 4.2                              | | constructed by the planner for any particular task in             |
    | | Type :String                             | | the PMC job.                                                      |
    +--------------------------------------------+---------------------------------------------------------------------+
    | | Property Key:                            | | The message string that pegasus-exitcode searches for in          |
    | |       pegasus.exitcode.failuremsg        | | the stdout and stderr of the job to flag failures.                |
    | | Profile Key: exitcode.failuremsg         |                                                                     |
    | | Scope : TC, SC, Abstract WF, Properties  |                                                                     |
    | | Since : 4.4                              |                                                                     |
    | | Type :String                             |                                                                     |
    +--------------------------------------------+---------------------------------------------------------------------+
    | | Property Key:                            | | The message string that pegasus-exitcode searches for in          |
    | |          pegasus.exitcode.successmsg     | | the stdout and stderr of the job to determine whether a           |
    | | Profile Key: exitcode.successmsg         | | job logged it’s success message or not. Note this value           |
    | | Scope : TC, SC, Abstract WF, Properties  | | is used to check for whether a job failed or not i.e if           |
    | | Since : 4.4                              | | this profile is specified, and pegasus-exitcode DOES NOT          |
    | | Type :String                             | | find the string in the job stdout or stderr, the job              |
    |                                            | | is flagged as failed. The complete rules for determining          |
    |                                            | | failure are described in the man page for pegasus-exitcode.       |
    +--------------------------------------------+---------------------------------------------------------------------+
    | | Property Key: pegasus.checkpoint.time    | | the expected time in minutes for a job after which it should      |
    | | Profile Key: checkpoint.time             | | be sent a TERM signal to generate a job checkpoint file           |
    | | Scope : TC, SC, Abstract WF, Properties  |                                                                     |
    | | Since : 4.5                              |                                                                     |
    | | Type : Integer                           |                                                                     |
    +--------------------------------------------+---------------------------------------------------------------------+
    | | Property Key: pegasus.maxwalltime        | the maximum walltime in minutes for a single execution of a job.    |
    | | Profile Key: maxwalltime                 |                                                                     |
    | | Scope : TC, SC, Abstract WF, Properties  |                                                                     |
    | | Since : 4.5                              |                                                                     |
    | | Type :Integer                            |                                                                     |
    +--------------------------------------------+---------------------------------------------------------------------+
    | | Property Key: pegasus.glite.arguments    | | specifies the extra arguments that must appear in the local       |
    | | Profile Key: glite.arguments             | | PBS or LRMS generated script for a job, when running workflows    |
    | | Scope : TC, SC, Abstract WF, Properties  | | on a local cluster with submissions through Glite. This is        |
    | | Since : 4.5                              | | useful when you want to pass through special options to           |
    | | Type :String                             | | underlying LRMS such as PBS e.g.                                  |
    |                                            | | you can set value -l walltime=01:23:45 -l nodes=2 to specify      |
    |                                            | | your job’s resource requirements.                                 |
    +--------------------------------------------+---------------------------------------------------------------------+
    | | Profile Key: auxillary.local             | | indicates whether auxillary jobs associated with a compute site   |
    | | Scope : SC                               | | X, can be run on local site. This CAN ONLY be specified as a      |
    | | Since : 4.6                              | | profile in the site catalog and should be set when the compute    |
    | | Type :Boolean                            | | site filesystem is accessible locally on the submit host.         |
    +--------------------------------------------+---------------------------------------------------------------------+
    | | Property Key:                            | | indicates whether condor quoting rules should be applied for      |
    | |      pegasus.condor.arguments.quote      | | writing out the arguments key in the condor submit file. By       |
    | | Profile Key: condor.arguments.quote      | | default it is true unless the job is schedule to a glite          |
    | | Scope : SC, Properties                   | | style site. The value is automatically set to false for           |
    | | Since : 4.6                              | | glite style sites, as condor quoting is broken in batch_gahp.     |
    | | Type :Boolean                            |                                                                     |
    +--------------------------------------------+---------------------------------------------------------------------+
    | | Profile Key: container.arguments         | | indicates additional arguments that will be appended to           |
    | | Scope : TC, Workflow                     | | the docker container run/singularity exec commands when           |
    | | Since : 5.0                              | | the container associated with this profile is executed            |
    | | Type :String                             |                                                                     |
    +--------------------------------------------+---------------------------------------------------------------------+
    | | Profile Key: container.launcher          | | A wrapper around docker/singularity execution. Applies            |
    | | Scope : TC, Workflow, Properties         | | only to containerized jobs. Useful, when the the user executable  |
    | | Since : 5.1                              | | has to be launched via srun for example.                          |
    | | Type :String                             |                                                                     |
    +--------------------------------------------+---------------------------------------------------------------------+
    | | Profile Key: container.launcher.arguments| | indicates additional arguments that will be appended to           |
    | | Scope : TC, Workflow, Properties         | | container wrapper. Comes into play only if a job has              |
    | | Since : 5.1                              | | container.launcher profile  associated with it.                   |
    | | Type :String                             |                                                                     |
    +--------------------------------------------+---------------------------------------------------------------------+
    | | Profile Key: pegasus_lite_env_source     | | indicates a path to a setup script residing on the submit host    |
    | | Scope : Site Catalog                     | | that needs to be sourced in PegasusLite when running the job.     |
    | | Since : 5.0                              | | This profile should be associated with local site in the          |
    | | Type :String                             | | Site Catalog. You can also specify an environment profile named   |
    | |                                          | | PEGASUS_LITE_ENV_SOURCE with the compute site in the              |
    | |                                          | | Site Catalog to indicate a setup script that already exists       |
    | |                                          | | on the compute nodes, but just needs to be sourced when           |
    |                                            | | executing the job.                                                |
    +--------------------------------------------+---------------------------------------------------------------------+
    | | Profile Key: relative.submit.dir         | | indicates the relative submit directory for the job, if the       |
    | | Scope : TC, Workflow                     | | Named Submit Mapper is enabled.                                   |
    | | Since : 5.0.1                            |                                                                     |
    | | Type :String                             |                                                                     |
    | | See Also :                               |                                                                     |
    | |  pegasus.dir.submit.mapper               |                                                                     |
    +--------------------------------------------+---------------------------------------------------------------------+
    | |                                          | | This profile allows you to turn off symlinking for a job.         |
    | | Profile Key: nosymlink                   | | It only comes into play when symlinking for the workflow is       |
    | | Scope : TC, SC, Abstract WF              | | turned on by setting the property                                 |
    | | Since : 5.0.3                            | | pegasus.transfer.links to true                                    |
    | | Type : Boolean                           |                                                                     |
    +--------------------------------------------+---------------------------------------------------------------------+


.. _task-resource-profiles:

Task Resource Requirements Profiles
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Startng Pegasus 4.6.0 Release, users can specify pegasus profiles to
describe resources requirements for their job. The planner will
automatically translate them to appropriate execution environment
specific directives. For example, the profiles are automatically
translated to Globus RSL keys if submitting job via CondorG to remote
GRAM instances, Condor Classad keys when running in a vanilla condor
pool and to appropriate shell variables for Glite that can be picked up
by the local attributes.sh. The profiles are described below.

.. table:: Task Resource Requirement Profiles.

    +---------------------------------------------+------------------------------------------------------------------+
    | Property Key                                | Description                                                      |
    +=============================================+==================================================================+
    | | Property Key: pegasus.runtime             | | This profile specifies the expected runtime of a job           |
    | | Profile Key: runtime                      | | in seconds. Refer to the                                       |
    | | Scope : TC, SC, Abstract WF, Properties   | | :ref:`Pegasus Clustering Guide <runtime-clustering>`           |
    | | Since : 2.0                               | | for description on using it for runtime clustering.            |
    | | Type : Long                               |                                                                  |
    +---------------------------------------------+------------------------------------------------------------------+
    | | Property Key: pegasus.clusters.maxruntime | | Please refer to the                                            |
    | | Profile Key: clusters.maxruntime          | | :ref:`Pegasus Clustering Guide <runtime-clustering>`           |
    | | Scope : TC, SC, Abstract WF, Properties   | | for detailed description. This profile specifies the           |
    | | Since : 4.0                               | | maximum runtime of a job in seconds.                           |
    | | Type : Integer                            |                                                                  |
    +---------------------------------------------+------------------------------------------------------------------+
    | | Property Key: pegasus.cores               | | The total number of cores, required for a job. This is also    |
    | | Profile Key:cores                         | | used for accounting purposes in the database while             |
    | | Scope : TC, SC, Abstract WF, Properties   | | generating statistics. It corresponds to the multiplier_factor |
    | | Since : 4.0                               | | in the job_instance table described                            |
    | | Type : Integer                            | | :ref:`here <stampede-schema-overview>` .                       |
    +---------------------------------------------+------------------------------------------------------------------+
    | | Property Key: pegasus.gpus                | | The total number of gpus, required for a job.                  |
    | | Profile Key:gpus                          | |                                                                |
    | | Scope : TC, SC, Abstract WF, Properties   | |                                                                |
    | | Since : 5.0                               | |                                                                |
    | | Type : Integer                            | |                                                                |
    +---------------------------------------------+------------------------------------------------------------------+
    | | Property Key: pegasus.nodes               | Indicates the the number of nodes a job requires.                |
    | | Profile Key: nodes                        |                                                                  |
    | | Scope : TC, SC, Abstract WF, Properties   |                                                                  |
    | | Since : 4.6                               |                                                                  |
    | | Type : Integer                            |                                                                  |
    +---------------------------------------------+------------------------------------------------------------------+
    | | Property Key: pegasus.ppn                 | | Indicates the number of processors per node . This profile is  |
    | | Profile Key: ppn                          | | best set in the Site Catalog and usually set when running      |
    | | Scope : TC, SC, Abstract WF, Properties   | | workflows with MPI jobs.                                       |
    | | Since : 4.6                               |                                                                  |
    | | Type : Integer                            |                                                                  |
    +---------------------------------------------+------------------------------------------------------------------+
    | | Property Key: pegasus.memory              | Indicates the maximum memory a job requires in MB.               |
    | | Profile Key: memory                       |                                                                  |
    | | Scope : TC, SC, Abstract WF, Properties   |                                                                  |
    | | Since : 4.6                               |                                                                  |
    | | Type : Long                               |                                                                  |
    +---------------------------------------------+------------------------------------------------------------------+
    | | Property Key: pegasus.diskspace           | Indicates the maximum diskspace a job requires in MB.            |
    | | Profile Key: diskspace                    |                                                                  |
    | | Scope : TC, SC, Abstract WF, Properties   |                                                                  |
    | | Since : 4.6                               |                                                                  |
    | | Type : Long                               |                                                                  |
    +---------------------------------------------+------------------------------------------------------------------+

The automatic translation to various execution environment specific
directives is explained below. It is important, to note that execution
environment specific keys take precedence over the Pegasus profile keys.
For example, Globus profile key maxruntime will be preferred over
Pegasus profile key runtime when running jobs via HTCondorG.

.. table:: Table mapping translation of Pegasus Task Requirements to corresponding execution environment keys.

   ============================================= ============================ ================================ ===============================================
   Pegasus Task Resource Requirement Profile Key Corresponding Globus RSL Key Corresponding Condor Classad Key KEY in +remote_cerequirements classad for GLITE
   ============================================= ============================ ================================ ===============================================
   runtime                                       maxruntime                   -                                WALLTIME
   cores                                         count                        request_cpus                     CORES
   nodes                                         hostcount                    -                                NODES
   ppn                                           xcount                       -                                PROCS
   memory                                        maxmemory                    request_memory                   PER_PROCESS_MEMORY
   diskspace                                     -                            request_diskspace                -
   ============================================= ============================ ================================ ===============================================

.. _hints-profiles:

The Hints Profile Namespace
---------------------------
The *hints* namespace is now deprecated and has been replaced by the
*selector* namespace. If you have any hints profiles in your configuration,
please change their namespace value to *selector* instead. The support for
*hints* namespace will be dropped in a future release.

.. _selector-profiles:

The Selector Profile Namespace
------------------------------

The *selector* namespace allows users to override the behavior of the
Workflow Mapper during site selection. This gives you finer grained
control over where a job executes and what executable it refers to. The
hints namespace keys ( execution.site and pfn ) can only be specified in
the input abstract workflow. It is important to note that these particular
keys once specified in the workflow, cannot be overridden like other
profiles.

.. table:: Useful Selector Profile Keys

    +--------------------------------------------+--------------------------------------------------------------------+
    | Key Attributes                             | Description                                                        |
    +============================================+====================================================================+
    | | Property Key: N/A                        | the execution site where a job should be executed.                 |
    | | Profile Key: execution.site              |                                                                    |
    | | Scope : Abstract WF                      |                                                                    |
    | | Since : 4.5                              |                                                                    |
    | | Type : String                            |                                                                    |
    +--------------------------------------------+--------------------------------------------------------------------+
    | | Property Key: N/A                        | | the physical file name to the main executable that a job refers  |
    | | Profile Key: pfn                         | | to. Overrides any entries specified in the transformation        |
    | | Scope : TC, SC, Abstract WF, Properties  | | catalog.                                                         |
    | | Since : 4.5                              |                                                                    |
    | | Type  : String                           |                                                                    |
    +--------------------------------------------+--------------------------------------------------------------------+
    | | Property Key: hints.grid.jobtype         | | applicable when submitting to remote sites via GRAM. The site    |
    | | Profile Key:grid.jobtype                 | | catalog allows you to associate multiple jobmanagers with a      |
    | | Scope : TC, SC, Abstract WF, Properties  | | GRAM site, for different type of jobs                            |
    | | Since : 4.5                              | | [compute, auxillary, transfer, register, cleanup ] that          |
    | | Type  : String                           | | Pegasus generates in the executable workflow. This profile is    |
    |                                            | | usually used to ensure that a compute job executes on another    |
    |                                            | | job manager. For example, if in site catalog you have            |
    |                                            | | headnode.example.com/jobmanager-condor for compute jobs,         |
    |                                            | | and headnode.example.com/jobmanager-fork for auxillary jobs.     |
    |                                            | | Associating this profile and setting value to auxillary for      |
    |                                            | | a compute job, will cause the compute job to run on the fork     |
    |                                            | | jobmanager instead of the condor jobmanager.                     |
    +--------------------------------------------+--------------------------------------------------------------------+

.. _props:

Properties
==========

Properties are primarily used to configure the behavior of the Pegasus
Workflow Planner at a global level. The properties file is actually a
java properties file and follows the same conventions as that to specify
the properties.

Please note that the values rely on proper capitalization, unless
explicitly noted otherwise.

Some properties rely with their default on the value of other
properties. As a notation, the curly braces refer to the value of the
named property. For instance, ${pegasus.home} means that the value
depends on the value of the pegasus.home property plus any noted
additions. You can use this notation to refer to other properties,
though the extent of the subsitutions are limited. Usually, you want to
refer to a set of the standard system properties. Nesting is not
allowed. Substitutions will only be done once.

There is a priority to the order of reading and evaluating properties.
Usually one does not need to worry about the priorities. However, it is
good to know the details of when which property applies, and how one
property is able to overwrite another. The following is a mutually
exclusive list ( highest priority first ) of property file locations.

1. --conf option to the tools. Almost all of the clients that use
   properties have a --conf option to specify the property file to pick
   up.
2. submit-dir/pegasus.xxxxxxx.properties file. All tools that work on
   the submit directory ( i.e after pegasus has planned a workflow) pick
   up the pegasus.xxxxx.properties file from the submit directory. The
   location for the pegasus.xxxxxxx.propertiesis picked up from the
   braindump file.
3. The properties defined in the user property file
   ${user.home}/.pegasusrc
   have lowest priority.

Starting Pegasus 5.0 release, pegasus properties can also be specified as
environment variables. The properties specified by an environment variable
have higher precedence than those specified in a properties file.

To specify a pegasus property as an environment variable you need to
do the following:

1. Convert your property name to upper case
2. Replace . with __ .
3. Add a leading _ to the property name.

For example, to specify pegasus.catalog.replica in your environment you
will specify

..

 _PEGASUS__CATALOG__REPLICA__FILE = /path/to/replicas.yml


Commandline properties have the highest priority. These override any
property loaded from a property file. Each commandline property is
introduced by a -D argument. Note that these arguments are parsed by the
shell wrapper, and thus the -D arguments must be the first arguments to
any command. Commandline properties are useful for debugging purposes.

From Pegasus 3.1 release onwards, support has been dropped for the
following properties that were used to signify the location of the
properties file

-  pegasus.properties
-  pegasus.user.properties

The basic properties that you may need to be set if using non default
   types and locations are for various catalogs are listed below:

   .. table:: Basic Properties that you may need to set

      ====================================== ===============================
      pegasus.catalog.replica                type of replica catalog backend
      pegasus.catalog.replica.file           path to replica catalog file
      pegasus.catalog.transformation         type of transformation catalog
      pegasus.catalog.transformation.file    path to transformation file
      pegasus.catalog.site.file              path to site catalog file
      pegasus.data.configuration             the data configuration mode for
                                             data staging.
      ====================================== ===============================

If you are in doubt which properties are actually visible, pegasus
during the planning of the workflow dumps all properties after reading
and prioritizing in the submit directory in a file with the suffix
properties.

.. _local-dir-props:

Local Directories Properties
----------------------------

This section describes the GNU directory structure conventions. GNU
distinguishes between architecture independent and thus sharable
directories, and directories with data specific to a platform, and thus
often local. It also distinguishes between frequently modified data and
rarely changing data. These two axis form a space of four distinct
directories.

.. table:: Local Directories Related Properties

    +---------------------------------------------+-------------------------------------------------------------------+
    | Key Attributes                              | Description                                                       |
    +=============================================+===================================================================+
    | | Property Key: pegasus.home.datadir        | | The datadir directory contains broadly visible and possibly     |
    | | Profile Key: N/A                          | | exported configuration files that rarely change. This           |
    | | Scope : Properties                        | | directory is currently unused.                                  |
    | | Since : 2.0                               |                                                                   |
    | | Type  : file path                         |                                                                   |
    | | Default : ${pegasus.home}/share           |                                                                   |
    +---------------------------------------------+-------------------------------------------------------------------+
    | | Property Key: pegasus.home.sysconfdir     | | The system configuration directory contains configuration       |
    | | Profile Key: N/A                          | | files that are specific to the machine or installation, and     |
    | | Scope : Properties                        | | that rarely change. This is the directory where the YAML/XML    |
    | | Since : 2.0                               | | schema definition copies are stored, and where the base         |
    | | Type  :file path                          | | site configuration file is stored.                              |
    | | Default : ${pegasus.home}/etc             |                                                                   |
    +---------------------------------------------+-------------------------------------------------------------------+
    | | Property Key: pegasus.home.sharedstatedir | | Frequently changing files that are broadly visible are          |
    | | Profile Key: N/A                          | | stored in the shared state directory. This is currently         |
    | | Scope : Properties                        | | unused.                                                         |
    | | Since : 2.0                               |                                                                   |
    | | Type :file path                           |                                                                   |
    | | Default : ${pegasus.home}/com             |                                                                   |
    +---------------------------------------------+-------------------------------------------------------------------+
    | | Property Key: pegasus.home.localstatedir  | | Frequently changing files that are specific to a machine        |
    | | Profile Key: N/A                          | | and/or installation are stored in the local state directory.    |
    | | Scope : Properties                        | | This is currently unused                                        |
    | | Since : 2.0                               |                                                                   |
    | | Type   :file path                         |                                                                   |
    | | Default : ${pegasus.home}/var             |                                                                   |
    +---------------------------------------------+-------------------------------------------------------------------+
    | | Property Key: pegasus.dir.submit.logs     | | This property can be used to specify the directory where the    |
    | | Profile Key: N/A                          | | condor logs for the workflow should go to. By default, starting |
    | | Scope : Properties                        | | 4.2.1 release, Pegasus will setup the log to be in the          |
    | | Since : 2.0                               | | workflow submit directory. This can create problems, in case    |
    | | Type :file path                           | | users submit directories are on NSF.                            |
    | | Default : (no default)                    | | This is done to ensure that the logs are created in a local     |
    |                                             | | directory even though the submit directory maybe on NFS.        |
    +---------------------------------------------+-------------------------------------------------------------------+

.. _site-dir-props:

Site Directories Properties
---------------------------

The site directory properties modify the behavior of remotely run jobs.
In rare occasions, it may also pertain to locally run compute jobs.

.. table:: Site Directories Related Properties

    +---------------------------------------------+-----------------------------------------------------------------------+
    | Key Attributes                              | Description                                                           |
    +=============================================+=======================================================================+
    | | Property Key: pegasus.dir.useTimestamp    | | While creating the submit directory, Pegasus employs a run          |
    | | Profile Key: N/A                          | | numbering scheme. Users can use this Boolean property to            |
    | | Scope : Properties                        | | use a timestamp based numbering scheme instead of the runxxxx       |
    | | Since : 2.1                               | | scheme.                                                             |
    | | Type  :Boolean                            |                                                                       |
    | | Default : false                           |                                                                       |
    +---------------------------------------------+-----------------------------------------------------------------------+
    | | Property Key: pegasus.dir.exec            | | This property modifies the remote location work directory in        |
    | | Profile Key: N/A                          | | which all your jobs will run. If the path is relative then          |
    | | Scope : Properties                        | | it is appended to the work directory (associated with the           |
    | | Since : 2.0                               | | site), as specified in the site catalog. If the path is             |
    | | Type  : file path                         | | absolute then it overrides the work directory specified in          |
    | | Default : (no default)                    | | the site catalog.                                                   |
    +---------------------------------------------+-----------------------------------------------------------------------+
    | | Property Key:pegasus.dir.submit.mapper    | | This property modifies determines how the directory for             |
    | | Profile Key:N/A                           | | job submit files are mapped on the submit host.                     |
    | | Scope : Properties                        | |                                                                     |
    | | Since : 4.7                               | | - **Flat**: This mapper results in Pegasus placing all the          |
    | | Type : Enumeration                        | |   job submit files in the submit directory as determined from       |
    | | Values : Flat|Hashed                      | |   the planner options. This can result in too many files in         |
    | | Default : Hashed                          | |   one directory for large workflows, and was the only option        |
    |                                             | |   before Pegasus 4.7.0 release.                                     |
    |                                             | | - **Named**: This mapper results in the creation of a deep          |
    |                                             | |   directory structure rooted at the submit directory. The           |
    |                                             | |   relative    directory for a compute job is determined based on a  |
    |                                             | |   pegasus profile named relative.submit.dir associated with the job |
    |                                             | |   If the profile is not associated then the relative directory is   |
    |                                             | |   derived from the logical transformation name associated with the  |
    |                                             | |   job. Auxillary files are placed in the base submit directory for  |
    |                                             | |   the worklfow.   To control behavior of this                       |
    |                                             | |   mapper, users can specify the following pegasus profiles          |
    |                                             | |                                                                     |
    |                                             | |   **relative.submit.dir** - relative submit dir to be used fo       |
    |                                             | |     the job.                                                        |
    |                                             | | - **Hashed**: This mapper results in the creation of a deep         |
    |                                             | |   directory structure rooted at the submit directory. The           |
    |                                             | |   base directory is the submit directory as determined from         |
    |                                             | |   the planner options. By default, the directory structure          |
    |                                             | |   created is two levels deep. To control behavior of this           |
    |                                             | |   mapper, users can specify the following properties                |
    |                                             | |                                                                     |
    |                                             | |   **pegasus.dir.submit.mapper.hashed.levels** - the number of       |
    |                                             | |     directory levels used to accomodate the files. Defaults to      |
    |                                             | |     2.                                                              |
    |                                             | |   **pegasus.dir.submit.mapper.hashed.multiplier** - the number      |
    |                                             | |     of files associated with a job in the submit directory.         |
    |                                             | |     Defaults to 5.                                                  |
    +---------------------------------------------+-----------------------------------------------------------------------+
    | | Property Key: pegasus.dir.staging.mapper  | | This property modifies determines how the job input and output      |
    | | Profile Key: N/A                          | | files are mapped on the staging site. This only applies when        |
    | | Scope : Properties                        | | the pegasus data configuration is set to nonsharedfs.               |
    | | Since : 4.7                               | |                                                                     |
    | | Type :Enumeration                         | | - **Flat**: This mapper results in Pegasus placing all the          |
    | | Values : Flat|Hashed                      | |   job submit files in the staging site directory as determined      |
    | | Default : Hashed                          | |   from the Site Catalog and planner options. This can result        |
    |                                             | |   in too many files in one directory for large workflows, and       |
    |                                             | |   was the only option before Pegasus 4.7.0 release.                 |
    |                                             | | - **Hashed**: This mapper results in the creation of a deep         |
    |                                             | |   directory structure rooted at the staging site directory          |
    |                                             | |   created by the create dir jobs. The binning is at the job         |
    |                                             | |   level,and not at the file level i.e each job will push out        |
    |                                             | |   it’s  outputs to the same directory on the staging site,          |
    |                                             | |   independent of the number of output files. To control             |
    |                                             | |   behavior of this mapper, users can specify the following          |
    |                                             | |   properties                                                        |
    |                                             | |                                                                     |
    |                                             | |   **pegasus.dir.staging.mapper.hashed.levels** - the number of      |
    |                                             | |   directory levels used to accomodate the files. Defaults to        |
    |                                             | |   2.                                                                |
    |                                             | |   **pegasus.dir.staging.mapper.hashed.multiplier**- the number      |
    |                                             | |   of files associated with a job in the submit directory.           |
    |                                             | |   Defaults to 5.                                                    |
    +---------------------------------------------+-----------------------------------------------------------------------+
    | | Property Key: pegasus.dir.storage.mapper  | | This property modifies determines how the output files are          |
    | | Profile Key : N/A                         | | mapped on the output site storage location. In order to             |
    | | Scope : Properties                        | | preserve backward compatibility, setting the boolean property       |
    | | Since : 4.3                               | | **pegasus.dir.storage.deep** results in the Hashed output mapper    |
    | | Type :Enumeration                         | | to be loaded, if no output mapper property is specified.            |
    | | Values : Flat|Fixed|Hashed|Replica        | |                                                                     |
    | | Default : Flat                            | | - **Flat**: By default, Pegasus will place the output files in      |
    |                                             | |   the storage directory specified in the site catalog for the       |
    |                                             | |   output site.                                                      |
    |                                             | | - **Fixed**: Using this mapper, users can specify an externally     |
    |                                             | |    accesible url to the storage directory in their properties       |
    |                                             | |    file. The following property needs to be set.                    |
    |                                             | |    **pegasus.dir.storage.mapper.fixed.url** - an externally         |
    |                                             | |    accessible URL to the storage directory on the output site       |
    |                                             | |    e.g. gsiftp://outputs.isi.edu/shared/outputs                     |
    |                                             | |    **Note:** For hierarchal workflows, the above property needs to  |
    |                                             | |    be set separately for each pegasusWorkflow/dax job, if you want  |
    |                                             | |    the sub workflow outputs to goto a different directory.          |
    |                                             | | - **Hashed**: This mapper results in the creation of a deep         |
    |                                             | |    directory structure on the output site, while populating         |
    |                                             | |    the results. The base directory on the remote end is             |
    |                                             | |    determined from the site catalog. Depending on the number        |
    |                                             | |    of files being staged to the remote site a Hashed File           |
    |                                             | |    Structure is created that ensures that only 256 files reside     |
    |                                             | |    in one directory. To create this directory structure on the      |
    |                                             | |    storage site, Pegasus relies on the directory creation           |
    |                                             | |    feature of the Grid FTP server, which appeared in globus 4.0.x   |
    |                                             | | - **Replica**: This mapper determines the path for an output        |
    |                                             | |    file on the output site by querying an output replica            |
    |                                             | |    catalog. The output site is one that is passed on the            |
    |                                             | |    command line. The output replica catalog can be configured       |
    |                                             | |    by specifiing the properties with the prefix                     |
    |                                             | |    **pegasus.dir.storage.replica**. By default, a Regex File        |
    |                                             | |    based backend is assumed unless overridden. For example          |
    |                                             | |    pegasus.dir.storage.mapper.replica       Regex|File              |
    |                                             | |    pegasus.dir.storage.mapper.replica.file  the RC file at the      |
    |                                             | |                           backend to use if using a file based RC   |
    +---------------------------------------------+-----------------------------------------------------------------------+
    | | Property Key: pegasus.dir.storage.deep    | | This Boolean property results in the creation of a deep             |
    | | Profile Key: N/A                          | | directory structure on the output site, while populating            |
    | | Scope : Properties                        | | the results. The base directory on the remote end is                |
    | | Since : 2.1                               | | determined from the site catalog.                                   |
    | | Type  : Boolean                           | | To this base directory, the relative submit directory               |
    | | Default : false                           | | structure ( $user/$vogroup/$label/runxxxx ) is appended.            |
    |                                             | | $storage = $base + $relative_submit_directory                       |
    |                                             | | This is the base directory that is passed to the storage mapper.    |
    |                                             | | **Note:** To preserve backward compatibilty, setting this property  |
    |                                             | | results in the Hashed mapper to be loaded unless                    |
    |                                             | | pegasus.dir.storage.mapper is explicitly specified. Before 4.3,     |
    |                                             | | this property resulted in HashedDirectory structure.                |
    +---------------------------------------------+-----------------------------------------------------------------------+
    | | Property Key: pegasus.dir.create.strategy | | If the **--randomdir** option is given to the Planner at            |
    | | Profile Key: N/A                          | | runtime, the Pegasus planner adds nodes that create the             |
    | | Scope : Properties                        | | random directories at the remote sites, before any jobs             |
    | | Since : 2.2                               | | are actually run. The two modes determine the placement             |
    | | Type :Enumeration                         | | of these nodes and their dependencies to the rest of the            |
    | | Values : HourGlass|Tentacles|Minimal      | | graph.                                                              |
    | | Default : Minimal                         | |                                                                     |
    |                                             | | - **HourGlass**: It adds a make directory node at the               |
    |                                             | |    top level of the graph, and all these concat to a single         |
    |                                             | |    dummy job before branching out to the root nodes of the          |
    |                                             | |    original/ concrete dag so far. So we introduce a classic         |
    |                                             | |    X shape at the top of the graph. Hence the name HourGlass.       |
    |                                             | | - **Tentacles**: This option places the jobs creating               |
    |                                             | |    directories at the top of the graph. However instead of          |
    |                                             | |    constricting it to an hour glass shape, this mode links          |
    |                                             | |    the top node to all the relevant nodes for which the create      |
    |                                             | |    dir job is necessary. It looks as if the node spreads its        |
    |                                             | |    tentacleas all around. This puts more load on the DAGMan         |
    |                                             | |    because of the added dependencies but removes the                |
    |                                             | |    restriction of the plan progressing only when all the            |
    |                                             | |    create directory jobs have progressed on the remote sites,       |
    |                                             | |    as is the case in the HourGlass model.                           |
    |                                             | | - **Minimal**: The strategy involves in walking the graph in        |
    |                                             | |   a BFS order, and updating a bit set associated with each          |
    |                                             | |   job based on the BitSet of the parent jobs. The BitSet            |
    |                                             | |   indicates whether an edge exists from the create dir job          |
    |                                             | |   to an ancestor of the node. For a node, the bit set is the        |
    |                                             | |   union of all the parents BitSets. The BFS traversal ensures       |
    |                                             | |   that the bitsets are of a node are only updated once the          |
    |                                             | |   parents have been processed.                                      |
    +---------------------------------------------+-----------------------------------------------------------------------+

.. _schema-props:

Schema File Location Properties
-------------------------------

This section defines the location of XML schema files that are used to
parse the various XML document instances in the PEGASUS. The schema
backups in the installed file-system permit PEGASUS operations without
being online.

.. table:: Schema File Location Properties

    +----------------------------------------------------+------------------------------------------------------------+
    | Key Attributes                                     | Description                                                |
    +====================================================+============================================================+
    | | Property Key: pegasus.schema.dax                 | | This file is a copy of the XML schema that describes     |
    | | Profile Key : N/A                                | | abstract DAG files that are the result of the abstract   |
    | | Scope : Properties                               | | planning process, and input into any concrete planning.  |
    | | Since : 2.0                                      | | Providing a copy of the schema enables the parser to use |
    | | Type :file path                                  | | the local copy instead of reaching out to the Internet,  |
    | | Default : ${pegasus.home.sysconfdir}/dax-3.4.xsd | | and obtaining the latest version from the Pegasus        |
    |                                                    | | website dynamically.                                     |
    +----------------------------------------------------+------------------------------------------------------------+
    | | Property Key:pegasus.schema.sc                   | | This file is a copy of the XML schema that describes     |
    | | Profile Key:N/A                                  | | the xml description of the site catalog. Providing a     |
    | | Scope : Properties                               | | copy of the schema enables the parser to use the local   |
    | | Since : 2.0                                      | | copy instead of reaching out to the internet, and        |
    | | Type :file path                                  | | obtaining the latest version from the GriPhyN website    |
    | | Default : ${pegasus.home.sysconfdir}/sc-4.0.xsd  | | dynamically.                                             |
    +----------------------------------------------------+------------------------------------------------------------+

.. _db-props:

Database Drivers For All Relational Catalogs
--------------------------------------------

.. table:: Database Driver Properties

    +-------------------------------------------------------+---------------------------------------------------------------------+
    | Property Key                                          | Description                                                         |
    +=======================================================+=====================================================================+
    | | Property Key: pegasus.catalog.*.db.driver           | | The database driver class is dynamically loaded, as               |
    | | Profile Key: N/A                                    | | required by the schema. Currently, only MySQL 5.x,                |
    | | Scope : Properties                                  | | PostGreSQL >= 8.1 and SQlite are supported. Their                 |
    | | Since : 2.0                                         | | respective JDBC3 driver is provided as part and parcel            |
    | | Type  : Enumeration                                 | | of the PEGASUS.                                                   |
    | | Values :MySQL|PostGres|SQLiteDefault : (no default) | | The * in the property name can be replaced by a                   |
    |                                                       | | catalog name to apply the property only for that                  |
    |                                                       | | catalog. Valid catalog names are replica.                         |
    +-------------------------------------------------------+---------------------------------------------------------------------+
    | | Property Key:pegasus.catalog.*.db.url               | | Each database has its own string to contact the                   |
    | | Profile Key:N/A                                     | | database on a given host, port, and database.                     |
    | | Scope : Properties                                  | | Although most driver URLs allow to pass arbitrary                 |
    | | Since : 2.0                                         | | arguments, please use the                                         |
    | | Type :Database URL                                  | | pegasus.catalog.[catalog-name].db.* keys or                       |
    | | Default : (no default)                              | | pegasus.catalog.*.db.* to preload these arguments.                |
    |                                                       | | THE URL IS A MANDATORY PROPERTY FOR ANY DBMS BACKEND.             |
    +-------------------------------------------------------+---------------------------------------------------------------------+
    | | Property Key:pegasus.catalog.*.db.user              | | In order to access a database, you must provide the               |
    | | Profile Key:N/A                                     | | name of your account on the DBMS. This property is                |
    | | Scope : Properties                                  | | database-independent. THIS IS A MANDATORY PROPERTY                |
    | | Since : 2.0                                         | | FOR MANY DBMS BACKENDS.                                           |
    | | Type :String                                        | | The * in the property name can be replaced by a                   |
    | | Default : (no default)                              | | catalog name to apply the property only for that                  |
    |                                                       | | catalog. Valid catalog names are                                  |
    |                                                       | | replica                                                           |
    +-------------------------------------------------------+---------------------------------------------------------------------+
    | | Property Key: pegasus.catalog.*.db.password         | | In order to access a database, you must provide an                |
    | | Profile Key: N/A                                    | | optional password of your account on the DBMS.                    |
    | | Scope : Properties                                  | | This property is database-independent. THIS IS A                  |
    | | Since : 2.0                                         | | MANDATORY PROPERTY, IF YOUR DBMS BACKEND ACCOUNT                  |
    | | Type :String                                        | | REQUIRES A PASSWORD.                                              |
    | | Default : (no default)                              | | The * in the property name can be replaced by a                   |
    |                                                       | | catalog name to apply the property only for that                  |
    |                                                       | | catalog. Valid catalog names are replica.                         |
    +-------------------------------------------------------+---------------------------------------------------------------------+
    | | Property Key: pegasus.catalog.*.db.*                | | Each database has a multitude of options to control               |
    | | Profile Key: N/A                                    | | in fine detail the further behaviour. You may want to             |
    | | Scope : Properties                                  | | check the JDBC3 documentation of the JDBC driver for              |
    | | Since : 2.0                                         | | your database for details. The keys will be passed as             |
    | | Type : String                                       | | part of the connect properties by stripping the                   |
    | | Default : (no default)                              | | “pegasus.catalog.[catalog-name].db.” prefix from them.            |
    |                                                       | | The catalog-name can be replaced by the following values          |
    |                                                       | | replica for Replica Catalog (RC)                                  |
    |                                                       | |                                                                   |
    |                                                       | | - Postgres >= 8.1 parses the following properties:                |
    |                                                       | |   pegasus.catalog.*.db.user                                       |
    |                                                       | |   pegasus.catalog.*.db.password                                   |
    |                                                       | |   pegasus.catalog.*.db.PGHOST                                     |
    |                                                       | |   pegasus.catalog.*.db.PGPORT                                     |
    |                                                       | |   pegasus.catalog.*.db.charSet                                    |
    |                                                       | |   pegasus.catalog.*.db.compatible                                 |
    |                                                       | | - MySQL 5.0 parses the following properties:                      |
    |                                                       | |   pegasus.catalog.*.db.user                                       |
    |                                                       | |   pegasus.catalog.*.db.password                                   |
    |                                                       | |   pegasus.catalog.*.db.databaseName                               |
    |                                                       | |   pegasus.catalog.*.db.serverName                                 |
    |                                                       | |   pegasus.catalog.*.db.portNumber                                 |
    |                                                       | |   pegasus.catalog.*.db.socketFactory                              |
    |                                                       | |   pegasus.catalog.*.db.strictUpdates                              |
    |                                                       | |   pegasus.catalog.*.db.ignoreNonTxTables                          |
    |                                                       | |   pegasus.catalog.*.db.secondsBeforeRetryMaster                   |
    |                                                       | |   pegasus.catalog.*.db.queriesBeforeRetryMaster                   |
    |                                                       | |   pegasus.catalog.*.db.allowLoadLocalInfile                       |
    |                                                       | |   pegasus.catalog.*.db.continueBatchOnError                       |
    |                                                       | |   pegasus.catalog.*.db.pedantic                                   |
    |                                                       | |   pegasus.catalog.*.db.useStreamLengthsInPrepStmts                |
    |                                                       | |   pegasus.catalog.*.db.useTimezone                                |
    |                                                       | |   pegasus.catalog.*.db.relaxAutoCommit                            |
    |                                                       | |   pegasus.catalog.*.db.paranoid                                   |
    |                                                       | |   pegasus.catalog.*.db.autoReconnect                              |
    |                                                       | |   pegasus.catalog.*.db.capitalizeTypeNames                        |
    |                                                       | |   pegasus.catalog.*.db.ultraDevHack                               |
    |                                                       | |   pegasus.catalog.*.db.strictFloatingPoint                        |
    |                                                       | |   pegasus.catalog.*.db.useSSL                                     |
    |                                                       | |   pegasus.catalog.*.db.useCompression                             |
    |                                                       | |   pegasus.catalog.*.db.socketTimeout                              |
    |                                                       | |   pegasus.catalog.*.db.maxReconnects                              |
    |                                                       | |   pegasus.catalog.*.db.initialTimeout                             |
    |                                                       | |   pegasus.catalog.*.db.maxRows                                    |
    |                                                       | |   pegasus.catalog.*.db.useHostsInPrivileges                       |
    |                                                       | |   pegasus.catalog.*.db.interactiveClient                          |
    |                                                       | |   pegasus.catalog.*.db.useUnicode                                 |
    |                                                       | |   pegasus.catalog.*.db.characterEncoding                          |
    |                                                       | |                                                                   |
    |                                                       | | The * in the property name can be replaced by a                   |
    |                                                       | | catalog name to apply the property only for that                  |
    |                                                       | | catalog. Valid catalog names are replica.                         |
    +-------------------------------------------------------+---------------------------------------------------------------------+
    | | Property Key: pegasus.catalog.*.timeout             | | This property sets a busy handler that sleeps for                 |
    | | Profile Key: N/A                                    | | a specified amount of time (in seconds) when a                    |
    | | Scope : Properties                                  | | table is locked. This property has effect only                    |
    | | Since : 4.5.1                                       | | in a sqlite database.                                             |
    | | Type :Integer                                       | | The * in the property name can be replaced by a                   |
    | | Default : (no default)                              | | catalog name to apply the property only for that                  |
    |                                                       | | catalog. Valid catalog names are                                  |
    |                                                       | | - master                                                          |
    |                                                       | | - workflow                                                        |
    +-------------------------------------------------------+---------------------------------------------------------------------+

.. _catalog-props:

Catalog Related Properties
--------------------------

.. table:: Replica Catalog Properties

    +----------------------------------------------------+----------------------------------------------------------------------------------+
    | Key Attributes                                     | Description                                                                      |
    +====================================================+==================================================================================+
    | | Property Key: pegasus.catalog.replica            | | Pegasus queries a Replica Catalog to discover the                              |
    | | Profile Key: N/A                                 | | physical filenames (PFN) for input files specified in                          |
    | | Scope : Properties                               | | the Abstract Workflow. Pegasus can interface with                              |
    | | Since : 2.0                                      | | various types of Replica Catalogs. This property                               |
    | | Default : File                                   | | specifies which type of Replica Catalog to use during                          |
    |                                                    | | the planning process.                                                          |
    |                                                    | |                                                                                |
    |                                                    | | **JDBCRC**                                                                     |
    |                                                    | |   In this mode, Pegasus queries a SQL                                          |
    |                                                    | |   based replica catalog that is accessed via JDBC.                             |
    |                                                    | |   To use JDBCRC, the user additionally needs to set                            |
    |                                                    | |   the following properties                                                     |
    |                                                    | |   pegasus.catalog.replica.db.driver = mysql \| postgres\|sqlite                |
    |                                                    | |   pegasus.catalog.replica.db.url = <jdbc url to the database>                  |
    |                                                    | |     e.g jdbc:mysql://database-host.isi.edu/database-name \|                    |
    |                                                    | |     jdbc:sqlite:/shared/jdbcrc.db                                              |
    |                                                    | |   pegasus.catalog.replica.db.user = database-user                              |
    |                                                    | |   pegasus.catalog.replica.db.password = database-password                      |
    |                                                    | |                                                                                |
    |                                                    | | **File**                                                                       |
    |                                                    | |   In this mode, Pegasus queries a file based                                   |
    |                                                    | |   replica catalog. It is neither transactionally safe,                         |
    |                                                    | |   nor advised to use for production purposes in any way.                       |
    |                                                    | |   Multiple concurrent instances will clobber each other!.                      |
    |                                                    | |   The site attribute should be specified whenever possible.                    |
    |                                                    | |   The attribute key for the site attribute is “site”.                          |
    |                                                    | |   The LFN may or may not be quoted. If it contains                             |
    |                                                    | |   linear whitespace, quotes, backslash or an equality                          |
    |                                                    | |   sign, it must be quoted and escaped. Ditto for the                           |
    |                                                    | |   PFN. The attribute key-value pairs are separated by                          |
    |                                                    | |   an equality sign without any whitespaces. The value                          |
    |                                                    | |   may be in quoted. The LFN sentiments about quoting                           |
    |                                                    | |   apply.                                                                       |
    |                                                    |                                                                                  |
    |                                                    |  ::                                                                              |
    |                                                    |                                                                                  |
    |                                                    |       LFN PFN                                                                    |
    |                                                    |       LFN PFN a=b [..]                                                           |
    |                                                    |       LFN PFN a="b" [..]                                                         |
    |                                                    |       "LFN w/LWS" "PFN w/LWS" [..]                                               |
    |                                                    |                                                                                  |
    |                                                    | |   To use File, the user additionally needs to specify                          |
    |                                                    | |   **pegasus.catalog.replica.file** property to                                 |
    |                                                    | |   specify the path to the file based RC. IF not                                |
    |                                                    | |   specified , defaults to $PWD/rc.txt file.                                    |
    |                                                    | |                                                                                |
    |                                                    | | **YAML**                                                                       |
    |                                                    | |   This is the new YAML based file format                                       |
    |                                                    | |   introduced in Pegasus 5.0. The format does support                           |
    |                                                    | |   regular expressions similar to Regex catalog type.                           |
    |                                                    | |   To specify regular expressions you need to associate                         |
    |                                                    | |   an attribute named regex and set to true.                                    |
    |                                                    | |   To use YAML, the user additionally needs to specify                          |
    |                                                    | |   **pegasus.catalog.replica.file** property to                                 |
    |                                                    | |   specify the path to the file based RC. IF not                                |
    |                                                    | |   specified , defaults to $PWD/replicas.yml file.                              |
    |                                                    | |                                                                                |
    |                                                    | | **Regex**                                                                      |
    |                                                    | |   In this mode, Pegasus queries a file                                         |
    |                                                    | |   based replica catalog. It is neither transactionally                         |
    |                                                    | |   safe, nor advised to use for production purposes in any                      |
    |                                                    | |   way. Multiple concurrent access to the File will end                         |
    |                                                    | |   up clobbering the contents of the file. The site                             |
    |                                                    | |   attribute should be specified whenever possible.                             |
    |                                                    | |   The attribute key for the site attribute is “site”.                          |
    |                                                    | |   The LFN may or may not be quoted. If it contains                             |
    |                                                    | |   linear whitespace, quotes, backslash or an equality                          |
    |                                                    | |   sign, it must be quoted and escaped. Ditto for the                           |
    |                                                    | |   PFN. The attribute key-value pairs are separated by                          |
    |                                                    | |   an equality sign without any whitespaces. The value                          |
    |                                                    | |   may be in quoted. The LFN sentiments about quoting                           |
    |                                                    | |   apply.                                                                       |
    |                                                    | |   In addition users can specifiy regular expression                            |
    |                                                    | |   based LFN’s. A regular expression based entry should                         |
    |                                                    | |   be qualified with an attribute named ‘regex’. The                            |
    |                                                    | |   attribute regex when set to true identifies the                              |
    |                                                    | |   catalog entry as a regular expression based entry.                           |
    |                                                    | |   Regular expressions should follow Java regular                               |
    |                                                    | |   expression syntax.                                                           |
    |                                                    | |   For example, consider a replica catalog as shown below.                      |
    |                                                    | |   Entry 1 refers to an entry which does not use a regular                      |
    |                                                    | |   expressions. This entry would only match a file named                        |
    |                                                    | |   ‘f.a’, and nothing else. Entry 2 referes to an entry                         |
    |                                                    | |   which uses a regular expression. In this entry f.a                           |
    |                                                    | |   refers to files having name as f[any-character]a                             |
    |                                                    | |   i.e. faa, f.a, f0a, etc.                                                     |
    |                                                    |                                                                                  |
    |                                                    |  ::                                                                              |
    |                                                    |                                                                                  |
    |                                                    |       f.a file:///Vol/input/f.a site="local"                                     |
    |                                                    |       f.a file:///Vol/input/f.a site="local" regex="true"                        |
    |                                                    |                                                                                  |
    |                                                    | |   Regular expression based entries also support                                |
    |                                                    | |   substitutions. For example, consider the regular                             |
    |                                                    | |   expression based entry shown below.                                          |
    |                                                    | |                                                                                |
    |                                                    | |   Entry 3 will match files with name alpha.csv,                                |
    |                                                    | |   alpha.txt, alpha.xml. In addition, values matched                            |
    |                                                    | |   in the expression can be used to generate a PFN.                             |
    |                                                    | |   For the entry below if the file being looked up is                           |
    |                                                    | |   alpha.csv, the PFN for the file would be generated as                        |
    |                                                    | |   file:///Volumes/data/input/csv/alpha.csv. Similary if                        |
    |                                                    | |   the file being lookedup was alpha.csv, the PFN for the                       |
    |                                                    | |   file would be generated as                                                   |
    |                                                    | |   file:///Volumes/data/input/xml/alpha.xml i.e.                                |
    |                                                    | |   The section [0], [1] will be replaced.                                       |
    |                                                    | |   Section [0] refers to the entire string                                      |
    |                                                    | |   i.e. alpha.csv. Section [1] refers to a partial                              |
    |                                                    | |   match in the input i.e. csv, or txt, or xml.                                 |
    |                                                    | |   Users can utilize as many sections as they wish.                             |
    |                                                    |                                                                                  |
    |                                                    |    ::                                                                            |
    |                                                    |                                                                                  |
    |                                                    |        alpha\.(csv|txt|xml) file:///Vol/input/[1]/[0] site="local" regex="true"  |
    |                                                    |                                                                                  |
    |                                                    | |   To use File, the user additionally needs to specify                          |
    |                                                    | |   pegasus.catalog.replica.file property to specify the                         |
    |                                                    | |   path to the file based RC.                                                   |
    |                                                    | |                                                                                |
    |                                                    | | **Directory**                                                                  |
    |                                                    | |   In this mode, Pegasus does a directory                                       |
    |                                                    | |   listing on an input directory to create the LFN to PFN                       |
    |                                                    | |   mappings. The directory listing is performed                                 |
    |                                                    | |   recursively, resulting in deep LFN mappings.                                 |
    |                                                    | |   For example, if an input directory $input is specified                       |
    |                                                    | |   with the following structure                                                 |
    |                                                    |                                                                                  |
    |                                                    |    ::                                                                            |
    |                                                    |                                                                                  |
    |                                                    |        $input                                                                    |
    |                                                    |        $input/f.1                                                                |
    |                                                    |        $input/f.2                                                                |
    |                                                    |        $input/D1                                                                 |
    |                                                    |        $input/D1/f.3                                                             |
    |                                                    |                                                                                  |
    |                                                    | |   Pegasus will create the mappings the following                               |
    |                                                    | |   LFN PFN mappings internally                                                  |
    |                                                    |                                                                                  |
    |                                                    |     ::                                                                           |
    |                                                    |                                                                                  |
    |                                                    |        f.1 file://$input/f.1  site="local"                                       |
    |                                                    |        f.2 file://$input/f.2  site="local"                                       |
    |                                                    |        D1/f.3 file://$input/D2/f.3 site="local"                                  |
    |                                                    |                                                                                  |
    |                                                    | |   If you don’t want the deep lfn’s to be created then,                         |
    |                                                    | |   you can set pegasus.catalog.replica.directory.flat.lfn                       |
    |                                                    | |   to true In that case, for the previous example, Pegasus                      |
    |                                                    | |   will create the following LFN PFN mappings internally.                       |
    |                                                    |                                                                                  |
    |                                                    |    ::                                                                            |
    |                                                    |                                                                                  |
    |                                                    |        f.1 file://$input/f.1  site="local"                                       |
    |                                                    |        f.2 file://$input/f.2  site="local"                                       |
    |                                                    |        D1/f.3 file://$input/D2/f.3 site="local"                                  |
    |                                                    |                                                                                  |
    |                                                    | |   pegasus-plan has –input-dir option that can be used                          |
    |                                                    | |   to specify an input directory.                                               |
    |                                                    | |   Users can optionally specify additional properties to                        |
    |                                                    | |   configure the behvavior of this implementation.                              |
    |                                                    | |   - **pegasus.catalog.replica.directory** to specify                           |
    |                                                    | |      the path to the directory containing the files                            |
    |                                                    | |   - **pegasus.catalog.replica.directory.site** to                              |
    |                                                    | |      specify a site attribute other than local to                              |
    |                                                    | |      associate with the mappings.                                              |
    |                                                    | |   - **pegasus.catalog.replica.directory.url.prefix**                           |
    |                                                    | |      to associate a URL prefix for the PFN’s constructed.                      |
    |                                                    | |      If not specified, the URL defaults to file://                             |
    |                                                    | |                                                                                |
    |                                                    | | **MRC**                                                                        |
    |                                                    | |   In this mode, Pegasus queries multiple                                       |
    |                                                    | |   replica catalogs to discover the file locations on the                       |
    |                                                    | |   grid. To use it set                                                          |
    |                                                    | |   pegasus.catalog.replica MRC                                                  |
    |                                                    | |   Each associated replica catalog can be configured via                        |
    |                                                    | |   properties as follows.                                                       |
    |                                                    | |   The user associates a variable name referred to as                           |
    |                                                    | |   [value] for each of the catalogs, where [value]                              |
    |                                                    | |   is any legal identifier                                                      |
    |                                                    | |   (concretely [A-Za-z][_A-Za-z0-9]*) . For each                                |
    |                                                    | |   associated replica catalogs the user specifies                               |
    |                                                    | |   the following properties.                                                    |
    |                                                    |                                                                                  |
    |                                                    |      ::                                                                          |
    |                                                    |                                                                                  |
    |                                                    |        pegasus.catalog.replica.mrc.[value] specifies the                         |
    |                                                    |                  type of replica catalog.                                        |
    |                                                    |        pegasus.catalog.replica.mrc.[value].key specifies                         |
    |                                                    |          a property name key for a particular catalog                            |
    |                                                    |                                                                                  |
    |                                                    |      ::                                                                          |
    |                                                    |                                                                                  |
    |                                                    |        pegasus.catalog.replica.mrc.directory1 Directory                          |
    |                                                    |        pegasus.catalog.replica.mrc.directory1.directory /input/dir1              |
    |                                                    |        pegasus.catalog.replica.mrc.directory1.directory.site  siteX              |
    |                                                    |        pegasus.catalog.replica.mrc.directory2 Directory                          |
    |                                                    |        pegasus.catalog.replica.mrc.directory2.directory /input/dir2              |
    |                                                    |        pegasus.catalog.replica.mrc.directory1.directory.site  siteY|             |
    |                                                    |                                                                                  |
    |                                                    | |   In the above example, directory1, directory2 are any                         |
    |                                                    | |   valid identifier names and url is the property key that                      |
    |                                                    | |   needed to be specified.                                                      |
    +----------------------------------------------------+----------------------------------------------------------------------------------+
    | | Property Key: pegasus.catalog.replica.file.      | | The path to a file based replica catalog backend                               |
    | | Profile Key: N/A                                 | |                                                                                |
    | | Scope : Properties                               | |                                                                                |
    | | Since : 2.0                                      | |                                                                                |
    | | Default : 1000                                   | |                                                                                |
    +----------------------------------------------------+----------------------------------------------------------------------------------+
    | | Property Key: pegasus.catalog.replica.chunk.size | | The pegasus-rc-client takes in an input file containing the                    |
    | | Profile Key: N/A                                 | | mappings upon which to work. This property determines, the                     |
    | | Scope : Properties                               | | number of lines that are read in at a time, and worked upon                    |
    | | Since : 2.0                                      | | at together. This allows the various operations like insert,                   |
    | | Default : 1000                                   | | delete happen in bulk if the underlying replica                                |
    |                                                    | | implementation supports it.                                                    |
    +----------------------------------------------------+----------------------------------------------------------------------------------+
    | | Property Key: pegasus.catalog.replica.cache.asrc | | This Boolean property determines whether to treat the                          |
    | | Profile Key : N/A                                | | cachefile specified as a supplemental replica catalog                          |
    | | Scope : Properties                               | | or not. User can specify on the command line to                                |
    | | Since : 2.0                                      | | pegasus-plan a comma separated list of cache files                             |
    | | Default : false                                  | | using the –cache option. By default, the LFN->PFN                              |
    |                                                    | | mappings contained in the cache file are treated                               |
    |                                                    | | as cache, i.e if an entry is found in a cache file                             |
    |                                                    | | the replica catalog is not queried. This results                               |
    |                                                    | | in only the entry specified in the cache file to be                            |
    |                                                    | | available for replica selection.                                               |
    |                                                    | | Setting this property to true, results in the cache                            |
    |                                                    | | files to be treated as supplemental replica catalogs.                          |
    |                                                    | | This results in the mappings found in the replica                              |
    |                                                    | | catalog (as specified by pegasus.catalog.replica)                              |
    |                                                    | | to be merged with the ones found in the cache files.                           |
    |                                                    | | Thus, mappings for a particular LFN found in both the                          |
    |                                                    | | cache and the replica catalog are available for                                |
    |                                                    | | replica selection.                                                             |
    +----------------------------------------------------+----------------------------------------------------------------------------------+
    | | Property Key: pegasus.catalog.replica.dax.asrc   | | This Boolean property determines whether to treat the                          |
    | | Profile Key :N/A                                 | | locations of files recorded in the DAX as a supplemental                       |
    | | Scope : Properties                               | | replica catalog or not. By default, the LFN->PFN                               |
    | | Since : 4.5.2                                    | | mappings contained in the DAX file overrides any                               |
    | | Default : false                                  | | specified in a replica catalog. This results in only                           |
    |                                                    | | the entry specified in the DAX file to be available                            |
    |                                                    | | for replica selection.                                                         |
    |                                                    | | Setting this property to true, results in the                                  |
    |                                                    | | locations of files recorded in the DAX files to                                |
    |                                                    | | be treated as a supplemental replica catalog.                                  |
    |                                                    | | This results in the mappings found in the replica                              |
    |                                                    | | catalog (as specified by pegasus.catalog.replica) to                           |
    |                                                    | | be merged with the ones found in the cache files.                              |
    |                                                    | | Thus, mappings for a particular LFN found in both                              |
    |                                                    | | the Abstract Workflow/DAX and the replica catalog                              |
    |                                                    | | are available for replica selection.                                           |
    +----------------------------------------------------+----------------------------------------------------------------------------------+
    | | Property Key: pegasus.catalog.replica.output.*   | | Normally, the registration jobs in the executable                              |
    | | Profile Key : N/A                                | | workflow register to the replica catalog specified                             |
    | | Scope : Properties                               | | by the user in the properties file . This property                             |
    | | Since : 4.5.3                                    | | prefix allows the user to specify a separate output                            |
    | | Default : None                                   | | replica catalog that is different from the one used                            |
    |                                                    | | for discovery of input files. This is normally the                             |
    |                                                    | | case, when a Directory or MRC based replica catalog                            |
    |                                                    | | backend that don’t support insertion of entries                                |
    |                                                    | | are used for discovery of input files.                                         |
    |                                                    | | For example to specify a separate file based output                            |
    |                                                    | | replica catalog, specify                                                       |
    |                                                    |                                                                                  |
    |                                                    |   ::                                                                             |
    |                                                    |                                                                                  |
    |                                                    |      pegasus.catalog.replica.output        File                                  |
    |                                                    |      pegasus.catalog.replica.output.file   /workflow/output.rc                   |
    +----------------------------------------------------+----------------------------------------------------------------------------------+

.. table:: Site Catalog Properties

    +---------------------------------------------+------------------------------------------------------------------+
    | Key Attributes                              | Description                                                      |
    +=============================================+==================================================================+
    | | Property Key: pegasus.catalog.site        | | Pegasus supports two different types of site catalogs in       |
    | | Profile Key: N/A                          | | :ref:`YAML <sc-YAML>` or :ref:`XML <sc-XML4>` formats          |
    | | Scope : Properties                        |                                                                  |
    | | Type : Enumeration                        |                                                                  |
    | | Values : YAML|XML                         | | Pegasus is able to auto-detect what schema a user site         |
    | | Since : 2.0                               | | catalog refers to. Hence, this property may no longer be set.  |
    | | Default : YAML                            |                                                                  |
    +---------------------------------------------+------------------------------------------------------------------+
    | | Property Key: pegasus.catalog.site.file   | | The path to the site catalog file, that describes the various  |
    | | Profile Key : N/A                         | | sites and their layouts to Pegasus.                            |
    | | Scope : Properties                        |                                                                  |
    | | Since : 2.0                               |                                                                  |
    | | Default : $PWD/sites.yml | $PWD/sites.xml |                                                                  |
    +---------------------------------------------+------------------------------------------------------------------+

.. table:: Transformation Catalog Properties

    +-----------------------------------------------------+------------------------------------------------------------+
    | Key Attributes                                      | Description                                                |
    +=====================================================+============================================================+
    | | Property Key: pegasus.catalog.transformation      | | Pegasus supports two different types of site catalogs in |
    | | Profile Key: N/A                                  | | :ref:`YAML <tc-YAML>` or :ref:`Text <tc-Text>` formats   |
    | | Scope : Properties                                | | Pegasus is able to auto-detect what schema a user site   |
    | | Since : 2.0                                       | | catalog refers to. Hence, this property may no longer be |
    | | Type : Enumeration                                | | set.                                                     |
    | | Values : YAML|Directory|Text                      | | For **Directory** type, refer to the catalogs chapter    |
    | | Default : YAML                                    | | in the ref:`reference guide <tc-directory>`.             |
    +-----------------------------------------------------+------------------------------------------------------------+
    | | Property Key: pegasus.catalog.transformation.file | | The path to the transformation catalog file, that        |
    | | Profile Key : N/A                                 | | describes the locations of the executables.              |
    | | Scope : Properties                                |                                                            |
    | | Since : 2.0                                       |                                                            |
    | | Default : $PWD/transformations.yml|$PWD/tc.txt    |                                                            |
    +-----------------------------------------------------+------------------------------------------------------------+

.. _replica-sel-props:

Replica Selection Properties
----------------------------

.. table:: Replica Selection Properties

    +-----------------------------------------------------+-----------------------------------------------------------------------+
    | Key Attributes                                      | Description                                                           |
    +=====================================================+=======================================================================+
    | | Property Key: pegasus.selector.replica            | | Each job in the DAX maybe associated with input LFN’s               |
    | | Profile Key: N/A                                  | | denoting the files that are required for the job to                 |
    | | Scope : Properties                                | | run. To determine the physical replica (PFN) for a LFN,             |
    | | Since : 2.0                                       | | Pegasus queries the replica catalog to get all the                  |
    | | Type :String                                      | | PFN’s (replicas) associated with a LFN. Pegasus then                |
    | | Default : Default                                 | | calls out to a replica selector to select a replica                 |
    | | See Also :                                        | | amongst the various replicas returned. This property                |
    | |  pegasus.selector.replica.*.ignore.stagein.sites  | | determines the replica selector to use for selecting                |
    | | See Also :                                        | | the replicas.                                                       |
    | |  pegasus.selector.replica.*.prefer.stagein.sites  |                                                                       |
    |                                                     |   **Default**                                                         |
    |                                                     |                                                                       |
    |                                                     | |  The selector orders the various candidate replica’s                |
    |                                                     | |  according to the following rules                                   |
    |                                                     |                                                                       |
    |                                                     | |  1. valid file URL’s . That is URL’s that have the site             |
    |                                                     | |  attribute matching the site where the executable                   |
    |                                                     | | pegasus-transfer is executed.                                       |
    |                                                     | |  2. all URL’s from preferred site (usually the compute site)        |
    |                                                     | |  3. all other remotely accessible ( non file) URL’s                 |
    |                                                     |                                                                       |
    |                                                     |   **Regex**                                                           |
    |                                                     |                                                                       |
    |                                                     | | This replica selector allows the user allows the user to            |
    |                                                     | | specific regular expressions that can be used to rank               |
    |                                                     | | various PFN’s returned from the Replica Catalog for a               |
    |                                                     | | particular LFN. This replica selector orders the                    |
    |                                                     | | replicas based on the rank. Lower the rank higher the               |
    |                                                     | | preference.                                                         |
    |                                                     | | The regular expressions are assigned different rank,                |
    |                                                     | | that determine the order in which the expressions are               |
    |                                                     | | employed. The rank values for the regex can expressed               |
    |                                                     | | in user properties using the property.                              |
    |                                                     |                                                                       |
    |                                                     | ::                                                                    |
    |                                                     |                                                                       |
    |                                                     |     pegasus.selector.replica.regex.rank.[value]   regex-expression    |
    |                                                     |                                                                       |
    |                                                     | | The value is an integer value that denotes the rank of              |
    |                                                     | | an expression with a rank value of 1 being the highest              |
    |                                                     | | rank.                                                               |
    |                                                     | | Please note that before applying any regular expressions            |
    |                                                     | | on the PFN’s, the file URL’s that dont match the preferred          |
    |                                                     | | site are explicitly filtered out.                                   |
    |                                                     |                                                                       |
    |                                                     |   **Restricted**                                                      |
    |                                                     |                                                                       |
    |                                                     | | This replica selector, allows the user to specify good sites        |
    |                                                     | | and bad sites for staging in data to a particular compute site.     |
    |                                                     | | A good site for a compute site X, is a preferred site from          |
    |                                                     | | which replicas should be staged to site X. If there are more        |
    |                                                     | | than one good sites having a particular replica, then a             |
    |                                                     | | random site is selected amongst these preferred sites.              |
    |                                                     | | A bad site for a compute site X, is a site from which               |
    |                                                     | | replica’s should not be staged. The reason of not accessing         |
    |                                                     | | replica from a bad site can vary from the link being down,          |
    |                                                     | | to the user not having permissions on that site’s data.             |
    |                                                     | | The good | bad sites are specified by the properties                |
    |                                                     |                                                                       |
    |                                                     | ::                                                                    |
    |                                                     |                                                                       |
    |                                                     |    pegasus.replica.*.prefer.stagein.sites                             |
    |                                                     |    pegasus.replica.*.ignore.stagein.sites                             |
    |                                                     |                                                                       |
    |                                                     | | where the * in the property name denotes the name of the            |
    |                                                     | | compute site. A * in the property key is taken to mean all sites.   |
    |                                                     | |                                                                     |
    |                                                     | | The **pegasus.replica.*.prefer.stagein.sites** property takes       |
    |                                                     | | precedence over **pegasus.replica.*.ignore.stagein.sites** property |
    |                                                     | | i.e. if for a site X, a site Y is specified both in the ignored     |
    |                                                     | | and the preferred set, then site Y is taken to mean as only a       |
    |                                                     | | preferred site for a site X.                                        |
    |                                                     |                                                                       |
    |                                                     |   **Local**                                                           |
    |                                                     |                                                                       |
    |                                                     | | This replica selector prefers replicas from the local host          |
    |                                                     | | and that start with a file: URL scheme. It is useful, when          |
    |                                                     | | users want to stagin files to a remote site from your submit        |
    |                                                     | | host using the Condor file transfer mechanism.                      |
    +-----------------------------------------------------+-----------------------------------------------------------------------+
    | | Property Key:                                     | | A comma separated list of storage sites from which to never         |
    | |   pegasus.selector.replica.*.ignore.stagein.sites | | stage in data to a compute site. The property can apply to          |
    | | Profile Key: N/A                                  | | all or a single compute site, depending on how the * in the         |
    | | Scope : Properties                                | | property name is expanded.                                          |
    | | Since : 2.0                                       | | The * in the property name means all compute sites unless           |
    | | Default : (no default)                            | | replaced by a site name.                                            |
    | | See Also : pegasus.selector.replica               | | For e.g setting                                                     |
    | | See Also :                                        | | pegasus.selector.replica.*.ignore.stagein.sites to usc              |
    | |   pegasus.selector.replica.*.prefer.stagein.sites | | means that ignore all replicas from site usc for staging in         |
    |                                                     | | to any compute site.                                                |
    |                                                     | | Setting pegasus.replica.isi.ignore.stagein.sites to usc             |
    |                                                     | | means that ignore all replicas from site usc for staging            |
    |                                                     | | in data to site isi.                                                |
    +-----------------------------------------------------+-----------------------------------------------------------------------+
    | | Property Key:                                     | | A comma separated list of preferred storage sites from which        |
    | |   pegasus.selector.replica.*.prefer.stagein.sites | | to stage in data to a compute site. The property can apply to       |
    | | Profile Key: N/A                                  | | all or a single compute site, depending on how the * in the         |
    | | Scope : Properties                                | | property name is expanded.                                          |
    | | Since : 2.0                                       | | The * in the property name means all compute sites unless           |
    | | Default : (no default)                            | | replaced by a site name.                                            |
    | | See Also : pegasus.selector.replica               | | For e.g setting                                                     |
    | | See Also :                                        | | pegasus.selector.replica.*.prefer.stagein.sites to usc              |
    | |   pegasus.selector.replica.*.ignore.stagein.sites | | means that prefer all replicas from site usc for staging            |
    |                                                     | | in to any compute site.                                             |
    |                                                     | | Setting pegasus.replica.isi.prefer.stagein.sites to usc             |
    |                                                     | | means that prefer all replicas from site usc for staging            |
    |                                                     | | in data to site isi.                                                |
    +-----------------------------------------------------+-----------------------------------------------------------------------+
    | | Property Key:                                     | | Specifies the regex expressions to be applied on the                |
    | |  pegasus.selector.replica.regex.rank.[value]      | | PFNs returned for a particular LFN. Refer to                        |
    | | Profile Key:N/A                                   |                                                                       |
    | | Scope : Properties                                | ..                                                                    |
    | | Since : 2.3.0                                     |   http://java.sun.com/javase/6/docs/api/java/util/regex/Pattern.html  |
    | | Default : (no default)                            |                                                                       |
    | | See Also : pegasus.selector.replica               | | on information of how to construct a regex expression.              |
    |                                                     | | The [value] in the property key is to be replaced by an             |
    |                                                     | | int value that designates the rank value for the regex              |
    |                                                     | | expression to be applied in the Regex replica selector.             |
    |                                                     | | The example below indicates preference for file URL’s               |
    |                                                     | | over URL’s referring to gridftp server at example.isi.edu           |
    |                                                     | |                                                                     |
    |                                                     |                                                                       |
    |                                                     | ..                                                                    |
    |                                                     |    pegasus.selector.replica.regex.rank.1 file://.*                    |
    |                                                     |    pegasus.selector.replica.regex.rank.2 gsiftp://example\.isi\.edu.* |
    |                                                     |                                                                       |
    |                                                     |                                                                       |
    +-----------------------------------------------------+-----------------------------------------------------------------------+

.. _site-sel-props:

Site Selection Properties
-------------------------

.. table:: Site Selection Properties

    +------------------------------------------------+------------------------------------------------------------------------+
    | Key Attributes                                 | Description                                                            |
    +================================================+========================================================================+
    | | Property Key: pegasus.selector.site          | | The site selection in Pegasus can be on basis of any of the          |
    | | Profile Key: N/A                             | | following strategies.                                                |
    | | Scope : Properties                           |                                                                        |
    | | Since : 2.0                                  |   **Random**                                                           |
    |                                                |                                                                        |
    | | Type  : String                               | | In this mode, the jobs will be randomly distributed among            |
    | | Default : Random                             | | the sites that can execute them.                                     |
    | | See Also :                                   |                                                                        |
    | |   pegasus.selector.site.path                 |   **RoundRobin**                                                       |
    |                                                |                                                                        |
    | | See Also :                                   | | In this mode. the jobs will be assigned in a round robin             |
    | |   pegasus.selector.site.timeout              | | manner amongst the sites that can execute them. Since                |
    | | See Also :                                   | | each site cannot execute everytype of job, the round                 |
    | |    pegasus.selector.site.keep.tmp            | | robin scheduling is done per level on a sorted list.                 |
    | | See Also :                                   | | The sorting is on the basis of the number of jobs a                  |
    | |    pegasus.selector.site.env.*               | | particular site has been assigned in that level so far.              |
    |                                                | | If a job cannot be run on the first site in the queue                |
    |                                                | | (due to no matching entry in the transformation catalog              |
    |                                                | | for the transformation referred to by the job), it goes              |
    |                                                | | to the next one and so on. This implementation defaults              |
    |                                                | | to classic round robin in the case where all the jobs                |
    |                                                | | in the workflow can run on all the sites.                            |
    |                                                |                                                                        |
    |                                                |   **NonJavaCallout**                                                   |
    |                                                |                                                                        |
    |                                                | | In this mode, Pegasus will callout to an external site               |
    |                                                | | selector.In this mode a temporary file is prepared                   |
    |                                                | | containing the job information that is passed to the                 |
    |                                                | | site selector as an argument while invoking it. The                  |
    |                                                | | path to the site selector is specified by setting                    |
    |                                                | | the property pegasus.site.selector.path.                             |
    |                                                | | The environment variables that need to be set to run                 |
    |                                                | | the site selector can be specified using the properties              |
    |                                                | | with a **pegasus.site.selector.env.** prefix. The temporary          |
    |                                                | | file contains information about the job that needs to be             |
    |                                                | | scheduled. It contains key value pairs with each key value           |
    |                                                | | pair being on a new line and separated by a =.                       |
    |                                                | | The following pairs are currently generated for the site             |
    |                                                | | selector temporary file that is generated in the NonJavaCallout.     |
    |                                                |                                                                        |
    |                                                | +----------------+---------------------------------------------------+ |
    |                                                | | Key            | Description                                       | |
    |                                                | +================+===================================================+ |
    |                                                | | version        | | is the version of the site selector api,        | |
    |                                                | |                | | currently 2.0.                                  | |
    |                                                | +----------------+---------------------------------------------------+ |
    |                                                | | transformation | | is the fully-qualified definition identifier    | |
    |                                                | |                | | for the transformation (TR)                     | |
    |                                                | |                | | namespace::name:version.                        | |
    |                                                | +----------------+---------------------------------------------------+ |
    |                                                | | derivation     | | is the fully qualified definition identifier    | |
    |                                                | |                | | for the derivation (DV),                        | |
    |                                                | |                | | namespace::name:version.                        | |
    |                                                | +----------------+---------------------------------------------------+ |
    |                                                | | job.level      | | is the job’s depth in the tree of the           | |
    |                                                | |                | | workflow DAG.                                   | |
    |                                                | +----------------+---------------------------------------------------+ |
    |                                                | | job.id         | | is the job’s ID, as used in the DAX file.       | |
    |                                                | +----------------+---------------------------------------------------+ |
    |                                                | | resource.id    | | is a site handle, followed by whitespace,       | |
    |                                                | |                | | followed by a gridftp server. Typically,        | |
    |                                                | |                | | each gridftp server is enumerated once,         | |
    |                                                | |                | | so you may have multiple occurances of          | |
    |                                                | |                | | the same site. There can be multiple            | |
    |                                                | |                | | occurances of this key.                         | |
    |                                                | +----------------+---------------------------------------------------+ |
    |                                                | | input.lfn      | | is an input LFN, optionally followed            | |
    |                                                | |                | | by a whitespace and file size. There            | |
    |                                                | |                | | can be multiple occurances of this              | |
    |                                                | |                | | key,one for each input LFN required by the job. | |
    |                                                | +----------------+---------------------------------------------------+ |
    |                                                | | wf.name        | | label of the dax, as found in the DAX’s         | |
    |                                                | |                | | root element. wf.index is the DAX index,        | |
    |                                                | |                | | that is incremented for each partition          | |
    |                                                | |                | | in case of deferred planning.                   | |
    |                                                | +----------------+---------------------------------------------------+ |
    |                                                | | wf.time        | | is the mtime of the workflow.                   | |
    |                                                | +----------------+---------------------------------------------------+ |
    |                                                | | wf.manager     | | is the name of the workflow manager             | |
    |                                                | |                | | being used .e.g condor                          | |
    |                                                | +----------------+---------------------------------------------------+ |
    |                                                | | vo.name        | | is the name of the virtual organization         | |
    |                                                | |                | | that is running this workflow.                  | |
    |                                                | |                | | It is currently set to NONE                     | |
    |                                                | +----------------+---------------------------------------------------+ |
    |                                                | | vo.group       | unused at present and is set to NONE.             | |
    |                                                | +----------------+---------------------------------------------------+ |
    |                                                |                                                                        |
    |                                                |   **Group**                                                            |
    |                                                |                                                                        |
    |                                                | | In this mode, a group of jobs will be assigned to the same site      |
    |                                                | | that can execute them. The use of the PEGASUS profile key            |
    |                                                | | group in the abstract workflow, associates a job with a              |
    |                                                | | particular group. The jobs that do not have the profile              |
    |                                                | | key associated with them, will be put in the default group.          |
    |                                                | | The jobs in the default group are handed over to the “Random”        |
    |                                                | | Site Selector for scheduling.                                        |
    |                                                |                                                                        |
    |                                                |   **Heft**                                                             |
    |                                                |                                                                        |
    |                                                | | In this mode, a version of the HEFT processor scheduling             |
    |                                                | | algorithm is used to schedule jobs in the workflow to multiple       |
    |                                                | | grid sites. The implementation assumes default data communication    |
    |                                                | | costs when jobs are not scheduled on to the same site. Later on      |
    |                                                | | this may be made more configurable.                                  |
    |                                                | | The runtime for the jobs is specified in the transformation          |
    |                                                | | catalog by associating the pegasus profile key runtime with the      |
    |                                                | | entries.                                                             |
    |                                                | | The number of processors in a site is picked up from the             |
    |                                                | | attribute idle-nodes associated with the vanilla jobmanager          |
    |                                                | | of the site in the site catalog.                                     |
    +------------------------------------------------+------------------------------------------------------------------------+
    | | Property Key: pegasus.selector.site.path     | | If one calls out to an external site selector using the              |
    | | Profile Key: N/A                             | | NonJavaCallout mode, this refers to the path where the               |
    | | Scope : Properties                           | | site selector is installed. In case other strategies are             |
    | | Since : 2.0                                  | | used it does not need to be set.                                     |
    | | Default : (no default)                       |                                                                        |
    +------------------------------------------------+------------------------------------------------------------------------+
    | | Property Key: pegasus.selector.site.env.*    | | The environment variables that need to be set while callout          |
    | | Profile Key: N/A                             | | to the site selector. These are the variables that the user          |
    | | Scope : Properties                           | | would set if running the site selector on the command line.          |
    | | Since : 2.0                                  | | The name of the environment variable is got by stripping the         |
    | | Default : (no default)                       | | keys of the prefix “pegasus.site.selector.env.” prefix from          |
    |                                                | | them. The value of the environment variable is the value of          |
    |                                                | | the property.                                                        |
    |                                                | | e.g                                                                  |
    |                                                |                                                                        |
    |                                                | ::                                                                     |
    |                                                |                                                                        |
    |                                                |     pegasus.site.selector.path.LD_LIBRARY_PATH /globus/lib             |
    |                                                |                                                                        |
    |                                                | | would lead to the site selector being called with the                |
    |                                                | | LD_LIBRARY_PATH set to /globus/lib.                                  |
    +------------------------------------------------+------------------------------------------------------------------------+
    | | Property Key: pegasus.selector.site.timeout  | | It sets the number of seconds Pegasus waits to hear back             |
    | | Profile Key:N/A                              | | from an external site selector using the NonJavaCallout              |
    | | Scope : Properties                           | | interface before timing out.                                         |
    | | Since : 2.3.0                                |                                                                        |
    | | Default : 60                                 |                                                                        |
    | | See Also : pegasus.selector.site             |                                                                        |
    +------------------------------------------------+------------------------------------------------------------------------+
    | | Property Key: pegasus.selector.site.keep.tmp | | It determines whether Pegasus deletes the temporary input            |
    | | Profile Key:N/A                              | | files that are generated in the temp directory or not.               |
    | | Scope : Properties                           | | These temporary input files are passed as input to the               |
    | | Since : 2.3.0                                | | external site selectors.                                             |
    | | Values : onerror|always|never                |                                                                        |
    | | Default : onerror                            |                                                                        |
    | | See Also : pegasus.selector.site             |                                                                        |
    +------------------------------------------------+------------------------------------------------------------------------+

.. _data-conf-props:

Data Staging Configuration Properties
-------------------------------------

.. table:: Data Configuration Properties

    +-------------------------------------------------------+--------------------------------------------------------+
    | Key Attributes                                        | Description                                            |
    +=======================================================+========================================================+
    | | Property Key: pegasus.data.configuration            | | This property sets up Pegasus to run in different    |
    | | Profile Key:data.configuration                      | | environments. For Pegasus 4.5.0 and above, users     |
    | | Scope : Properties, Site Catalog                    | | can set the pegasus profile data.configuration with  |
    | | Since : 4.0.0                                       | | the sites in their site catalog, to run multisite    |
    | | Values : sharedfs|nonsharedfs|condorio              | | workflows with each site having a different data     |
    | | Default : condorio                                  | | configuration.                                       |
    | | See Also : pegasus.transfer.bypass.input.staging    |                                                        |
    |                                                       |   **sharedfs**                                         |
    |                                                       |                                                        |
    |                                                       | | If this is set, Pegasus will be setup to execute     |
    |                                                       | | jobs on the shared filesystem on the execution site. |
    |                                                       | | This assumes, that the head node of a cluster and    |
    |                                                       | | the worker nodes share a filesystem. The staging     |
    |                                                       | | site in this case is the same as the execution site. |
    |                                                       | | Pegasus adds a create dir job to the executable      |
    |                                                       | |  workflow that creates a workflow specific           |
    |                                                       | | directory on the shared filesystem . The data        |
    |                                                       | | transfer jobs in the executable workflow             |
    |                                                       | | ( stage\_in_ , stage\_inter\_ , stage\_out\_ )       |
    |                                                       | | transfer the data to this directory.The compute      |
    |                                                       | |  jobs in the executable workflow are launched in     |
    |                                                       | | the directory on the shared filesystem.              |
    |                                                       |                                                        |
    |                                                       |   **condorio**                                         |
    |                                                       |                                                        |
    |                                                       | | If this is set, Pegasus will be setup to run jobs    |
    |                                                       | | in a pure condor pool, with the nodes not sharing    |
    |                                                       | | a filesystem. Data is staged to the compute nodes    |
    |                                                       | | from the submit host using Condor File IO. The       |
    |                                                       | | planner is automatically setup to use the submit     |
    |                                                       | | host ( site local ) as the staging site. All the     |
    |                                                       | | auxillary jobs added by the planner to the           |
    |                                                       | | executable workflow ( create dir, data stagein       |
    |                                                       | | and stage-out, cleanup ) jobs refer to the workflow  |
    |                                                       | | specific directory on the local site. The data       |
    |                                                       | | transfer jobs in the executable workflow             |
    |                                                       | | ( stage\_in\_ , stage\_inter\_ , stage\_out\_ )      |
    |                                                       | | transfer the data to this directory. When the        |
    |                                                       | | compute jobs start, the input data for each job is   |
    |                                                       | | shipped from the workflow specific directory on      |
    |                                                       | | the submit host to compute/worker node using         |
    |                                                       | | Condor file IO. The output data for each job is      |
    |                                                       | | similarly shipped back to the submit host from the   |
    |                                                       | |  compute/worker node. This setup is particularly     |
    |                                                       | | helpful when running workflows in the cloud          |
    |                                                       | | environment where setting up a shared filesystem     |
    |                                                       | | across the VM’s may be tricky.                       |
    |                                                       |                                                        |
    |                                                       | ::                                                     |
    |                                                       |                                                        |
    |                                                       |    pegasus.gridstart                    PegasusLite    |
    |                                                       |    pegasus.transfer.worker.package      true           |
    |                                                       |                                                        |
    |                                                       |                                                        |
    |                                                       |   **nonsharedfs**                                      |
    |                                                       |                                                        |
    |                                                       | | If this is set, Pegasus will be setup to execute     |
    |                                                       | | jobs on an execution site without relying on a       |
    |                                                       | | shared filesystem between the head node and the      |
    |                                                       | | worker nodes. You can specify staging site           |
    |                                                       | | ( using –staging-site option to pegasus-plan)        |
    |                                                       | | to indicate the site to use as a central             |
    |                                                       | | storage location for a workflow. The staging         |
    |                                                       | | site is independant of the execution sites on        |
    |                                                       | |  which a workflow executes. All the auxillary        |
    |                                                       | | jobs added by the planner to the executable          |
    |                                                       | | workflow ( create dir, data stagein and              |
    |                                                       | | stage-out, cleanup ) jobs refer to the workflow      |
    |                                                       | | specific directory on the staging site. The          |
    |                                                       | | data transfer jobs in the executable workflow        |
    |                                                       | | ( stage\_in\_ , stage\_inter\_ , stage\_out\_        |
    |                                                       | | transfer the data to this directory. When the        |
    |                                                       | | compute jobs start, the input data for each          |
    |                                                       | | job is shipped from the workflow specific            |
    |                                                       | | directory on the submit host to compute/worker       |
    |                                                       | | node using pegasus-transfer. The output data         |
    |                                                       | | for each job is similarly shipped back to the        |
    |                                                       | | submit host from the compute/worker node. The        |
    |                                                       | | protocols supported are at this time SRM,            |
    |                                                       | | GridFTP, iRods, S3. This setup is particularly       |
    |                                                       | | helpful when running workflows on OSG where          |
    |                                                       | | most of the execution sites don’t have enough        |
    |                                                       | | data storage. Only a few sites have large            |
    |                                                       | | amounts of data storage exposed that can be used     |
    |                                                       | | to place data during a workflow run. This setup      |
    |                                                       | | is also helpful when running workflows in the        |
    |                                                       | | cloud environment where setting up a                 |
    |                                                       | | shared filesystem across the VM’s may be tricky.     |
    |                                                       | | On loading this property, internally the             |
    |                                                       | | following properies are set                          |
    |                                                       |                                                        |
    |                                                       |                                                        |
    |                                                       | ::                                                     |
    |                                                       |                                                        |
    |                                                       |    pegasus.gridstart  PegasusLite                      |
    |                                                       |    pegasus.transfer.worker.package      true           |
    |                                                       |                                                        |
    +-------------------------------------------------------+--------------------------------------------------------+
    | | Property Key: pegasus.transfer.bypass.input.staging | | When executing in a non shared filesystem setup      |
    | | Profile Key:N/A                                     | | i.e data configuration set to nonsharedfs or         |
    | | Scope : Properties                                  | | condorio, Pegasus always stages the input files      |
    | | Since : 4.3.0                                       | | through the staging site i.e the stage-in job        |
    | | Type :Boolean                                       | | stages in data from the input site to the staging    |
    | | Default : false                                     | | site. The PegasusLite jobs that start up on the      |
    | | See Also : pegasus.data.configuration               | | worker nodes, then pull the input data from the      |
    |                                                       | | staging site for each job.                           |
    |                                                       | | This property can be used to setup the               |
    |                                                       | | PegasusLite jobs to pull input data directly         |
    |                                                       | | from the input site without going through the        |
    |                                                       | | staging server. This is based on the assumption      |
    |                                                       | | that the worker nodes can access the input site.     |
    |                                                       | | If users set this to true, they should be aware      |
    |                                                       | | that the access to the input site is no longer       |
    |                                                       | | throttled ( as in case of stage in jobs). If         |
    |                                                       | | large number of compute jobs start at the same       |
    |                                                       | | time in a workflow, the input server will see        |
    |                                                       | | a connection from each job.                          |
    +-------------------------------------------------------+--------------------------------------------------------+
    | | Property Key: pegasus.transfer.container.onhost     | | When a job is specified to run in an application     |
    | | Profile Key:N/A                                     | | container such as docker or singularity, Pegasus     |
    | | Scope : Properties                                  | | has two options in PegasusLite on how data transfers |
    | | Since : 5.1.0                                       | | for the job occur. The transfers can happen either   |
    | | Type :Boolean                                       | | on the HOST OS before the container in which the job |
    | | Default : true                                      | | has to execute OR                                    |
    | | See Also : pegasus.data.configuration               | | inside the application container, before the user    |
    |                                                       | | code is invoked.                                     |
    |                                                       | | This property can be used to control this behavior.  |
    |                                                       | | Prior to Pegasus 5.1.0 release, the default value    |
    |                                                       | | for this was false i.e. the data transfers happened  |
    |                                                       | | inside the application container.                    |
    +-------------------------------------------------------+--------------------------------------------------------+

.. _transfer-props:

Transfer Configuration Properties
---------------------------------

.. table:: Transfer Configuration Properties

    +--------------------------------------------------+------------------------------------------------------------------------------+
    | Key Attributes                                   | Description                                                                  |
    +==================================================+==============================================================================+
    | | Property Key: pegasus.transfer.*.impl          | | Each compute job usually has data products that are                        |
    | | Profile Key: N/A                               | | required to be staged in to the execution site,                            |
    | | Scope : Properties                             | | materialized data products staged out to a final resting                   |
    | | Since : 2.0.0                                  | | place, or staged to another job running at a different                     |
    | | Values : Transfer|GUC                          | | site. This property determines the underlying grid                         |
    | | Default : Transfer                             | | transfer tool that is used to manage the transfers.                        |
    | | See Also : pegasus.transfer.refiner            | |                                                                            |
    |                                                  | | The * in the property name can be replaced to achieve                      |
    |                                                  | | finer grained control to dictate what type of transfer                     |
    |                                                  | | jobs need to be managed with which grid transfer tool.                     |
    |                                                  | | Usually,the arguments with which the client is invoked                     |
    |                                                  | | can be specified by                                                        |
    |                                                  |                                                                              |
    |                                                  | - the property pegasus.transfer.arguments                                    |
    |                                                  |                                                                              |
    |                                                  | - associating the PEGASUS profile key transfer.arguments                     |
    |                                                  |                                                                              |
    |                                                  | | The table below illustrates all the possible variations                    |
    |                                                  | | of the property.                                                           |
    |                                                  |                                                                              |
    |                                                  | +--------------------------------+---------------------------------+         |
    |                                                  | | Property Name                  | Applies to                      |         |
    |                                                  | +================================+=================================+         |
    |                                                  | | pegasus.transfer.stagein.impl  | the stage in transfer jobs      |         |
    |                                                  | +--------------------------------+---------------------------------+         |
    |                                                  | | pegasus.transfer.stageout.impl | the stage out transfer jobs     |         |
    |                                                  | +--------------------------------+---------------------------------+         |
    |                                                  | | pegasus.transfer.inter.impl    | the inter site transfer jobs    |         |
    |                                                  | +--------------------------------+---------------------------------+         |
    |                                                  | | pegasus.transfer.setup.impl    | the setup transfer job          |         |
    |                                                  | +--------------------------------+---------------------------------+         |
    |                                                  | | pegasus.transfer.*.impl        | apply to types of transfer jobs |         |
    |                                                  | +--------------------------------+---------------------------------+         |
    |                                                  |                                                                              |
    |                                                  | | **Note:** Since version 2.2.0 the worker package is staged                 |
    |                                                  | | automatically during staging of executables to the remote                  |
    |                                                  | | site. This is achieved by adding a setup transfer job to                   |
    |                                                  | | the workflow. The setup transfer job by default uses                       |
    |                                                  | | *pegasus-transfer* to stage the data. The implementation                   |
    |                                                  | | to use can be configured by setting the property                           |
    |                                                  |                                                                              |
    |                                                  | ::                                                                           |
    |                                                  |                                                                              |
    |                                                  |    pegasus.transfer.setup.impl                                               |
    |                                                  |                                                                              |
    |                                                  | | The various grid transfer tools that can be used to                        |
    |                                                  | | manage data transfers are explained below                                  |
    |                                                  | |                                                                            |
    |                                                  |                                                                              |
    |                                                  | | **Transfer**: This results in pegasus-transfer to be used                  |
    |                                                  | | for transferring of files. It is a python based wrapper                    |
    |                                                  | | around various transfer clients like globus-url-copy,                      |
    |                                                  | | lcg-copy, wget, cp, ln . pegasus-transfer looks at source                  |
    |                                                  | | and destination url and figures out automatically which                    |
    |                                                  | | underlying client to use. pegasus-transfer is distributed                  |
    |                                                  | | with the PEGASUS and can be found at                                       |
    |                                                  | | $PEGASUS_HOME/bin/pegasus-transfer.                                        |
    |                                                  | | For remote sites, Pegasus constructs the default path to                   |
    |                                                  | | pegasus-transfer on the basis of PEGASUS_HOME env profile                  |
    |                                                  | | specified in the site catalog. To specify a different                      |
    |                                                  | | path to the pegasus-transfer client , users can add an                     |
    |                                                  | | entry into the transformation catalog with fully qualified                 |
    |                                                  | | logical name as pegasus::pegasus-transfer                                  |
    |                                                  |                                                                              |
    |                                                  | | **GUC**: This refers to the new guc client that does                       |
    |                                                  | | multiple file transfers per invocation. The                                |
    |                                                  | | globus-url-copy client distributed with Globus 4.x                         |
    |                                                  | | is compatible with this mode.                                              |
    +--------------------------------------------------+------------------------------------------------------------------------------+
    | | Property Key: pegasus.transfer.arguments       | | This determines the extra arguments with which the transfer                |
    | | Profile Key:transfer.arguments                 | | implementation is invoked. The transfer executable that                    |
    | | Scope : Properties                             | | is invoked is dependant upon the transfer mode that has                    |
    | | Since : 2.0.0                                  | | been selected. The property can be overloaded by                           |
    | | Type :String                                   | | associated the pegasus profile key transfer.arguments                      |
    | | Default : (no default)                         | | either with the site in the site catalog or the                            |
    | | See Also : pegasus.transfer.lite.arguments     | | corresponding transfer executable in the transformation                    |
    |                                                  | | catalog.                                                                   |
    +--------------------------------------------------+------------------------------------------------------------------------------+
    | | Property Key: pegasus.transfer.threads         | | This property set the number of threads pegasus-transfer                   |
    | | Profile Key: transfer.threads                  | | uses to transfer the files. This property to applies to                    |
    | | Scope : Properties                             | | the separate data transfer nodes that are added by Pegasus                 |
    | | Since : 4.4.0                                  | | to the executable workflow. The property can be overloaded                 |
    | | Type :Integer                                  | | by associated the pegasus profile key transfer.threads                     |
    | | Default : 2                                    | | either with the site in the site catalog or the                            |
    |                                                  | | corresponding transfer executable in the transformation                    |
    |                                                  | | catalog.                                                                   |
    +--------------------------------------------------+------------------------------------------------------------------------------+
    | | Property Key: pegasus.transfer.lite.arguments  | | This determines the extra arguments with which the                         |
    | | Profile Key: transfer.lite.arguments           | | PegasusLite transfer implementation is invoked. The                        |
    | | Scope : Properties                             | | transfer executable that is invoked is dependant upon the                  |
    | | Since : 4.4.0                                  | | PegasusLite transfer implementation that has been                          |
    | | Type :String                                   | | selected.                                                                  |
    | | Default : (no default)                         |                                                                              |
    | | See Also : pegasus.transfer.arguments          |                                                                              |
    +--------------------------------------------------+------------------------------------------------------------------------------+
    | | Property Key: pegasus.transfer.worker.package  | | By default, Pegasus relies on the worker package to be                     |
    | | Profile Key: N/A                               | | installed in a directory accessible to the worker nodes                    |
    | | Scope : Properties                             | | on the remote sites . Pegasus uses the value of                            |
    | | Since : 2.0.0                                  | | PEGASUS_HOME environment profile in the site catalog                       |
    | | Type :Boolean                                  | | for the remote sites, to then construct paths to pegasus                   |
    | | Default : false                                | | auxillary executables like kickstart, pegasus-transfer,                    |
    | | See Also : pegasus.data.configuration          | | seqexec etc.                                                               |
    |                                                  | | If the Pegasus worker package is not installed on the                      |
    |                                                  | | remote sites users can set this property to true to                        |
    |                                                  | | get Pegasus to deploy worker package on the nodes.                         |
    |                                                  | | In the case of sharedfs setup, the worker package is                       |
    |                                                  | | deployed on the shared scratch directory for the workflow,                 |
    |                                                  | | that is accessible to all the compute nodes of the                         |
    |                                                  | | remote sites.                                                              |
    |                                                  | | When running in nonsharefs environments, the worker                        |
    |                                                  | | package is first brought to the submit directory and then                  |
    |                                                  | | transferred to the worker node filesystem using Condor                     |
    |                                                  | | file IO.                                                                   |
    +--------------------------------------------------+------------------------------------------------------------------------------+
    | | Property Key:                                  | | If PegasusLite does not find a worker package install                      |
    | |   pegasus.transfer.worker.package.autodownload | | matching the pegasus lite job on the worker node, it                       |
    | | Profile Key:N/A                                | | automatically downloads the correct worker package                         |
    | | Scope : Properties                             | | from the Pegasus website. However, this can mask user                      |
    | | Since : 4.6.1                                  | | errors in configuration. This property can be set to                       |
    | | Type :Boolean                                  | | false to disable auto downloads.                                           |
    | | Default : true                                 |                                                                              |
    | | See Also : pegasus.transfer.worker.package     |                                                                              |
    +--------------------------------------------------+------------------------------------------------------------------------------+
    | | Property Key:                                  | | In PegasusLite mode, the pegasus worker package for                        |
    | |  pegasus.transfer.worker.package.strict        | | the jobs is shipped along with the jobs. This property                     |
    | | Profile Key: N/A                               | | controls whether PegasusLite will do a strict match                        |
    | | Scope : Properties                             | | against the architecture and os on the local worker                        |
    | | Since : 4.6.1                                  | | node, along with pegasus version. If the strict match                      |
    | | Type :Boolean                                  | | fails, then PegasusLite will revert to the pegasus                         |
    | | Default : true                                 | | website to download the correct worker package.                            |
    | | See Also : pegasus.transfer.worker.package     |                                                                              |
    +--------------------------------------------------+------------------------------------------------------------------------------+
    | | Property Key:pegasus.transfer.links            | | If this is set, and the transfer implementation is                         |
    | | Profile Key: N/A                               | | set to Transfer i.e. using the transfer executable                         |
    | | Scope : Properties                             | | distributed with the PEGASUS. On setting this property,                    |
    | | Since : 2.0.0                                  | | if Pegasus while fetching data from the Replica Catalog                    |
    | | Type :Boolean                                  | | sees a “site” attribute associated with the PFN that                       |
    | | Default : false                                | | matches the execution site on which the data has to                        |
    |                                                  | | be transferred to, Pegasus instead of the URL                              |
    |                                                  | | returned by the Replica Catalog replaces it with a                         |
    |                                                  | | file based URL. This is based on the assumption that                       |
    |                                                  | | the if the “site” attributes match, the filesystems                        |
    |                                                  | | are visible to the remote execution directory where                        |
    |                                                  | | input data resides. On seeing both the source and                          |
    |                                                  | | destination urls as file based URLs the transfer                           |
    |                                                  | | executable spawns a job that creates a symbolic                            |
    |                                                  | | link by calling ln -s on the remote site.                                  |
    +--------------------------------------------------+------------------------------------------------------------------------------+
    | | Property Key:                                  | | By default Pegasus looks at the source and destination                     |
    | |      pegasus.transfer.*.remote.sites           | | URL’s for to determine whether the associated transfer                     |
    | | Profile Key:N/A                                | | job runs on the submit host or the head node of a                          |
    | | Scope : Properties                             | | remote site, with preference set to run a transfer job                     |
    | | Since : 2.0.0                                  | | to run on submit host.                                                     |
    | | Type :comma separated list of sites            | | Pegasus will run transfer jobs on the remote sites                         |
    | | Default : (no default)                         |                                                                              |
    |                                                  | -  if the file server for the compute site is a file                         |
    |                                                  |    server i.e url prefix file://                                             |
    |                                                  | -  symlink jobs need to be added that require the                            |
    |                                                  |    symlink transfer jobs to be run remotely.                                 |
    |                                                  |                                                                              |
    |                                                  | | This property can be used to change the default behaviour                  |
    |                                                  | | of Pegasus and force pegasus to run different types of                     |
    |                                                  | | transfer jobs for the sites specified on the remote site.                  |
    |                                                  | | The table below illustrates all the possible variations                    |
    |                                                  | | of the property.                                                           |
    |                                                  |                                                                              |
    |                                                  | +----------------------------------------+---------------------------------+ |
    |                                                  | | Property Name                          | Applies to                      | |
    |                                                  | +========================================+=================================+ |
    |                                                  | | pegasus.transfer.stagein.remote.sites  | the stage in transfer jobs      | |
    |                                                  | +----------------------------------------+---------------------------------+ |
    |                                                  | | pegasus.transfer.stageout.remote.sites | the stage out transfer jobs     | |
    |                                                  | +----------------------------------------+---------------------------------+ |
    |                                                  | | pegasus.transfer.inter.remote.sites    | the inter site transfer jobs    | |
    |                                                  | +----------------------------------------+---------------------------------+ |
    |                                                  | | pegasus.transfer.*.remote.sites        | apply to types of transfer jobs | |
    |                                                  | +----------------------------------------+---------------------------------+ |
    |                                                  |                                                                              |
    |                                                  | | In addition * can be specified as a property value, to                     |
    |                                                  | | designate that it applies to all sites.                                    |
    |                                                  |                                                                              |
    |                                                  |                                                                              |
    +--------------------------------------------------+------------------------------------------------------------------------------+
    | | Property Key:                                  | | Pegasus supports executable staging as part of the                         |
    | |      pegasus.transfer.staging.delimiter        | | workflow. Currently staging of statically linked                           |
    | | Profile Key: N/A                               | | executables is supported only. An executable is                            |
    | | Scope : Properties                             | | normally staged to the work directory for the                              |
    | | Since : 2.0.0                                  | | workflow/partition on the remote site. The basename                        |
    | |  Type :String                                  | | of the staged executable is derived from the                               |
    | | Default : :                                    | | namespace,name and version of the transformation                           |
    |                                                  | | in the transformation catalog. This property sets                          |
    |                                                  | | the delimiter that is used for the construction                            |
    |                                                  | | of the name of the staged executable.                                      |
    +--------------------------------------------------+------------------------------------------------------------------------------+
    | | Property Key:                                  | | During staging of executables to remote sites, chmod                       |
    | |     pegasus.transfer.disable.chmod.sites       | | jobs are added to the workflow. These jobs run on                          |
    | | Profile Key: N/A                               | | the remote sites and do a chmod on the staged                              |
    | | Scope : Properties                             | | executable. For some sites, this maynot be required.                       |
    | | Since : 2.0.0                                  | | The permissions might be preserved, or there maybe                         |
    | | Type :comma separated list of sites            | | an automatic mechanism that does it.                                       |
    | | Default : (no default)                         | | This property allows you to specify the list of                            |
    |                                                  | | sites, where you do not want the chmod jobs to                             |
    |                                                  | | be executed. For those sites, the chmod jobs are                           |
    |                                                  | | replaced by NoOP jobs. The NoOP jobs are executed                          |
    |                                                  | | by Condor, and instead will immediately have a                             |
    |                                                  | | terminate event written to the job log file and                            |
    |                                                  | | removed from the queue.                                                    |
    +--------------------------------------------------+------------------------------------------------------------------------------+
    | | Property Key:                                  | | This property specifies the base URL to the                                |
    | |    pegasus.transfer.setup.source.base.url      | | directory containing the Pegasus worker package                            |
    | | Profile Key: N/A                               | | builds. During Staging of Executable, the Pegasus                          |
    | | Scope : Properties                             | | Worker Package is also staged to the remote site.                          |
    | | Since : 2.0.0                                  | | The worker packages are by default pulled from                             |
    | | Type :URL                                      | | the http server at pegasus.isi.edu. This property                          |
    | | Default : (no default)                         | | can be used to override the location from where                            |
    |                                                  | | the worker package are staged. This maybe                                  |
    |                                                  | | required if the remote computes sites don’t allow                          |
    |                                                  | | files transfers from a http server.                                        |
    +--------------------------------------------------+------------------------------------------------------------------------------+

.. _monitoring-props:

Monitoring Properties
---------------------

.. table:: Monitoring Properties

    +---------------------------------------------------------+--------------------------------------------------------------------------+
    | Key Attributes                                          | Description                                                              |
    +=========================================================+==========================================================================+
    | | Property Key: pegasus.monitord.events                 | | This property determines whether pegasus-monitord                      |
    | | Profile Key: N/A                                      | | generates log events. If log events are disabled using                 |
    | | Scope : Properties                                    | | this property, no bp file, or database will be created,                |
    | | Since : 3.0.2                                         | | even if the pegasus.monitord.output property is                        |
    | | Type : String                                         | | specified.                                                             |
    | | Default : true                                        |                                                                          |
    | | See Also :pegasus.catalog.workflow.url                |                                                                          |
    +---------------------------------------------------------+--------------------------------------------------------------------------+
    | | Property Key: pegasus.catalog.workflow.url            | | This property specifies the destination for generated                  |
    | | Profile Key: N/A                                      | | log events in pegasus-monitord. By default, events are                 |
    | | Scope : Properties                                    | | stored in a sqlite database in the workflow directory,                 |
    | | Since : 4.5                                           | | which will be created with the workflow’s name, and a                  |
    | | Type : String                                         | | “.stampede.db” extension. Users can specify an                         |
    | | Default : SQlite database in submit directory.        | | alternative database by using a SQLAlchemy                             |
    | | See Also : pegasus.monitord.events                    | | connection string. Details are available at:                           |
    |                                                         |                                                                          |
    |                                                         | ::                                                                       |
    |                                                         |                                                                          |
    |                                                         |    http://www.sqlalchemy.org/docs/05/reference/dialects/index.html       |
    |                                                         |                                                                          |
    |                                                         | | It is important to note that users will need to have                   |
    |                                                         | | the appropriate db interface library installed. Which is               |
    |                                                         | | to say, SQLAlchemy is a wrapper around the mysql interface             |
    |                                                         | | library (for instance), it does not provide a MySQL                    |
    |                                                         | | driver itself. The Pegasus distribution includes                       |
    |                                                         | | both SQLAlchemy and the SQLite Python driver. As a                     |
    |                                                         | | final note, it is important to mention that unlike                     |
    |                                                         | | when using SQLite databases, using SQLAlchemy with                     |
    |                                                         | | other database servers, e.g. MySQL or Postgres ,                       |
    |                                                         | | the target database needs to exist. Users can also                     |
    |                                                         | | specify a file name using this property in order                       |
    |                                                         | | to create a file with the log events.                                  |
    |                                                         | | Example values for the SQLAlchemy connection string for                |
    |                                                         | | various end points are listed below                                    |
    |                                                         |                                                                          |
    |                                                         | +-----------------------+----------------------------------------------+ |
    |                                                         | | SQL Alchemy End Point | Example Value                                | |
    |                                                         | +=======================+==============================================+ |
    |                                                         | | Netlogger BP File     | file:///submit/dir/myworkflow.bp             | |
    |                                                         | +-----------------------+----------------------------------------------+ |
    |                                                         | | SQL Lite Database     | sqlite:///submit/dir/myworkflow.db           | |
    |                                                         | +-----------------------+----------------------------------------------+ |
    |                                                         | | MySQL Database        | mysql://user:password@host:port/databasename | |
    |                                                         | +-----------------------+----------------------------------------------+ |
    +---------------------------------------------------------+--------------------------------------------------------------------------+
    | | Property Key: pegasus.catalog.master.url              | | This property specifies the destination for the workflow               |
    | | Profile Key: N/A                                      | | dashboard database. By default, the workflow dashboard                 |
    | | Scope : Properties                                    | | datbase defaults to a sqlite database named workflow.db                |
    | | Since : 4.2                                           | | in the $HOME/.pegasus directory. This is database is                   |
    | | Type: String                                          | | shared for all workflows run as a particular user. Users               |
    | | Default :                                             | | can specify an alternative database by using a SQLAlchemy              |
    | | sqlite database in $HOME/.pegasus/workflow.db         | | connection string. Details are available at                            |
    | | See Also : pegasus.catalog.workflow.url               |                                                                          |
    |                                                         | ::                                                                       |
    |                                                         |                                                                          |
    |                                                         |   http://www.sqlalchemy.org/docs/05/reference/dialects/index.html        |
    |                                                         |                                                                          |
    |                                                         | | It is important to note that users will need to have the               |
    |                                                         | | appropriate db interface library installed. Which is to                |
    |                                                         | | say, SQLAlchemy is a wrapper around the mysql interface                |
    |                                                         | | library (for instance), it does not provide a MySQL                    |
    |                                                         | | driver itself. The Pegasus distribution includes both                  |
    |                                                         | | SQLAlchemy and the SQLite Python driver. As a final                    |
    |                                                         | | note, it is important to mention that unlike when using                |
    |                                                         | | SQLite databases, using SQLAlchemy with other database                 |
    |                                                         | | servers, e.g. MySQL or Postgres , the target database                  |
    |                                                         | | needs to exist. Users can also specify a file name                     |
    |                                                         | | using this property in order to create a file with                     |
    |                                                         | | the log events.                                                        |
    |                                                         |                                                                          |
    |                                                         | | Example values for the SQLAlchemy connection string                    |
    |                                                         | | for various end points are listed below                                |
    |                                                         |                                                                          |
    |                                                         | +-----------------------+----------------------------------------------+ |
    |                                                         | | SQL Alchemy End Point | Example Value                                | |
    |                                                         | +=======================+==============================================+ |
    |                                                         | | SQL Lite Database     | sqlite:///shared/myworkflow.db               | |
    |                                                         | +-----------------------+----------------------------------------------+ |
    |                                                         | | MySQL Database        | mysql://user:password@host:port/databasename | |
    |                                                         | +-----------------------+----------------------------------------------+ |
    +---------------------------------------------------------+--------------------------------------------------------------------------+
    | | Property Key: pegasus.monitord.output                 | | This property has been deprecated in favor of                          |
    | | Profile Key: N/A                                      | | pegasus.catalog.workflow.url that introduced in                        |
    | | Scope : Properties                                    | | 4.5 release. Support for this property will be                         |
    | | Since : 3.0.2                                         | | dropped in future releases.                                            |
    | | Type : String                                         |                                                                          |
    | | Default : SQlite database in submit directory.        |                                                                          |
    | | See Also : pegasus.monitord.events                    |                                                                          |
    +---------------------------------------------------------+--------------------------------------------------------------------------+
    | | Property Key: pegasus.dashboard.output                | | This property has been deprecated in favore of                         |
    | | Profile Key: N/A                                      | | pegasus.catalog.master.url that was introduced                         |
    | | Scope : Properties                                    | | in 4.5 release. Support for this property will                         |
    | | Since : 4.2                                           | | be dropped in future releases.                                         |
    | | Type : String                                         |                                                                          |
    | | Default :                                             |                                                                          |
    | | sqlite database in $HOME/.pegasus/workflow.db         |                                                                          |
    | | See Also : pegasus.monitord.output                    |                                                                          |
    +---------------------------------------------------------+--------------------------------------------------------------------------+
    | | Property Key: pegasus.monitord.notifications          | | This property determines how many notification                         |
    | | Profile Key: N/A                                      | | scripts pegasus-monitord will call concurrently.                       |
    | | Scope : Properties                                    | | Upon reaching this limit, pegasus-monitord will                        |
    | | Since : 3.1.0                                         | | wait for one notification script to finish before                      |
    | | Type :Boolean                                         | | issuing another one. This is a way to keep the                         |
    | | Default : true                                        | | number of processes under control at the submit                        |
    | | See Also : pegasus.monitord.notifications.max         | | host. Setting this property to 0 will disable                          |
    | | See Also : pegasus.monitord.notifications.timeout     | | notifications completely.                                              |
    +---------------------------------------------------------+--------------------------------------------------------------------------+
    | | Property Key: pegasus.monitord.notifications.max      | | This property determines whether pegasus-monitord                      |
    | | Profile Key: N/A                                      | | processes notifications. When notifications are                        |
    | | Scope : Properties                                    | | enabled, pegasus-monitord will parse the .notify                       |
    | | Since : 3.1.0                                         | | file generated by pegasus-plan and will invoke                         |
    | | Type :Integer                                         | | notification scripts whenever conditions matches                       |
    | | Default : 10                                          | | one of the notifications.                                              |
    | | See Also : pegasus.monitord.notifications             |                                                                          |
    | | See Also : pegasus.monitord.notifications.timeout     |                                                                          |
    +---------------------------------------------------------+--------------------------------------------------------------------------+
    | | Property Key: pegasus.monitord.notifications.timeout  | | This property determines how long will                                 |
    | | Profile Key: N/A                                      | | pegasus-monitord let notification scripts run                          |
    | | Scope : Properties                                    | | before terminating them. When this property is set                     |
    | | Since : 3.1.0                                         | | to 0 (default), pegasus-monitord will not terminate                    |
    | | Type :Integer                                         | | any notification scripts, letting them run                             |
    | | Default : true                                        | | indefinitely. If some notification scripts                             |
    | | See Also : pegasus.monitord.notifications             | | missbehave, this has the potential problem of                          |
    | | See Also : pegasus.monitord.notifications.max         | | starving pegasus-monitord’s notification slots                         |
    |                                                         | | (see the pegasus.monitord.notifications.max                            |
    |                                                         | | property), and block further notifications. In                         |
    |                                                         | | addition, users should be aware that                                   |
    |                                                         | | pegasus-monitord will not exit until all                               |
    |                                                         | | notification scripts are finished.                                     |
    +---------------------------------------------------------+--------------------------------------------------------------------------+
    | | Property Key: pegasus.monitord.stdout.disable.parsing | | By default, pegasus-monitord parses the                                |
    | | Profile Key:N/A                                       | | stdout/stderr section of the kickstart to populate                     |
    | | Scope : Properties                                    | | the applications captured stdout and stderr in the                     |
    | | Since : 3.1.1                                         | | job instance table for the stampede schema. For                        |
    | | Type :Boolean                                         | | large workflows, this may slow down monitord                           |
    | | Default : false                                       | | especially if the application is generating a                          |
    |                                                         | | lot of output to it’s stdout and stderr. This                          |
    |                                                         | | property, can be used to turn of the database                          |
    |                                                         | | population.                                                            |
    +---------------------------------------------------------+--------------------------------------------------------------------------+
    | | Property Key: pegasus.monitord.arguments              | | This property specifies additional command-line                        |
    | | Profile Key: N/A                                      | | arguments that should be passed to pegasus-monitord                    |
    | | Scope : Properties                                    | | at startup. These additional arguments are appended                    |
    | | Since : 4.6                                           | | to the arguments given to pegasus-monitord.                            |
    | | Type :String                                          |                                                                          |
    | | Default : N/A                                         |                                                                          |
    +---------------------------------------------------------+--------------------------------------------------------------------------+

.. _job-clustering-props:

Job Clustering Properties
-------------------------

.. table:: Job Clustering Properties

    +--------------------------------------------------------------+--------------------------------------------------------+
    | Key Attributes                                               | Description                                            |
    +==============================================================+========================================================+
    | | Property Key: pegasus.clusterer.job.aggregator             | | A large number of workflows executed through         |
    | | Profile Key: N/A                                           | | Pegasus, are composed of several jobs that run       |
    | | Scope : Properties                                         | | for only a few seconds or so. The overhead of        |
    | | Since : 2.0                                                | | running any job on the grid is usually 60            |
    | | Type : String                                              | | seconds or more. Hence, it makes sense to            |
    | | Values : seqexec|mpiexec|AWSBatch                          | | cluster small independent jobs into a larger         |
    | | Default : seqexec                                          | | job. This property determines, the executable        |
    |                                                              | | that will be used for running the larger job         |
    |                                                              | | on the remote site.                                  |
    |                                                              | |                                                      |
    |                                                              | | **seqexec**: In this mode, the executable            |
    |                                                              | | used to run the merged job is “pegasus-cluster”      |
    |                                                              | | that runs each of the smaller jobs sequentially      |
    |                                                              | | on the same node. The executable                     |
    |                                                              | | “pegasus-cluster” is a PEGASUS tool                  |
    |                                                              | | distributed in the PEGASUS worker package, and       |
    |                                                              | | can be usually found at                              |
    |                                                              | | {pegasus.home}/bin/pegasus-cluster.                  |
    |                                                              | |                                                      |
    |                                                              | | **mpiexec**: In this mode, the executable used       |
    |                                                              | | to run the clustered job is “pegasus-mpi-cluster”    |
    |                                                              | | (PMC) that runs the smaller jobs via mpi on n nodes  |
    |                                                              | | where n is the nodecount associated with the merged  |
    |                                                              | | job. The executable “pegasus-mpi-cluster” is a       |
    |                                                              | | PEGASUS tool distributed in the PEGASUS distribution |
    |                                                              | | and is built only if mpi compiler is available.      |
    |                                                              | |                                                      |
    |                                                              | | **AWSBatch**: In this mode, the executable used to   |
    |                                                              | | run the merged job is “pegasus-aws-batch” that runs  |
    |                                                              | | in local universe on the submit and runs the jobs    |
    |                                                              | | making up the cluster on AWS Batch.                  |
    +--------------------------------------------------------------+--------------------------------------------------------+
    | | Property Key: pegasus.clusterer.job.aggregator.arguments   | | The additional arguments with which a clustering     |
    | | Profile Key: job.aggregator.arguments                      | | executable should be invoked.                        |
    | | Default : None                                             |                                                        |
    | | Scope : TC, SC, Abstract WF, Properties                    |                                                        |
    | | Since : 5.0.2                                              |                                                        |
    | | See Also :                                                 |                                                        |
    | |    pegasus.clusterer.job.aggregator                        |                                                        |
    +--------------------------------------------------------------+--------------------------------------------------------+
    | | Property Key: pegasus.clusterer.job.aggregator.seqexec.log | | The tool pegasus-cluster logs the progress of the    |
    | | Profile Key: N/A                                           | | jobs that are being run by it in a progress file     |
    | | Scope : Properties                                         | | on the remote cluster where it is executed.          |
    | | Since : 2.3                                                | | This property sets the Boolean flag, that indicates  |
    | | Type :Boolean                                              | | whether to turn on the logging or not.               |
    | | Default : false                                            |                                                        |
    | | See Also :                                                 |                                                        |
    | |    pegasus.clusterer.job.aggregator                        |                                                        |
    | | See Also :                                                 |                                                        |
    | | pegasus.clusterer.job.aggregator.seqexec.log.global        |                                                        |
    +--------------------------------------------------------------+--------------------------------------------------------+
    | | Property Key:                                              | | The tool pegasus-cluster logs the progress of        |
    | |  pegasus.clusterer.job.aggregator.seqexec.log              | | the jobs that are being run by it in a               |
    | | Profile Key: N/A                                           | | progress file on the remote cluster where it         |
    | | Scope : Properties                                         | | is executed. The progress log is useful for          |
    | | Since : 2.3                                                | | you to track the progress of your computations       |
    | | Type :Boolean                                              | | and remote grid debugging. The progress log          |
    | | Default : false                                            | | file can be shared by multiple pegasus-cluster       |
    | | See Also : pegasus.clusterer.job.aggregator                | | jobs that are running on a particular cluster        |
    | | See Also :                                                 | | as part of the same workflow. Or it can be           |
    | |    pegasus.clusterer.job.aggregator.seqexec.log.global     | | per job.                                             |
    |                                                              | | This property sets the Boolean flag, that            |
    |                                                              | | indicates whether to have a single global log        |
    |                                                              | | for all the pegasus-cluster jobs on a                |
    |                                                              | | particular cluster or progress log per job.          |
    +--------------------------------------------------------------+--------------------------------------------------------+
    | | Property Key:                                              | | By default “pegasus-cluster” does not stop           |
    | |  pegasus.clusterer.job.aggregator.seqexec.firstjobfail     | | execution even if one of the clustered jobs          |
    | | Profile Key: N/A                                           | | it is executing fails. This is because               |
    | | Scope : Properties                                         | | “pegasus-cluster” tries to get as much work          |
    | | Since : 2.2                                                | | done as possible.                                    |
    | | Type :Boolean                                              | | This property sets the Boolean flag, that            |
    | | Default : true                                             | | indicates whether to make “pegasus-cluster”          |
    | | See Also : pegasus.clusterer.job.aggregator                | | stop on the first job failure it detects.            |
    +--------------------------------------------------------------+--------------------------------------------------------+
    | | Property Key :pegasus.clusterer.allow.single               | | By default, Pegasus does not launch clusters         |
    | | Profile Key: N/A                                           | | that contain a single job using the                  |
    | | Scope : Properties                                         | | clustering/job aggregator executable. This           |
    | | Since : 4.9                                                | | property allows you to override this behaviour       |
    | | Type : Boolean                                             | | and have single job clusters to be created.          |
    | | Default : False                                            | | Applies to both horizontal and label based           |
    |                                                              | | clustering.                                          |
    +--------------------------------------------------------------+--------------------------------------------------------+
    | | Property Key: pegasus.clusterer.label.key                  | | While clustering jobs in the workflow into           |
    | | Profile Key: N/A                                           | | larger jobs, you can optionally label your           |
    | | Scope : Properties                                         | | graph to control which jobs are clustered            |
    | | Since : 2.0                                                | | and to which clustered job they belong. This         |
    | | Type : String                                              | | done using a label based clustering scheme           |
    | | Default : label                                            | | and is done by associating a profile/label           |
    |                                                              | | key in the PEGASUS namespace with the jobs           |
    |                                                              | | in the DAX. Each job that has the same               |
    |                                                              | | value/label value for this profile key,              |
    |                                                              | | is put in the same clustered job.                    |
    |                                                              | | This property allows you to specify the              |
    |                                                              | | PEGASUS profile key that you want to use             |
    |                                                              | | for label based clustering.                          |
    +--------------------------------------------------------------+--------------------------------------------------------+

.. _logging-props:

Logging Properties
------------------

.. table:: Logging Properties

    +-----------------------------------------------+-----------------------------------------------------------------------------+
    | Key Attributes                                | Description                                                                 |
    +===============================================+=============================================================================+
    | | Property Key: pegasus.log.manager           | | This property sets the logging implementation to use for                  |
    | | Profile Key: N/A                            | | logging.                                                                  |
    | | Scope : Properties                          |                                                                             |
    | | Since : 2.2.0                               | | **Default**:                                                              |
    | | Type : String                               | | This implementation refers to the legacy                                  |
    | | Values : Default|Log4J                      | | Pegasus logger, that logs directly to stdout and stderr.                  |
    | | Default : Default                           | |  It however, does have the concept of levels similar to                   |
    | | See Also :pegasus.log.manager.formatter     | | log4j or syslog.                                                          |
    |                                               |                                                                             |
    |                                               | | **Log4j**:                                                                |
    |                                               | | This implementation, uses Log4j 2.x (2.17 at time of implementation to    |
    |                                               | |  log messages. The log4j properties can be specified in                   |
    |                                               | |  a properties file, the location of which is specified                    |
    |                                               | |  by the property pegasus.log.manager.log4j.conf .                         |
    +-----------------------------------------------+-----------------------------------------------------------------------------+
    | | Property Key: pegasus.log.manager.formatter | | This property sets the formatter to use for formatting                    |
    | | Profile Key: N/A                            | | the log messages while logging.                                           |
    | | Scope : Properties                          |                                                                             |
    | | Since : 2.2.0                               | | **Simple**                                                                |
    | | Type : String                               | | This formats the messages in a simple format. The                         |
    | | Values : Simple|Netlogger                   | | messages are logged as is with minimal formatting.                        |
    | | Default : Simple                            | | Below are sample log messages in this format while                        |
    | | See Also :pegasus.log.manager               | | ranking an abstract workflow according to performance.                    |
    |                                               |                                                                             |
    |                                               | ::                                                                          |
    |                                               |                                                                             |
    |                                               |    event.pegasus.ranking dax.id se18-gda.dax  - STARTED                     |
    |                                               |    event.pegasus.parsing.dax dax.id se18-gda-nested.dax  - STARTED          |
    |                                               |    event.pegasus.parsing.dax dax.id se18-gda-nested.dax  - FINISHED         |
    |                                               |    job.id jobGDA                                                            |
    |                                               |    job.id jobGDA query.name getpredicted performace time 10.00              |
    |                                               |    event.pegasus.ranking dax.id se18-gda.dax  - FINISHED                    |
    |                                               |                                                                             |
    |                                               |                                                                             |
    |                                               | | **Netlogger**                                                             |
    |                                               | | This formats the messages in the Netlogger format , that                  |
    |                                               | | is based on key value pairs. The netlogger format is useful               |
    |                                               | | for loading the logs into a database to do some meaningful                |
    |                                               | | analysis. Below are sample log messages in this format while              |
    |                                               | | ranking an abstract workflow                                              |
    |                                               |                                                                             |
    |                                               | ::                                                                          |
    |                                               |                                                                             |
    |                                               |   ts=2008-09-06T12:26:20.100502Z event=event.pegasus.ranking.start          |
    |                                               |     msgid=6bc49c1f-112e-4cdb-af54-3e0afb5d593c                              |
    |                                               |   eventId=event.pegasus.ranking_8d7c0a3c-9271-4c9c-a0f2-1fb57c6394d5        |
    |                                               |     dax.id=se18-gda.dax prog=Pegasus                                        |
    |                                               |   ts=2008-09-06T12:26:20.100750Z event=event.pegasus.parsing.dax.start      |
    |                                               |     msgid=fed3ebdf-68e6-4711-8224-a16bb1ad2969                              |
    |                                               |     eventId=event.pegasus.parsing.dax_887134a8-39cb-40f1-b11c-b49def0c5232\ |
    |                                               |      dax.id=se18-gda-nested.dax prog=Pegasus                                |
    |                                               |                                                                             |
    +-----------------------------------------------+-----------------------------------------------------------------------------+
    | | Property Key: pegasus.log.*                 | | This property sets the path to the file where all the                     |
    | | Profile Key: N/A                            | | logging for Pegasus can be redirected to. Both stdout                     |
    | | Scope : Properties                          | | and stderr are logged to the file specified.                              |
    | | Since : 2.0                                 |                                                                             |
    | | Type :file path                             |                                                                             |
    | | Default : no default                        |                                                                             |
    +-----------------------------------------------+-----------------------------------------------------------------------------+
    | | Property Key: pegasus.log.memory.usage      | | This property if set to true, will result in the                          |
    | | Profile Key: N/A                            | | planner writing out JVM heap memory statistics at                         |
    | | Scope : Properties                          | | the end of the planning process at the INFO level.                        |
    | | Since : 4.3.4                               | | This is useful, if users want to fine tune their                          |
    | | Type :Boolean                               | | java memory settings by setting JAVA_HEAPMAX and                          |
    | | Default : false                             | | JAVA_HEAPMIN for large workflows.                                         |
    +-----------------------------------------------+-----------------------------------------------------------------------------+
    | | Property Key: pegasus.metrics.app           | | This property namespace allows users to pass                              |
    | | Profile Key:N/A                             | | application level metrics to the metrics server.                          |
    | | Scope : Properties                          | | The value of this property is the name of the                             |
    | | Since : 4.3.0                               | | application.                                                              |
    | | Type :String Default : (no default)         | | Additional application specific attributes can                            |
    |                                               | | be passed by using the prefix pegasus.metrics.app                         |
    |                                               |                                                                             |
    |                                               | ::                                                                          |
    |                                               |                                                                             |
    |                                               |   pegasus.metrics.app.[arribute-name]       attribute-value                 |
    |                                               |                                                                             |
    |                                               | | Note: the attribute cannot be named name. This attribute                  |
    |                                               | | is automatically assigned the value from pegasus.metrics.app              |
    +-----------------------------------------------+-----------------------------------------------------------------------------+


.. _cleanup-props:

Cleanup Properties
------------------

.. table:: Cleanup Properties

    +------------------------------------------------------+----------------------------------------------------------+
    | Key Attributes                                       | Description                                              |
    +======================================================+==========================================================+
    | | Property Key: pegasus.file.cleanup.strategy        | | This property is used to select the strategy of how    |
    | | Profile Key: N/A                                   | | the cleanup nodes are added to the executable          |
    | | Scope :Properties                                  | | workflow.                                              |
    | | Since :2.2                                         |                                                          |
    | | Type :String                                       | | **InPlace**                                            |
    | | Default :InPlace                                   | | The default cleanup strategy. Adds cleanup nodes per   |
    |                                                      | | level of the workflow.                                 |
    |                                                      |                                                          |
    |                                                      | | **Constraint**                                         |
    |                                                      | | Adds cleanup nodes to constraint the amount of storage |
    |                                                      | | space used by a workflow.                              |
    |                                                      |                                                          |
    |                                                      | | **Note:**                                              |
    |                                                      | | This property is overridden by the –cleanup option     |
    |                                                      | | used in pegasus-plan.                                  |
    +------------------------------------------------------+----------------------------------------------------------+
    | | Property Key:pegasus.file.cleanup.impl             | | This property is used to select the executable that    |
    | | Profile Key: N/A                                   | | is used to create the working directory on the         |
    | | Scope : Properties                                 | | compute sites.                                         |
    | | Since : 2.2                                        | |                                                        |
    | | Type :String                                       | | **Cleanup**                                            |
    | | Default : Cleanup                                  | | The default executable that is used to delete files    |
    |                                                      | | is the “pegasus-transfer” executable shipped with      |
    |                                                      | | Pegasus. It is found at                                |
    |                                                      | | $PEGASUS_HOME/bin/pegasus-transfer in the Pegasus      |
    |                                                      | | distribution. An entry for transformation              |
    |                                                      | | pegasus::dirmanager needs to exist in the              |
    |                                                      | | Transformation Catalog or the PEGASUS_HOME             |
    |                                                      | | environment variable should be specified in the        |
    |                                                      | | site catalog for the sites for this mode to work.      |
    |                                                      |                                                          |
    |                                                      | | **RM**                                                 |
    |                                                      | | This mode results in the rm executable to be used      |
    |                                                      | | to delete files from remote directories. The rm        |
    |                                                      | | executable is standard on \*nix systems and is usually |
    |                                                      | | found at /bin/rm  location.                            |
    +------------------------------------------------------+----------------------------------------------------------+
    | | Property Key: pegasus.file.cleanup.clusters.num    | | In case of the InPlace strategy for adding the         |
    | | Profile Key: N/A                                   | | cleanup nodes to the workflow, this property           |
    | | Scope : Properties                                 | | specifies the maximum number of cleanup jobs           |
    | | Since : 4.2.0                                      | | that are added to the executable workflow on each      |
    | | Type :Integer                                      | | level.                                                 |
    +------------------------------------------------------+----------------------------------------------------------+
    | | Property Key: pegasus.file.cleanup.clusters.size   | | In case of the InPlace strategy this property sets     |
    | | Profile Key: N/A                                   | | the number of cleanup jobs that get clustered into     |
    | | Scope : Properties                                 | | a bigger cleanup job. This parameter is only used      |
    | | Since : 4.2.0                                      | | if pegasus.file.cleanup.clusters.num is not set.       |
    | | Type :Integer                                      |                                                          |
    | | Default : 2                                        |                                                          |
    +------------------------------------------------------+----------------------------------------------------------+
    | | Property Key: pegasus.file.cleanup.scope           | | By default in case of deferred planning InPlace file   |
    | | Profile Key: N/A                                   | | cleanup is turned OFF. This is because the cleanup     |
    | | Scope : Properties                                 | | algorithm does not work across partitions. This        |
    | | Since : 2.3.0                                      | | property can be used to turn on the cleanup in case    |
    | | Type :Enumeration                                  | | of deferred planning.                                  |
    | | Value :fullahead|deferred                          |                                                          |
    | | Default : fullahead                                | | **fullahead**:                                         |
    |                                                      | | This is the default scope. The pegasus cleanup         |
    |                                                      | | algorithm does not work across partitions in           |
    |                                                      | | deferred planning. Hence the cleanup is always         |
    |                                                      | |  turned OFF , when deferred planning occurs and        |
    |                                                      | | cleanup scope is set to full ahead.                    |
    |                                                      |                                                          |
    |                                                      |                                                          |
    |                                                      | | **deferred**:                                          |
    |                                                      | | If the scope is set to deferred, then Pegasus          |
    |                                                      | | will not disable file cleanup in case of deferred      |
    |                                                      | | planning. This is useful for scenarios where the       |
    |                                                      | | partitions themselves are independant                  |
    |                                                      | | ( i.e. dont share files ). Even if the scope is        |
    |                                                      | | set to deferred, users can turn off cleanup by         |
    |                                                      | | specifying –nocleanup option to pegasus-plan.          |
    +------------------------------------------------------+----------------------------------------------------------+
    | | Property Key:                                      | | This property is used to set the maximum available     |
    | |     pegasus.file.cleanup.constraint.*.maxspace     | | space (i.e., constraint) per site in Bytes. The        |
    | | Profile Key: N/A                                   | | * in the property name denotes the name of the         |
    | | Scope :Properties                                  | | compute site. A * in the property key is taken to      |
    | | Since :4.6.0                                       | | mean all sites.                                        |
    | | Type :String                                       |                                                          |
    | | Default :10737418240                               |                                                          |
    +------------------------------------------------------+----------------------------------------------------------+
    | | Property Key:                                      | | This property is used to determine whether stage       |
    | |  pegasus.file.cleanup.constraint.deferstageins     | | in jobs may be deferred. If this property is set       |
    | | Profile Key: N/A                                   | | to False (default), all stage in jobs will be marked   |
    | | Scope :Properties                                  | | as executing on the current compute site and will be   |
    | | Since :4.6.0                                       | | executed before any task. This property has no         |
    | | Type :Boolean                                      | | effect when running in a multi site case.              |
    | | Default :False                                     |                                                          |
    +------------------------------------------------------+----------------------------------------------------------+
    | | Property Key: pegasus.file.cleanup.constraint.csv  | | This property is used to specify a CSV file            |
    | | Profile Key: N/A                                   | | with a list of LFNs and their respective sizes         |
    | | Scope : Properties                                 | | in Bytes. The CSV file must be composed of two         |
    | | Since : 4.6.1                                      | | columns: filename and length.                          |
    | | Type : String                                      |                                                          |
    | | Default: (no default)                              |                                                          |
    +------------------------------------------------------+----------------------------------------------------------+

.. _aws-batch-props:

AWS Batch Properties
--------------------

.. table:: AWS Batch Properties

    +-------------------------------------------------------+--------------------------------------------------------+
    | Key Attributes                                        | Description                                            |
    +=======================================================+========================================================+
    | | Property Key: pegasus.aws.account                   | | This property is used to specify the amazon account  |
    | | Profile Key: N/A                                    | | under which you are running jobs.                    |
    | | Scope : Properties                                  |                                                        |
    | | Since : 4.9.0                                       |                                                        |
    | | Type :String                                        |                                                        |
    | | Default : (no default)                              |                                                        |
    +-------------------------------------------------------+--------------------------------------------------------+
    | | Property Key: pegasus.aws.region                    | | This property is used to specify the amazon region   |
    | | Profile Key: N/A                                    | | in which you are running jobs.                       |
    | | Scope : Properties                                  |                                                        |
    | | Since : 4.9.0                                       |                                                        |
    | | Type :String                                        |                                                        |
    | | Default : (no default)                              |                                                        |
    +-------------------------------------------------------+--------------------------------------------------------+
    | | Property Key: pegasus.aws.batch.job_definition      | | This property is used to specify                     |
    | | Profile Key: N/A                                    |                                                        |
    | | Scope : Properties                                  | | - the JSON file containing job definition to register|
    | | Since : 4.9.0                                       | | for executing jobs OR                                |
    | | Type :String                                        | |                                                      |
    | | Default : (no default)                              | | - the ARN of existing job definition OR              |
    |                                                       | |                                                      |
    |                                                       | | - basename of an existing job definition             |
    +-------------------------------------------------------+--------------------------------------------------------+
    | | Property Key: pegasus.aws.batch.compute_environment | | This property is used to specify                     |
    | | Profile Key:N/A                                     |                                                        |
    | | Scope : Properties                                  | | - the JSON file containing compute environment to    |
    | | Since : 4.9.0                                       | |   register for executing jobs OR                     |
    | | Type :String                                        | |                                                      |
    | | Default : (no default)                              | | - the ARN of existing compute environment OR         |
    |                                                       | |                                                      |
    |                                                       | | - basename of an existing compute environment        |
    +-------------------------------------------------------+--------------------------------------------------------+
    | | Property Key: pegasus.aws.batch.job_queue           | | This property is used to specify                     |
    | | Profile Key: N/A                                    |                                                        |
    | | Scope : Properties                                  | | - the JSON file containing Job Queue to use for      |
    | | Since : 4.9.0                                       | | executing jobs OR                                    |
    | | Type :String                                        | |                                                      |
    | | Default : (no default)                              | | - the ARN of existing job queue OR                   |
    |                                                       | |                                                      |
    |                                                       | | - basename of an existing job queue                  |
    +-------------------------------------------------------+--------------------------------------------------------+
    | | Property Key: pegasus.aws.batch.s3_bucket           | | This property is used to specify the S3 Bucket       |
    | | Profile Key: N/A                                    | | URL to use for data transfers while executing        |
    | | Scope : Properties                                  | | jobs on AWS Batch.                                   |
    | | Since : 4.9.0                                       |                                                        |
    | | Type :URL                                           |                                                        |
    | | Default : (no default)                              |                                                        |
    +-------------------------------------------------------+--------------------------------------------------------+

.. _misc-props:

Miscellaneous Properties
------------------------

.. table:: Miscellaneous Properties


    +---------------------------------------------------+-------------------------------------------------------------+
    | Key Attributes                                    | Description                                                 |
    +===================================================+=============================================================+
    | | Property Key: pegasus.code.generator            | | This property is used to load the appropriate Code        |
    | | Profile Key: N/A                                | | Generator to use for writing out the executable           |
    | | Scope : Properties                              | | workflow.                                                 |
    | | Since : 3.0                                     |                                                             |
    | | Type : String                                   | | **Condor**                                                |
    | | Values : Condor|Shell|PMC                       | | This is the default code generator for Pegasus .          |
    | | Default : Condor                                | | This generator generates the executable workflow as a     |
    | | See Also : pegasus.log.manager.formatter        | | Condor DAG file and associated job submit files. The      |
    |                                                   | | Condor DAG file is passed as input to Condor DAGMan       |
    |                                                   | | for job execution.                                        |
    |                                                   |                                                             |
    |                                                   | | **Shell**                                                 |
    |                                                   | | This Code Generator generates the executable workflow     |
    |                                                   | | as a shell script that can be executed on the submit      |
    |                                                   | | host. While using this code generator, all the jobs       |
    |                                                   | | should be mapped to site local i.e specify –sites         |
    |                                                   | | local to pegasus-plan.                                    |
    |                                                   |                                                             |
    |                                                   | | **PMC**                                                   |
    |                                                   | | This Code Generator generates the executable workflow     |
    |                                                   | | as a PMC task workflow. This is useful to run on          |
    |                                                   | | platforms where it not feasible to run Condor such        |
    |                                                   | | as the new XSEDE machines such as Blue Waters. In         |
    |                                                   | | this mode, Pegasus will generate the executable           |
    |                                                   | | workflow as a PMC task workflow and a sample PBS          |
    |                                                   | | submit script that submits this workflow.                 |
    +---------------------------------------------------+-------------------------------------------------------------+
    | | Property Key: pegasus.integrity.checking        | | This property determines the dial for pegasus             |
    | | Profile Key: N/A                                | | integrity checking. Currently the following dials are     |
    | | Scope : Properties                              | | supported                                                 |
    | | Since : 4.9.0                                   |                                                             |
    | | Type :none|full|nosymlink                       | | **none**                                                  |
    | | Default : full                                  | | no integrity checking occurs.                             |
    |                                                   |                                                             |
    |                                                   | | **full**                                                  |
    |                                                   | | In this mode, integrity checking happens at 3 levels      |
    |                                                   | |                                                           |
    |                                                   | | 1. after the input data has been staged to staging server |
    |                                                   | | pegasus-transfer verifies integrity of the staged files.  |
    |                                                   | | 2. before a compute task starts on a remote compute node  |
    |                                                   | | This ensures that checksums of the data staged in match   |
    |                                                   | | the checksums specified in the input replica catalog      |
    |                                                   | | or the ones computed when that piece of data was          |
    |                                                   | | generated as part of previous task in the workflow.       |
    |                                                   | | 3. After the workflow output data has been transferred    |
    |                                                   | | to user servers - This ensures that output data staged    |
    |                                                   | | to the final location was not corrupted in transit.       |
    |                                                   |                                                             |
    |                                                   | | **nosymlink**                                             |
    |                                                   | | No integrity checking is performed on input files         |
    |                                                   | | that are symlinked. You should consider turning           |
    |                                                   | | this on, if you think that your input files at rest       |
    |                                                   | | are at a low risk of data corruption, and want to         |
    |                                                   | | save on the checksum computation overheads against        |
    |                                                   | | the shared filesystem.noneNo integrity checking           |
    |                                                   | | is performed.                                             |
    +---------------------------------------------------+-------------------------------------------------------------+
    | | Property Key: pegasus.mode                      | | This property determines the mode under which pegasus     |
    | | Profile Key: N/A                                | | operates. Currently the following modes are               |
    | | Scope : Properties                              | | supported                                                 |
    | | Since : 5.0                                     |                                                             |
    | | Type : production|development|tutorial|debug    | | **production**                                            |
    | | Default : production                            | | default mode. jobs are retried 3 times, pegasus transfer  |
    |                                                   | | executable makes 3 attempts for each transfer, and jobs   |
    |                                                   | | remain in held state for 30 minutes, before being removed.|
    |                                                   |                                                             |
    |                                                   | | **development**                                           |
    |                                                   | | In this mode, integrity checking is disabled; jobs are    |
    |                                                   | | are not retried in case of failure; pegasus-transfer      |
    |                                                   | | executable makes only one attempt for each transfer,and   |
    |                                                   | | jobs remain in held state for 30 seconds, before being    |
    |                                                   | | removed.                                                  |
    |                                                   |                                                             |
    |                                                   | | **debug**                                                 |
    |                                                   | | In this mode, integrity checking is disabled; jobs are    |
    |                                                   | | are not retried in case of failure; pegasus-transfer      |
    |                                                   | | executable makes only one attempt for each transfer,and   |
    |                                                   | | jobs remain in held state for 30 seconds, before being    |
    |                                                   | | removed. It also increases logging for the various        |
    |                                                   | | subcomponents of Pegasus such as the planner, monitoring  |
    |                                                   | | daemon, transfers in jobs, PegasusLite jobs and the       |
    |                                                   | | registration jobs                                         |
    |                                                   |                                                             |
    |                                                   | | **tutorial**                                              |
    |                                                   | | In this mode, jobs are not retried in case of failure;    |
    |                                                   | | pegasus-transfer executable makes only one attempt for    |
    |                                                   | | each transfer,and jobs remain in held state for 30 seconds|
    |                                                   | | before being removed.                                     |
    +---------------------------------------------------+-------------------------------------------------------------+
    | | Property Key: pegasus.condor.concurrency.limits | | This Boolean property is used to determine whether        |
    | | Profile Key: N/A                                | | Pegasus associates default HTCondor concurrency           |
    | | Scope : Properties                              | | limits with jobs or not. Setting this property to         |
    | | Since : 4.5.3                                   | | true, allows you to throttle jobs across workflows,       |
    | | Type : Boolean                                  | | if the workflow are set to run in pure condor             |
    | | Default :False                                  | | environment.                                              |
    +---------------------------------------------------+-------------------------------------------------------------+
    | | Property Key: pegasus.register                  | | Pegasus creates registration jobs to register the         |
    | | Profile Key: N/A                                | | output files in the replica catalog. An output            |
    | | Scope : Properties                              | | file is registered only                                   |
    | | Since : 4.1.-                                   |                                                             |
    | | Type : Boolean                                  | | - if a user has configured a replica catalog in the       |
    | | Default : true                                  | | properties                                                |
    |                                                   | |                                                           |
    |                                                   | | - the register flags for the output files in the          |
    |                                                   | | abstract workflow are set to true                         |
    |                                                   | |                                                           |
    |                                                   | | This property can be used to turn off the creation        |
    |                                                   | | of the registration jobs even though the files            |
    |                                                   | | maybe marked to be registered in the replica catalog.     |
    +---------------------------------------------------+-------------------------------------------------------------+
    | | Property Key: pegasus.register.deep             | | By default, Pegasus always registers the complete LFN     |
    | | Profile Key: N/A                                | | that is associated with the output files in the DAX       |
    | | Scope : Properties                              | | i.e if the LFN has / in it, then lfn registered in        |
    | | Since : 4.5.3                                   | | the replica catalog has the whole part. For example,      |
    | | Type : Boolean                                  | | if in your Abstract Workflow you have rupture/0001.rx     |
    | | Default : true                                  | | as the name attribute for the uses tag, then in the       |
    |                                                   | | Replica Catalog the LFN is registered as                  |
    |                                                   | |  rupture/0001.rx                                          |
    |                                                   | | On setting this property to false, only the basename      |
    |                                                   | | is considered while registering in the replica catalog.   |
    |                                                   | | In the above case, 0001.rx will be registered instead     |
    |                                                   | | of rupture/0001.rx                                        |
    +---------------------------------------------------+-------------------------------------------------------------+
    | | Property Key: pegasus.data.reuse.scope          | | This property is used to control the behavior of the      |
    | | Profile Key: N/A                                | | data reuse algorithm in Pegasus                           |
    | | Scope : Properties                              |                                                             |
    | | Since : 4.5.0                                   | | **none**                                                  |
    | | Type :Enumeration                               | | This is same as disabling data reuse. It is equivalent    |
    | | Value :none|partial|full                        | | to passing the –force option to pegasus-plan on the       |
    | | Default : full                                  | | command line.                                             |
    |                                                   |                                                             |
    |                                                   | | **partial**                                               |
    |                                                   | | In this case, only certain jobs ( those that have pegasus |
    |                                                   | | profile key enable_for_data_reuse set to true ) are       |
    |                                                   | | checked for presence of output files in the replica       |
    |                                                   | | catalog. This gives users control over what jobs are      |
    |                                                   | | deleted as part of the data reuse algorithm.              |
    |                                                   |                                                             |
    |                                                   | | **full**                                                  |
    |                                                   | | This is the default behavior, where all the jobs output   |
    |                                                   | |  files are looked up in the replica catalog.              |
    +---------------------------------------------------+-------------------------------------------------------------+
    | | Property Key:                                   | | Pegasus supports transfer of statically linked            |
    | |    pegasus.catalog.transformation.mapper        | | executables as part of the executable workflow.           |
    | | Profile Key:N/A                                 | | At present, there is only support for staging of          |
    | | Scope : Properties                              | | executables referred to by the compute jobs specified     |
    | | Since : 2.0 Type :Enumeration                   | | in the DAX file. Pegasus determines the source locations  |
    | | Value :All|Installed|Staged|Submit              | | of the binaries from the transformation catalog, where    |
    | | Default : All                                   | | it searches for entries of type STATIC_BINARY for a       |
    |                                                   | | particular architecture type. The PFN for these entries   |
    |                                                   | | should refer to a globus-url-copy valid and accessible    |
    |                                                   | | remote URL. For transfer of executables, Pegasus          |
    |                                                   | | constructs a soft state map that resides on top of the    |
    |                                                   | | transformation catalog, that helps in determining the     |
    |                                                   | | locations from where an executable can be staged to the   |
    |                                                   | | remote site.                                              |
    |                                                   | |                                                           |
    |                                                   | | This property determines, how that map is created.        |
    |                                                   |                                                             |
    |                                                   | | **All**                                                   |
    |                                                   | | In this mode, all sources with entries of type            |
    |                                                   | | STATIC_BINARY for a particular transformation are         |
    |                                                   | |  considered valid sources for the transfer of             |
    |                                                   | |  executables. This the most general mode, and             |
    |                                                   | | results in the constructing the map as a result           |
    |                                                   | | of the cartesian product of the matches.                  |
    |                                                   |                                                             |
    |                                                   | | **Installed**                                             |
    |                                                   | | In this mode, only entries that are of type INSTALLED     |
    |                                                   | | are used while constructing the soft state map.           |
    |                                                   | | This results in Pegasus never doing any transfer          |
    |                                                   | | of executables as part of the workflow. It always         |
    |                                                   | | prefers the installed executables at the remote sites.    |
    |                                                   |                                                             |
    |                                                   | | **Staged**                                                |
    |                                                   | | In this mode, only entries that are of type               |
    |                                                   | | STATIC_BINARY are used while constructing the soft state  |
    |                                                   | | map. This results in the concrete workflow referring      |
    |                                                   | | only to the staged executables, irrespective of the       |
    |                                                   | | fact that the executables are already installed at the    |
    |                                                   | | remote end.                                               |
    |                                                   |                                                             |
    |                                                   |                                                             |
    |                                                   | | **Submit**                                                |
    |                                                   | | In this mode, only entries that are of type               |
    |                                                   | | STATIC_BINARY and reside at the submit host               |
    |                                                   | | (“site” local), are used while constructing the soft      |
    |                                                   | | state map. This is especially helpful, when the user      |
    |                                                   | | wants to use the latest compute code for his computations |
    |                                                   | | on the grid and that relies on his submit host.           |
    +---------------------------------------------------+-------------------------------------------------------------+
    | | Property Key: pegasus.selector.transformation   | | In case of transfer of executables, Pegasus could have    |
    | | Profile Key: N/A                                | | various transformations to select from when it schedules  |
    | | Scope : Properties                              | | to run a particular compute job at a remote site.         |
    | | Since : 2.0                                     | |  For e.g it can have the choice of staging an executable  |
    | |  Type : Enumeration                             | | from a particular remote site, from the local             |
    | | Value :Random|Installed|Staged|Submit           | | (submit host) only, use the one that is installed on the  |
    | | Default : Random                                | | remote site only.                                         |
    |                                                   | | This property determines, how a transformation amongst    |
    |                                                   | | the various candidate transformations is selected, and    |
    |                                                   | | is applied after the property for transformation mapper   |
    |                                                   | | has been applied. For e.g specifying                      |
    |                                                   | | pegasus.catalog.transformation.mapper as Staged and       |
    |                                                   | | then pegasus.transformation.selector as INSTALLED         |
    |                                                   | | does not work, as by the time this property is            |
    |                                                   | | applied, the soft state map only has entries of type      |
    |                                                   | | STAGEABLE.                                                |
    |                                                   |                                                             |
    |                                                   | | **Random**                                                |
    |                                                   | | In this mode, a random matching candidate transformation  |
    |                                                   | |  is selected to be staged to the remote execution site.   |
    |                                                   |                                                             |
    |                                                   | | **Installed**                                             |
    |                                                   | | In this mode, only entries that are of type INSTALLED     |
    |                                                   | | are selected. This means that the executable workflow     |
    |                                                   | | only refers to the transformations already pre installed  |
    |                                                   | | on the remote sites.                                      |
    |                                                   |                                                             |
    |                                                   | | **Staged**                                                |
    |                                                   | | In this mode, only entries that are of type STATIC_BINARY |
    |                                                   | | are selected, ignoring the ones that are installed at     |
    |                                                   | | the remote site.                                          |
    |                                                   |                                                             |
    |                                                   | | **Submit**                                                |
    |                                                   | | In this mode, only entries that are of type STATIC_BINARY |
    |                                                   | | and reside at the submit host (“site” local), are         |
    |                                                   | | selected as sources for staging the executables to the    |
    |                                                   | | remote execution sites.                                   |
    +---------------------------------------------------+-------------------------------------------------------------+
    | | Property Key:                                   | | The DAX Parser normally does not preserve line breaks     |
    | |   pegasus.parser.dax.preserver.linebreaks       | | while parsing the CDATA section that appears in the       |
    | | Profile Key:N/A                                 | | arguments section of the job element in the DAX.          |
    | | Scope : Properties                              | | On setting this to true, the DAX Parser preserves any     |
    | | Since : 2.2.0                                   | | line line breaks that appear in the CDATA section.        |
    | | Type :Boolean                                   |                                                             |
    | | Default : false                                 |                                                             |
    +---------------------------------------------------+-------------------------------------------------------------+
    | | Property Key:                                   | | If this property is set to true, then the planner         |
    | |     pegasus.parser.dax.data.dependencies        | | will automatically add edges between jobs in the          |
    | | Profile Key: N/A                                | | DAX on the basis of exisitng data dependencies            |
    | | Scope : Properties                              | | between jobs. For example, if a JobA generates an         |
    | | Since : 4.4.0                                   | | output file that is listed as input for JobB, then        |
    | | Type :Boolean                                   | | the planner will automatically add an edge between        |
    | | Default : true                                  | | JobA and JobB.                                            |
    +---------------------------------------------------+-------------------------------------------------------------+
    | | Property Key: pegasus.parser.document.size      | | For parsing YAML documents, this property controls the    |
    | |                                                 | | behavior of the snakeyaml library in terms of the maximum |
    | | Profile Key: N/A                                | | size in MB of a document that can be parsed.              |
    | | Scope : Properties                              | |                                                           |
    | | Since : 5.1.1                                   | |                                                           |
    | | Type : Integer in MB                            | |                                                           |
    | | Default : 500                                   | |                                                           |
    +---------------------------------------------------+-------------------------------------------------------------+
