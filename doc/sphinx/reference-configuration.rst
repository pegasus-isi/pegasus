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

-  dax level

-  in the site catalog

-  in the transformation catalog

Unfortunately, a different syntax applies to each level and context.
This section shows the different profile sources and syntaxes. However,
at the foundation of each profile lies the triple of namespace, key and
value.

User Profiles in Properties
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Users can specify all profiles in the properties files where the
property name is **[namespace].key** and **value** of the property is
the value of the profile.

Namespace can be env|condor|globus|dagman|pegasus

Any profile specified as a property applies to the whole workflow i.e
(all jobs in the workflow) unless overridden at the DAX level , Site
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

Profiles in DAX
~~~~~~~~~~~~~~~

The user can associate profiles with logical transformations in DAX.
Environment settings required by a job's application, or a maximum
estimate on the run-time are examples for profiles at this stage.

::

   <job id="ID000001" namespace="asdf" name="preprocess" version="1.0"
    level="3" dv-namespace="voeckler" dv-name="top" dv-version="1.0">
     <argument>-a top -T10  -i <filename file="voeckler.f.a"/>
    -o <filename file="voeckler.f.b1"/>
    <filename file="voeckler.f.b2"/></argument>
     <profile namespace="pegasus" key="walltime">2</profile>
     <profile namespace="pegasus" key="diskspace">1</profile>
     &mldr;
   </job>

Profiles in Site Catalog
~~~~~~~~~~~~~~~~~~~~~~~~

If it becomes necessary to limit the scope of a profile to a single
site, these profiles should go into the site catalog. A profile in the
site catalog applies to all jobs and all application run at the site.
Commonly, site catalog profiles set environment settings like the
LD_LIBRARY_PATH, or globus rsl parameters like queue and project names.

Currently, there is no tool to manipulate the site catalog, e.g. by
adding profiles. Modifying the site catalog requires that you load it
into your editor.

The XML version of the site catalog uses the following syntax:

::

   <profile namespace="namespace" key="key">value</profile>

::

   <site  handle="CCG" arch="x86_64" os="LINUX">
        <grid  type="gt5" contact="obelix.isi.edu/jobmanager-fork" scheduler="Fork" jobtype="auxillary"/>

        <directory type="shared-scratch" path="/shared-scratch">
               <file-server operation="all" url="gsiftp://headnode.isi.edu/shared-scratch"/>
        </directory>
        <directory type="local-storage" path="/local-storage">
               <file-server operation="all" url="gsiftp://headnode.isi.edu/local-storage"/>
        </directory>
        <profile namespace="pegasus" key="clusters.num">1</profile>
        <profile namespace="env" key="PEGASUS_HOME">/usr</profile>
   </site>

Profiles in Transformation Catalog
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Some profiles require a narrower scope than the site catalog offers.
Some profiles only apply to certain applications on certain sites, or
change with each application and site. Transformation-specific and
CPU-specific environment variables, or job clustering profiles are good
candidates. Such profiles are best specified in the transformation
catalog.

Profiles associate with a physical transformation and site in the
transformation catalog. The Database version of the transformation
catalog also permits the convenience of connecting a transformation with
a profile.

The Pegasus tc-client tool is a convenient helper to associate profiles
with transformation catalog entries. As benefit, the user does not have
to worry about formats of profiles in the various transformation catalog
instances.

::

   tc-client -a -P -E -p /home/shared/executables/analyze -t INSTALLED -r isi_condor -e env::GLOBUS_LOCATION=&rdquor;/home/shared/globus&rdquor;

The above example adds an environment variable GLOBUS_LOCATION to the
application /home/shared/executables/analyze on site isi_condor. The
transformation catalog guide has more details on the usage of the
tc-client.

::

   tr example::keg:1.0 {

   #specify profiles that apply for all the sites for the transformation
   #in each site entry the profile can be overriden

     profile env "APP_HOME" "/tmp/myscratch"
     profile env "JAVA_HOME" "/opt/java/1.6"

     site isi {
       profile env "HELLo" "WORLD"
       profile condor "FOO" "bar"
       profile env "JAVA_HOME" "/bin/java.1.6"
       pfn "/path/to/keg"
       arch "x86"
       os "linux"
       osrelease "fc"
       osversion "4"
       type "INSTALLED"
     }

     site wind {
       profile env "CPATH" "/usr/cpath"
       profile condor "universe" "condor"
       pfn "file:///path/to/keg"
       arch "x86"
       os "linux"
       osrelease "fc"
       osversion "4"
       type "STAGEABLE"
     }
   }

Most of the users prefer to edit the transformation catalog file
directly in the editor.

Profiles Conflict Resolution
----------------------------

Irrespective of where the profiles are specified, eventually the
profiles are associated with jobs. Multiple sources may specify the same
profile for the same job. For instance, DAX may specify an environment
variable X. The site catalog may also specify an environment variable X
for the chosen site. The transformation catalog may specify an
environment variable X for the chosen site and application. When the job
is concretized, these three conflicts need to be resolved.

Pegasus defines a priority ordering of profiles. The higher priority
takes precedence (overwrites) a profile of a lower priority.

1. Transformation Catalog Profiles

2. Site Catalog Profiles

3. DAX Profiles

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
    | | Profile Key: clusters.num                | | `Pegasus Clustering Guide <#horizontal_clustering>`__             |
    | | Scope : TC, SC, Abstract WF, Properties  | | for detailed description. This option determines the              |
    | | Since : 3.0                              | | total number of clusters per level. Jobs are evenly spread        |
    | | Type :Integer                            | | across clusters.                                                  |
    +--------------------------------------------+---------------------------------------------------------------------+
    | | Property Key: pegasus.clusters.size      | | Please refer to the                                               |
    | | Profile Key:clusters.size                | | `Pegasus Clustering Guide <#horizontal_clustering>`__             |
    | | Scope : TC, SC, Abstract WF, Properties  | | for detailed description. This profile determines the number of   |
    | | Since : 3.0                              | | jobs in each cluster. The number of clusters depends on the total |
    | | Type : Integer                           | | number of jobs on the level.                                      |
    +--------------------------------------------+---------------------------------------------------------------------+
    | | Property Key: pegasus.job.aggregator     | | Indicates the clustering executable that is used to run the       |
    | | Profile Key:job.aggregator               | | clustered job on the remote site.                                 |
    | | Scope : TC, SC, Abstract WF, Properties  |                                                                     |
    | | Since : 2.0                              |                                                                     |
    | | Type :Integer                            |                                                                     |
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
    | | Property Key: pegasus.stagein.clusters   | | This key determines the maximum number of stage-in jobs that      |
    | | Profile Key: stagein.clusters            | | are can executed locally or remotely per compute site per         |
    | | Scope : TC, SC, Abstract WF, Properties  | | workflow. This is used to configure the                           |
    | | Since : 4.0                              | | `BalancedCluster <#transfer-refiner-balanced-cluster>`__          |
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
    | | Since : 4.0                              | | `BalancedCluster <#transfer-refiner-balanced-cluster>`__          |
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
    | | Since : 5.0                              | |the container associated with this profile is executed             |
    | | Type :String                             |                                                                     |
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
    | | Scope : TC, SC, Abstract WF, Properties   | | `Pegasus Clustering Guide <#runtime_clustering>`__             |
    | | Since : 2.0                               | | for description on using it for runtime clustering.            |
    | | Type : Long                               |                                                                  |
    +---------------------------------------------+------------------------------------------------------------------+
    | | Property Key: pegasus.clusters.maxruntime | | Please refer to the                                            |
    | | Profile Key: clusters.maxruntime          | | `Pegasus Clustering Guide <#runtime_clustering>`__             |
    | | Scope : TC, SC, Abstract WF, Properties   | | for detailed description. This profile specifies the           |
    | | Since : 4.0                               | | maximum runtime of a job in seconds.                           |
    | | Type : Integer                            |                                                                  |
    +---------------------------------------------+------------------------------------------------------------------+
    | | Property Key: pegasus.cores               | | The total number of cores, required for a job. This is also    |
    | | Profile Key:cores                         | | used for accounting purposes in the database while             |
    | | Scope : TC, SC, Abstract WF, Properties   | | generating statistics. It corresponds to the multiplier_factor |
    | | Since : 4.0                               | | in the job_instance table described                            |
    | | Type : Integer                            | | `here <#stampede_schema_overview>`__ .                         |
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
---------------------------

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

The following example provides a sensible set of properties to be set by
the user property file. These properties use mostly non-default
settings. It is an example only, and will not work for you:

::

   pegasus.catalog.replica              File
   pegasus.catalog.replica.file         ${pegasus.home}/etc/sample.rc.data
   pegasus.catalog.transformation       Text
   pegasus.catalog.transformation.file  ${pegasus.home}/etc/sample.tc.text
   pegasus.catalog.site.file            ${pegasus.home}/etc/sample.sites.xml

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

   =============================================================================================================================================================================== ===========================================================================================================================================================================================================================================================================================================================================================
   **Key Attributes**                                                                                                                                                              **Description**
   **Property Key:**\ pegasus.schema.dax\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 2.0 **Type :**\ file path **Default :** ${pegasus.home.sysconfdir}/dax-3.4.xsd This file is a copy of the XML schema that describes abstract DAG files that are the result of the abstract planning process, and input into any concrete planning. Providing a copy of the schema enables the parser to use the local copy instead of reaching out to the Internet, and obtaining the latest version from the Pegasus website dynamically.
   **Property Key:**\ pegasus.schema.sc\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 2.0 **Type :**\ file path **Default :** ${pegasus.home.sysconfdir}/sc-4.0.xsd   This file is a copy of the XML schema that describes the xml description of the site catalog. Providing a copy of the schema enables the parser to use the local copy instead of reaching out to the internet, and obtaining the latest version from the GriPhyN website dynamically.
   **Property Key:**\ pegasus.schema.ivr\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 2.0 **Type :**\ file path **Default :** ${pegasus.home.sysconfdir}/iv-2.0.xsd  This file is a copy of the XML schema that describes invocation record files that are the result of the a grid launch in a remote or local site. Providing a copy of the schema enables the parser to use the local copy instead of reaching out to the Internet, and obtaining the latest version from the Pegasus website dynamically.
   =============================================================================================================================================================================== ===========================================================================================================================================================================================================================================================================================================================================================

.. _db-props:

Database Drivers For All Relational Catalogs
--------------------------------------------

.. table:: Database Driver Properties

   ===================================================================================================================================================================================================== ==============================================================================================================================================================================================================================================================================================================================================================================================================================================================
   **Property Key**                                                                                                                                                                                      **Description**
   **Property Key:**\ pegasus.catalog.*.db.driver\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 2.0 **Type :**\ Enumeration **Values :**\ MySQL|PostGres|SQLite\ **Default :** (no default) The database driver class is dynamically loaded, as required by the schema. Currently, only MySQL 5.x, PostGreSQL >= 8.1 and SQlite are supported. Their respective JDBC3 driver is provided as part and parcel of the PEGASUS.

                                                                                                                                                                                                         The \* in the property name can be replaced by a catalog name to apply the property only for that catalog. Valid catalog names are

                                                                                                                                                                                                         ::

                                                                                                                                                                                                            replica
   **Property Key:**\ pegasus.catalog.*.db.url\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 2.0 **Type :**\ Database URL **Default :** (no default)                                        Each database has its own string to contact the database on a given host, port, and database. Although most driver URLs allow to pass arbitrary arguments, please use the pegasus.catalog.[catalog-name].db.\* keys or pegasus.catalog.*.db.\* to preload these arguments.
                                                                                                                                                                                                         THE URL IS A MANDATORY PROPERTY FOR ANY DBMS BACKEND.
   **Property Key:**\ pegasus.catalog.*.db.user\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 2.0 **Type :**\ String **Default :**                                                          In order to access a database, you must provide the name of your account on the DBMS. This property is database-independent. THIS IS A MANDATORY PROPERTY FOR MANY DBMS BACKENDS.

                                                                                                                                                                                                         The \* in the property name can be replaced by a catalog name to apply the property only for that catalog. Valid catalog names are

                                                                                                                                                                                                         ::

                                                                                                                                                                                                            replica
   **Property Key:**\ pegasus.catalog.*.db.password\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 2.0 **Type :**\ String **Default :** (no default)                                         In order to access a database, you must provide an optional password of your account on the DBMS. This property is database-independent. THIS IS A MANDATORY PROPERTY, IF YOUR DBMS BACKEND ACCOUNT REQUIRES A PASSWORD.

                                                                                                                                                                                                         The \* in the property name can be replaced by a catalog name to apply the property only for that catalog. Valid catalog names are

                                                                                                                                                                                                         ::

                                                                                                                                                                                                            replica
   **Property Key:**\ pegasus.catalog.*.db.\*\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 2.0 **Type :**\ String **Default :** (no default)                                               Each database has a multitude of options to control in fine detail the further behaviour. You may want to check the JDBC3 documentation of the JDBC driver for your database for details. The keys will be passed as part of the connect properties by stripping the "pegasus.catalog.[catalog-name].db." prefix from them. The catalog-name can be replaced by the following values provenance for Provenance Catalog (PTC), replica for Replica Catalog (RC)

                                                                                                                                                                                                         Postgres >= 8.1 parses the following properties:

                                                                                                                                                                                                         ::

                                                                                                                                                                                                            pegasus.catalog.*.db.user
                                                                                                                                                                                                            pegasus.catalog.*.db.password
                                                                                                                                                                                                            pegasus.catalog.*.db.PGHOST
                                                                                                                                                                                                            pegasus.catalog.*.db.PGPORT
                                                                                                                                                                                                            pegasus.catalog.*.db.charSet
                                                                                                                                                                                                            pegasus.catalog.*.db.compatible

                                                                                                                                                                                                         MySQL 5.0 parses the following properties:

                                                                                                                                                                                                         ::

                                                                                                                                                                                                            pegasus.catalog.*.db.user
                                                                                                                                                                                                            pegasus.catalog.*.db.password
                                                                                                                                                                                                            pegasus.catalog.*.db.databaseName
                                                                                                                                                                                                            pegasus.catalog.*.db.serverName
                                                                                                                                                                                                            pegasus.catalog.*.db.portNumber
                                                                                                                                                                                                            pegasus.catalog.*.db.socketFactory
                                                                                                                                                                                                            pegasus.catalog.*.db.strictUpdates
                                                                                                                                                                                                            pegasus.catalog.*.db.ignoreNonTxTables
                                                                                                                                                                                                            pegasus.catalog.*.db.secondsBeforeRetryMaster
                                                                                                                                                                                                            pegasus.catalog.*.db.queriesBeforeRetryMaster
                                                                                                                                                                                                            pegasus.catalog.*.db.allowLoadLocalInfile
                                                                                                                                                                                                            pegasus.catalog.*.db.continueBatchOnError
                                                                                                                                                                                                            pegasus.catalog.*.db.pedantic
                                                                                                                                                                                                            pegasus.catalog.*.db.useStreamLengthsInPrepStmts
                                                                                                                                                                                                            pegasus.catalog.*.db.useTimezone
                                                                                                                                                                                                            pegasus.catalog.*.db.relaxAutoCommit
                                                                                                                                                                                                            pegasus.catalog.*.db.paranoid
                                                                                                                                                                                                            pegasus.catalog.*.db.autoReconnect
                                                                                                                                                                                                            pegasus.catalog.*.db.capitalizeTypeNames
                                                                                                                                                                                                            pegasus.catalog.*.db.ultraDevHack
                                                                                                                                                                                                            pegasus.catalog.*.db.strictFloatingPoint
                                                                                                                                                                                                            pegasus.catalog.*.db.useSSL
                                                                                                                                                                                                            pegasus.catalog.*.db.useCompression
                                                                                                                                                                                                            pegasus.catalog.*.db.socketTimeout
                                                                                                                                                                                                            pegasus.catalog.*.db.maxReconnects
                                                                                                                                                                                                            pegasus.catalog.*.db.initialTimeout
                                                                                                                                                                                                            pegasus.catalog.*.db.maxRows
                                                                                                                                                                                                            pegasus.catalog.*.db.useHostsInPrivileges
                                                                                                                                                                                                            pegasus.catalog.*.db.interactiveClient
                                                                                                                                                                                                            pegasus.catalog.*.db.useUnicode
                                                                                                                                                                                                            pegasus.catalog.*.db.characterEncoding

                                                                                                                                                                                                         MS SQL Server 2000 support the following properties (keys are case-insensitive, e.g. both "user" and "User" are valid):

                                                                                                                                                                                                         ::

                                                                                                                                                                                                            pegasus.catalog.*.db.User
                                                                                                                                                                                                            pegasus.catalog.*.db.Password
                                                                                                                                                                                                            pegasus.catalog.*.db.DatabaseName
                                                                                                                                                                                                            pegasus.catalog.*.db.ServerName
                                                                                                                                                                                                            pegasus.catalog.*.db.HostProcess
                                                                                                                                                                                                            pegasus.catalog.*.db.NetAddress
                                                                                                                                                                                                            pegasus.catalog.*.db.PortNumber
                                                                                                                                                                                                            pegasus.catalog.*.db.ProgramName
                                                                                                                                                                                                            pegasus.catalog.*.db.SendStringParametersAsUnicode
                                                                                                                                                                                                            pegasus.catalog.*.db.SelectMethod

                                                                                                                                                                                                         The \* in the property name can be replaced by a catalog name to apply the property only for that catalog. Valid catalog names are

                                                                                                                                                                                                         ::

                                                                                                                                                                                                            replica
   **Property Key:**\ pegasus.catalog.*.timeout\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 4.5.1 **Type :**\ Integer **Default :** (no default)                                          This property sets a busy handler that sleeps for a specified amount of time (in seconds) when a table is locked. This property has effect only in a sqlite database.

                                                                                                                                                                                                         The \* in the property name can be replaced by a catalog name to apply the property only for that catalog. Valid catalog names are

                                                                                                                                                                                                         ::

                                                                                                                                                                                                            master
                                                                                                                                                                                                            workflow
   ===================================================================================================================================================================================================== ==============================================================================================================================================================================================================================================================================================================================================================================================================================================================

.. _catalog-props:

Catalog Related Properties
--------------------------

.. table:: Replica Catalog Properties

   ========================================================================================================================================== ==========================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================
   **Key Attributes**                                                                                                                         **Description**
   **Property Key:**\ pegasus.catalog.replica\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 2.0 **Default :** File               Pegasus queries a Replica Catalog to discover the physical filenames (PFN) for input files specified in the DAX. Pegasus can interface with various types of Replica Catalogs. This property specifies which type of Replica Catalog to use during the planning process.

                                                                                                                                              JDBCRC
                                                                                                                                                 In this mode, Pegasus queries a SQL based replica catalog that is accessed via JDBC. To use JDBCRC, the user additionally needs to set the following properties

                                                                                                                                                 1. pegasus.catalog.replica.db.driver = mysql \| postgres \|sqlite
                                                                                                                                                 2. pegasus.catalog.replica.db.url = <jdbc url to the database> e.g jdbc:mysql://database-host.isi.edu/database-name \| jdbc:sqlite:/shared/jdbcrc.db
                                                                                                                                                 3. pegasus.catalog.replica.db.user = database-user
                                                                                                                                                 4. pegasus.catalog.replica.db.password = database-password

                                                                                                                                              File
                                                                                                                                                 In this mode, Pegasus queries a file based replica catalog. It is neither transactionally safe, nor advised to use for production purposes in any way. Multiple concurrent instances *will clobber* each other!. The site attribute should be specified whenever possible. The attribute key for the site attribute is "site".

                                                                                                                                                 The LFN may or may not be quoted. If it contains linear whitespace, quotes, backslash or an equality sign, it must be quoted and escaped. Ditto for the PFN. The attribute key-value pairs are separated by an equality sign without any whitespaces. The value may be in quoted. The LFN sentiments about quoting apply.

                                                                                                                                                 ::

                                                                                                                                                    LFN PFN
                                                                                                                                                    LFN PFN a=b [..]
                                                                                                                                                    LFN PFN a="b" [..]
                                                                                                                                                    "LFN w/LWS" "PFN w/LWS" [..]

                                                                                                                                                 To use File, the user additionally needs to specify **pegasus.catalog.replica.file** property to specify the path to the file based RC. IF not specified , defaults to $PWD/rc.txt file.

                                                                                                                                              Regex
                                                                                                                                                 In this mode, Pegasus queries a file based replica catalog. It is neither transactionally safe, nor advised to use for production purposes in any way. Multiple concurrent access to the File will end up clobbering the contents of the file. The site attribute should be specified whenever possible. The attribute key for the site attribute is "site".

                                                                                                                                                 The LFN may or may not be quoted. If it contains linear whitespace, quotes, backslash or an equality sign, it must be quoted and escaped. Ditto for the PFN. The attribute key-value pairs are separated by an equality sign without any whitespaces. The value may be in quoted. The LFN sentiments about quoting apply.

                                                                                                                                                 In addition users can specifiy regular expression based LFN's. A regular expression based entry should be qualified with an attribute named 'regex'. The attribute regex when set to true identifies the catalog entry as a regular expression based entry. Regular expressions should follow Java regular expression syntax.

                                                                                                                                                 For example, consider a replica catalog as shown below.

                                                                                                                                                 Entry 1 refers to an entry which does not use a resular expressions. This entry would only match a file named 'f.a', and nothing else. Entry 2 referes to an entry which uses a regular expression. In this entry f.a referes to files having name as f[any-character]a i.e. faa, f.a, f0a, etc.

                                                                                                                                                 ::

                                                                                                                                                    f.a file:///Vol/input/f.a site="local"
                                                                                                                                                    f.a file:///Vol/input/f.a site="local" regex="true"

                                                                                                                                                 Regular expression based entries also support substitutions. For example, consider the regular expression based entry shown below.

                                                                                                                                                 Entry 3 will match files with name alpha.csv, alpha.txt, alpha.xml. In addition, values matched in the expression can be used to generate a PFN.

                                                                                                                                                 For the entry below if the file being looked up is alpha.csv, the PFN for the file would be generated as file:///Volumes/data/input/csv/alpha.csv. Similary if the file being lookedup was alpha.csv, the PFN for the file would be generated as file:///Volumes/data/input/xml/alpha.xml i.e. The section [0], [1] will be replaced. Section [0] refers to the entire string i.e. alpha.csv. Section [1] refers to a partial match in the input i.e. csv, or txt, or xml. Users can utilize as many sections as they wish.

                                                                                                                                                 ::

                                                                                                                                                    alpha\.(csv|txt|xml) file:///Vol/input/[1]/[0] site="local" regex="true"

                                                                                                                                                 To use File, the user additionally needs to specify pegasus.catalog.replica.file property to specify the path to the file based RC.

                                                                                                                                              Directory
                                                                                                                                                 In this mode, Pegasus does a directory listing on an input directory to create the LFN to PFN mappings. The directory listing is performed recursively, resulting in deep LFN mappings. For example, if an input directory $input is specified with the following structure

                                                                                                                                                 ::

                                                                                                                                                    $input
                                                                                                                                                    $input/f.1
                                                                                                                                                    $input/f.2
                                                                                                                                                    $input/D1
                                                                                                                                                    $input/D1/f.3

                                                                                                                                                 Pegasus will create the mappings the following LFN PFN mappings internally

                                                                                                                                                 ::

                                                                                                                                                    f.1 file://$input/f.1  site="local"
                                                                                                                                                    f.2 file://$input/f.2  site="local"
                                                                                                                                                    D1/f.3 file://$input/D2/f.3 site="local"

                                                                                                                                                 If you don't want the deep lfn's to be created then, you can set pegasus.catalog.replica.directory.flat.lfn to true In that case, for the previous example, Pegasus will create the following LFN PFN mappings internally.

                                                                                                                                                 ::

                                                                                                                                                    f.1 file://$input/f.1  site="local"
                                                                                                                                                    f.2 file://$input/f.2  site="local"
                                                                                                                                                    f.3 file://$input/D2/f.3 site="local"

                                                                                                                                                 pegasus-plan has --input-dir option that can be used to specify an input directory.

                                                                                                                                                 Users can optionally specify additional properties to configure the behvavior of this implementation.

                                                                                                                                                 **pegasus.catalog.replica.directory** to specify the path to the directory containing the files

                                                                                                                                                 **pegasus.catalog.replica.directory.site** to specify a site attribute other than local to associate with the mappings.

                                                                                                                                                 **pegasus.catalog.replica.directory.url.prefix** to associate a URL prefix for the PFN's constructed. If not specified, the URL defaults to file://

                                                                                                                                              MRC
                                                                                                                                                 In this mode, Pegasus queries multiple replica catalogs to discover the file locations on the grid. To use it set

                                                                                                                                                 ::

                                                                                                                                                    pegasus.catalog.replica MRC

                                                                                                                                                 Each associated replica catalog can be configured via properties as follows.

                                                                                                                                                 The user associates a variable name referred to as [value] for each of the catalogs, where [value] is any legal identifier (concretely [A-Za-z][_A-Za-z0-9]*) For each associated replica catalogs the user specifies the following properties.

                                                                                                                                                 ::

                                                                                                                                                    pegasus.catalog.replica.mrc.[value]       specifies the type of \
                                                                                                                                                                                              replica catalog.
                                                                                                                                                    pegasus.catalog.replica.mrc.[value].key   specifies a property name\
                                                                                                                                                                                              key for a particular catalog

                                                                                                                                                 ::

                                                                                                                                                    pegasus.catalog.replica.mrc.directory1 Directory
                                                                                                                                                    pegasus.catalog.replica.mrc.directory1.directory /input/dir1
                                                                                                                                                    pegasus.catalog.replica.mrc.directory1.directory.site  siteX
                                                                                                                                                    pegasus.catalog.replica.mrc.directory2 Directory
                                                                                                                                                    pegasus.catalog.replica.mrc.directory2.directory /input/dir2
                                                                                                                                                    pegasus.catalog.replica.mrc.directory1.directory.site  siteY

                                                                                                                                                 In the above example, directory1, directory2 are any valid identifier names and url is the property key that needed to be specified.
   **Property Key:**\ pegasus.catalog.replica.chunk.size\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 2.0 **Default :** 1000    The pegasus-rc-client takes in an input file containing the mappings upon which to work. This property determines, the number of lines that are read in at a time, and worked upon at together. This allows the various operations like insert, delete happen in bulk if the underlying replica implementation supports it.
   **Property Key:**\ pegasus.catalog.replica.cache.asrc\ **Profile Key :**\ N/A\ **Scope :** Properties **Since :** 2.0 **Default :** false  This Boolean property determines whether to treat the cache file specified as a supplemental replica catalog or not. User can specify on the command line to pegasus-plan a comma separated list of cache files using the --cache option. By default, the LFN->PFN mappings contained in the cache file are treated as cache, i.e if an entry is found in a cache file the replica catalog is not queried. This results in only the entry specified in the cache file to be available for replica selection.

                                                                                                                                              Setting this property to true, results in the cache files to be treated as supplemental replica catalogs. This results in the mappings found in the replica catalog (as specified by pegasus.catalog.replica) to be merged with the ones found in the cache files. Thus, mappings for a particular LFN found in both the cache and the replica catalog are available for replica selection.
   **Property Key:**\ pegasus.catalog.replica.dax.asrc\ **Profile Key :**\ N/A\ **Scope :** Properties **Since :** 4.5.2 **Default :** false  This Boolean property determines whether to treat the locations of files recorded in the DAX as a supplemental replica catalog or not. By default, the LFN->PFN mappings contained in the DAX file overrides any specified in a replica catalog. This results in only the entry specified in the DAX file to be available for replica selection.

                                                                                                                                              Setting this property to true, results in the locations of files recorded in the DAX files to be treated as a supplemental replica catalog. This results in the mappings found in the replica catalog (as specified by pegasus.catalog.replica) to be merged with the ones found in the cache files. Thus, mappings for a particular LFN found in both the DAX and the replica catalog are available for replica selection.
   **Property Key:**\ pegasus.catalog.replica.output\ **.\* Profile Key :**\ N/A\ **Scope :** Properties **Since :** 4.5.3 **Default :** None Normally, the registration jobs in the executable workflow register to the replica catalog specified by the user in the properties file . This property prefix allows the user to specify a separate output replica catalog that is different from the one used for discovery of input files. This is normally the case, when a Directory or MRC based replica catalog backend that don't support insertion of entries are used for discovery of input files. For example to specify a separate file based output replica catalog, specify

                                                                                                                                              ::

                                                                                                                                                 pegasus.catalog.replica.output        File
                                                                                                                                                 pegasus.catalog.replica.output.file   /workflow/output.rc
   ========================================================================================================================================== ==========================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================

.. table:: Site Catalog Properties

   ========================================================================================================================================= ====================================================================================================================
   **Key Attributes**                                                                                                                        **Description**
   **Property Key:**\ pegasus.catalog.site\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 2.0 **Default :** XML                  Pegasus supports two different types of site catalogs in XML format conforming

                                                                                                                                             -  sc-3.0.xsd http://pegasus.isi.edu/schema/sc-3.0.xsd

                                                                                                                                             -  sc-4.0.xsd http://pegasus.isi.edu/schema/sc-4.0.xsd

                                                                                                                                             Pegasus is able to auto-detect what schema a user site catalog refers to. Hence, this property may no longer be set.
   **Property Key:**\ pegasus.catalog.site.file\ **Profile Key :**\ N/A\ **Scope :** Properties **Since :** 2.0 **Default :** $PWD/sites.xml The path to the site catalog file, that describes the various sites and their layouts to Pegasus.
   ========================================================================================================================================= ====================================================================================================================

.. table:: Transformation Catalog Properties

   =========================================================================================================================================== =======================================================================================================================================================================================================================================================================
   **Key Attributes**                                                                                                                          **Description**
   **Property Key:**\ pegasus.catalog.transformation\ **Profile Key:**\ N/A\ **Scope :**\ Properties **Since :** 2.0 **Default :** Text        The only recommended and supported version of Transformation Catalog for Pegasus is Text. For the old File based formats, users should use pegasus-tc-converter to convert File format to Text Format.

                                                                                                                                               Text
                                                                                                                                                  In this mode, a multiline file based format is understood. The file is read and cached in memory. Any modifications, as adding or deleting, causes an update of the memory and hence to the file underneath. All queries are done against the memory representation.

                                                                                                                                                  The file sample.tc.text in the etc directory contains an example

                                                                                                                                                  Here is a sample textual format for transfomation catalog containing one transformation on two sites

                                                                                                                                                  ::

                                                                                                                                                     tr example::keg:1.0 {
                                                                                                                                                     #specify profiles that apply for all the sites for the transformation
                                                                                                                                                     #in each site entry the profile can be overriden
                                                                                                                                                     profile env "APP_HOME" "/tmp/karan"
                                                                                                                                                     profile env "JAVA_HOME" "/bin/app"
                                                                                                                                                     site isi {
                                                                                                                                                     profile env "me" "with"
                                                                                                                                                     profile condor "more" "test"
                                                                                                                                                     profile env "JAVA_HOME" "/bin/java.1.6"
                                                                                                                                                     pfn "/path/to/keg"
                                                                                                                                                     arch  "x86"
                                                                                                                                                     os    "linux"
                                                                                                                                                     osrelease "fc"
                                                                                                                                                     osversion "4"
                                                                                                                                                     type "INSTALLED"
                                                                                                                                                     site wind {
                                                                                                                                                     profile env "me" "with"
                                                                                                                                                     profile condor "more" "test"
                                                                                                                                                     pfn "/path/to/keg"
                                                                                                                                                     arch  "x86"
                                                                                                                                                     os    "linux"
                                                                                                                                                     osrelease "fc"
                                                                                                                                                     osversion "4"
                                                                                                                                                     type "STAGEABLE"
   **Property Key:**\ pegasus.catalog.transformation\ **Profile Key :**\ N/A\ **Scope :** Properties **Since :** 2.0 **Default :** $PWD/tc.txt The path to the transformation catalog file, that describes the locations of the executables.
   =========================================================================================================================================== =======================================================================================================================================================================================================================================================================

.. _replica-sel-props:

Replica Selection Properties
----------------------------

.. table:: Replica Selection Properties

   ================================================================================================================================================================================================================================================================================== =====================================================================================================================================================================================================================================================================================================================================================================================================================================================
   **Key Attributes**                                                                                                                                                                                                                                                                 **Description**
   **Property Key:**\ pegasus.selector.replica\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 2.0 **Type :**\ String **Default :** Default **See Also :** pegasus.selector.replica.*.ignore.stagein.sites\ **See Also :** pegasus.selector.replica.*.prefer.stagein.sites Each job in the DAX maybe associated with input LFN's denoting the files that are required for the job to run. To determine the physical replica (PFN) for a LFN, Pegasus queries the replica catalog to get all the PFN's (replicas) associated with a LFN. Pegasus then calls out to a replica selector to select a replica amongst the various replicas returned. This property determines the replica selector to use for selecting the replicas.

                                                                                                                                                                                                                                                                                      Default
                                                                                                                                                                                                                                                                                         The selector orders the various candidate replica's according to the following rules

                                                                                                                                                                                                                                                                                         1. valid file URL's . That is URL's that have the site attribute matching the site where the executable *pegasus-transfer* is executed.

                                                                                                                                                                                                                                                                                         2. all URL's from preferred site (usually the compute site)

                                                                                                                                                                                                                                                                                         3. all other remotely accessible ( non file) URL's

                                                                                                                                                                                                                                                                                      Regex
                                                                                                                                                                                                                                                                                         This replica selector allows the user allows the user to specific regular expressions that can be used to rank various PFN's returned from the Replica Catalog for a particular LFN. This replica selector orders the replicas based on the rank. Lower the rank higher the preference.

                                                                                                                                                                                                                                                                                         The regular expressions are assigned different rank, that determine the order in which the expressions are employed. The rank values for the regex can expressed in user properties using the property.

                                                                                                                                                                                                                                                                                         ::

                                                                                                                                                                                                                                                                                            pegasus.selector.replica.regex.rank.[value]   regex-expression

                                                                                                                                                                                                                                                                                         The value is an integer value that denotes the rank of an expression with a rank value of 1 being the highest rank.

                                                                                                                                                                                                                                                                                         Please note that before applying any regular expressions on the PFN's, the file URL's that dont match the preferred site are explicitly filtered out.

                                                                                                                                                                                                                                                                                      Restricted
                                                                                                                                                                                                                                                                                         This replica selector, allows the user to specify good sites and bad sites for staging in data to a particular compute site. A good site for a compute site X, is a preferred site from which replicas should be staged to site X. If there are more than one good sites having a particular replica, then a random site is selected amongst these preferred sites.

                                                                                                                                                                                                                                                                                         A bad site for a compute site X, is a site from which replica's should not be staged. The reason of not accessing replica from a bad site can vary from the link being down, to the user not having permissions on that site's data.

                                                                                                                                                                                                                                                                                         The good \| bad sites are specified by the properties

                                                                                                                                                                                                                                                                                         ::

                                                                                                                                                                                                                                                                                            pegasus.replica.*.prefer.stagein.sites
                                                                                                                                                                                                                                                                                            pegasus.replica.*.ignore.stagein.sites

                                                                                                                                                                                                                                                                                         where the \* in the property name denotes the name of the compute site. A \* in the property key is taken to mean all sites.

                                                                                                                                                                                                                                                                                         The pegasus.replica.*.prefer.stagein.sites property takes precedence over pegasus.replica.*.ignore.stagein.sites property i.e. if for a site X, a site Y is specified both in the ignored and the preferred set, then site Y is taken to mean as only a preferred site for a site X.

                                                                                                                                                                                                                                                                                      Local
                                                                                                                                                                                                                                                                                         This replica selector prefers replicas from the local host and that start with a file: URL scheme. It is useful, when users want to stagin files to a remote site from your submit host using the Condor file transfer mechanism.
   **Property Key:**\ pegasus.selector.replica.*.ignore.stagein.sites\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 2.0 **Default :** (no default)\ **See Also :** pegasus.selector.replica\ **See Also :** pegasus.selector.replica.*.prefer.stagein.sites              A comma separated list of storage sites from which to never stage in data to a compute site. The property can apply to all or a single compute site, depending on how the \* in the property name is expanded.

                                                                                                                                                                                                                                                                                      The \* in the property name means all compute sites unless replaced by a site name.

                                                                                                                                                                                                                                                                                      For e.g setting pegasus.selector.replica.*.ignore.stagein.sites to usc means that ignore all replicas from site usc for staging in to any compute site. Setting pegasus.replica.isi.ignore.stagein.sites to usc means that ignore all replicas from site usc for staging in data to site isi.
   **Property Key:**\ pegasus.selector.replica.*.prefer.stagein.sites\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 2.0 **Default :** (no default)\ **See Also :** pegasus.selector.replica\ **See Also :** pegasus.selector.replica.*.ignore.stagein.sites              A comma separated list of preferred storage sites from which to stage in data to a compute site. The property can apply to all or a single compute site, depending on how the \* in the property name is expanded.

                                                                                                                                                                                                                                                                                      The \* in the property name means all compute sites unless replaced by a site name.

                                                                                                                                                                                                                                                                                      For e.g setting pegasus.selector.replica.*.prefer.stagein.sites to usc means that prefer all replicas from site usc for staging in to any compute site. Setting pegasus.replica.isi.prefer.stagein.sites to usc means that prefer all replicas from site usc for staging in data to site isi.
   **Property Key:**\ pegasus.selector.replica.regex.rank.[value]\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 2.3.0 **Default :** (no default)\ **See Also :** pegasus.selector.replica                                                                                Specifies the regex expressions to be applied on the PFNs returned for a particular LFN. Refer to

                                                                                                                                                                                                                                                                                      ::

                                                                                                                                                                                                                                                                                         http://java.sun.com/javase/6/docs/api/java/util/regex/Pattern.html

                                                                                                                                                                                                                                                                                      on information of how to construct a regex expression.

                                                                                                                                                                                                                                                                                      The [value] in the property key is to be replaced by an int value that designates the rank value for the regex expression to be applied in the Regex replica selector.

                                                                                                                                                                                                                                                                                      The example below indicates preference for file URL's over URL's referring to gridftp server at example.isi.edu

                                                                                                                                                                                                                                                                                      ::

                                                                                                                                                                                                                                                                                         pegasus.selector.replica.regex.rank.1 file://.*
                                                                                                                                                                                                                                                                                         pegasus.selector.replica.regex.rank.2 gsiftp://example\.isi\.edu.*
   ================================================================================================================================================================================================================================================================================== =====================================================================================================================================================================================================================================================================================================================================================================================================================================================

.. _site-sel-props:

Site Selection Properties
-------------------------

.. table:: Site Selection Properties

   =================================================================================================================================================================================================================================================================================================================================== =====================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================
   **Key Attributes**                                                                                                                                                                                                                                                                                                                  **Description**
   **Property Key:**\ pegasus.selector.site\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 2.0 **Type :**\ String **Default :** Random **See Also :** pegasus.selector.site.path\ **See Also :** pegasus.selector.site.timeout **See Also :** pegasus.selector.site.keep.tmp\ **See Also :**\ pegasus.selector.site.env.\* The site selection in Pegasus can be on basis of any of the following strategies.

                                                                                                                                                                                                                                                                                                                                       Random
                                                                                                                                                                                                                                                                                                                                          In this mode, the jobs will be randomly distributed among the sites that can execute them.
                                                                                                                                                                                                                                                                                                                                       RoundRobin
                                                                                                                                                                                                                                                                                                                                          In this mode. the jobs will be assigned in a round robin manner amongst the sites that can execute them. Since each site cannot execute everytype of job, the round robin scheduling is done per level on a sorted list. The sorting is on the basis of the number of jobs a particular site has been assigned in that level so far. If a job cannot be run on the first site in the queue (due to no matching entry in the transformation catalog for the transformation referred to by the job), it goes to the next one and so on. This implementation defaults to classic round robin in the case where all the jobs in the workflow can run on all the sites.
                                                                                                                                                                                                                                                                                                                                       NonJavaCallout
                                                                                                                                                                                                                                                                                                                                          In this mode, Pegasus will callout to an external site selector.In this mode a temporary file is prepared containing the job information that is passed to the site selector as an argument while invoking it. The path to the site selector is specified by setting the property pegasus.site.selector.path. The environment variables that need to be set to run the site selector can be specified using the properties with a pegasus.site.selector.env. prefix. The temporary file contains information about the job that needs to be scheduled. It contains key value pairs with each key value pair being on a new line and separated by a =.

                                                                                                                                                                                                                                                                                                                                          The following pairs are currently generated for the site selector temporary file that is generated in the NonJavaCallout.

                                                                                                                                                                                                                                                                                                                                          ============== ==============================================================================================================================================================================================================================
                                                                                                                                                                                                                                                                                                                                          version        is the version of the site selector api,currently 2.0.
                                                                                                                                                                                                                                                                                                                                          transformation is the fully-qualified definition identifier for the transformation (TR) namespace::name:version.
                                                                                                                                                                                                                                                                                                                                          derivation     is teh fully qualified definition identifier for the derivation (DV), namespace::name:version.
                                                                                                                                                                                                                                                                                                                                          job.level      is the job's depth in the tree of the workflow DAG.
                                                                                                                                                                                                                                                                                                                                          job.id         is the job's ID, as used in the DAX file.
                                                                                                                                                                                                                                                                                                                                          resource.id    is a site handle, followed by whitespace, followed by a gridftp server. Typically, each gridftp server is enumerated once, so you may have multiple occurances of the same site. There can be multiple occurances of this key.
                                                                                                                                                                                                                                                                                                                                          input.lfn      is an input LFN, optionally followed by a whitespace and file size. There can be multiple occurances of this key,one for each input LFN required by the job.
                                                                                                                                                                                                                                                                                                                                          wf.name        label of the dax, as found in the DAX's root element. wf.index is the DAX index, that is incremented for each partition in case of deferred planning.
                                                                                                                                                                                                                                                                                                                                          wf.time        is the mtime of the workflow.
                                                                                                                                                                                                                                                                                                                                          wf.manager     is the name of the workflow manager being used .e.g condor
                                                                                                                                                                                                                                                                                                                                          vo.name        is the name of the virtual organization that is running this workflow. It is currently set to NONE
                                                                                                                                                                                                                                                                                                                                          vo.group       unused at present and is set to NONE.
                                                                                                                                                                                                                                                                                                                                          \
                                                                                                                                                                                                                                                                                                                                          ============== ==============================================================================================================================================================================================================================

                                                                                                                                                                                                                                                                                                                                       Group
                                                                                                                                                                                                                                                                                                                                          In this mode, a group of jobs will be assigned to the same site that can execute them. The use of the PEGASUS profile key group in the dax, associates a job with a particular group. The jobs that do not have the profile key associated with them, will be put in the default group. The jobs in the default group are handed over to the "Random" Site Selector for scheduling.
                                                                                                                                                                                                                                                                                                                                       Heft
                                                                                                                                                                                                                                                                                                                                          In this mode, a version of the HEFT processor scheduling algorithm is used to schedule jobs in the workflow to multiple grid sites. The implementation assumes default data communication costs when jobs are not scheduled on to the same site. Later on this may be made more configurable.

                                                                                                                                                                                                                                                                                                                                          The runtime for the jobs is specified in the transformation catalog by associating the pegasus profile key runtime with the entries.

                                                                                                                                                                                                                                                                                                                                          The number of processors in a site is picked up from the attribute idle-nodes associated with the vanilla jobmanager of the site in the site catalog.
   **Property Key:**\ pegasus.selector.site.path\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 2.0 **Default :** (no default)                                                                                                                                                                                             If one calls out to an external site selector using the NonJavaCallout mode, this refers to the path where the site selector is installed. In case other strategies are used it does not need to be set.
   **Property Key:**\ pegasus.selector.site.env.\*\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 2.0 **Default :** (no default)                                                                                                                                                                                           The environment variables that need to be set while callout to the site selector. These are the variables that the user would set if running the site selector on the command line. The name of the environment variable is got by stripping the keys of the prefix "pegasus.site.selector.env." prefix from them. The value of the environment variable is the value of the property.

                                                                                                                                                                                                                                                                                                                                       e.g pegasus.site.selector.path.LD_LIBRARY_PATH /globus/lib would lead to the site selector being called with the LD_LIBRARY_PATH set to /globus/lib.
   **Property Key:**\ pegasus.selector.site.timeout\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 2.3.0 **Default :** 60\ **See Also :** pegasus.selector.site                                                                                                                                                            It sets the number of seconds Pegasus waits to hear back from an external site selector using the NonJavaCallout interface before timing out.
   **Property Key:**\ pegasus.selector.site.keep.tmp\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 2.3.0 **Values** : onerror|always|never **Default :** onerror\ **See Also :** pegasus.selector.site                                                                                                                    It determines whether Pegasus deletes the temporary input files that are generated in the temp directory or not. These temporary input files are passed as input to the external site selectors.

                                                                                                                                                                                                                                                                                                                                       A temporary input file is created for each that needs to be scheduled.
   =================================================================================================================================================================================================================================================================================================================================== =====================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================

.. _data-conf-props:

Data Staging Configuration Properties
-------------------------------------

.. table:: Data Configuration Properties

   =================================================================================================================================================================================================================================================================== ===========================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================
   **Key Attributes**                                                                                                                                                                                                                                                  **Description**
   **Property Key:**\ pegasus.data.configuration\ **Profile Key:**\ data.configuration\ **Scope :** Properties, Site Catalog **Since :** 4.0.0 **Values** : sharedfs|nonsharedfs|condorio **Default :** sharedfs\ **See Also :** pegasus.transfer.bypass.input.staging This property sets up Pegasus to run in different environments. For Pegasus 4.5.0 and above, users can set the pegasus profile data.configuration with the sites in their site catalog, to run multisite workflows with each site having a different data configuration.

                                                                                                                                                                                                                                                                       sharedfs
                                                                                                                                                                                                                                                                          If this is set, Pegasus will be setup to execute jobs on the shared filesystem on the execution site. This assumes, that the head node of a cluster and the worker nodes share a filesystem. The staging site in this case is the same as the execution site. Pegasus adds a create dir job to the executable workflow that creates a workflow specific directory on the shared filesystem . The data transfer jobs in the executable workflow ( stage_in\_ , stage_inter\_ , stage_out\_ ) transfer the data to this directory.The compute jobs in the executable workflow are launched in the directory on the shared filesystem.
                                                                                                                                                                                                                                                                       condorio
                                                                                                                                                                                                                                                                          If this is set, Pegasus will be setup to run jobs in a pure condor pool, with the nodes not sharing a filesystem. Data is staged to the compute nodes from the submit host using Condor File IO. The planner is automatically setup to use the submit host ( site local ) as the staging site. All the auxillary jobs added by the planner to the executable workflow ( create dir, data stagein and stage-out, cleanup ) jobs refer to the workflow specific directory on the local site. The data transfer jobs in the executable workflow ( stage_in\_ , stage_inter\_ , stage_out\_ ) transfer the data to this directory. When the compute jobs start, the input data for each job is shipped from the workflow specific directory on the submit host to compute/worker node using Condor file IO. The output data for each job is similarly shipped back to the submit host from the compute/worker node. This setup is particularly helpful when running workflows in the cloud environment where setting up a shared filesystem across the VM's may be tricky.
                                                                                                                                                                                                                                                                          ::

                                                                                                                                                                                                                                                                             pegasus.gridstart                    PegasusLite
                                                                                                                                                                                                                                                                             pegasus.transfer.worker.package      true

                                                                                                                                                                                                                                                                       nonsharedfs
                                                                                                                                                                                                                                                                          If this is set, Pegasus will be setup to execute jobs on an execution site without relying on a shared filesystem between the head node and the worker nodes. You can specify staging site ( using --staging-site option to pegasus-plan) to indicate the site to use as a central storage location for a workflow. The staging site is independant of the execution sites on which a workflow executes. All the auxillary jobs added by the planner to the executable workflow ( create dir, data stagein and stage-out, cleanup ) jobs refer to the workflow specific directory on the staging site. The data transfer jobs in the executable workflow ( stage_in\_ , stage_inter\_ , stage_out\_ ) transfer the data to this directory. When the compute jobs start, the input data for each job is shipped from the workflow specific directory on the submit host to compute/worker node using pegasus-transfer. The output data for each job is similarly shipped back to the submit host from the compute/worker node. The protocols supported are at this time SRM, GridFTP, iRods, S3. This setup is particularly helpful when running workflows on OSG where most of the execution sites don't have enough data storage. Only a few sites have large amounts of data storage exposed that can be used to place data during a workflow run. This setup is also helpful when running workflows in the cloud environment where setting up a shared filesystem across the VM's may be tricky. On loading this property, internally the following properies are set
                                                                                                                                                                                                                                                                          ::

                                                                                                                                                                                                                                                                             pegasus.gridstart                    PegasusLite
                                                                                                                                                                                                                                                                             pegasus.transfer.worker.package      true
   **Property Key:**\ pegasus.transfer.bypass.input.staging\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 4.3.0 **Type :**\ Boolean **Default :** false\ **See Also :** pegasus.data.configuration                                                        When executiing in a non shared filesystem setup i.e data configuration set to nonsharedfs or condorio, Pegasus always stages the input files through the staging site i.e the stage-in job stages in data from the input site to the staging site. The PegasusLite jobs that start up on the worker nodes, then pull the input data from the staging site for each job.

                                                                                                                                                                                                                                                                       This property can be used to setup the PegasusLite jobs to pull input data directly from the input site without going through the staging server. This is based on the assumption that the worker nodes can access the input site. If users set this to true, they should be aware that the access to the input site is no longer throttled ( as in case of stage in jobs). If large number of compute jobs start at the same time in a workflow, the input server will see a connection from each job.
   =================================================================================================================================================================================================================================================================== ===========================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================

.. _transfer-props:

Transfer Configuration Properties
---------------------------------

.. table:: Transfer Configuration Properties

   ================================================================================================================================================================================================================================= =========================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================
   **Key Attributes**                                                                                                                                                                                                                **Description**
   **Property Key:**\ pegasus.transfer.*.impl\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 2.0.0 **Values** : Transfer|GUC **Default :** Transfer\ **See Also :** pegasus.transfer.refiner
   **Property Key:**\ pegasus.transfer.arguments\ **Profile Key:**\ transfer.arguments\ **Scope :** Properties **Since :** 2.0.0 **Type :**\ String\ **Default :** (no default)\ **See Also :** pegasus.transfer.lite.arguments      This determines the extra arguments with which the transfer implementation is invoked. The transfer executable that is invoked is dependant upon the transfer mode that has been selected. The property can be overloaded by associated the pegasus profile key transfer.arguments either with the site in the site catalog or the corresponding transfer executable in the transformation catalog.
   **Property Key:**\ pegasus.transfer.threads\ **Profile Key:**\ transfer.threads\ **Scope :** Properties **Since :** 4.4.0 **Type :**\ Integer\ **Default :** 2                                                                    This property set the number of threads pegasus-transfer uses to transfer the files. This property to applies to the separate data transfer nodes that are added by Pegasus to the executable workflow. The property can be overloaded by associated the pegasus profile key transfer.threads either with the site in the site catalog or the corresponding transfer executable in the transformation catalog.
   **Property Key:**\ pegasus.transfer.lite.arguments\ **Profile Key:**\ transfer.lite.arguments\ **Scope :** Properties **Since :** 4.4.0 **Type :**\ String\ **Default :** (no default)\ **See Also :** pegasus.transfer.arguments This determines the extra arguments with which the PegasusLite transfer implementation is invoked. The transfer executable that is invoked is dependant upon the PegasusLite transfer implementation that has been selected.
   **Property Key:**\ pegasus.transfer.worker.package\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 2.0.0 **Type :**\ Boolean\ **Default :** false\ **See Also :** pegasus.data.configuration                           By default, Pegasus relies on the worker package to be installed in a directory accessible to the worker nodes on the remote sites . Pegasus uses the value of PEGASUS_HOME environment profile in the site catalog for the remote sites, to then construct paths to pegasus auxillary executables like kickstart, pegasus-transfer, seqexec etc.

                                                                                                                                                                                                                                     If the Pegasus worker package is not installed on the remote sites users can set this property to true to get Pegasus to deploy worker package on the nodes.

                                                                                                                                                                                                                                     In the case of sharedfs setup, the worker package is deployed on the shared scratch directory for the workflow , that is accessible to all the compute nodes of the remote sites.

                                                                                                                                                                                                                                     When running in nonsharefs environments, the worker package is first brought to the submit directory and then transferred to the worker node filesystem using Condor file IO.
   **Property Key:**\ pegasus.transfer.worker.package.autodownload\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 4.6.1 **Type :**\ Boolean\ **Default :** true\ **See Also :** pegasus.transfer.worker.package          If PegasusLite does not find a worker package install matching the pegasus lite job on the worker node, it automatically downloads the correct worker package from the Pegasus website. However, this can mask user errors in configuration. This property can be set to false to disable auto downloads.
   **Property Key:**\ pegasus.transfer.worker.package.strict\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 4.6.1 **Type :**\ Boolean\ **Default :** true\ **See Also :** pegasus.transfer.worker.package                In PegasusLite mode, the pegasus worker package for the jobs is shipped along with the jobs. This property controls whether PegasusLite will do a strict match against the architecture and os on the local worker node, along with pegasus version. If the strict match fails, then PegasusLite will revert to the pegasus website to download the correct worker package.
   **Property Key:**\ pegasus.transfer.links\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 2.0.0 **Type :**\ Boolean\ **Default :** false                                                                               If this is set, and the transfer implementation is set to Transfer i.e. using the transfer executable distributed with the PEGASUS. On setting this property, if Pegasus while fetching data from the Replica Catalog sees a "site" attribute associated with the PFN that matches the execution site on which the data has to be transferred to, Pegasus instead of the URL returned by the Replica Catalog replaces it with a file based URL. This is based on the assumption that the if the "site" attributes match, the filesystems are visible to the remote execution directory where input data resides. On seeing both the source and destination urls as file based URLs the transfer executable spawns a job that creates a symbolic link by calling ln -s on the remote site.
   **Property Key:**\ pegasus.transfer.*.remote.sites\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 2.0.0 **Type :**\ comma separated list of sites\ **Default :** (no default)                                         By default Pegasus looks at the source and destination URL's for to determine whether the associated transfer job runs on the submit host or the head node of a remote site, with preference set to run a transfer job to run on submit host.

                                                                                                                                                                                                                                     Pegasus will run transfer jobs on the remote sites

                                                                                                                                                                                                                                     ::

                                                                                                                                                                                                                                        -  if the file server for the compute site is a file server i.e url prefix file://
                                                                                                                                                                                                                                        -  symlink jobs need to be added that require the symlink transfer jobs to
                                                                                                                                                                                                                                        be run remotely.

                                                                                                                                                                                                                                     This property can be used to change the default behaviour of Pegasus and force pegasus to run different types of transfer jobs for the sites specified on the remote site.

                                                                                                                                                                                                                                     The table below illustrates all the possible variations of the property.

                                                                                                                                                                                                                                     ====================================== ===============================
                                                                                                                                                                                                                                     Property Name                          Applies to
                                                                                                                                                                                                                                     pegasus.transfer.stagein.remote.sites  the stage in transfer jobs
                                                                                                                                                                                                                                     pegasus.transfer.stageout.remote.sites the stage out transfer jobs
                                                                                                                                                                                                                                     pegasus.transfer.inter.remote.sites    the inter site transfer jobs
                                                                                                                                                                                                                                     pegasus.transfer.*.remote.sites        apply to types of transfer jobs
                                                                                                                                                                                                                                     \
                                                                                                                                                                                                                                     ====================================== ===============================

                                                                                                                                                                                                                                     In addition \* can be specified as a property value, to designate that it applies to all sites.
   **Property Key:**\ pegasus.transfer.staging.delimiter\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 2.0.0 **Type :**\ String\ **Default :** :                                                                        Pegasus supports executable staging as part of the workflow. Currently staging of statically linked executables is supported only. An executable is normally staged to the work directory for the workflow/partition on the remote site. The basename of the staged executable is derived from the namespace,name and version of the transformation in the transformation catalog. This property sets the delimiter that is used for the construction of the name of the staged executable.
   **Property Key:**\ pegasus.transfer.disable.chmod.sites\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 2.0.0 **Type :**\ comma separated list of sites\ **Default :** (no default)                                    During staging of executables to remote sites, chmod jobs are added to the workflow. These jobs run on the remote sites and do a chmod on the staged executable. For some sites, this maynot be required. The permissions might be preserved, or there maybe an automatic mechanism that does it.

                                                                                                                                                                                                                                     This property allows you to specify the list of sites, where you do not want the chmod jobs to be executed. For those sites, the chmod jobs are replaced by NoOP jobs. The NoOP jobs are executed by Condor, and instead will immediately have a terminate event written to the job log file and removed from the queue.
   **Property Key:**\ pegasus.transfer.setup.source.base.url\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 2.0.0 **Type :**\ URL **Default :** (no default)                                                             This property specifies the base URL to the directory containing the Pegasus worker package builds. During Staging of Executable, the Pegasus Worker Package is also staged to the remote site. The worker packages are by default pulled from the http server at pegasus.isi.edu. This property can be used to override the location from where the worker package are staged. This maybe required if the remote computes sites don't allows files transfers from a http server.
   ================================================================================================================================================================================================================================= =========================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================

.. _monitoring-props:

Monitoring Properties
---------------------

.. table:: Monitoring Properties

   ==================================================================================================================================================================================================================================================================== =================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================
   **Key Attributes**                                                                                                                                                                                                                                                   **Description**
   **Property Key:**\ pegasus.monitord.events\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 3.0.2 **Type** : String **Default :** true\ **See Also :**\ pegasus.catalog.workflow.url                                                                       This property determines whether pegasus-monitord generates log events. If log events are disabled using this property, no bp file, or database will be created, even if the pegasus.monitord.output property is specified.
   **Property Key:**\ pegasus.catalog.workflow.url\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 4.5 **Type** : String **Default :** SQlite database in submit directory. **See Also :** pegasus.monitord.events                                           This property specifies the destination for generated log events in pegasus-monitord. By default, events are stored in a sqlite database in the workflow directory, which will be created with the workflow's name, and a ".stampede.db" extension. Users can specify an alternative database by using a SQLAlchemy connection string. Details are available at:

                                                                                                                                                                                                                                                                        ::

                                                                                                                                                                                                                                                                           http://www.sqlalchemy.org/docs/05/reference/dialects/index.html

                                                                                                                                                                                                                                                                        It is important to note that users will need to have the appropriate db interface library installed. Which is to say, SQLAlchemy is a wrapper around the mysql interface library (for instance), it does not provide a MySQL driver itself. The Pegasus distribution includes both SQLAlchemy and the SQLite Python driver. As a final note, it is important to mention that unlike when using SQLite databases, using SQLAlchemy with other database servers, e.g. MySQL or Postgres , the target database needs to exist. Users can also specify a file name using this property in order to create a file with the log events.

                                                                                                                                                                                                                                                                        Example values for the SQLAlchemy connection string for various end points are listed below

                                                                                                                                                                                                                                                                        ===================== ============================================
                                                                                                                                                                                                                                                                        SQL Alchemy End Point Example Value
                                                                                                                                                                                                                                                                        Netlogger BP File     file:///submit/dir/myworkflow.bp
                                                                                                                                                                                                                                                                        SQL Lite Database     sqlite:///submit/dir/myworkflow.db
                                                                                                                                                                                                                                                                        MySQL Database        mysql://user:password@host:port/databasename
                                                                                                                                                                                                                                                                        \
                                                                                                                                                                                                                                                                        ===================== ============================================
   **Property Key:**\ pegasus.catalog.master.url\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 4.2 **Type** : String **Default :** sqlite database in $HOME/.pegasus/workflow.db\ **See Also :** pegasus.catalog.workflow.url                              This property specifies the destination for the workflow dashboard database. By default, the workflow dashboard datbase defaults to a sqlite database named workflow.db in the $HOME/.pegasus directory. This is database is shared for all workflows run as a particular user. Users can specify an alternative database by using a SQLAlchemy connection string. Details are available at:

                                                                                                                                                                                                                                                                        ::

                                                                                                                                                                                                                                                                           http://www.sqlalchemy.org/docs/05/reference/dialects/index.html

                                                                                                                                                                                                                                                                        It is important to note that users will need to have the appropriate db interface library installed. Which is to say, SQLAlchemy is a wrapper around the mysql interface library (for instance), it does not provide a MySQL driver itself. The Pegasus distribution includes both SQLAlchemy and the SQLite Python driver. As a final note, it is important to mention that unlike when using SQLite databases, using SQLAlchemy with other database servers, e.g. MySQL or Postgres , the target database needs to exist. Users can also specify a file name using this property in order to create a file with the log events.

                                                                                                                                                                                                                                                                        Example values for the SQLAlchemy connection string for various end points are listed below

                                                                                                                                                                                                                                                                        ===================== ============================================
                                                                                                                                                                                                                                                                        SQL Alchemy End Point Example Value
                                                                                                                                                                                                                                                                        SQL Lite Database     sqlite:///shared/myworkflow.db
                                                                                                                                                                                                                                                                        MySQL Database        mysql://user:password@host:port/databasename
                                                                                                                                                                                                                                                                        \
                                                                                                                                                                                                                                                                        ===================== ============================================
   **Property Key:**\ pegasus.monitord.output\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 3.0.2 **Type** : String **Default :** SQlite database in submit directory. **See Also :** pegasus.monitord.events                                              This property has been deprecated in favore of pegasus.catalog.workflow.url that introduced in 4.5 release. Support for this property will be dropped in future releases.
   **Property Key:**\ pegasus.dashboard.output\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 4.2 **Type** : String **Default :** sqlite database in $HOME/.pegasus/workflow.db\ **See Also :** pegasus.monitord.output                                     This property has been deprecated in favore of pegasus.catalog.master.url that introduced in 4.5 release. Support for this property will be dropped in future releases.
   **Property Key:**\ pegasus.monitord.notifications\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 3.1.0 **Type :**\ Boolean **Default :** true\ **See Also :** pegasus.monitord.notifications.max\ **See Also :** pegasus.monitord.notifications.timeout  This property determines how many notification scripts pegasus-monitord will call concurrently. Upon reaching this limit, pegasus-monitord will wait for one notification script to finish before issuing another one. This is a way to keep the number of processes under control at the submit host. Setting this property to 0 will disable notifications completely.
   **Property Key:**\ pegasus.monitord.notifications.max\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 3.1.0 **Type :**\ Integer **Default :** 10\ **See Also :** pegasus.monitord.notifications **See Also :** pegasus.monitord.notifications.timeout     This property determines whether pegasus-monitord processes notifications. When notifications are enabled, pegasus-monitord will parse the .notify file generated by pegasus-plan and will invoke notification scripts whenever conditions matches one of the notifications.
   **Property Key:**\ pegasus.monitord.notifications.timeout\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 3.1.0 **Type :**\ Integer **Default :** true\ **See Also :** pegasus.monitord.notifications.\ **See Also :** pegasus.monitord.notifications.max This property determines how long will pegasus-monitord let notification scripts run before terminating them. When this property is set to 0 (default), pegasus-monitord will not terminate any notification scripts, letting them run indefinitely. If some notification scripts missbehave, this has the potential problem of starving pegasus-monitord's notification slots (see the pegasus.monitord.notifications.max property), and block further notifications. In addition, users should be aware that pegasus-monitord will not exit until all notification scripts are finished.
   **Property Key:**\ pegasus.monitord.stdout.disable.parsing\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 3.1.1 **Type :**\ Boolean **Default :** false                                                                                                  By default, pegasus-monitord parses the stdout/stderr section of the kickstart to populate the applications captured stdout and stderr in the job instance table for the stampede schema. For large workflows, this may slow down monitord especially if the application is generating a lot of output to it's stdout and stderr. This property, can be used to turn of the database population.
   **Property Key:**\ pegasus.monitord.arguments\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 4.6 **Type :**\ String **Default :** N/A                                                                                                                    This property specifies additional command-line arguments that should be passed to pegasus-monitord at startup. These additional arguments are appended to the arguments given to pegasus-monitord.
   ==================================================================================================================================================================================================================================================================== =================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================

.. _job-clustering-props:

Job Clustering Properties
-------------------------

.. table:: Job Clustering Properties

   =========================================================================================================================================================================================================================================================================================== =========================================================================================================================================================================================================================================================================================================================================================================================================================
   **Key Attributes**                                                                                                                                                                                                                                                                          **Description**
   **Property Key:**\ pegasus.clusterer.job.aggregator\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 2.0 **Type** : String **Values** : seqexec|mpiexec|AWSBatch **Default :** seqexec                                                                                            A large number of workflows executed through the Virtual Data System, are composed of several jobs that run for only a few seconds or so. The overhead of running any job on the grid is usually 60 seconds or more. Hence, it makes sense to collapse small independent jobs into a larger job. This property determines, the executable that will be used for running the larger job on the remote site.

                                                                                                                                                                                                                                                                                               seqexec
                                                                                                                                                                                                                                                                                                  In this mode, the executable used to run the merged job is "pegasus-cluster" that runs each of the smaller jobs sequentially on the same node. The executable "pegasus-cluster" is a PEGASUS tool distributed in the PEGASUS worker package, and can be usually found at {pegasus.home}/bin/seqexec.
                                                                                                                                                                                                                                                                                               mpiexec
                                                                                                                                                                                                                                                                                                  In this mode, the executable used to run the merged job is "pegasus-mpi-cluster" (PMC) that runs the smaller jobs via mpi on n nodes where n is the nodecount associated with the merged job. The executable "pegasus-mpi-cluster" is a PEGASUS tool distributed in the PEGASUS distribution and is built only if mpi compiler is available.
                                                                                                                                                                                                                                                                                               AWSBatch
                                                                                                                                                                                                                                                                                                  In this mode, the executable used to run the merged job is "pegasus-aws-batch" that runs in local universe on the submit and runs the jobs making up the cluster on AWS Batch.
   **Property Key:**\ pegasus.clusterer.job.aggregator.seqexec.log\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 2.3 **Type :**\ Boolean **Default :** false\ **See Also :** pegasus.clusterer.job.aggregator\ **See Also :** pegasus.clusterer.job.aggregator.seqexec.log.global The tool pegasus-cluster logs the progress of the jobs that are being run by it in a progress file on the remote cluster where it is executed.

                                                                                                                                                                                                                                                                                               This property sets the Boolean flag, that indicates whether to turn on the logging or not.
   **Property Key:**\ pegasus.clusterer.job.aggregator.seqexec.log\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 2.3 **Type :**\ Boolean **Default :** false\ **See Also :** pegasus.clusterer.job.aggregator\ **See Also :** pegasus.clusterer.job.aggregator.seqexec.log.global The tool pegasus-cluster logs the progress of the jobs that are being run by it in a progress file on the remote cluster where it is executed. The progress log is useful for you to track the progress of your computations and remote grid debugging. The progress log file can be shared by multiple pegasus-cluster jobs that are running on a particular cluster as part of the same workflow. Or it can be per job.

                                                                                                                                                                                                                                                                                               This property sets the Boolean flag, that indicates whether to have a single global log for all the pegasus-cluster jobs on a particular cluster or progress log per job.
   **Property Key:**\ pegasus.clusterer.job.aggregator.seqexec.firstjobfail\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 2.2 **Type :**\ Boolean **Default :** true\ **See Also :** pegasus.clusterer.job.aggregator                                                             By default "pegasus-cluster" does not stop execution even if one of the clustered jobs it is executing fails. This is because "pegasus-cluster" tries to get as much work done as possible.

                                                                                                                                                                                                                                                                                               This property sets the Boolean flag, that indicates whether to make "pegasus-cluster" stop on the first job failure it detects.
   **Property Key:**\ pegasus.clusterer.allow.single\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 4.9 **Type** : Boolean **Default :** False
   **Property Key:**\ pegasus.clusterer.label.key\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 2.0 **Type** : String **Default :** label
   =========================================================================================================================================================================================================================================================================================== =========================================================================================================================================================================================================================================================================================================================================================================================================================

.. _logging-props:

Logging Properties
------------------

.. table:: Logging Properties

   =========================================================================================================================================================================================================================== =========================================================================================================================================================================================================================================================================================
   **Key Attributes**                                                                                                                                                                                                          **Description**
   **Property Key:**\ pegasus.log.manager\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 2.2.0 **Type** : String **Values** : Default|Log4J **Default :** Default\ **See Also :**\ pegasus.log.manager.formatter   This property sets the logging implementation to use for logging.

                                                                                                                                                                                                                               Default
                                                                                                                                                                                                                                  This implementation refers to the legacy Pegasus logger, that logs directly to stdout and stderr. It however, does have the concept of levels similar to log4j or syslog.
                                                                                                                                                                                                                               Log4j
                                                                                                                                                                                                                                  This implementation, uses Log4j to log messages. The log4j properties can be specified in a properties file, the location of which is specified by the property
                                                                                                                                                                                                                                  ::

                                                                                                                                                                                                                                     pegasus.log.manager.log4j.conf
   **Property Key:**\ pegasus.log.manager.formatter\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 2.2.0 **Type** : String **Values** : Simple|Netlogger **Default :** Simple\ **See Also :**\ pegasus.log.manager
   **Property Key:**\ pegasus.log.\*\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 2.0 **Type :**\ file path **Default :** no default                                                                             This property sets the path to the file where all the logging for Pegasus can be redirected to. Both stdout and stderr are logged to the file specified.
   **Property Key:**\ pegasus.log.memory.usage\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 4.3.4 **Type :**\ Boolean **Default :** false                                                                        This property if set to true, will result in the planner writing out JVM heap memory statistics at the end of the planning process at the INFO level. This is useful, if users want to fine tune their java memory settings by setting JAVA_HEAPMAX and JAVA_HEAPMIN for large workflows.
   **Property Key:**\ pegasus.metrics.app\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 4.3.0 **Type :**\ String **Default :** (no default)                                                                       This property namespace allows users to pass application level metrics to the metrics server. The value of this property is the name of the application.

                                                                                                                                                                                                                               Additional application specific attributes can be passed by using the prefix pegasus.metrics.app

                                                                                                                                                                                                                               ::

                                                                                                                                                                                                                                  pegasus.metrics.app.[arribute-name]       attribute-value

                                                                                                                                                                                                                               Note: the attribute cannot be named name. This attribute is automatically assigned the value from pegasus.metrics.app
   =========================================================================================================================================================================================================================== =========================================================================================================================================================================================================================================================================================

.. _cleanup-props:

Cleanup Properties
------------------

.. table:: Cleanup Properties

   ============================================================================================================================================================================================== ===================================================================================================================================================================================================================================================================================================================================================================================================================
   **Key Attributes**                                                                                                                                                                             **Description**
   **Property Key:**\ pegasus.file.cleanup.strategy **Profile Key:**\ N/A **Scope :**\ Properties **Since :**\ 2.2- **Type :**\ String **Default :**\ InPlace                                     This property is used to select the strategy of how the cleanup nodes are added to the executable workflow.

                                                                                                                                                                                                  InPlace
                                                                                                                                                                                                     The default cleanup strategy. Adds cleanup nodes per level of the workflow.
                                                                                                                                                                                                  Constraint
                                                                                                                                                                                                     Adds cleanup nodes to constraint the amount of storage space used by a workflow.

                                                                                                                                                                                                  Note that this property is overridden by the --cleanup option used in pegasus-plan.
   **Property Key:**\ pegasus.file.cleanup.impl\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 2.2 **Type :**\ String **Default :** Cleanup                                           This property is used to select the executable that is used to create the working directory on the compute sites.

                                                                                                                                                                                                  Cleanup
                                                                                                                                                                                                     The default executable that is used to delete files is the "pegasus-transfer" executable shipped with Pegasus. It is found at $PEGASUS_HOME/bin/pegasus-transfer in the Pegasus distribution. An entry for transformation pegasus::dirmanager needs to exist in the Transformation Catalog or the PEGASUS_HOME environment variable should be specified in the site catalog for the sites for this mode to work.
                                                                                                                                                                                                  RM
                                                                                                                                                                                                     This mode results in the rm executable to be used to delete files from remote directories. The rm executable is standard on \*nix systems and is usually found at /bin/rm location.
   **Property Key:**\ pegasus.file.cleanup.clusters.num\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 4.2.0 **Type :**\ Integer                                                      In case of the InPlace strategy for adding the cleanup nodes to the workflow, this property specifies the maximum number of cleanup jobs that are added to the executable workflow on each level.
   **Property Key:**\ pegasus.file.cleanup.clusters.size\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 4.2.0 **Type :**\ Integer **Default :** 2                                     In case of the InPlace strategy this property sets the number of cleanup jobs that get clustered into a bigger cleanup job. This parameter is only used if pegasus.file.cleanup.clusters.num is not set.
   **Property Key:**\ pegasus.file.cleanup.scope\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 2.3.0 **Type :**\ Enumeration **Value :**\ fullahead|deferred **Default :** fullahead By default in case of deferred planning InPlace file cleanup is turned OFF. This is because the cleanup algorithm does not work across partitions. This property can be used to turn on the cleanup in case of deferred planning.

                                                                                                                                                                                                  fullahead
                                                                                                                                                                                                     This is the default scope. The pegasus cleanup algorithm does not work across partitions in deferred planning. Hence the cleanup is always turned OFF , when deferred planning occurs and cleanup scope is set to full ahead.
                                                                                                                                                                                                  deferred
                                                                                                                                                                                                     If the scope is set to deferred, then Pegasus will not disable file cleanup in case of deferred planning. This is useful for scenarios where the partitions themselves are independant ( i.e. dont share files ). Even if the scope is set to deferred, users can turn off cleanup by specifying --nocleanup option to pegasus-plan.
   **Property Key:**\ pegasus.file.cleanup.constraint.*.maxspace **Profile Key:**\ N/A **Scope :**\ Properties **Since :**\ 4.6.0 **Type :**\ String **Default :**\ 10737418240                   This property is used to set the maximum avaialble space (i.e., constraint) per site in Bytes. The \* in the property name denotes the name of the compute site. A \* in the property key is taken to mean all sites.
   **Property Key:**\ pegasus.file.cleanup.constraint.deferstageins **Profile Key:**\ N/A **Scope :**\ Properties **Since :**\ 4.6.0 **Type :**\ Boolean **Default :**\ False                     This property is used to determine whether stage in jobs may be deferred. If this property is set to False (default), all stage in jobs will be marked as executing on the current compute site and will be executed before any task. This property has no effect when running in a multi site case.
   **Property Key:**\ pegasus.file.cleanup.constraint.csv **Profile Key:**\ N/A **Scope :**\ Properties **Since :**\ 4.6.1 **Type :**\ String **Default :**\ (no default)                         This property is used to specify a CSV file with a list of LFNs and their respective sizes in Bytes. The CSV file must be composed of two columns: **filename** and **length**.
   ============================================================================================================================================================================================== ===================================================================================================================================================================================================================================================================================================================================================================================================================

.. _aws-batch-props:

AWS Batch Properties
--------------------

.. table:: Miscellaneous Properties

   ======================================================================================================================================================================= ===============================================================================================================
   **Key Attributes**                                                                                                                                                      **Description**
   **Property Key:**\ pegasus.aws.account\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 4.9.0 **Type :**\ String **Default :** (no default)                   This property is used to specify the amazon account under which you are running jobs.
   **Property Key:**\ pegasus.aws.region\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 4.9.0 **Type :**\ String **Default :** (no default)                    This property is used to specify the amazon region in which you are running jobs.
   **Property Key:**\ pegasus.aws.batch.job_definition\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 4.9.0 **Type :**\ String **Default :** (no default)      This property is used to specify

                                                                                                                                                                           -  the JSON file containing job definition to register for executing jobs **OR**

                                                                                                                                                                           -  the ARN of existing job definition **OR**

                                                                                                                                                                           -  basename of an existing job definition
   **Property Key:**\ pegasus.aws.batch.compute_environment\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 4.9.0 **Type :**\ String **Default :** (no default) This property is used to specify

                                                                                                                                                                           -  the JSON file containing compute environment to register for executing jobs **OR**

                                                                                                                                                                           -  the ARN of existing compute environment **OR**

                                                                                                                                                                           -  basename of an existing compute environment
   **Property Key:**\ pegasus.aws.batch.job_queue\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 4.9.0 **Type :**\ String **Default :** (no default)           This property is used to specify

                                                                                                                                                                           -  the JSON file containing Job Queue to use for executing jobs **OR**

                                                                                                                                                                           -  the ARN of existing job queue **OR**

                                                                                                                                                                           -  basename of an existing job queue
   **Property Key:**\ pegasus.aws.batch.s3_bucket\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 4.9.0 **Type :**\ URL **Default :** (no default)              This property is used to specify the S3 Bucket URL to use for data transfers while executing jobs on AWS Batch.
   ======================================================================================================================================================================= ===============================================================================================================

.. _misc-props:

Miscellaneous Properties
------------------------

.. table:: Miscellaneous Properties

   =========================================================================================================================================================================================================================== ===========================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================
   **Key Attributes**                                                                                                                                                                                                          **Description**
   **Property Key:**\ pegasus.code.generator\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 3.0 **Type** : String **Values** : Condor|Shell|PMC **Default :** Condor\ **See Also :** pegasus.log.manager.formatter This property is used to load the appropriate Code Generator to use for writing out the executable workflow.

                                                                                                                                                                                                                               Condor
                                                                                                                                                                                                                                  This is the default code generator for Pegasus . This generator generates the executable workflow as a Condor DAG file and associated job submit files. The Condor DAG file is passed as input to Condor DAGMan for job execution.
                                                                                                                                                                                                                               Shell
                                                                                                                                                                                                                                  This Code Generator generates the executable workflow as a shell script that can be executed on the submit host. While using this code generator, all the jobs should be mapped to site local i.e specify --sites local to pegasus-plan.
                                                                                                                                                                                                                               PMC
                                                                                                                                                                                                                                  This Code Generator generates the executable workflow as a PMC task workflow. This is useful to run on platforms where it not feasible to run Condor such as the new XSEDE machines such as Blue Waters. In this mode, Pegasus will generate the executable workflow as a PMC task workflow and a sample PBS submit script that submits this workflow.
   **Property Key:**\ pegasus.condor.concurrency.limits\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 4.5.3 **Type :**\ Boolean\ **Default :**\ False                                                             This Boolean property is used to determine whether Pegasus associates default HTCondor concurrency limits with jobs or not. Setting this property to true, allows you to `throttle <#job_throttling_across_workflows>`__ jobs across workflows, if the workflow are set to run in pure condor environment.
   **Property Key:**\ pegasus.register\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 4.1.- **Type** : Boolean **Default :** true                                                                                  Pegasus creates registration jobs to register the output files in the replica catalog. An output file is registered only if

                                                                                                                                                                                                                               1) a user has configured a replica catalog in the properties 2) the register flags for the output files in the DAX are set to true

                                                                                                                                                                                                                               This property can be used to turn off the creation of the registration jobs even though the files maybe marked to be registered in the replica catalog.
   **Property Key:**\ pegasus.register.deep\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 4.5.3.- **Type** : Boolean **Default :** true                                                                           By default, Pegasus always registers the complete LFN that is associated with the output files in the DAX i.e if the LFN has / in it, then lfn registered in the replica catalog has the whole part. For example, if in your DAX you have rupture/0001.rx as the name attribute for the uses tag, then in the Replica Catalog the LFN is registered as rupture/0001.rx

                                                                                                                                                                                                                               On setting this property to false, only the basename is considered while registering in the replica catalog. In the above case, 0001.rx will be registered instead of rupture/0001.rx
   **Property Key:**\ pegasus.data.reuse.scope\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 4.5.0 **Type :**\ Enumeration **Value :**\ none|partial|full **Default :** full                                      This property is used to control the behavior of the data reuse algorithm in Pegasus

                                                                                                                                                                                                                               none
                                                                                                                                                                                                                                  This is same as disabling data reuse. It is equivalent to passing the --force option to pegasus-plan on the command line.
                                                                                                                                                                                                                               partial
                                                                                                                                                                                                                                  In this case, only certain jobs ( those that have pegasus profile key enable_for_data_reuse set to true ) are checked for presence of output files in the replica catalog. This gives users control over what jobs are deleted as part of the data reuse algorithm.
                                                                                                                                                                                                                               full
                                                                                                                                                                                                                                  This is the default behavior, where all the jobs output files are looked up in the replica catalog.
   **Property Key:**\ pegasus.catalog.transformation.mapper\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 2.0 **Type :**\ Enumeration **Value :**\ All|Installed|Staged|Submit **Default :** All                  Pegasus supports transfer of statically linked executables as part of the executable workflow. At present, there is only support for staging of executables referred to by the compute jobs specified in the DAX file. Pegasus determines the source locations of the binaries from the transformation catalog, where it searches for entries of type STATIC_BINARY for a particular architecture type. The PFN for these entries should refer to a globus-url-copy valid and accessible remote URL. For transfer of executables, Pegasus constructs a soft state map that resides on top of the transformation catalog, that helps in determining the locations from where an executable can be staged to the remote site.

                                                                                                                                                                                                                               This property determines, how that map is created.

                                                                                                                                                                                                                               All
                                                                                                                                                                                                                                  In this mode, all sources with entries of type STATIC_BINARY for a particular transformation are considered valid sources for the transfer of executables. This the most general mode, and results in the constructing the map as a result of the cartesian product of the matches.
                                                                                                                                                                                                                               Installed
                                                                                                                                                                                                                                  In this mode, only entries that are of type INSTALLED are used while constructing the soft state map. This results in Pegasus never doing any transfer of executables as part of the workflow. It always prefers the installed executables at the remote sites.
                                                                                                                                                                                                                               Staged
                                                                                                                                                                                                                                  In this mode, only entries that are of type STATIC_BINARY are used while constructing the soft state map. This results in the concrete workflow referring only to the staged executables, irrespective of the fact that the executables are already installed at the remote end.
                                                                                                                                                                                                                               Submit
                                                                                                                                                                                                                                  In this mode, only entries that are of type STATIC_BINARY and reside at the submit host ("site" local), are used while constructing the soft state map. This is especially helpful, when the user wants to use the latest compute code for his computations on the grid and that relies on his submit host.
   **Property Key:**\ pegasus.selector.transformation\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 2.0 **Type :**\ Enumeration **Value :**\ Random|Installed|Staged|Submit **Default :** Random                  In case of transfer of executables, Pegasus could have various transformations to select from when it schedules to run a particular compute job at a remote site. For e.g it can have the choice of staging an executable from a particular remote site, from the local (submit host) only, use the one that is installed on the remote site only.

                                                                                                                                                                                                                               This property determines, how a transformation amongst the various candidate transformations is selected, and is applied after the property pegasus.tc has been applied. For e.g specifying pegasus.tc as Staged and then pegasus.transformation.selector as INSTALLED does not work, as by the time this property is applied, the soft state map only has entries of type STAGED.

                                                                                                                                                                                                                               Random
                                                                                                                                                                                                                                  In this mode, a random matching candidate transformation is selected to be staged to the remote execution site.
                                                                                                                                                                                                                               Installed
                                                                                                                                                                                                                                  In this mode, only entries that are of type INSTALLED are selected. This means that the concrete workflow only refers to the transformations already pre installed on the remote sites.
                                                                                                                                                                                                                               Staged
                                                                                                                                                                                                                                  In this mode, only entries that are of type STATIC_BINARY are selected, ignoring the ones that are installed at the remote site.
                                                                                                                                                                                                                               Submit
                                                                                                                                                                                                                                  In this mode, only entries that are of type STATIC_BINARY and reside at the submit host ("site" local), are selected as sources for staging the executables to the remote execution sites.
   **Property Key:**\ pegasus.parser.dax.preserver.linebreaks\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 2.2.0 **Type :**\ Boolean **Default :** false                                                         The DAX Parser normally does not preserve line breaks while parsing the CDATA section that appears in the arguments section of the job element in the DAX. On setting this to true, the DAX Parser preserves any line line breaks that appear in the CDATA section.
   **Property Key:**\ pegasus.parser.dax.data.dependencies\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 4.4.0 **Type :**\ Boolean **Default :** true                                                             If this property is set to true, then the planner will automatically add edges between jobs in the DAX on the basis of exisitng data dependencies between jobs. For example, if a JobA generates an output file that is listed as input for JobB, then the planner will automatically add an edge between JobA and JobB.
   **Property Key:**\ pegasus.integrity.checking\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 4.9.0 **Type :**\ none|full **Default :** full                                                                     This property determines the dial for pegasus integrity checking. Currently the following dials are supported

                                                                                                                                                                                                                               full
                                                                                                                                                                                                                                  In this mode, integrity checking happens at 3 levels

                                                                                                                                                                                                                                  1. after the input data has been staged to staging server - pegasus-transfer verifies integrity of the staged files.

                                                                                                                                                                                                                                  2. before a compute task starts on a remote compute node - This ensures that checksums of the data staged in match the checksums specified in the input replica catalog or the ones computed when that piece of data was generated as part of previous task in the workflow.

                                                                                                                                                                                                                                  3. after the workflow output data has been transferred to user servers - This ensures that output data staged to the final location was not corrupted in transit.

                                                                                                                                                                                                                               nosymlink
                                                                                                                                                                                                                                  No integrity checking is performed on input files that are symlinked. You should consider turning this on, if you think that your input files at rest are at a low risk of data corruption, and want to save on the checksum computation overheads against the shared filesystem.
                                                                                                                                                                                                                               none
                                                                                                                                                                                                                                  No integrity checking is performed.
   =========================================================================================================================================================================================================================== ===========================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================
