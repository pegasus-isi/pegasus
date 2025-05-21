## Pegasus 4.4.x Series

### Pegasus 4.4.2

**Release Date:** March 16, 2015

We are happy to annouce the release of Pegasus 4.4.2. Pegasus 4.4.2 is
a minor release, which contains minor enhancements and fixes bugs to
Pegasus 4.4.1 release.

#### Enhancements

1) Support for recursive clustering

   Pegasus now supports recursive clustering, where users can employ
   multiple clustering techniques on the same graph. For example a
   user can do label based clustering on the graph, and then do a
   level based clustering.

   More details at
    PM-817 [\#935](https://github.com/pegasus-isi/pegasus/issues/935)

2) Planner reports file breakdowns in the metrics sent to the metrics
   server.

   The planner now send file breakdowns ( number of input,
   intermediate and output files ) as part of the metrics message to
   the metrics server. This is also reported in the metrics file left
   in the submit directory.

3) pegasus-transfer does not hide scp errors.

4) more helpul message is thrown if user does not set
pegasus.catalog.site.file property

#### Bugs Fixed

1) work dir in job instance table was populated incorrectly

   The work directory in the job instance table of the monitoring
   database was populated by the submit directory instead of the
   directory in wihch the job was executed. This resulted in
   pegasus-analyzer displaying the submit directory for the failed job
   instead of the directory in which the job actually ran on the
   remote node.

   More details at
    PM-817 [\#935](https://github.com/pegasus-isi/pegasus/issues/935)

2) pegasus-status showed master dag job as failure also

   When a job in a workflow fails, pegasus-status also includes the
   corresonding dag job as failed. This leads to it reporting 1 more
   than the actual number of user compute jobs that failed. this is
   now fixed.

   More details at
    PM-811 [\#929](https://github.com/pegasus-isi/pegasus/issues/929)

3) local-scratch directory not picked up for PegasusLite jobs

   Users can specify a local-scratch directory in the site catalog for
   a site, to designate the local directory on the worker node where a
   PegasusLite job should be run. However, this was not picked up by
   the planner and set for the jobs. This is now fixed. This only
   works when user is executing workflows in nonsharedfs mode.

4) pegasus dashboard tables were not updated in real time.

   Fixed off by 1 error in the flush command, where we decide whether
   we want to flush the event to the database or batch them up. This
   one off error affected the pegasus dashboard as the workflow start
   and end events were not updated to the database when they happened
   by pegasus-monitord. this is now fixed.


5) Input files in the DAX where transfer flag is set to false, should
   not be considered for cleanup, as they are never staged to scratch
   directory on the staging site by the stage in jobs

6) pegasus.gridstart.arguments was not set for all clustered jobs.

   User provided extra arguments for kickstart invocation were not
   passed to all the constitutent jobs making up a job cluster, in
   case of job clustering.  This is now fixed.

   More details at  PM-823 [\#941](https://github.com/pegasus-isi/pegasus/issues/941)

7) MPI_ERR_TRUNCATE: message truncated in PMC

   This error was encountered in certain conditions and was a result
   of  mismatched tag/source between an MPI_Probe/MPI_Iprobe and
   MPI_Recv. This is now fixed

   More details at  PM-848 [\#966](https://github.com/pegasus-isi/pegasus/issues/966)

8) Setting pegasus.catalog.site XML4 raised an error

   Introduced backward compatibility for this.
   More details at  PM-815 [\#933](https://github.com/pegasus-isi/pegasus/issues/933)

9) pegasus-plan --help resulted in metrics to be sent.

   This is now fixed
   More details at  PM-816 [\#934](https://github.com/pegasus-isi/pegasus/issues/934)

### Pegasus  4.4.1

**Release Date:** December 19, 2014

We are happy to annouce the release of Pegasus 4.4.1. Pegasus 4.4.1 is
a minor release, which contains minor enhancements and fixes bugs to
Pegasus 4.4.0 release.

#### Enhancements

1) Leaf cleanup job failures don't trigger workflow failures

2) Finer grained capturing of GridFTP errors

   Moved to only ignore common failures of GridFTP removals, instead
   of ignoring all errors

3) pegasus-transfer threading enhancements

   Allow two retrie with threading before falling back on
   single-threaded transfers. This prevents pegasus-transfer from
   overwhelming remote file servers when failures happen.

4) Support for MPI Jobs when submitting using Glite to PBS

   For user specified MPI jobs in the DAX, the only way to ensure that
   the MPI job launches in the right directory through GLITE and blahp
   is to have a wrapper around the user mpi job and refer to that in
   the transformation catalog. The wrapper should cd in to the
   directory set by Pegasus in the job's environment. The following
   environment variable is set _PEGASUS_SCRATCH_DIR

5) Updated quoting support for glite jobs

   Quoting in the blahp layer in Condor for glite jobs is
   broken. There were fixes made to the planner and
   pbs_loca_submit_attributes.sh files such that env. var values can
   contain spaces or double quotes.

   The fix relies on users to put the pbs_local_submit_attributes.sh
   from the pegasus distribution to the condor glite bin directory.

   More details at  PM-802 [\#920](https://github.com/pegasus-isi/pegasus/issues/920)

6) pegasus-s3 now has support for copying objects larger than 5GB

7) pegasus-tc-converter code was cleaned up . support for database
    backed TC was dropped.

8) The planner now complaisn for deep LFN's when using condor file transfers

9) The planner stack trace is enabled for errors with a single -v (
i.e INFO messagae level or higher)

More details at  PM-800 [\#918](https://github.com/pegasus-isi/pegasus/issues/918)


#### Bugs Fixed

1) Change in  how monitord parses job output and error files

   Earlier pegasus-monitord had a race condition, at it tried to parse
   the .out and .err file when a JOB_FAILURE or JOB_SUCCESS happened,
   instead of doing it at POST_SCRIPT_SUCCESS or POST_SCRIPT_FAILURE
   message, if a postscript was associated . This resulted in it
   detecting empty kickstart output files, as postscript might have
   moved it before monitord opened a file handle to it. The fix for
   this , changed the monitord logic to parse files on JOB_FAILURE  or
   JOB_SUCCESS only if postscript is not associated with the job

   More details at  PM-793 [\#911](https://github.com/pegasus-isi/pegasus/issues/911)

2) pegasus-monitord did not handle aborted jobs well

   For aborted jobs that failed with signal,  monitord did not parse
   the  job status . Because of that no corresponding JOB_FAILURE was
   recorded, and hence the exitcode for the inv.end event is not
   recorded.

    PM-805 [\#923](https://github.com/pegasus-isi/pegasus/issues/923)

3) A set of portability fixes from the Debian packaging were
incorporated into pegasus builds.

4) Clusters of size 1 should be allowed when using PMC

   An earlier fix for 4.4.0 allowed single jobs to be clustered using
   PMC. However, this resulted in regular MPI jobs that should not be
   clustered, to be clustered also using PMC. The logic was updated to
   only wrap a single job with PMC if label based clustering is turned
   on and the job is associated with a label.

   More details at  PM-745 [\#863](https://github.com/pegasus-isi/pegasus/issues/863)

5) Round robin site selector did not do correct distribution

   The selector was not distributing the jobs round robin at each
   level as it was suppposed to.
 More details at  PM-775 [\#893](https://github.com/pegasus-isi/pegasus/issues/893)

6) Based on user configuration, the leaf cleanup jobs tried to delete the
submit directory for the workflow

   A user can configure a workflow such that the workflow submit
   directory and the workflow scratch directory are the same on local
   site. This can result in stuck workflows if the leaf cleanup jobs
   are enabled. The planner now throws an error during planning if it
   detects the directories are the same

   More details at  PM-773 [\#891](https://github.com/pegasus-isi/pegasus/issues/891)

7) pegasus-cleanup needs to add wildcards to s3:// URLs when
--recursive is used

   More details at  PM-790 [\#908](https://github.com/pegasus-isi/pegasus/issues/908)

8) leaf cleanup jobs delete directory that a workflow corresponding to
dax job may require

    For hierarchical workflows, there maybe a case where the jobs that
    make up the workflow referred to by the subdax job may run in a
    child directory of the scratch directory in whcih jobs of top
    level worklfow are running. With leaf cleanup enabled, the parent
    scratch directory maybe cleaned before the subdax job has been
    completed. Fix for this involved, putting in explicit dependencies
    between the leaf cleanup job and the subdax jobs.

    More details at  PM-795 [\#913](https://github.com/pegasus-isi/pegasus/issues/913)

9)  pegasus-analyzer did not show planner prescript log for failed subdax jobs

    For prescript failures for sub dax jobs ( i.e the failure of
    planning operation on the sub workflow ), pegasus-analyzer never
    showed the content of the log. It only pointed to the location of
    the log in the submit directory. This is now fixed.

     PM-808 [\#926](https://github.com/pegasus-isi/pegasus/issues/926)

10)  pegasus-analyzer shows job stderr for failed pegasus-lite jobs

    When a Pegasus Lite job fails, pegasus-analyzer showed stderr from
    both the Kickstart record and the job stderr. This was pretty
    confusing as stderr for those jobs are used to log all kinds of
    PegasusLite stuff, and has usually nothing to do with the
    failure. To make these jobs easier to debug for our users, we
    added logic to only show the Kickstart stderr in these cases.

    More details at  PM-798 [\#916](https://github.com/pegasus-isi/pegasus/issues/916)

11) Planner did not validate pegasus.data.configuration value.

    AS a result, because of a typo in the properties file planner failed with NPE.
    More details at  PM-799 [\#917](https://github.com/pegasus-isi/pegasus/issues/917)

12) pegasus-statistics output padding

    Value padding is done only for text output files so they are human
    readable. However, due to a bug the value padding computation were
    being done for CSV file as well at one point in code. This caused an
    exception when output filetype for job statistics was csv


### Pegasus 4.4.0

**Release Date:** July 9, 2014

We are happy to announce the release of Pegasus 4.4.0

Pegasus 4.4.0 is a major release of Pegasus which contains all the enhancements and bugfixes in 4.3.2

New features and Improvements in 4.4.0 include

- substantial performance improvements for the planner for large workflows
- leaf cleanup jobs in the workflow
- new default transfer refiner
- abitlity to automatically add data flow dependencies
- new mode for runtime clustering
- pegasus-transfer is now multithreaded
- updates to replica catalog backends


#### New Features

1) Improved Planner Performance

   This release has major performance improvements to the planner that
   should help in planning larger DAX'es than earlier. Additionally,
   the planner can now optionally log JAVA HEAP memory usage on the
   INFO log at the end of the planning process, if the property
   pegasus.log.memory.usage is set to true.

2) Leaf Cleanup Jobs

   Pegasus now has a new cleanup option called Leaf that adds a leaf
   cleanup jobs symmetric to the create dir jobs. The leaf cleanup
   jobs remove the directory from the staging site that the create dir
   jobs create at the end of the workflow. The leaf cleanup is turned
   on by passing --cleanup Leaf to pegasus-plan.

   Care should be taken while enabling this option for hierarchal
   workflows. Leaf cleanup jobs will create problems, if there are data
   dependencies between sub workflows in a hierarchal workflow. In
   that  case, the cleanup option needs to be explicitly set to None
   for the  pegasus-plan invocations for the dax jobs in the hierachal
   DAX.

3) New Default Transfer Refiner

   This release has a new default transfer refiner called
   BalancedCluster that does round robin distribution at the file
   level instead of the job level, while creating clustered stagein
   and  stageout jobs. This refiner by default adds two stagein and two
   stageout jobs per level of the workflow.

4) Planner can automatically infer and data flow dependencies in the DAG

   The planner can now automatically add dependencies on the basis of
   data dependencies implied by input and output files for jobs. For
   example if Job A creates an output file X and job B consumes it,
   then  the planner should automatically add a dependency between A
   -> B if it does not exist already.

   This feature is turned on by default and can be turned off by
   setting  the property pegasus.parser.dax.data.dependencies to
   false. More  details at  PM-746 [\#864](https://github.com/pegasus-isi/pegasus/issues/864)

5) Update to Replica Catalog Backends

   The replica catalog backends ( File, Regex and JDBCRC) have been
   updated to consider lfn, pfn mapping but with different pool/handle
    as different entries.

   For the JDBCRC the database schema has been updated. To migrate
   your  existing JDBCRC backend, users are recommended to use the
   alter-my-rc.py script located into 'share/pegasus/sql' to migrate
   the database.

   Note that you will need to edit the script to update the database
    name, host, user, and password. Details at
     PM-732 [\#850](https://github.com/pegasus-isi/pegasus/issues/850)

6) Improved Credential Handing for data transfers

   In case of data transfer jobs, it is now possible to associate
   different credentials for a single file transfer ( one for the
   source server and the other for the destination server) . For
   example, when leveraging GridFTP transfers between two sides that
   accept different grid credentials such as  XSEDE Stampede site and
   NCSA Bluewaters. In that case, Pegasus picks up  the associated
   credentials from the site catalog entries for the source   and the
   destination sites associated with the transfer.

   Also starting 4.4, the credentials should be associated as Pegasus
   profiles with the site entries in the site catalog, if you want
   them  transferred with the job to the remote site.

   Details about credential handling in Pegasus can be found here
   https://pegasus.isi.edu/wms/docs/4.4.0cvs/reference.php#cred_staging

   Associated JIRA item for the improvement
    PM-731 [\#849](https://github.com/pegasus-isi/pegasus/issues/849)

   The credential handling support in pegasus-transfer,
   pegasus-createdir and pegasus-cleanup were also updated

7) New mode for runtime clustering

   This release has a new mode added for runtime clustering.

   Mode 1: The module groups tasks into clustered job such that no
   clustered job runs longer than the maxruntime input parameter to
   the module.

   Mode 2(New): New mode now allows users to group tasks into a fixed
   number of clustered jobs. The module distributes tasks evenly
   (based on job runtime) across jobs, such that each clustered job
   takes approximately the same time. This mode is helpful when users
   are aware of the number of resources available to them at the time
   of execution.

8) pegasus-transfer is now threaded

   pegasus-transfer is now multithreaded.  Pegasus exposes two knobs
   to control the number of threads pegasus-transfer can use depending
   on whether  you want to control standard transfer jobs, or you want
   to control transfers that happen as a part of a PegasusLite job
   . For the former, see the pegasus.transfer.threads property, and
   for the latter the pegasus.transfer.lite.threads property. For
   4.4.0 pegasus.transfer.threads defaults to 2 and
   pegasus.transfer.lite.threads defaults to 1.

9) pegasus-analyzer recurses into subworkflows

   pegasus-analyzer has a --recurse option that sets it to
   automatically recurse into failed sub workflows.  By default, if a
   workflow has a sub workflow in it, and that sub workflow fails ,
   pegasus-analyzer reports that the sub workflow node failed, and
   lists a command invocation that the user must execute to determine
   what jobs in the sub workflow failed. If this option is set, then
   the analyzer automatically issues the command invocation and in
   addition displays the failed jobs in the sub workflow.

   Details  at  PM-730 [\#848](https://github.com/pegasus-isi/pegasus/issues/848)

10) Support for Fixed Output Mapper

   Using this output mapper, users can specify  an externally
   accessible URL in the properties file, pointing to a directory
   where the output files needs to be transferred to. To use this
   mapper, set the following  properties

   pegasus.dir.storage.mapper Fixed
   pegasus.dir.storage.mapper.fixed.url  <url to the storage directory
   e.g. gsiftp://outputs.isi.edu/shared/outputs>

11) Extra ways for user application to flag errors

   CondorG does not propogate exitcodes correctly from GRAM. As a
   result, a job in a Pegasus workflow that is not launched via
   pegasus-kickstart maynot have the right exitcode propogated from
   user application -> GRAM -> CondorG -> Workflow.  For example, in
   Pegasus MPI jobs are never launched using
   pegasus-kickstart. Usually ways of handling this error is to have a
   wrapper script that detects failure and then having the postscript
   fail on the basis of the message logged.

   Starting 4.4.0, Pegasus provides a mechanism of logging something
   on stdout /stderr that can be used to designate failures. This
   obviates the need for users to have a wrapper script. Users can
   associate two pegasus profiles with the jobs

   exitcode.failuremsg -The message string that pegasus-exitcode
   searches for in the stdout and stderr of the job to flag failures.
   exitcode.successmsg - The message string that pegasus-exitcode
   searches for in the stdout and stderr of the job to determine
   whether a job logged it's success message or not. Note this value
   is used to check for whether a job failed or not i.e if this
   profile is specified, and pegasus-exitcode DOES NOT find the string
   in the job stdout or stderr, the job is flagged as failed. The
   complete rules for determining failure are described in the man
   page for pegasus-exitcode.

   More details at http://jira.isi.edu/browse/PM-737

12) Updated examples for Glite submission directly to local PBS

   The 4.4.0 release has improvements for the submission of workflows
   directly to local PBS using the Condor Glite interfaces. The
   documentation on how to use this through Pegasus is documented at

   http://pegasus.isi.edu/wms/docs/4.4.0/execution_environments.php#glite

   It is important to note that to use this, you need to use the
   pbs_local_attributes.sh file shipped with Pegasus in the
   share/pegasus/htcondor/glite directory and put in the glite bin
   directory of your condor installation.

   Additionally, there is a new example in the examples directory that
   illustrates how to execute an MPI job using this submission
   mechanism through Pegasus.

13) Finer grained specification of linux versions for worker package
    staging

   Planner now has added logic for users to specify finer grained
   linux versions to stage the worker package for .

   Users can now specify in the site catalog the osrelease and
   osversion attributes e.g.

   <site handle="exec-site" arch="x86_64" os="LINUX" osrelease="deb" osversion="7">

   If a supported release version combination is not specified, then
   planner throws a warning and defaults to the default combination
   for the OS.

   More details at  PM-732 [\#850](https://github.com/pegasus-isi/pegasus/issues/850)

14) pegasus-kickstart can now copy all of applications stdio if -b all
is passed

   Added an option to capture all stdio. This is a feature that
   HUBzero requested. Kickstart will now copy all stdout and stderr of
   the job to the invocation record if the user specifies '-B all'.

15) Tutorial includes pegasus-dashboard

    The tutorial comes configured with pegasus-dashboard.

16) Improved formatting of extra long values for pegasus-statistics
   More details at   PM-744 [\#862](https://github.com/pegasus-isi/pegasus/issues/862)

17) Changed timeout parameters for pegasus-gridftp

   Increased the timeout parameter for GridFTPClient to 60
   seconds. The globus jar defaults to 30 seconds. The timeout was
   increased to ensure that transfers don't fail against heavliy
   loaded GridFTP servers.

#### Bugs Fixed

1) IRODS support in pegasus-transfer , pegasus-createdir was broken

   irods mkdir command got the wrong path when invoked by
   pegasus-transfer. this is now fixed

2) Data reuse algorithm does not cascade the deletion upwards

   In certain cases, the cascading of deletion in data reuse did not
   happen completely. This is now fixed.  More details at
    PM-742 [\#860](https://github.com/pegasus-isi/pegasus/issues/860)

3) Improved argument management for PMC

   This was done to address the case where a task has quoted arguments
   with spaces.

4) Clusters of size 1 should be allowed when using PMC

   For label based clustering with PMC single node clusters are
   allowed. This is important as in some cases, PMC jobs might have
   been set to work with the relevant globus profiles.

    PM-745 [\#863](https://github.com/pegasus-isi/pegasus/issues/863)

5) nonascii characters in application stdout broke parsing in monitord

   The URL quoting logic was updated to encode unicode strings  as
   UTF-8 before the string was passed to the quote fuction. More
   details at

     PM-757 [\#875](https://github.com/pegasus-isi/pegasus/issues/875)

6) Removing a workflow using pegasus-remove does not update the
stampede database

   If you remove a running workflow, using pegasus-remove, the
   stampede database is not updated to reflect that the workflow
   failed. Changes were made to pegasus-dagman to ensure that
   pegasus-monitord gets 100 seconds to complete the population before
   sending a kill signal.

7) Translation of values from days to years/days was borken in
pegasus-statistics

   This is now fixed.
