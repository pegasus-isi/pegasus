## Pegasus 4.3.x Series

### Pegasus 4.3.2

**Release Date:** March 18, 2014

We are happy to annouce the release of Pegasus 4.3.2. Pegasus 4.3.2 is
a minor release, that has minor enhancements and fixes bugs to Pegasus
4.3.1 release.

#### Enhancements

1) Better error recording for PegasusLite failures in the monitoring
   database

   If a  PegasusLite job failed because of an error encountered while
   retrieving hte input files from the staging site, then no kickstart
   record for the main user job is generated ( as the job is never
   launched). As a result, in the database no record is populated
   indicating a failure of the job. This was fixed to ensure that
   monitord now populates an invocation recording containing the error
   message from the err file of the PegasusLite job.

   More details can be found at
    PM-733 [\#851](https://github.com/pegasus-isi/pegasus/issues/851)

2) PMC Changes

   By default PMC now clears the CPU mask using sched_setaffinity. If
   libnuma is available, it also resets the NUMA memory policy using
   set_mempolicy. If the user wants to keep the inherit
   affinity/policy, then they can use the --keep-affinity argument.

   More details can be found at
    PM-735 [\#853](https://github.com/pegasus-isi/pegasus/issues/853)

   The number of open files tracked in the internal file descriptor
   cache was decreased from 4096 to 256. Also if an error is
   encountered because, the fd limit is exceeded on a system then PMC
   logs the number of file descriptors it has open helping the user
   identify the number of FD's open by PMC.

3) pegasus-transfer changes

   pegasus-transfer checks in the local cp mode to ensure that src and
   dst is not the same file.
   pegasus-transfer sets the -fast option by default to GUC  for 3rd
   party gsiftp transfers

4) pegasus-status changes

   Minor fix for when the parent dag disappears before the job (can
   happen for held jobs)


5) Changes to java memory settings

   The pegasus-plan wrapper script takes into account ulimit -v
   settings while determining the java heap memory for the planner.


#### Bugs Fixed

1) Symlinking in PegasusLite against SRM server

   In the case, where the data on the staging server is directly
   accessible to the worker nodes it is possible to enable symlinking
   in Pegasus that results in PegasusLite to symlink the data against
   the data on the staging site. When this was enabled, the source URL
   for the symlink transfer referred to a SRM URL resulting in pegasus
   lite doing a duplicate transfer. The planner needed to be changed
   to resolve the SRM URL to a file URL that is visible from the
   worker node.

   Also the planner never symlinks the executable files in Pegasus
   Lite as it can create problems with the setting of the x bit on the
   executables staged. For executable staging to work, the executable
   need to be copied to the worker node filesystem.

   More details can be found at
    PM-734 [\#852](https://github.com/pegasus-isi/pegasus/issues/852)

2) The input file corresponding to the DAX for the DAX jobs was not
   associated correctly when the planner figures out the transfers
   required for the DAX job. This happened, if the DAX job only referred
   to the DAX file as an input file and that was generated by a parent
   dax generation job in the workflow.

3) File dependency between a compute job and a sub workflow job

   The planner failed while planning a dax job for an input file, that
   a parent job of the corresponding DAX job generated. This is now
   fixed as the cache file for the parent workflow is passed to the
   sub workflow planner invocations.

   More details can be found at
    PM-736 [\#854](https://github.com/pegasus-isi/pegasus/issues/854)

4) Error when data reuse and cleanup is enabled

   The planner failed in the case, where cleanup was enabled in
   conjuction with data reuse, where jobs removed by the data reuse
   algorithm were the ones for which output files are required by the
   user on the output site. In that case, the planner adds stageout
   jobs to stage the data from the location in the replica catalog to
   the output site. The addition of this stageout job was resulting in
   an execption in the cleanup module. This is now fixed.

   More details can be found at
    PM-739 [\#857](https://github.com/pegasus-isi/pegasus/issues/857)

5) pegasus-analyzer not reporting the planner prescript log for failed
sub workflows

   In the case, where a workflow fails because the planner invoked in
   the prescript for the sub workflow failed, pegasus-analyzer did not
   point the user to the planner log file for the sub workflow. This
   is now fixed.

   More details can be found at
    PM-704 [\#822](https://github.com/pegasus-isi/pegasus/issues/822)

### Pegasus 4.3.1

**Release Date:** November 25, 2013

We are happy to announce the release of Pegasus 4.3.1 .

Pegasus 4.3.1 is a minor release, that has minor enhancements and fixes bugs to Pegasus 4.3.0 release.

#### Enhancements

1) Support for Fixed Output Mapper

   Using this output mapper, users can specify  an externally
   accessible URL in the properties file, pointing to a directory
   where the output files needs to be transferred to. To use this
   mapper, set the following  properties

	pegasus.dir.storage.mapper Fixed
	pegasus.dir.storage.mapper.fixed.url  <url to the storage directory e.g. gsiftp://outputs.isi.edu/shared/outputs>


#### Bugs Fixed

1) pegasus-analyzer does not detect jobs that are condor_rm'ed if no
postscript is associated with the job

   By default, each job has a postscript associated that detects an
   empty job stdout and flags it as a failure. However, if a job is
   not asscociated with a postscript and a user/system condor_rm's the
   job, the failure is not detected. This is now fixed, and a
   JOB_ABORTED event is logged in the stampede database, when a job is
   aborted.

2) IRODS support in pegasus-transfer

   The IRODS support in pegasus-transfer was broken. This is now fixed.

3) pegasus-kickstart compilation warnings for character encodings

   kickstart maintains a table to escape characters correctly for
   putting them in a XML document. The non-ascii characters in the
   table were latin1 not UTF-8. This caused a warning on newer
   versions of gcc, which could not be disabled across all platforms.
   kickstart now writes out it;s output in UTF-8 encoding and the xml
   escaping was updated accordingly.

4) Fix to URL handling in the planner

   Changed the regex in PegasusURL so that we can pass urls with just
   the hostnames specified. e.g http://isis.isi.edu Note: no trailing
   / . Before the fix, the planner was throwing an exception if a user
   specified an input URL with path names containing only one
   directory.

5) planner had a rogue debug statement

   There was a rogue system.out statement in the planner output that
   led to a statement being logged for each job in the workflow.

6) pegasus-statistics had 2.6 style code

   Pegasus is distributed as part of OSG software stack, and one of
   the supported platforms there is EL5 systems that come with Python
   2.4. pegasus-statistics had some code that was compatible with
   2.5. This is now fixed.

### Pegasus 4.3

**Release Date:** October 25, 2013

We are happy to announce the release of Pegasus 4.3.

Pegasus 4.3 is a major release of Pegasus which contains all the
enhancements and bugfixes in 4.2.2 release

New features and Improvements in 4.3 include

- improvements to pegasus lite and optimizations for input file
staging in non shared filesystem deployments
- support for output mappers, allowing users finer grained control
over where to place the outputs on an output site
- support for SSH based submissions on top of Condor BOSCO.
- substantial improvements to pegasus-kickstart including abiltiy
to track peak memory usage for jobs
- improvements to pegasus-s3 and pegasus-transfer


#### NEW FEATURES

1) Support for bypassing transfer of input files via the staging site

   In the non shared filesystem deployments (
   pegasus.data.configuration = nonsharedfs|condorio) users, can now
   setup pegasus to transfer the input files directly to the worker
   nodes without going through the staging site.  This can be done by
   setting the following property to true

   pegasus.transfer.bypass.input.staging

   In the nonsharedfs case, if the input files are already present on
   a shared disk accessible from the worker nodes, pegasus lite can
   symlink instead of copying them over to the local directory on the
   worker node.  The cleanup algorithm was updated to ignore files
   that are directly pulled to the worker nodes from the input site
   locations.

2) Support for DAX generation jobs in hierarchal workflows

   Pegasus now has support for having a dax generation job in the
   workflow. This allows users to add long running dax generation jobs
   as a compute job in the workflow, that can be run remotely. These
   dax generation jobs need to be a parent of the associated DAX
   job. Pegasus will ensure that the DAX generated on a remote site is
   brought to the local site for the associated sub workflow
   corresponding to the DAX job to be planned.

   Earlier, the only way for hierarchal workflows was that the DAX'es
   for the sub workflows had to be pre generated and the paths to the
   dax files was specified in the DAX jobs.  Pegasus did not
   automatically handle separate DAX generations jobs out of the
   box. More details can be found at
    PM-667 [\#785](https://github.com/pegasus-isi/pegasus/issues/785)
   the share/pegasus/examples/dynamic-hierarchy directory.

3) Support for output mappers

   Pegasus now has support for output mappers, that allow users fine
   grained control over how the output files on the output site are
   laid out.  The mappers can be configured by setting the following
   property

    pegasus.dir.storage.mapper

   The following mappers are supported

   Flat: By default, Pegasus will place the output files in the
   storage directory specified in the site catalog for the output
   site.

   Hashed: This mapper results in the creation of a deep directory
   structure on the output site, while populating the results. The
   base directory on the remote end is determined from the site
   catalog. Depending on the number of files being staged to the
   remote site a Hashed File Structure is created that ensures that
   only 256 files reside in one directory. To create this directory
   structure on the storage site, Pegasus relies on the directory
   creation feature of the Grid FTP server, which appeared in globus
   4.0.x

   Replica: This mapper determines the path for an output file on the
   output site by querying an output replica catalog. The output site
   is one that is passed on the command line. The output replica
   catalog can be configured by specifiing the properties

	pegasus.dir.storage.mapper.replica       Regex|File
	pegasus.dir.storage.mapper.replica.file  the RC file at the
	backend to use if using a file based RC

4) Support for SSH based submissions

   Pegasus now exposes a ssh style to enable submission to remote
   sites using SSH. This builds upon the Condor BOSCO funtionality
   that allows for submission over ssh.

   Check out the bosco-shared-fs example in the share/pegasus/examples
   directory for a sample site catalog and configuration.


5) Support for JDBC based Replica Catalog

   Resurrected support for JDBC  backed Replica Catalog in
   Pegasus. Users can use pegasus-rc-client to interact with the JDBC
   backend.

6) Reduced Dependencies for create dir jobs

   Pegasus earlier added edges between create dir jobs and all the
   compute jobs scheduled for that particular site. Pegasus now adds
   edges from the create dir job to a compute job only if a create dir
   job is not reachable from one of the parents of the job. This
   strategy is now the default for 4.3.

7) New tool called pegasus-archive

   Pegasus 4.3 has a new tool called pegasus-archive that compresses a
   workflow submit directory in a way that allows pegasus-dashboard,
   pegasus-statistics, pegasus-plots, and pegasus-analyzer to keep
   working. More information can be found in the manpage for the
   tool.

8) pegasus-transfer enhancements

   The internal pegasus-transfer tool was improved to do multi-hop
   staging in the case of two incompatible protocols being used for
   source and destination of a transfer. For example, if a workflow
   requires the transfer of a file from GridFTP to S3,
   pegasus-transfer will split the transfer up into two transfers:
   GridFPT->Local and Local->S3. This is transparent to the end-user
   and the Pegasus planner.


9) pegasus-mpi-cluster enhancements

   Added a --maxfds to control size of FDCache. This argument to PMC
   that enables the user to set the maximum number of file descriptors
   that will be cached by PMC in I/O forwarding. This is to help SCEC
   accomplish coscheduling on BlueWaters.

10) pegasus-kickstart can track peak memory usage for the jobs launched by it

   pegasus-kickstart now add per-pid I/O, memory and CPU usage.  These
   changes add one or more <proc> elements inside all of the <*job>
   elements. The new <proc> elements are only available on Linux
   systems with kernels >2.5.60 that support ptrace with exit
   events. The new <proc> element contains information about

   	   - the peak memory usage of each child process,
	   - the start and end times of the processes,
	   - the number of characters and bytes read and written,
	   - the utime and stime, and the pid and parent pid.

   This information can be used to compute the resource usage of a job.

11) pegasus-kickstart enhancements

   Added a -q option to reduce output . This new option omits the
   <data> part of the <statcall> recordsfor stdout and stderr if the
   job succeeds. If the job fails, then the <data> is included.

   When kickstart is executed on a Linux kernel >= 3.0, logic in the
   machine extensions code of kickstart prevented the proc statistics
   gathering, because it was a reasonable assumption that the API
   might have changed (it did between 2.4 and 2.6). This restriction
   is now removed.

   The behavior of the -B option was changed so that it grabs the last
   N bytes of stdio instead of the first N bytes of stdio if thesize
   of stdio is larger than the -B option.

   The invocation record that kickstart writes out is now consistent
   with new invocation schema version 2.2 . This version adds the
   <proc> element under <*job>, and renames the <linux>/<proc> element
   to <linux>/<procs> to eliminate the name collision.

   pegasus-kickstart now sets the max size of a single argument to
   128k instead of earlier 2048 characters, which appears to be the
   individual limit in Linux. If the total size of all the arguments
   is over the total limit, then execve will fail, so we don't try to
   catch that in the argument parser.

12) pegasus-s3 enhancements

   The put -b/--create-bucket option was made more efficient. There is
   no need to check if the bucket exists before calling create_bucket
   because it is a noop if the bucket already exists.

   pegasus-s3 does not rely on mmap for upload and download. This
   should reduces the memory usage of pegasus-s3 for large files.

   Updated the boto version Boto 2.5.2 to better support multipart
   uploads.


   Added upload rate info to put command output.

   pegasus-s3 now supports transfers from one s3 bucket to another.

13) pegasus-analyzer enhancements

   pegasus-analyzer earlier did not detect prescript failures. If a
   job's prescript failed ( for example the planner instance on a
   subworkflow for a hierarchal workflow ) , then that failure was
   not recorded in the monitoring database. This led pegasus-analyzer
   to not report the prescript failures.  Changes were made in the
   monitoring daemon to ensure those errors are detected and
   associated correctly in the database. More details can be found at
    PM-704 [\#822](https://github.com/pegasus-isi/pegasus/issues/822)

   pegasus-analyzer can be used to debug pegasus lite workflows now
   using the --debug-job option.It facilitates the debugging of a
   failed pegasus lite job by creating a shell script that can be run
   locally.  The --debug-job option creates a shell script in a tmp
   directory that can be executed to pull in the input data and
   execute the job. It also now has a --local-executable option that
   can be used to pass to the local executable for the job that is
   being debugged.

14) pegasus-statistics can generate statistics across multiple root
workflows

    pegasus-statistics now has a -m option to generate statitsics
    across multiple root workflows. User can pass either multiple
    workflow submit directories or workflow uuids separated by
    whitespace.

    This feature is also useful is the runs for multiple root
    workflows are populated in the same database in mysql.

    For e.g
    pegasus-statistics -Dpegasus.monitord.output=mysql://user:password@host:port/databasename  -s all -m


15) pegasus-lite stages out output files in case of failure
   In the nonsharedfs case, PegasusLite now always attempt to
   transfer the output files even if the main command of the script
   fails.

   Details at  PM-701 [\#819](https://github.com/pegasus-isi/pegasus/issues/819)

16) Directory backed Replica Catalog now supports flat lfn's

   By default the directory based replica catalog backend constructs
   deep lfn's while traversing an input directory.

   For example, if input directory is points to a directory input then
   input/deep/f.a file results in LFN called deep/f.a

   If a user sets,  pegasus.catalog.replica.directory.flat.lfn to true

   then the leaf is only constructed for creating the lfn.
   For example input/deep/f.a will result in lfn f.a

17) Updated jglobus jars and globus rls client jar

   Pegasus now ships with updated jglobus and globus rls client jars
   that allow us to use the proxies generated using newer certificates
   to authenticate against a RLS deployment. The RLS client jar
   shipped with pegasus  works with JGlobus 2.0.5.

18) Updated proxy detection logic in the planner.

   pegasus.local.env property is no longer supported. To use it users
   need to just do env.VARIABLE_NAME in their properties.  The planner
   now uses GSSManager class from jglobus to determe the DN of the
   proxy for writing out in the braindump file.

19) Support for SQLite 3.7.13 for the stampede statistics layer

   SQLIte 3.7.12 introduced a bug as to how the nested aggregate
   queries are handled. This is fixed in version 3.7.14 , but version
   3.7.13 is what Debian installs when one does it through apt. The
   query that generates the jobs.txt file was updated so as to not to
   fail . The update query works across all the recent SQLite versions

20) Changes to tutorial VM image

   The tutorial image was updated so that the udev persistent rules
   for eth0 are disabled. Added a GNOME X desktop to the VM. The VM
   image can now grow to 8GB

21) dax2dot now implements transitive reduction algorithm to reduce
extra edges to the workflow

   The dax2dot now implments a transitive reduction algorithm to
   remove extra edges from the workflow. It also has Improved handling
   of -f option. This fixes PM-721 by treating files and jobs as
   equivalent Nodes so that transitive reduction works in the case
   where the DAG contains a mix of File Nodes and Job
   Nodes. Non-redundant Job-Job edges will still be rendered if the
   user specifies -f, but redundant edges will be removed. If the user
   specifies both -f and -s, then there will be many redundant edges
   in a typical workflow. Sometimes the -f option will cause cycles in
   the graph (e.g. files with "inout" linkage, or jobs with a file
   that is both an input and output). In those cases the -s option
   must also be specified.


22) Better handling in monitord for submit host crashes

   monitord now detects consecutive workflow started events. In this
   case, it inserts an intervening workflow end event with status set
   to 2 to indicate unknown failure. This case can happen, when condor
   dies on the submit host, say because of power failure. The
   intervening workflow end event is inserted to ensure that the
   queries don't to the database don't fail because of mismatched
   start and end events.

23) Application Metrics Reporting

   Applications can now enable the planer to pass application defined
   metrics to the metrics server.

   This allows the metrics on the server to be grouped by application
   name.

   In order to do that, please set the property
   pegasus.metrics.app      application-name

   Users can also associated arbitary key value pairs that can be
   passed to the server.

	pegasus.metrics.app.attribute1 value1



24) Change of maxpre settings for hierarchal workflows

   Changed the default for maxpre from 2 to 1. More sensible in context
   of ensemble manager.


#### BUGS FIXED


1) memory explosion for monitord when parsing large PMC workflows

   For large SCEC workflows using PMC it was noticed that monitord
   memory usage exploded when parsing large hierarchal workflows with
   PMC enabled ( tens of thousands of jobs in one PMC cluster). This
   is now fixed . More details can be found at
    PM-712 [\#830](https://github.com/pegasus-isi/pegasus/issues/830)

2) kickstart segfault in tracing

   If the job forks lots of children then the size of the buffer for
   the final invocation record fills up with <proc> tags and causes a
   segfault.

3) kickstart segafault on missing argument

   kickstart segfaulted on missing arguments. This is now fixed.

4) pegasus-dagman used pegasus bindir in the search path for
    determining condor location

   Fixed bug bringing in the location of Pegasus when determining
   which HTCondor to use

5) JAVA DAX API stdout| stderr handling

   Changed the handling for stdout | stderr | stdin files in the JAVA
   DAX API. Corresponding uses files are now only added when we are
   printing out the ADAG contents in the toXML method if not already
   specified by the user. This also removes the warning messages,
   where a user adds a uses section for a stdout file with different
   transfer and register flags explicitly in their DAX generators.


6) Fix for heft site selector

   The Heft site selector was not correctly initialized if a user did
   not specify any execution sites on the command line. this is now
   fixed.
