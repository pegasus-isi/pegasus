## Pegasus 2.x Series

### Pegasus 2.4.3

**Release Date:** August 24, 2010

This is minor release, that fixes some RPM and DEB packaging bugs. It
has improvements to the Data Reuse Algorithm and pegasus-analyzer.

#### NEW FEATURES


1) pegasus-analyzer has debug-job feature for pure condor environment

   pegasus-analyzer now has an option debug-job that generates a shell
   script for a failed job and allows users to run it later. This is
   only valid for pure condor environment where we relying on condor
   to do the file transfers.
   The script will copy all necessary files to a local directory and
   invoke the job with the necessary command-line options. More
   details  in JIRA issue PM-92. Two options were added --debug-job,
   and  --debug-dir, where the first one indicated which job should
   be  debugged. The second option is used to specify where the user
   wants to debug this job (default is to create a temp dir).

2) New Implementation of Data Reuse Algorithm

   The data reuse algorithm reduces the workflow on the basis of
   existing  output files of the workflow found in the Replica
   Catalog. The algorithm works in two passes.

   1) In the first pass , we determine all the jobs whose output files
   exist  in the Replica Catalog. An output file with the transfer
   flag set to false is treated equivalent to the file existing in the
   Replica Catalog , if

   the output file is not an input to any of the children of the job X

   2) In the second pass, we remove the job whose output files exist in
   the Replica Catalog and try to cascade the deletion upwards to the
   parent jobs. We start the breadth first traversal of the workflow
   bottom up.

```
   A node is marked for deletion if -

     ( It is already marked for deletion in pass 1
       OR
        ( ALL of it's children have been marked for deletion
       	  AND
           Node's output files have transfer flags set to false
      	 )
     )
```

3)  Workflow with NOOP Job is created when workflow is reduced fully

    In the case where the Data Reuse Algorithm reduces all the jobs in
    the workflow, a workflow with a single NOOP job is created.

    This is to ensure that pegasus-run works correctly, and in case of
    workflows of workflows the empty sub workflows dont trigger an
    error in the outer level workflow.


#### Bugs FIXED

1) RPMs had mismatched permissions on the jars versus the setup.sh/
   setup.csh scripts. The result was empty CLASSPATHs after sourcing
   the setup scripts. The scripts have now been opened up to allow
   for the permissions of the files in the RPMs

2) A Debian packaging problem due to .svn files being left in the deb

3) The setup scripts can now guess JAVA_HOME using a common system
   install locations. This is useful for RPMs and DEBs which already
   have declared dependencies on certain JREs

4) pegasus-status incorrectly reported status of workflow when it
   starts up.

   pegasus-status incorrectly reported the number of workflows and %
   done when a workflow of workflows started. This is now fixed both
   in branch 2.4 and head.

5) NPE while label based clustering

   The logger object in the label based clusterer was incorrectly
   instantiated leading to a null pointer exception

   This is fixed both in branch 2.4 and head

   More details at PM-144 [\#264](https://github.com/pegasus-isi/pegasus/issues/264)

6) Incorrect -w option to kickstart for clustered jobs with Condor
   file staging.

   When using condor file transfers and label-based clustering Pegasus
   generated the -w working directory option and sets it to a
   generated  path in /tmp. This broke the workflow because condor
   transfers  all the input files to the condor exec directory. This
   problem did not get triggered if the workflow was not clustered.

   More details at PM-145 [\#265](https://github.com/pegasus-isi/pegasus/issues/265)

7) Condor stdout and stderr streaming

   Condor streaming is now supported in Condor for both grid and non
   grid universe jobs. We always put in the streaming keys. They
   default to false.

   But can be overridden by properties.

   pegasus.condor.output.stream
   pegasus.condor.error.stream

8) Bug fix for exploding relative-dir option when using pdax

   There was a bug whereby the relative-dir option constructed for the
   sub workflows exploded when a user passed a pdax file to
   pegasus-plan.

   Full details at PM-142 [\#262](https://github.com/pegasus-isi/pegasus/issues/262)

9) Specifying SUNOS in old site catalog format

   The 2.4 branch was not translating OS'es correctly. This lead to a
   NPE if a user specified SUNOS in the old XML site catalog format.

   The conversion functions that convert internally to new format were
   expanded to support more OS

### Pegasus 2.4.2

**Release Date:** May 12, 2010

This is minor release, that has some feature enhancements to pegasus-status
and pegasus-analyzer for working with Condor SUBDAG's.

#### NEW FEATURES

1) pegasus-status tracks workflows of workflows better now

   pegasus-status now always shows top most level dag last in the long
   output. It also displays the percentage  of work done on the outer
   most dag. It tracks SUBDAG correctly, even if the submit directory
   for those workflows is not rooted in the top level dag's submit
   directory.


2) pegasus-analyzer

   pegasus-analyzer now parses the VARS lines in the DAG file to perform
   variable substitution while figuring out the output and error files for
   the failed jobs. This is triggered when --strict option is passed to
   pegasus-analyzer. In case of workflows of workflows all invocations
   of pegasus-analyzer for SUBDAGS have the --strict option included.

### Pegasus 2.4.1

**Release Date:** March 28, 2010

This is minor release, that fixes some critical bugs discovered
after 2.4.0 release and some feature enhancements.

#### NEW FEATURES

1) Specifying different relative submit and execution directories

   Earlier --relative-dir was used to determine the relative submit
   directory for a workflow and the relative execution directory on
   the remote site. Users now can optionally specify --relative-submit-dir
   if they want the relative submit directory to be different from
   the remtoe execution directory.

   This is useful when a user wants certain sub workflows in a
   DAX 3.0 dax to be executed in the same execution directory , while
   having the submit directories different.

   This feature addition was tracked in JIRA via PM-116  [\#237](https://github.com/pegasus-isi/pegasus/issues/237)

2) Python tools to visualize the DAX and DAG

   There are new tools dag2dot.py and dax2dot.py in the bin directory.
   - dag2dot.py reads a DAGMan .dag file (and the corresponding .sub files, if present).
   - dax2dot.py reads a Pegasus DAX file.

   Notable features of the new scripts include:
   - Removal of redundant edges in the workflow to make the
     resulting diagrams easier to look at.
   - Coloring by transformation type.
   - Removal of nodes by transformation type.

   The new scripts are in the $PEGASUS_HOME/bin directory.

3) Generating a site catalog for OSG for VO other than LIGO/Engage

   pegasus-get-sites can now generate a site catalog for OSG for any
   VO using the OSGMM backend. Earlier a user could only generate
   a site catalog for LIGO and Engage VO.

   This feature addition was tracked in JIRA via PM-67 [\#188](https://github.com/pegasus-isi/pegasus/issues/188)

4) New features to pegasus-analyzer

   pegasus-analyzer can now run on other users submit directories.

   - Earlier pegasus-analyzer could not analyze workflow submit
     directories for which the user running pegasus-analyzer did
     not have the write permissions. This was because pegasus-analyzer
     tried to generate a jobstate.log file in the workflow submit
     directory. This is now directed to a tmp file in the /tmp directory

   - pegasus-analyzer can be used to analyze SUBDAGS/workflows that
     are not planned via Pegasus

   - New command line option --print option that optionally can print
     the invocation and prescript for the failed jobs.

5) pegasus-status has a --long option

   If the --long option is set,  pegasus-status retrieves the last updated
   status of the workflow from the dagman out file, instead of displaying
   the running jobs in the condorq.

   For e.g here is how the sample output looks like

   $ pegasus-status --long .
   ./inspiral_hipe_datafind-0.dag
   03/25 04:24:22 Done Pre Queued Post Ready Un-Ready Failed
   03/25 04:24:22 === === === === === === ===
   03/25 04:24:22 5 0 50 0 43 0 0


#### BUGS FIXED

1) Mismatched source and destination urls in input files for stagein jobs

   In certain cases, it was found that the source and destination pair in
   the input files for the stagein jobs were mismatched.

   The source url referred to one file and the destination file to another.

   This was triggered when pegasus detected circular symlinks . i.e a file
   that was to be symlinked in the remote execution directory already existed
   ( accd to the entry in the replica catalog )

   More details at PM-125 [\#246](https://github.com/pegasus-isi/pegasus/issues/246)

2) symlink jobs were not launched correctly via kickstart

   The symlink jobs while being launched via kickstart did not have the stdin
   file transferred correctly from the submit host.

   This is now fixed.

   More details at PM-124 [\#245](https://github.com/pegasus-isi/pegasus/issues/245)

3) Determining Condor Version

   Pegasus executes the condor_version command to determine the version of
   condor running on the submit host. This is used to create the correct
   version of the dagman.sub files, when workflows of workflows are executed.

   Pegasus did not parse the following output correctly

   $CondorVersion: 7.4.1 Dec 17 2009 UWCS-PRE $

   The internal regex was updated to follow the following rule
   $CondorVersion: 7.4.1 Dec 17 2009 <ANY_ARBITRARY_STRING> $

   the version number and date will always be there.

   <ANY_ARBITRARY_STRING> may or may not be there, and can
   include spaces but is really completely arbitrary. don't rely
   on that for the version information.just use the version and date.


### Pegasus 2.4.0

**Release Date:** FEBRUARY 14, 2010

#### NEW FEATURES

1) Support for Pegasus DAX 3.0

   Pegasus now also can accept DAX'es in Pegasus 3.0 format

   Some salient features of the new format are
   - Users can specify locations of the files in the DAX
   - Users can specify what executables to use in the DAX
   - Users can specify sub dax in the DAX using the dax element. The
     dax jobs result in a separate subworkflow being launched with the
     appropriate pegasus-plan command as the prescript
   - Users can specify condor DAG's in the DAX using the dag
     element. The dag job is passed on the Condor DAGMAN as a SUBDAG
     for execution.

   A sample 3.0  DAX can be found at
   http://pegasus.isi.edu/mapper/docs/schemas/dax-3.0/two_node_dax-3.0_v6.xml

   In the next Pegasus release ( Pegasus 3.0 ) a JAVA DAX API will be
   made available. Certain more extensions will be added to the
   schema. For feature requests email pegasus@isi.edu

2) Support for running workflows on EC2 using S3 for storage

   Users while running on Amazon EC2 can use S3 for storage backend
   for the workflow execution. The details below assume that a user
   configures a condor pool on the nodes allocated from EC3

   To enable Pegasus for S3 the following properties need to be set.

   - pegasus.execute.*.filesystem.local = true
   - pegasus.transfer.*.impl = S3
   - pegasus.transfer.sls.*.impl = S3
   - pegasus.dir.create.impl = S3
   - pegasus.file.cleanup.impl = S3
   - pegasus.gridstart = SeqExec
   - pegasus.transfer.sls.s3.stage.sls.file = false

   For data stagein and creating S3 buckets for workflows pegasus
   relies on the amazon provided s3cmd command line client.

   Pegasus looks for a transformation with namespace amazon and
   logical name as s3cmd in the transformation catalog to figure out
   the location of the s3cmd client. for e.g in the File based
   Transformation Catalog the full name for transformation will be
   amazon::s3cmd

   In order to enable stdtout and stderr streaming correctly from
   Condor on EC2 we recommend adding certain profiles in the site
   catalog for the cloud site.
   Here is a sample site catalog

   <site handle="ec2" sysinfo="INTEL32::LINUX">
      <profile namespace="env" key="PEGASUS_HOME">/usr/local/pegasus/default</profile>
      <profile namespace="env" key="GLOBUS_LOCATION">/usr/local/globus/default</profile>
      <profile namespace="env" key="LD_LIBRARY_PATH">/usr/local/globus/default/lib</profile>

       <!-- the directory where a user wants to run the jobs on the
   	nodes retrived from ec2 -->
       <profile namespace="env" key="wntmp">/mnt</profile>

       <profile namespace="pegasus" key="style">condor</profile>

       <!-- to be set to ensure condor streams stdout and stderr back
	to submit host -->
       <profile namespace="condor" key="should_transfer_files">YES</profile>
       <profile namespace="condor" key="transfer_output">true</profile>
       <profile namespace="condor" key="transfer_error">true</profile>
       <profile namespace="condor" key="WhenToTransferOutput">ON_EXIT</profile>

       <profile namespace="condor" key="universe">vanilla</profile>

       <profile namespace="condor" key="requirements">(Arch==Arch)&amp;&amp;(Disk!=0)&amp;&amp;(Memory!=0)&amp;&amp;(OpSys==OpSys)&amp;&amp;(FileSystemDomain!="")</profile>
       <profile namespace="condor" key="rank">SlotID</profile>

       <lrc url="rls://example.com"/>
       <gridftp url="s3://" storage="" major="2" minor="4" patch="3"/>
       <jobmanager universe="vanilla" url="example.com/jobmanager-pbs" major="2" minor="4" patch="3"/>
       <jobmanager universe="transfer" url="example.com/jobmanager-fork" major="2" minor="4" patch="3"/>

       <!-- create a new bucket for each wf
           <workdirectory >/</workdirectory>
        -->
        <!-- use an existing bucket -->
   	<workdirectory>existing-bucket</workdirectory>
   </site>

   Relevant JIRA links
-    PM-68 [\#189](https://github.com/pegasus-isi/pegasus/issues/189)
-    PM-20 [\#141](https://github.com/pegasus-isi/pegasus/issues/141)
-    PM-85 [\#206](https://github.com/pegasus-isi/pegasus/issues/206)



3) pegasus-analyzer

   There is a new tool called pegasus-analyzer. It helps the users to
   analyze the workflows after the workflow has finished executing.

   It is not meant to be run while the workflow is still running. To
   track the status of a running workflow for now, the users are
   recommended to use pegasus-status.

   pegasus-analyzer looks at the workflow submit directory and parses
   the condor dagman logs and the job.out files to print a summary of
   the workflow execution.

   The tool prints out the following summary of the workflow

   Total jobs
   jobs succeeded
   jobs failed
   jobs unsubmitted

   For all the failed jobs the tool prints out the contents of job.out
   and job.err file.

   The user can use the --quiet option to display only the paths to
   the .out and .err files. This is useful when the job output is
   particularly big or when kickstart is used to launch the jobs.

   For pegasus 3.0 the tool will be updated to parse kickstart output
   files and provide a concise view rather than displaying the whole
   output

4) Support for Condor Glite

   Pegasus now supports a new style named glite for generating the submit
   files. This allows pegasus to create submit files for a glite
   environment where a glite blahp talks to the scheduler instead of
   GRAM. At a minimum the following profiles need to be associated with
   the job.

   pegasus profile style - value set to glite
   condor profile grid_resource - value set to the remote scheduler to
   	  	  		  which glite blahp talks to .

    This style should only be used when the condor on the submit host
    can directly talk to scheduler running on the cluster. In Pegasus
    site  catalog there should be a separate compute site that has
    this style associated with it. This style should not be specified
    for the local site.

    As part of applying the style to the job, this style adds the
    following classads expressions to the job description

    +remote_queue - value picked up from globus profile queue
    +remote_cerequirements - See below

    The remote CE requirements are constructed from the following
    profiles associated with the job. The profiles for a job are
    derived from various sources

    - user properties
    - transformation catalog
    - site catalog
    - DAX

    Note it is upto the user to specify these or a subset of them.

    The following globus profiles if associated with the job are picked up

   - hostcount -> PROCS
   - count -> NODES
   - maxwalltime-> WALLTIME

    The following condor profiles if associated with the job are picked up

    priority -> PRIORITY

    All the env profiles are translated to MYENV

    For e.g. the expression in the submit file may look as

```
    +remote_cerequirements = "PROCS==18 && NODES==1 && PRIORITY==10 && WALLTIME==3600
       && PASSENV==1 && JOBNAME==\"TEST JOB\" && MYENV ==\"FOO=BAR,HOME=/home/user\""
```

All the jobs that have this style applied dont have a remote
directory specified in the submit directory. They rely on
kickstart to change to the working directory when the job is
launched on the remote node.


5) Generating a site catalog for OSG using OSGMM
   The pegasus-get-sites tool has been modified to query the OSGMM (
   OSG Match Maker) to generate a site catalog for a VO

   It builds upon the earlier Engage implementation. It has now been
   generalized and renamed to OSGMM

   To pegasus-get-sites the source option now needs to be OSGMM
   instead of Engage

   Some of the changes are

   The condor collector host can be specified at command line or in
   properties by specifying the property pegasus.catalog.site.osgmm.collector.host .
   It defaults to ligo-osgmm.renci.org

   If a user is part of the Engage VO they should set
   pegasus.catalog.site.osgmm.collector.host=engage-central.renci.org

   The default VO used is LIGO. Can be overriden by specifying the
   --vo option to pegasus-get-sites , or specifying the property
   pegasus.catalog.site.osgmm.vo

   By default the implementation always returns validated sites.
   To retrieve all sites for a VO set
   pegasus.catalog.site.osgmm.retrieve.validated.sites to false.

   In case of multiple gatekeepers are associated with the same osg
   site, multiple site catalog entries are created in the site
   catalog. A suffix is added to the extra sites (__index , where
   index starts from 1)

   Sample Usage
   ```
   pegasus-get-sites --source OSGMM --sc osg-sites.xml --vo LIGO --grid OSG
   ```
   Tracked in JIRA at  PM-67 [\#188](https://github.com/pegasus-isi/pegasus/issues/188)

   Currently, there is no way to filter sites according to the grid ( OSG|OSG-ITB ) in OSGMM

   The site catalog generated has storage directories that have a VO component in them.


6) Generating a site catalog for OSG using MYOSG
   pegasus-get-sites has now been modified to generate a site catalog by querying MyOSG

   To use MYOSG as the backend the source option needs to be set to MYOSG

   Sample usage

   pegasus-get-sites --source MYOSG --sc myosg-sites-new.xml -vvvvv --vo  ligo --grid osg

   This was tracked in JIRA
    PM-61 [\#182](https://github.com/pegasus-isi/pegasus/issues/182)

   Pegasus Team recommends using OSGMM for generating a site catalog.

7) Separation of Symlink and Stagein Transfer Jobs

   The following transfer refiners
   - Default
   - Bundle
   - Cluster
   now support the separation of the symlink jobs from the stage in
   jobs. While using these refiners, the files that need to be
   symlinked against existing files on a compute site will have a
   separate symlink job. The files that need to be actually copied to
   a remote site, will appear in the stage_in_ jobs.

   This distinction, allows for the users to stage in data using third
   party transfers that run on the submit host, and at the same time
   be able to symlink against existing datasets.

   The symlink jobs run on the remote compute sites. Earlier this was
   not possible, and hence for a user to use symlinking they had to
   turn off third party transfers. This resulted in an increased load
   on the head node as the stage in jobs executed there.

   By default, Pegasus will use the transfer executable shipped with
   the worker package to do the symbolic linking .

   If the user wants to change the executable to use , they can set the following property

   pegasus.transfer.symlink.impl

   The above also allows us to use separate executables for staging in
   data and for symbolic linking.
   For e.g. we can use GUC to stage in data by setting

   pegasus.transfer.stagein.impl GUC

   To control the symlinking granularity in the Bundle and Cluster
   refiners the following Pegasus profile keys can be associated

   bundle.symlink
   cluster.symlink

   The feature implementation was tracked in JIRA at
    PM-54 [\#175](https://github.com/pegasus-isi/pegasus/issues/175)

8) Bypassing First Level Staging of Files for worker node execution

   Pegasus now has  capability to bypass first level staging if the
   input files in the replica catalog have a pool attribute matching
   the site at which a job is being run. This applies in case of
   worker node execution.

   The cache file generated in the submit directory is the transient
   replica catalog. It also now has locations of where the inpute
   files are staged on the remote sites. Earlier it was only the files
   that were generated by the workflow.

   Tracked in JIRA here
   -  PM-20 [\#141](https://github.com/pegasus-isi/pegasus/issues/141)
   -  PM-62 [\#183](https://github.com/pegasus-isi/pegasus/issues/183)

9) Resolving SRM URL's for file URL's on a filesystem

   There is now support to resolve the SRM urls in the replica catalog to
   the file url on a site. The user needs to specify the URL prefix
   and the mount point of the filesystem.

   This can be done by specifying the properties
```
   pegasus.transfer.srm.[sitename].service.url
   pegasus.transfer.srm.[sitename].service.mountpoint
```
   Pegasus will then map SRM URL's associate with site to a paht on
   the filesytem by replacing the service url component with the mount
   point.

   For example if user has this specified
```
   pegasus.transfer.srm.ligo-cit.service.url          srm://osg-se.ligo.caltech.edu:10443/srm/v2/server?SFN=/mnt/hadoop
   pegasus.transfer.srm.ligo-cit.service.mountpoint   /mnt/hadoop/
```
   then url
   srm://osg-se.ligo.caltech.edu:10443/srm/v2/server?SFN=/mnt/hadoop/ligo/frames/S5/test.gwf
   will resolve to

   /mnt/hadoop/ligo/frames/S5/test.gwf

10) New Transfer implementation Symlink

   Pegasus has now support for a perl executable called symlink
   shipped with the Pegasus worker package, that can be used to create
   multiple symlinks against input datasets in a single invocation

   The Transfer implementation that uses the transfer executable also
   has the same functionality.
   However, the transfer executable complains if it cannot find the
   Globus client libraries.

   In order to use this executable for the symlink jobs, users need to
   set the following property

   pegasus.transfer.symlink.impl Symlink

   Later on ( pegasus 3.0 release onwards ) this will be made the
   default executable to be used for symlinking jobs.

11) Passing options forward to pegasus-run in pegasus-plan

   Users can now pass forward option to pegasus-run invocation that is
   used to submit the workflows in case of successful mapping.

   There is a  --forward option[=value] to pegasus-plan . This option
   allows a user to forward options to pegasus-run.
   For e.g. nogrid option can be passed to pegasus-run as follows
   pegasus-plan --forward nogrid

   The option can be repeated multiple times to forward multiple
   options to pegasus-run. The longopt version should always be
   specified for pegasus-run.

12) Passing extra arguments SLS transfer implementations

   Users now can specify pegasus.transfer.sls.arguments to pass extra
   options at runtime to the SLS Implementations used by Pegasus.
   The following SLS transfer implementations accept the above property.

   S3
   Transfer

13) Passing non standard java options to dax jobs in DAX 3.0

   The non standard jvm options (-X[option]) can now be specified for
   the sub workflows in the arguments section for the dax jobs.

   For example for the DAX jobs , user can set the java max heap size
   to 1024m by specifying -X1024m in the arguments for the DAX job



14) Location of Condor Logs directory on the submit host

   By default, pegasus designates the condor logs to be created in the
   /tmp directory. This is done to ensure that the logs are created in
   a local directory even though the submit directory maybe on NFS.

   In the submit directory the symbolic link to the appropriate log
   file in the /tmp exists. However, since /tmp is automatically
   purged in most cases, users may want to preserve their condor logs
   in a directory on the local filesystem other than /tmp

   The new property

   pegasus.dir.submit.logs

   allows a user to designate the logs directory on the submit host
   for condor logs.

15) Removing profile keys as part of overriding profiles

   There is now a notion of empty profile key valus in Pegasus.  The
   default action on empty key value is to remove the key. Currently
   the following namespaces follow this convention

       - Condor
       - Globus
       - Pegasus

   This allows a user to unset values as part of overriding
   profiles. Normally a user can only update a profile value i.e they
   can update the value of a key, but the key remains associated with
   the job This allows the user to remove the key from the profile
   namespace.

   For e.g.

   A user may have a profile X set in the site catalog.

   Now for a particular job a user does not want that profile key to
   be used. He can now specify the same profile X with empty value in
   the transformation catalog for that job. This results in the
   profile key X being removed from the job.

16) Constructing Paths to Condor DAGMan for recursive/hierarichal
   workflows

   The entry for condor::dagman is no longer required for site local
   in the transformation catalog.
   Instead pegasus constructs path from the following environment
   variables. CONDOR_HOME, CONDOR_LOCATION

   The priority order is as follows

   1) CONDOR_HOME defined in the environment
   2) CONDOR_LOCATION defined in the environment
   3) entry for condor::dagman for site local

   This is useful when running workflows that refer to sub workflows
   as in the new DAX 3.0 format.

   This was tracked in JIRA
    PM-50 [\#171](https://github.com/pegasus-isi/pegasus/issues/171)

17) Constructing path to kickstart

   By default the path to kickstart is determined on the basis of the
   environment variable PEGASUS_HOME associated with a site entry in
   the site catalog.

   However, in some cases a user might want to use their own modified
   version of kickstart.

   In order to enable that

   The path to kickstart will be constructed according to the following rule
   1) pegasus profile gridstart.path specified in the site catalog for
      the site in question.
   2) If 1 is not specified, then a path is constructed on the basis
   of the environment variable PEGASUS_HOME for the site in the site
   catalog.

   The above was tracked in JIRA
    PM-60 [\#181](https://github.com/pegasus-isi/pegasus/issues/181)

18) Bulk Lookups to Replica Catalog using rc-client

   rc-client now can do bulk lookups similar to how it does bulk
   inserts and deletes

   Details at
    PM-75 [\#196](https://github.com/pegasus-isi/pegasus/issues/196)

19) Additions to show-job workflow visualization script

   show-job now has a --title option to list add a user provided title for the generated gantt chart.

   show-job can also visualize workflow of workflows

20) Absolute paths for certain properties in the properties file

   The properties file that is written out now in the submit directory
   has absolute paths specified for the following property values.

   pegasus.catalog.replica.file
   pegasus.catalog.transformation.file
   pegasus.catalog.site.file

   This is even though user may have specified relative paths in properties file.


21) The default horizontal clustering factor

   Updated the default clustering factor as collapse with value = 1, instead of earlier value of 3

   This ensures, that users can cluster only jobs of certain types,
   and let others remain unclustered. Another way was to specify the
   collapse factor as 1 explicitly for jobs that users dont want
   clustering for.

#### BUGS FIXED

1) Handling of standard universe in condor style
   In Condor style , standard universe if specified for a job is ONLY
   associated for compute jobs. This ensures that pegasus auxillary
   jobs never execute in standard universe.

2) Bug Fix for replica selection bug 43
   Checked in the fix for JIRA bug 43  PM-43 [\#164](https://github.com/pegasus-isi/pegasus/issues/164)

   The ReplicaLocation class now has a clone method that does a
   shallow clone
   This clone method is called in the selectReplica methods in the replica selectors.

3) rc-client did not implement  pegasus.catalog.replica.lrc.ignore property
   This is now fixed.
   This bug was tracked in JIRA  PM-42 [\#163](https://github.com/pegasus-isi/pegasus/issues/163)

4) DAX'es created while partitioning a workflow
   During the partitioning of the workflows , the DAX for a partition
   was created incorrectly as the register flags were not correctly
   parsed by the VDL DAX parser.

   This was tracked in JIRA
    PM-48 [\#169](https://github.com/pegasus-isi/pegasus/issues/169)

5) Handling of initialdir and remote_initialdir keys
   Changed the internal handling for the initialdir and
   remote_initialdir keys. The initialdir key is now only associated
   for standard universe jobs. For glidein and condor style we now
   associate remote_initialdir unless it is a standard universe job.

   This was tracked in JIRA
    PM-58 [\#179](https://github.com/pegasus-isi/pegasus/issues/179)

6) Querying RLI for non existent LFN using rc-client
   rc-client had inconsistent behavior when querying RLI for a LFN
   that does not exist in the RLS.
   This affected the rc-client lookup command option.

   Details at
    PM-74 [\#195](https://github.com/pegasus-isi/pegasus/issues/195)

7) Running clustered jobs on the cloud in directory other than /tmp
   There was a bug whereby the clustered jobs executing in worker node
   execution mode did not honor the wntmp environment variable
   specified in the Site Catalog for the site.

   The bug fix was tracked through JIRA at PM-83 [\#204](https://github.com/pegasus-isi/pegasus/issues/204)


8) Bug Fix for worker package deployment
   The regex employed to determine the pegasus version from a URL to a
   worker package was insufficient. It only took care of x86 builds.

   For e.g. it could not parse the following
   urlhttp://pegasus.isi.edu/mapper/download/nightly/pegasus-worker-2.4.0cvs-ia64_rhas_3.tar.gz
   STATIC_BINARY INTEL64::LINUX NULL

   This is now fixed.
   Related to JIRA PM-33

9) Destination URL construction for worker package staging
   Earlier the worker package input files always had third party
   URL's, even if the worker package deployment job executed on the
   remote site ( in push / pull mode ).

   Now, the third party URL's are only constructed if the worker
   package deployment job is actually run in third party mode.
   In push-pull mode, the destination URL's are file URLs

   Tracked in JIRA at PM-89 [\#207](https://github.com/pegasus-isi/pegasus/issues/207)

#### Documentation

1) User Guides
   The Running on different Grids Guide now has information on how to
   run workflows using glite.
   - Pegasus Replica Selection

   The guides are checked in $PEGASUS_HOME/doc/guides

   They can be found online at
   http://pegasus.isi.edu/mapper/doc.php

2) Property Document was updated with the new properties introduced.


### Pegasus 2.3.0

**Release Date:** April 22, 2009

#### NEW FEATURES

1) Regex Based Replica Selection
   Pegasus now allows users to use regular expression based replica
   selection. To use this replica selector, users need to set the
   following property

   pegasus.selector.replica  Regex

   The Regex replica selector allows the user allows the user to
   specifiy the regex expressions to use for ranking various PFNs
   returned from the Replica Catalog for a particular LFN. This
   replica selector selects the highest ranked PFN i.e the replica
   with the lowest rank value.

   The regular expressions are assigned different rank, that determine
   the order in which the expressions are employed. The rank values
   for the regex can expressed in user properties using the property.

   pegasus.selector.replica.regex.rank.[value]

   The value is an integer value that denotes the rank of an
   expression with a rank value of 1 being the highest rank.

   For example, a user can specify the following regex expressions
   that will ask Pegasus to prefer file URL's over gsiftp url's from
   example.isi.edu

   pegasus.selector.replica.regex.rank.1 file://.*
   pegasus.selector.replica.regex.rank.2 gsiftp://example\.isi\.edu.*

   User can specify as many regex expressions as they want.
   Since Pegasus is in Java , the regex expression support is what
   Java supports. It is pretty close to what is supported by
   Perl. More details can be found at
   http://java.sun.com/j2se/1.5.0/docs/api/java/util/regex/Pattern.html

   There is documentation about the new replica selector in the
   properties document . It can also be found at
   $PEGASUS_HOME/etc/sample.properties

   To use this set pegasus.selector.replica Regex


2) Automatic Determination of pool attributes in RLS Replica Catalog

   Pegasus can now associate a pool attribute with the replica catalog
   entries returned from querying a LRC if the pool attribute is not
   already specified.

   This is achieved by associating the site handles with corresponding
   LRC url's in the properties file. This mapping tells us what
   default pool attribute should be assigned while querying a
   particular LRC. For example

   pegasus.catalog.replica.lrc.site.llo rls://ldas.ligo-la.caltech.edu:39281
   pegasus.catalog.replica.lrc.site.lho rls://ldas.ligo-wa.caltech.edu:39281

   tells Pegasus that all results from LRC
   rls://ldas.ligo-la.caltech.edu:39281 are associated with site llo

   Using this feature only makes sense, when a LRC *ONLY* contains
   mapping for data on one site, as in case of LIGO LDR deployment.

3) Pegasus auxillary jobs on submit host now execute in local universe

   All the scheduler universe jobs are now executed in local
   universe. Also any job planned for site local will by default run
   in local universe instead of scheduler universe.

   Additionally, extra checks were put in to handle the Condor File
   Transfer Mechansim issues in case local/scheduler universe. This
   was tracked in bugzilla at
   http://vtcpc.isi.edu/bugzilla/show_bug.cgi?id=40

   A user can override the local universe generation by specifying the
   condor profile key universe and setting it to the value desired.

4) Python API for generating DAX and PDAX

   Pegasus now includes a Python API for generating DAXes and PDAXes.

   An example can be found online at
   http://vtcpc.isi.edu/pegasus/index.php/ChangeLog#Added_Python_API_for_DAX_and_PDAX

   For more information on the DAX API type: pydoc Pegasus.DAX2
   For more information on the PDAX API type: pydoc Pegasus.PDAX2


5) Interface to Engage VO for OSG

   There is a new Site Catalog Implementation called Engage that
   interfaces with the Engage VO to discover resource information
   about OSG from the information published in RENCI glue classads.

   To use it set
   pegasus.catalog.site Engage

   To generate a site catalog using pegasus-get-sites set the source
   option to Engage

   pegasus-get-sites --source Engage --sc engage.sc.xml

6) Gensim now reports Seqexec Times and Seqexec Delays


   Gensim script ($PEGASUS_HOME/contrib/showlog/gensim) now reports
   the seqexec time and the seqexec delay for the clustered jobs.

   There are two new columns in the jobs file created by seqexec
   - seqexec
   - seqexec delay.

   The seqexec time is determined from the last line of the .out file
   of the clustered jobs. E.g format [struct stat="OK", lines=4,
   count=4, failed=0,
   duration=21.836,start="2009-02-20T16:14:56-08:00"]

   The seqexec delay is the seqexec time - kickstart time.

   This useful for analyzing large scale workflow runs.

7) Properties to turn on or off the seqexec progress logging

   The property  pegasus.clusterer.job.aggregator.seqexec.hasgloballog
   is now deprecated.

   It has been replaced by  two boolean properties
   - pegasus.clusterer.job.aggregator.seqexec.log whether to log
     progress or not
   - pegasus.clusterer.job.aggregator.seqexec.log.global whether to
     log progress to global file or not.

     The pegasus.clusterer.job.aggregator.seqexec.log.global only
     comes into effect when
     pegasus.clusterer.job.aggregator.seqexec.log is set to true


8) Passing of the DAX label to kickstart invocation

   Now, the kickstart invocation for the jobs is always passed the dax
   label using the -L option. To disable the passing of the DAX label,
   user needs to set pegasus.gridstart.label to false

   Additionally, the basename option to pegasus-plan overrides the
   label value retrieved from the DAX.

9) show-job works on MAC OSX platform

   $PEGASUS_HOME/contrib/showlog/show-job now does not fail on
   unavailability of convert program. It only logs a warning and
   creates the    EPS File , but not the png files. This allows us to
   run show-job on MAC OSX systems.

10) Enabling InPlace cleanup in deferred planning

    By default in case of deferred planning cleanup is turned off as
    the cleanup algorithm does not work across partitions.
    However, in scenarios where the partitions themseleves are
    independant ( i.e. dont share files ), user can safely turn on
    cleanup.

    This can now be done by setting
    pegasus.file.cleanup.scope  deferred

    If the property is set to deferred, and the users wants to disable
    cleanup , they can still specify --nocleanup option on command
    line and that is honored.

    However in case of scope fullahead for deferred planning, the
    command line options are ignored and always nocleanup is set.

11) New Pegasus Job Classad

    Pegasus now publishes a job runtime classad with the jobs. The
    class ad key name is pegasus_job_runtime. The value passed to it
    is picked up from the Pegasus Profile runtime. If the Pegaus
    Profile is not associated, then the globus maxwalltime profile key
    is used. If both are not set, then a value of zero is published.

    This job classad can be used for users in case of glidein, to
    ensure that the jobs complete before the nodes expire.

    For the coral glidein service the sub expression to job
    requirement swould look something like this

    (CorralTimeLeft > MY.pegasus_job_runtime)

12) [workflow].job.map file

    Pegasus now creates a [workflow].job.map file that links jobs in
    the DAG with the jobs in the DAX. The contents of the file are in
    netlogger format.

    The [workflow] is replaced by the name of the workflow i.e. same
    prefix as the .dag file

    In the file there are two types of events.
    a) pegasus.job
    b) pegasus.job.map

    pegasus.job - This event is for all the jobs in the DAG. The
    following information is associated with this event.

    - job.id the id of the job in the DAG
    - job.class an integer designating the type of the job
    - job.xform the logical transformation which the job refers to.
    - task.count the number of tasks associated with the job. This is
       equal to the number of pegasus.job.task events created for that
     job.

    pegasus.job.map - This event allows us to associate a job in the
    DAG with the jobs in the DAX. The following information is
    associated with this event.

    -task.id the id of the job in the DAG
    -task.class an integer designating the type of the job
    -task.xform the logical transformation which the job refers to.


13) Source Directory for Worker Package Staging

    Users now can specify the  property
    pegasus.transfer.setup.source.base.url to specify the URL to the
    source directory containing the pegasus worker packages. If it is
    not specified, then the worker packages are pulled from the http
    server at pegasus.isi.edu during staging of executables.


#### BUGS FIXED

1)  Critical Bug Fix to rc-client

    SCEC reported a bug with the rc-client while doing bulk inserts
    into RLS. The bug was related to how logging is initialized
    internally in the client.

    Details of the bug fix can be found at
    http://vtcpc.isi.edu/bugzilla/show_bug.cgi?id=38

2)  Bug Fix to tailstatd for parsing jobnames with . in them

    There was a bug where tailstatd incorrectly generated events in
    the jobstate.log while parsing condor logs. This was due to an
    errorneous regex expression for determining the event
    POST|PRE SCRIPT STARTED.

    The earlier expression did not allow for . in jobnames. This is
    especially prevalent in LIGO workflows where the DAX labels have
    . in them.

    An example of the problem line in DAGMan log
    1/24 10:11:21 Running POST script of Node
    inspiral_hipe_eobinj_cat2_veto.EOBINJ_CAT_2_VETO.daxlalapps_sire_ID000731...

    Earlier the job id was parsed as inspiral_hipe_eobinj_cat2_veto
    instead of
    inspiral_hipe_eobinj_cat2_veto.EOBINJ_CAT_2_VETO.daxlalapps_sire_ID000731

3) Pegasus Builds on FC10

   Earlier the Pegasus builds were failed on FC10 as the invoke c tool
   did not build correctly. This is now fixed.

   Details at
   http://vtcpc.isi.edu/bugzilla/show_bug.cgi?id=41

4) tailstatd killing jobs by detecting starvation

   tailstatd removes a job after four hours when the job has been
   waiting in the queue WITHOUT being marked as EXECUTE in the condor
   log. To override tailstatd has an option of setting starvation time
   to 0 via command line or via pegasus.max.idletime property.  The if
   condition in the perl script was not accepting 0 as a value when
   trying to override the default 4 hour starvation time. This fix
   allows the value to be set to 0 (turn of starvation checks) or any
   other value via the property pegasus.max.idletime.

   This was tracked in pegasus jira as bug 40
    PM-40 [\#161](https://github.com/pegasus-isi/pegasus/issues/161)

#### Documentation

1) User Guides
   The release has new user guides about the following
   - Pegasus Job Clustering
   - Pegasus Profiles
   - Pegasus Replica Selection

   The guides are checked in $PEGASUS_HOME/doc/guides

   They can be found online at
   http://pegasus.isi.edu/mapper/doc.php

2) Property Document was updated with the new properties introduced.

### Pegasus 2.2.0

**Release Date:** January 11, 2009

#### NEW FEATURES

1) Naming scheme changed for auxillary jobs

   Pegasus during the refinement of the abstract workflow to the
   executable workflows adds auxillary jobs to do data stagein/stageout,
   create work directories for workflow etc. The prefixes/suffixes added
   for these jobs has been changed.

   Type of Job			    |   Old Prefix	 | New Prefix
   -------------------------|----------------|--------------------------
   Data Stage In Job		    |  rc_tx_		 | stage_in_
   Data Stage Out Job 	 	    |  new_rc_tx_	 | stage_out_
   Data Stage In Job between sites  |  inter_tx_	 | stage_inter_
   Data Registration Job	    |  new_rc_register_	 | register_
   Cleanup Job	     		    |  cln_	         | clean_up_
   Transfer job to transfer the	    |  setup_tx_ 	 | stage_worker_
   worker package  	    	    |  			 |

   Additionally, the suffixes for the create dir jobs are now replaced
   by prefixes

   Type of Job			    |   Old Suffix	 | New Prefix
   -------------------------|----------------|-------------------------
   Directory creation job	    |  _cdir		 | create_dir_
   Synch Job in HourGlass mode	    |  pegasus_concat	 | pegasus_concat_


2) Staging of worker package to remote sites

   Pegasus now supports staging of worker package as part of the workflow.

   This feature is tracked through pegasus bugzilla .

   http://vtcpc.isi.edu/bugzilla/show_bug.cgi?id=35

   The worker package is staged automatically to the remote site, by
   adding a setup transfer job to the workflow.

   The setup transfer job by default uses GUC to stage the data.
   However, this can be configured by setting the property
   pegasus.transfer.setup.impl property. If you also, have
   pegasus.transfer.*.impl set in your properties file, then you need
   explicilty set pegasus.transfer.setup.impl to GUC


   The code discovers the worker package by looking up pegasus::worker
   in the transformation catalog.
   Note: that the basename of the url's should not be changed. Pegasus
   parses the basename to determine the version of the worker package.
   Pegasus automatically determines the location of the worker package
   to deploy on the remote site. Currently default mappings are as
   follows

   INTEL32 => x86
   AMD64 => x86_64 or x86 if not available
   INTEL64 =>x86

   OS LINUX = rhel3

   There is an untar job added to the workflow after the setup job that
   un tars the worker package on the remote site. It defaults to /bin/tar .
   However can be overriden by specifying the entry tar in the
   transformation catalog for a particular site.


3) New Site Catalog Schema

   This release of Pegasus has support for site catalog schema version 3.

   HTML visualization of schema:
   http://pegasus.isi.edu/mapper/docs/schemas/sc-3.0/sc-3.0.html

   Schema itself:
   http://pegasus.isi.edu/schema/sc-3.0.xsd

   A sample xml file :
   http://pegasus.isi.edu/schema/sc-3.0-sample.xml

   To use a site catalog in the new format set
   pegasus.catalog.site  XML3

   Changes to sc-client

   sc-client command line tool was updated to convert an existing
   site catalog from old format to the new format.

   Sample usage
```
   sc-client -i ldg-old-sites.xml -I XML -o ldg-new-sites.xml -O XML3
   sc-client --help gives detailed help
```
4) pegasus-get-sites

   pegasus-get-sites was recoded in JAVA and now generates the site
   catalog confromant to schema sc-3.0.

   Sample Usage to query VORS to generate a site catalog for OSG.
   pegasus-get-sites --source VORS --grid osg -s ./sites-new.xml

   The value passed to the source option is case sensitive.

   Additionally, the VORS module of pegasus-get-sites determines the
   value of GLOBUS_LOCATION variable dependant on whether the
   auxillary jobmanager is of type fork or not.

   If it is of type fork then picks up the value of GLOBUS_LOCATION
   variable published in VORS for that site. else it picks up the
   value from OSG_GRID variable published in VORS for that
   site. i.e. GLOBUS_LOCATION is set to $OSG_GRID/globus

5) Overhaul of logging

   The Pegasus logging interfaces have been reworked.
   Now users can specify the logger they want to use, by specifying the
   property pegasus.log.manager .

   Currently, two logging implementations are supported.

   - Default - Pegasus homegrown logger that logs to stdout and stderr
   	     directly.
   - Log4j  -  Uses log4j to log the messages.

   The Log4j properties can be specified at runtime by specifying the
   property pegasus.log.manager.log4j.conf

   The format of the log message themselves can be specified at runtime
   by specifying the property pegasus.log.manager.formatter

   Right now two formatting modes are supported

   a) Simple - This formats the messages in a simple format. The
      messages are logged as is with minimal formatting. Below are
      sample log messages in this format while ranking a dax according
      to performance.
```
      event.pegasus.ranking dax.id se18-gda.dax - STARTED
      event.pegasus.parsing.dax dax.id se18-gda-nested.dax - STARTED
      event.pegasus.parsing.dax dax.id se18-gda-nested.dax - FINISHED
      job.id jobGDA
      job.id jobGDA query.name getpredicted performace time 10.00
      event.pegasus.ranking dax.id se18-gda.dax - FINISHED
```

   b) Netlogger - This formats the messages in the Netlogger format ,
      that is based on key value pairs. The netlogger format is useful
      for loading the logs into a database to do some meaningful analysis.
      Below are sample log messages in this format while ranking a dax
      according to performance.
```
      ts=2008-09-06T12:26:20.100502Z event=event.pegasus.ranking.start \
      msgid=6bc49c1f-112e-4cdb-af54-3e0afb5d593c \
      eventId=event.pegasus.ranking_8d7c0a3c-9271-4c9c-a0f2-1fb57c6394d5 \
      dax.id=se18-gda.dax prog=Pegasus

      ts=2008-09-06T12:26:20.100750Z event=event.pegasus.parsing.dax.start \
      msgid=fed3ebdf-68e6-4711-8224-a16bb1ad2969 \
      eventId=event.pegasus.parsing.dax_887134a8-39cb-40f1-b11c-b49def0c5232\
      dax.id=se18-gda-nested.dax prog=Pegasus

      ts=2008-09-06T12:26:20.100894Z event=event.pegasus.parsing.dax.end \
      msgid=a81e92ba-27df-451f-bb2b-b60d232ed1ad \
      eventId=event.pegasus.parsing.dax_887134a8-39cb-40f1-b11c-b49def0c5232

      ts=2008-09-06T12:26:20.100395Z event=event.pegasus.ranking \
      msgid=4dcecb68-74fe-4fd5-aa9e-ea1cee88727d \
      eventId=event.pegasus.ranking_8d7c0a3c-9271-4c9c-a0f2-1fb57c6394d5 \
      job.id="jobGDA"

      ts=2008-09-06T12:26:20.100395Z event=event.pegasus.ranking \
      msgid=4dcecb68-74fe-4fd5-aa9e-ea1cee88727d \
      eventId=event.pegasus.ranking_8d7c0a3c-9271-4c9c-a0f2-1fb57c6394d5 \
      job.id="jobGDA" query.name="getpredicted performace" time="10.00"

      ts=2008-09-06T12:26:20.102003Z event=event.pegasus.ranking.end \
      msgid=31f50f39-efe2-47fc-9f4c-07121280cd64 \
      eventId=event.pegasus.ranking_8d7c0a3c-9271-4c9c-a0f2-1fb57c6394d5
```

6) New Transfer Refiner

   Pegasus has a new transfer refiner named Cluster.

   In this refinement strategy, clusters of stage-in and stageout jobs are
   created per level of the workflow. It builds upon the Bundle refiner.

   The differences between the Bundle and Cluster refiner are as follows.
   - stagein is also clustered/bundled per level. In Bundle it was
     for the whole workflow.
   - keys that control the clustering ( old name bundling are )
     cluster.stagein and cluster.stageout instead of bundle.stagein and
     bundle.stageout

   This refinement strategy also adds dependencies between the stagein
   transfer jobs on different levels of the workflow to ensure that stagein
   for the top level happens first and so on.

   An image of the workflow with this refinement strategy can be found
   at
   http://vtcpc.isi.edu/pegasus/index.php/ChangeLog#Added_a_Cluster_Transfer_Refiner

7) New Transfer Implementation for GUC from globus 4.x

   Pegasus has a new transfer implementation that allows it to use GUC
   from globus 4.x series to transfer multiple files in one job.

   In order to use this transfer implementation
      - the property pegasus.transfer.*.impl must be set to value GUC.

   There should be an entry in the transformation catalog with the
   fully qualified  name as globus::guc for all the sites where
   workflow is run, or on the local site in case of third party
   transfers.

   Pegasus can automatically construct the path to the globus-url-copy
   client, if the environment variable GLOBUS_LOCATION is specified in
   the site catalog for the site.

   The arguments with which the client is invoked can be specified
         - by specifying the property pegasus.transfer.arguments
         - associating the Pegasus profile key transfer.arguments


8) Recursive DAX'es

   There is prototypical support for recursive dax'es. Recursive
   DAX'es give you the ability to specify a job in the DAX that points
   to another DAX that has to be executed.

   There is a sample recursive dax at
   $PEGASUS_HOME/examples/recursive.dax

   The dax refers to pegasus jobs in  turn plan and execute a dax

   To get this dax planned by pegasus you will need to have additional
   entries for dagman and pegasus in your transformation catalog.

   For e.g.
```
   local   condor::dagman  /opt/condor/7.1.0/bin/condor_dagman     INSTALLED       INTEL32::LINUX  NULL
   local  pegasus::pegasus-plan:2.0       /lfs1/software/install/pegasus/default  INSTALLED       INTEL32::LINUX  NULL
```

   The recursive dax needs to be planned for site local, since the
   pegasus itself runs on local site. The jobs in the dax specify -s
   option where you want each of your workflows to run.

   Recursive DAX do not need to contain only pegasus jobs. They can
   contain application/normal jobs that one usually specifies in a
   DAX. Pegasus determines that a particular job is planning and
   execute job by looking for a pegasus profile key named type with
   value recursive e.g.

```xml
   <job id="ID0000003" namespace="pegasus" name="pegasus-plan" version="2.0">
    <profile namespace="pegasus" key="type">recursive</profile>
    <argument>-Dpegasus.user.properties=/lfs1/work/conf/properties
    --dax /lfs1/work/dax3  -s tacc -o local --nocleanup  --force
    --rescue 1 --cluster horizontal -vvvvv --dir ./dag_3
    </argument>
   </job>
```
09) Rescue option to pegasus-plan for deferred planning

    A rescue option to pegasus-plan has been added. The rescue option
    takes in an integer value, that determines the number of times
    rescue dags are submitted before re-planning is triggered in case
    of failures in deferred planning. For this to work, Condor 7.1.0
    or higher is required as it relies on the recently implemented
    auto rescue feature in Condor DAGMan.

    Even though re-planning is triggered, Condor DAGMan still ends up
    submitting the rescue dag as it auto detects. The fix to it is to
    remove the  rescue dag files in case of re-planning. This is still
    to be implemented

10) -j|--job-prefix option to pegasus-plan

    pegasus-plan can now be passed the -j|--job-prefix option to
    designate the prefix that needs to be used for constructing the
    job submit file.


11) Executing workflows on Amazon EC2

   Pegasus now has support of running workflows on EC2 with the
   storage of files on S3.  This feature is still in testing phase and
   has not been tested fully.

   To execute workflows on EC2/S3, Pegasus needs to be configured to
   use S3 specific implementations of it's internal API's

   a) First level Staging API - The S3 implementation stages in from the
      local site ( submit node ) to a bucket on S3. Similarly the data is
      staged back from the bucket to the local site ( submit node )
      . All the first level transfers happen between the submit node
      and the cloud. This means that input data can *only* be present
      on the submit node when running on the cloud, and the output
      data can be staged back only to the submit node.

   b) Second Level Staging API - The S3 implementation retrieves input
      data from the bucket to the worker node tmp directory and puts
      created data back in the bucket.

   c) Directory creation API - The S3 implementation creates a bucket
      on S3 for the workflow instead of a directory.

   d) Cleanup API - To cleanup files from the workflow specific bucket
      on S3 during workflow execution.

   The above implementations rely on s3cmd command line client to
   interface with S3 filesystem. There should be an entry in the
   transformation catalog with the fully qualified name as
   amazon::s3cmd for the site corresponding to the cloud and the local
   site.

   To configure Pegasus to use these implementations set the following
   properties
```
   pegasus.transfer.*.impl                      S3
   pegasus.transfer.sls.*.impl                S3
   pegasus.dir.create.impl                      S3
   pegasus.file.cleanup.impl                  S3
```
   pegasus.execute.*.filesystem.local   true


12) Support for OSU Datacutter jobs

   Pegasus has new gridstart mode called DCLauncher. This allows us to
   launch the Data Cutter jobs using the wrapper that OSU group wrote.

   Pegasus now supports the condor parallel universe.
   To launch a job using DCLauncher, the following pegasus profile
   keys need to be associated with the job
```
   gridstart          to DCLauncher
   gridstart.path     the path to the DCLauncher script
```
13) New Pegasus Profiles Keys

    a) create.dir - this profile key triggers kicstart to create and
       change directories before launching a job.

    b) gridstart.path - this profile key specifies the path to the
       gridstart used to launch a particular job

    c) runtime - this profile key is useful when using Heft based site
       selection. It allows users to associate expected runtimes of
       jobs with the job description in DAX.

14) Kickstart captures machine information
    Kickstart now logs machine information in the invocation record
    that it creates for each job invocation. The Kickstart JAVA parser
    can parse both records in old and new format.

    A snippet of machine information captured is show below
```xml
    <machine page-size="4096" provider="LINUX">
     <stamp>2008-09-23T13:58:05.211-07:00</stamp>
     <uname system="linux" nodename="viz-login" release="2.6.11.7"
       machine="i686">#2 SMP Thu Apr 28 18:41:14 PDT  2005</uname>
     <ram total="2125209600" free="591347712" shared="0" buffer="419291136"/>
     <swap total="2006884352" free="2006876160"/>
     <boot idle="943207.170">2008-09-12T12:03:49.772-07:00</boot>
     <cpu count="4" speed="2400" vendor="GenuineIntel">Intel(R)
       Xeon(TM) CPU 2.40GHz</cpu>
     <load min1="0.07" min5="0.04" min15="0.00"/>
     <proc total="110" running="1" sleeping="109" vmsize="614912000"
      rss="206729216"/>
     <task total="133" running="1" sleeping="132"/>
  </machine>
```
15) Kickstart works in cygwin environment

    Kickstart now compiles on cygwin. Kickstart could not find
    SYS_NMLN variable in Cygwin to determine the uname datastructure's
    size. Added a fix in the Makefile to add CFLAGS -DSYS_NMLN=20 when
    the OS is Cygwin/Windows

    The kickstart records generated on cygwin are slightly different
    from the ones generated unix platforms. The kickstart parser was
    modified to handle that.

    The differences are as follows -

    a) On cygwin inode value is double. The inode value is parsed as
    double , but cast to long to prevent errors.

    b) On cygwin the uid and gid values are long. They are passed as
    long, but cast to int to prevent errors.

16) Changes to dirmanager
    The dirmanager executable can now remove and create multiple
    directories. This is achieved by specifying a whitespace separated
    list of directories to the --dir option.


17) Added color-file option to showjob

    There is now a  --color-file option to show-job in
    $PEGASUS_HOME/contrib/showlog to pass a file that has the mappings
    from transformation name to colors.

    The format of each line is as follows
    transformation-name color

    This can be used to assign different colors to compute jobs in a
    workflow. The default color assigned is gray if none is
    specified.

18) jobstate-summary tool

    There is a new tool at $PEGASUS_HOME/bin/jobstate-summary.

    It attempts to give a summary for the workflow. Should help in
    jobstate-summ debugging failed job information. It will shows all
    the information associated with a failed job. It gets the list of
    failed job from the jobstate.log file. After that it parses latest
    kickstart file for each failed job and show the exit code and all
    the other information.

    Usage:
    ```
    jobstate-summary --i <input directory> [--v(erbose)]
           [--V(ersion)] [--h(elp)]
    ```
    Input directory is the place where all the log files including jobstate.log file reside.

    - v option is for verbose debugging.
    - V option gives the pegasus version.
    - h option prints the help message.
    - A sample run is like jobstate-summary -i /dags/pegasus/diamond/run0013 -v

19) Support for DAGMan node categories

    Pegasus now supports DAGMan node categories. DAGMan now allows to
    specify CATEGORIES for jobs, and then specify tuning parameters (
    like maxjobs ) per category. This functionality is exposed in
    Pegasus as follows

    The user can associate a dagman profile key category with the
    jobs. The key attribute for the profile is category and value is
    the category to which the job belongs to. For example you can set
    the dagman category in the DAX for a job as follows

```xml
    <job id="ID000001" namespace="vahi" name="preprocess" version="1.0" level="3" dv-namespace="vahi" dv-name="top" dv-version="1.0">
       <profile namespace="dagman" key="CATEGORY">short-running</profile>
       <argument>-a top -T 6  -i <filename file="david.f.a"/>  -o
         <filename file="vahi.f.b1"/>
         <filename file="vahi.f.b2"/>
       </argument>
       <uses file="david.f.a" link="input" register="false"
       transfer="true" type="data"/>
       <uses file="vahi.f.b1" link="output" register="true"
       transfer="true" />
       <uses file="vahi.f.b2" link="output" register="true"
       transfer="true" />
    </job>
```
The property pegasus.dagman.[category].maxjobs can be used to
control the value.

For the above example, the user can set the property as follows

```
    pegasus.dagman.short-running.maxjobs 2
```

In the DAG file generated you will see the category associated
with jobs. For the above example, it will look as follows

```
    MAXJOBS short-running 2
    CATEGORY preprocess_ID000001 short-running
    JOB preprocess_ID000001 preprocess_ID000001.sub
    RETRY preprocess_ID000001 2
```
20) Handling of pass through LFN

    If a job in a DAX, specifies the same LFN as an input and an
    output, it is a pass through LFN. Internally, the LFN is tagged
    only as an input for the job. The reason for this, being that we
    need to make sure that the replica catalog is queried for the
    location of the LFN. If this is not handled specially, then LFN is
    tagged internally as inout ( meaning it is generated during
    workflow execution ). LFN's with type inout are not queried for in
    the Replica Catalog in the force mode of operation


21) Tripping seqexec on first job failures

    By default seqexec does not stop execution even if one of the
    clustered jobs it is executing fails. This is because seqexec
    tries to get as much work done as possible. If for some reason,
    you want to make seqexec stop on first job failure, set the
    following property in the properties file

    pegasus.clusterer.job.aggregator.seqexec.firstjobfail true


22) New properties to choose the cleanup implementation

    Two new properties were added to select the strategy and
    implementation for file cleanup.

    pegasus.file.cleanup.strategy
    pegasus.file.cleanup.implementation

    Currently there is only one cleanup strategy ( InPlace ) that can
    be used and is loaded by default.

    The cleanup implementations that can be used are
    	- Cleanup ( default)
	- RM
	- S3

    Detailed documentation can be found at
    $PEGASUS_HOME/etc/sample.properties.

23) New properties to choose the create dir implementation

    The property pegasus.dir.create was deprecated.

    It has been replaced by
    pegasus.dir.create.strategy

    Additionally, a user can specify a property to choose the
    implementation used to create the directory on the remote sites.

    pegasus.dir.create.impl

    The create directory implementation that can be used are
    - DefaultImplementation   uses $PEGASUS_HOME/bin/dirmanager
                              executable to create a directory on the
    			      remote site.
    - S3   		      usese s3cmd to create a bucket on amazon S3.


#### BUGS FIXED

1) Makefile for kickstart to build on Cygwin

   Kickstart could not find SYS_NMLN variable in Cygwin to determine
   the uname datastructure's size. Added a fix in the Makefile to add
   CFLAGS -DSYS_NMLN=20 when the OS is Cygwin/Windows


2) Bug fix to getsystem release tools

   Some systems have started using / in their system version name
   which causes failures in Pegasus build process. Fixed the getsystem
   release script which converts / into _

3) Bug fix in file cleanup module when stageout is enabled.

   There was a bug in how the dependencies are added between the
   stageout jobs and the file cleanup jobs. In certain cases, cleanup
   could occur before the output was staged out. This is fixed now.

   This bug was tracked through bugzilla
   http://vtcpc.isi.edu/bugzilla/show_bug.cgi?id=37

4) Bug fix to deferred planning

   Deferred planning used to fail if pegasus-plan was not given -o
   option .

   This is fixed now and was tracked through bugzilla
   http://vtcpc.isi.edu/bugzilla/show_bug.cgi?id=34

5) Bug fix to caching on entries from Transformation Catalog

   In certain cases, caching of entries did not work for the INSTALLED
   case.

   This is fixed now and was tracked through bugzilla
   http://vtcpc.isi.edu/bugzilla/show_bug.cgi?id=33

### Pegasus 2.1.0

**Release Date:** May 6, 2008

#### NEW FEATURES

1) Support for Second Level Staging

   Normally, Pegasus transfers the data to and from a directory on the
   shared filesystem on the head node of a compute site. The directory
   needs to be visible to both the head node and the worker nodes for
   the compute jobs to execute correctly.

   In the case, where the worker nodes cannot see the filesystem of
   the head node there needs to be a Second Level Staging (SLS)
   process that transfers the data from the head node to a directory
   on the worker node tmp. To achieve this, Pegasus uses the pre-job
   and post-job feature of kickstart to pull the input data from the
   head  node and push back the output data of a job to the head
   node.

   Even though we do SLS, Pegasus still relies on the existence of a
   shared file system due to the following two reasons

   a) for the transfer executable to pick up the proxy, that we
   transfer from the submit host to the head node.

   b) to access sls input and output files that contain the file
   transfer urls to manage the transfer of data to worker node and
   back to headnode.


   Additionally, if you are running your workflows on a Condor pool,
   one can bypass the use of kickstart to do the SLS. Please contact
   pegasus@isi.edu for more details of this scenario. In this case,
   the workflows generated by Pegasus have been shown to run in total
   non shared filesystem environment.

   To use this feature, user needs to set

   pegasus.execute.*.filesystem.local  true

   The above change was tracked via bugzilla
   http://vtcpc.isi.edu/bugzilla/show_bug.cgi?id=21


2) New DAX schema
   The new has release moved to the new DAX schema version 2.1. Schema
   is  available online http://pegasus.isi.edu/schema/dax-2.1.xsd

   The main change in it is that the dontTransfer and dontRegister
   flags have been replaced by transfer and register flags. Changes
   were made both to the Java DAX Generator and Pegasus to conform to
   the new schema.

   Additionally, the DAX parser in Pegasus looks at the schema version to
   determine whether to pick up dontTransfer and dontRegister flags (
   to support backward compatibility with the older daxes).

   Also with the filename type added a type attribute. It defaults to
   data. Additionally user can have the values
   executable|pattern. Users can use type=executable to specify any
   dependant executables that their jobs required. All executable
   files are tracked in the transformation catalog.

   The above change was tracked via bugzilla
   http://vtcpc.isi.edu/bugzilla/show_bug.cgi?id=6


3) Workflow and Planner Metrics Logging
   Workflow and Planning metrics are now logged for each workflow that
   is planned by Pegasus. By default, they are logged to
   $PEGASUS_HOME/var/pegasus.log

   To turn metrics logging off, set pegasus.log.metrics to false

   To change the file to which the metrics are logged set
   pegasus.log.metrics.file  path/to/log/file

   Here is a snippet from the log file that shows what is logged
```
  {
   user = vahi
   vogroup = pegasus-ligo
   submitdir.base = /nfs/asd2/vahi/jbproject/Pegasus/dags
   submitdir.relative = /vahi/pegasus-ligo/blackdiamond/run0064
   planning.start = 2007-09-24T18:14:23-07:00
   planning.end = 2007-09-24T18:14:29-07:00
   properties
                 =/nfs/asd2/vahi/jbproject/Pegasus/dags/vahi/pegasus-ligo/blackdiamond/run0064/pegasus.6766.properties
   dax = /nfs/asd2/vahi/jbproject/Pegasus/blackdiamond_dax.xml
   dax-label = blackdiamond
   compute-jobs.count = 3
   si-jobs.count = 1
   so-jobs.count = 3
   inter-jobs.count = 0
   reg-jobs.count = 3
   cleanup-jobs.count = 2
   total-jobs.count = 14
   }
```

4) Support for querying multiple replica catalogs

   Pegasus now allows the users to query multiple replica catalogs at
   the same time to discover the locations of input data sets.

   For this a new Replica Catalog implmentation was developed.

   The users need to do the following to use it.

   Set the replica catalog to MRC in the properties file.

   pegasus.catalog.replica MRC

   Each associated replica catalog can be configured via properties as
   follows. The user associates a variable name referred to as [value]
   for each of the catalogs, where [value] is any legal identifier
   (concretely [A-Za-z][_A-Za-z0-9]*)

   For each associated replica catalogs the user needs to specify the following properties.

   pegasus.catalog.replica.mrc.[value]      to specify the type of  replica catalog
   pegasus.catalog.replica.mrc.[value].key  to specify a property name key for a
                                            particular catalog


   For example, if a user wants to query two lrc's at the same time he/she can specify as follows

    pegasus.catalog.replica.mrc.lrc1 LRC
    pegasus.catalog.replica.mrc.lrc2.url rls://sukhna

    pegasus.catalog.replica.mrc.lrc2 LRC
    pegasus.catalog.replica.mrc.lrc2.url rls://smarty

   In the above example, lrc1, lrc2 are any valid identifier names and
   url is the property key that needed to be specified.


5) Local Replica Selector

   Pegasus has a new local replica selector that only prefers replicas
   from the local host and that start with a file: URL scheme.  It is
   useful, when users want to stagin files to a remote site from your
   submit host using the Condor file transfer mechanism.

   In order to use this, set the replica selector to Local in the
   properties.

        - pegasus.selector.replica  Local


6) Heft Based Site Selector

   Added a new site selector that is based on the HEFT processor
   scheduling algorithm.

   The implementation assumes default data communication costs when jobs
   are  not scheduled on to the same site. Later on this may be made more
   configurable.

   The runtime for the jobs is specified in the transformation catalog
   by associating the pegasus profile key runtime with the entries.

   The number of processors in a site is picked up from the attribute
   idle-nodes associated with the vanilla jobmanager of the site in
   the site catalog.


   To use this site selector, users need to set the following property

   pegasus.selector.site Heft


7) Using multiple grid ftp servers for stageout
   If a user specifies multiple grid ftp servers for the output site
   in the site catalog, the stageout jobs will be distributed over all
   of them.

   More info can be found at
   http://vtcpc.isi.edu/bugzilla/show_bug.cgi?id=3


8) Scalable Directory structure on the stageout site
   Users can now distribute their output files in a directory
   structure on the output site. On setting the Boolean property
   pegasus.dir.storage.deep to true, the relative submit directory
   structure is replicated on the output site. Additionally, within
   this directory the files are distributed into sub directories with
   each subdirectory having 256 files.

   The subdirectories are named in decimal format.


9) Specifying the jobmanager universe for the compute jobs in the DAX
   Users can know specify the jobmanager type for the compute jobs in the
   DAX. This is achieved by specifying the jobmanager.universe profile
   key in the hints namespace.

   Valid values for this are transfer|vanilla.

   This is useful for users who are running on a grid site, with the
   worker nodes behind a firewall and want a subset of their jobs to
   run on the head node.

   More info can be found at
   http://vtcpc.isi.edu/bugzilla/show_bug.cgi?id=5


10) Stork Support for doing transfers

   The internal transfer interfaces of Pegasus were updated to use the
   latest version of Stork for managing data transfers.

   To use Stork implementations set the following
   pegasus.transfer.refiner = SDefault
   pegasus.transfer.*.impl=Stork

11) nogrid option for pegasus-run

 pegasus-run has now a --nogrid option. This bypasses the checks for
   proxy existence that are done before submitting the workflow for
   execution. It disables all globus checks like check for environment
   variables GLOBUS_lOCATION and LD_LIBRARY_PATH.

   This is useful for running workflows in native Condor environments.

12) Submitting workflows directly using pegauss-plan

   A new option --submit|-S option was added to pegasus-plan. This
   allows users to submit workflows directly, after they have been
   planned.

13) Specifying relative submit directory
   Since pegasus 2.0 , pegasus-plan creates a directory structure in
   the base submit directory. The base submit directory is specified
   by --dir option to pegasus-plan. If a user, want to specify a
   relative submit directory, he can use the --relative-dir option to
   pegasus-plan.

   The above change was tracked via bugzilla
   http://vtcpc.isi.edu/bugzilla/show_bug.cgi?id=14


#### BUGS FIXED

1) Specifying Relative Path to the DAX

   An incorrect path to the dax was generated internally when a user
   specified a relative path to the dax to pegasus-plan

   This is fixed now, and was tracked via bugzilla
   http://vtcpc.isi.edu/bugzilla/show_bug.cgi?id=13


2) RLS java api bug fix 4114 (globus bugzilla number )

   globus_rls_client.jar updated with the bug fix 4114. Also added the
   jar for java 1.4 in lib/java1.4

3) Passing of DAGMan parameters via properties in case of deferred
   planning

   In case of deferred planning, the properties that control DAGMan
   execution were not being passed as options to DAGMan. This is now
   fixed.

   The following properties are being handled correctly now.

   pegasus.dagman.maxjobs
   pegasus.dagman.maxpre
   pegasus.dagman.maxidle
   pegasus.dagman.maxpost


### Pegasus 2.0.1

**Release Date:** July 19, 2007

There is new documentation in the form of a quick start guide and glossary
in the docs directory. More documentation will be coming soon and will be available
in the release as well as on the pegasus website under documentation.

#### NEW FEATURES

1) Pegasus now can store provenance data into PASOA. The actions taken
   by the various refiners are logged into the store. It is still an
   experimental feature. To turn it on, set the property

   pegasus.catalog.provenance.refinement  pasoa

   The PASOA store needs to run on localhost on port 8080
   https://localhost:8080/preserv-1.0

2) You can also use Pegasus to store execution provenance in PASOA.
   To use set the properties
   pegasus.exitcode.impl=pasoa
   pegasus.exitcode.path.pasoa=${pegasus.home}/bin/pasoa-client
   pegasus.exitcode.arguments=<dax file> <dag file>

#### BUGS FIXED

1) sitecatalog-converter
   patch to fix pegasus profile conversion

2) pegasus-submit-dag
   added --maxidle option to allow setting number of idle jobs on
   the remote site.

3) VORS.pm
   Fixed a small typo in there, that lead to perl compilation
   errors.

4) pegasus-get-sites
   Removed local tc entries and added environments
   for PEGASUS_HOME, GLOBUS_LOCATION and LD_LIBRARY_PATH to local
   site.

5) mpiexec
   The execution of clustered jobs via mpiexec was broken in 2.0
   release. That is now fixed.

6) exitcode/exitpost
   Fixed a bug in exitcode that caused a call to the DB PTC even though
   the property pegasus.catalog.provenance was not set.


### Pegasus 2.0.0

**Release Date:** July 19, 2007

#### NEW FEATURES

pegasus-plan.
	This is the main client for invoking pegasus. The earlier gencdag command is now called pegasus-plan

pegasus-run
	This is the client that submits the planned workflow to Condor and starts a monitoring tailstatd daemon

pegasus-status
	This client lets you monitor a particular workflow. Its a wrapper around condor-q

pegasus-remove.
	This client lets you remove a running workflow from the condor queue.
        A rescue dag will be generated which can be submitted by just running pegasus-run on the dag directory.


