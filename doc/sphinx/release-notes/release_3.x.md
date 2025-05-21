## Pegasus 3.x Series

### Pegasus 3.1.1

**Release Date:** February 14, 2012

This is a minor release, that fixes some bugs and has minor enhancements.


#### NEW FEATURES

1) the jobs file created by pegasus-statistics includes hostname and
exitcode

   The hostname of the host on which the job ran and the exitcode with
   which job exited is now included in the jobs file created by
   pegasus-statistics .

    PM-487 [\#605](https://github.com/pegasus-isi/pegasus/issues/605)


2) pegasus-monitord truncates stdout/stderr

   pegasus-monitord truncates stdout/stderr if they exceed the maximum
   size allowed by the database.

   The boolean property to disable stdout/stderr parsing is

   pegasus.monitord.stdout.disable.parsing

3) logging improvements for pegasus-monitord

   monitord adds logging information when exiting upon a signal. The
   netlogger DB backend code is now more verbose when errors happen.

   pegasus-monitord also prints the start and end of the DB flushing
   function in its logs.

   pegasus-monitord prints its pid whenever it outputs the
   starting/ending messages. This can be used to track if multiple
   pegasus-monitord instances are running at the same time.

4) Switched precedence for env loading for local site.

   On certain Unix systems like debian, java overrides the
   LD_LIBRARY_PATH on linux.

   Now a value specified for the local site in the site catalog is
   preferred over the one in the environment. This is to ensure, that
   a user can set LD_LIBRARY_PATH in the site catalog, and it
   overrides the the JAVA provided one.

   Details at
    PM-471 [\#589](https://github.com/pegasus-isi/pegasus/issues/589)

5) seqexec always fails on first job failure

   The default behaviour for the clustered jobs has been changed to
   make seqexec fail on first job that fails.

   To change it , to run all the jobs in a cluster users need to set
   the following property to false

   pegasus.clusterer.job.aggregator.seqexec.firstjobfail


#### BUGS FIXED

1) kickstart with stdout redirection

   The stdout redirection feature of kickstart failed on some systems
   failed, as newer versions of libc() don't allow for the realpath()
   function to point to a non existing filename. This prevented
   kickstart from redirecting the stdout of a job to a file.

   This is now fixed.

   More details at
    PM-469 [\#587](https://github.com/pegasus-isi/pegasus/issues/587)

2) seqexec incorrectly reported the summary section in its records

   seqexec incorrectly reported the summary section in its records,
   when launched through condor. It reported jobs succeeded even
   though jobs had failed.

   More details at
    PM-541 [\#659](https://github.com/pegasus-isi/pegasus/issues/659)


3) Fixed a bug in pegasus-monitord that was preventing the database
population of certain pre and post script events.

4) Fixed some python 2.4 compatibility issues in pegasus-plots.

   More details at
    PM-534 [\#652](https://github.com/pegasus-isi/pegasus/issues/652)

5) SubDAG jobs referred in a DAX were launched by pegasus-dagman

   DAX can contain references to condor DAGS, that are not planned through
   Pegasus. They were launched by pegasus-dagman instead of directly
   being launched by condor_dagman. This is now fixed

   More details at
    PM-553 [\#671](https://github.com/pegasus-isi/pegasus/issues/671)

6)  invoke option for kickstart -I flag triggered incorrectly for  clustered jobs

    There was a bug in how pegasus handled invoke to get around the
    long arguments specified for the jobs in the DAX. In that case, a .arg
    created that is transferred with the job. This was not happening for
    clustered jobs.

    More details at
     PM-526 [\#644](https://github.com/pegasus-isi/pegasus/issues/644)


### Pegasus 3.1.0

**Release Date:** August 22, 2011

This is a major release of Pegasus with support for notifications and
a redesigned backend database to store information about workflows and  jobs.
There are updated and redesigned auxillary tools in the release
namely
   - pegasus-status
   - pegasus-statistics
   - pegasus-plots

There has been a change on how locations of properties files are
passed to the planner and tools. From Pegasus 3.1 release onwards,
support has been dropped for the following properties that were used
to signify the location of the properties file
  - pegasus.properties
  - pegasus.user.properties
Instead, users should use the --conf option for the tools.
More details at
http://pegasus.isi.edu/wms/docs/3.1/reference.php#Properties


The user guide has been reorganized and now has a new user walkthrough
and reference guide for all command line tools.

   http://pegasus.isi.edu/wms/docs/3.1/

#### NEW FEATURES

1) Support for Notifications

  This release of Pegasus has support for workflow level and job
  notifications. Currently, the user can annotate the DAX to specify
  what notifications they want associated with the workflow or/and
  individual jobs. Associating a notification with job entails
  specifying the condition when notification should be sent, the
  executable that needs to be invoked and the arguments with which it
  needs to be invoked. All notifications are invoked on the submit
  host by the monitoring daemon pegasus-monitord. The release comes
  bundled with default notification scripts that users can use for
  notifications.

  The DAX API's have also been updated to allow for associating
  notifications with the jobs and the workflow.

  More details about how notifications can be found here
  http://pegasus.isi.edu/wms/docs/3.1/reference.php#notifications

2) Workflows and Jobs Database

   The backend database schema to which pegasus-monitord populates
   runtime information about jobs in the workflow has been
   redesigned. Now in addition to jobs in the executable workflow,
   information about tasks in the DAX is also tracked and can be
   connected to the corresponding kickstart records.

   Also, pegasus-monitord no longer dies on database related
   errors. The statistics and plotting tools have in built checks that
   will notify a user if a DB was not populated fully for a workflow
   run.

    monitord now logs timestamps in monitord.log and monitord.done
    files to reflect the time monitord finishes processing specific
    sub-workflows.

   Information about the updated database schema can be found here.
   http://pegasus.isi.edu/wms/docs/3.1/monitoring_debugging_stats.php#monitoring_pegasus-monitord

3) Updated pegasus-statistics and plots

   pegasus-statistics and pegasus-plots have been updated to retrive
   all information from the runtime stampede database. pegasus plots
   now generates plots using protoviz and generates charts showing
   invocation breakdown, workflow gantt chart, host over time chart
   that shows how jobs ran on various hosts and a Time chart shows job
   instance/invocation count and runtime of the workflow run over time

   More information about updated statistics and tools can be found
   here
   http://pegasus.isi.edu/wms/docs/3.1/monitoring_debugging_stats.php#plotting_statistics

4) Updated pegasus-status tool

   The pegasus-status tool has been reimplemented for this
   release. The new tool shows the current state of a Condor Q and
   formats it better. For hierarichal workflows, the tool now displays
   jobs correctly grouped by sub workflows.

   More information can be found here
   http://pegasus.isi.edu/wms/docs/3.1/reference.php#pegasus-status

5) Improved support for S3

   With 3.1 release, there is a pegasus-s3 client that uses the amazon
   api to create buckets, put and retrieve files from buckets. This
   client has further been incorporated into pegasus-transfer . The
   pegasus-s3 looks up a configuration file to look up connection
   parameters and authentication tokens. The S3 config file is
   automatically transferred to the cloud with jobs when a
   workflow is configured to run in the S3 mode.

   In the S3 mode, jobs will run in the cloud without requiring a
   shared filesystem.

   More information about S3 mode in Pegasus can be found here
   http://pegasus.isi.edu/wms/docs/3.1/running_workflows.php#running_on_cloud


6) Tools now have a --conf option

   Most of the command line tools now have a --conf option that can be
   used to pass a properties file to the tools. Properties can no
   longer be passed to a tool using
   -Dpegasus.properties=/path/to/props or
   -Dpegasus.user.properties=/path/to/props


7) Improved rescue dag semantics for hierarchal workflows

   In earlier releases, a rescue dag submission of a hierarchal
   workflow lead to re-planning of the sub workflows even though
   rescue dags were submitted for the sub workflows. This could create
   problems as the re-planning resulted in the braindump files being
   over-written and monitord attempting to load information into the
   stampede database with a new workflow uuid.

   In this release, this issue has been addressed. By default for sub
   workflows  rescue dags are always submitted unless a --force-replan
   option is provided to pegasus-plan. In case of replanning, now a
   new submit directory is created for the sub workflow. The submit
   directories for sub workflows are now symlinks that point to the
   current submit directory for a sub workflow. This ensures that
   there are no race conditions between monitord and the workflow
   while populating to the database.

8) Default categories for certain types of jobs.

   subdax, subdag , cleanup and registration jobs now have default
   DAGMan categories associated with them.

JOB TYPE     | CATEGORY NAME
-------------|---------------
dax		     | subwf
dag          | subwf
cleanup      | cleanup
registration | registration

   This allows for a user to control maxjobs for these categories
   easily in properties by specifying
   dagman.[CATEGORY NAME].maxjobs property

If a file based replica catalog is used, then maxjobs for
registration jobs is set to 1. This is to ensure, that multiple
registration jobs are not run at the same time.

9) Automatic loading of DAXParser based on schema number

   Earlier the users needed to specify the pegasus.schemea.dax
   property to point to the corresponding DAX schema definition file
   to get Pegasus to load DAX'es with version < 3.2 and plan it.

   Pegasus now inspects the version number in the adag element to
   determine what parser will be loaded.

10) pegasus-tc-client

   pegasus-tc-client now displays output in the new multi line Text
   format, rather than the old File format.

   The support for the File format for the Transformation Catalog will
   be removed in the upcoming releases.

11) Removed the requirements for specifying grid gateways in Site
Catalog

   Grid Gateways are associated with a site in a Site Catalog to
   designate the jobmanagers associated with a grid site. However, in
   the case where jobs were submitted in a pure condor enviornment or
   on a local sites ( where jobs are not submitted via jobmanagers),
   Pegasus still required users to associate dummy grid gateways with
   the site.  This is no longer required . The Grid Gateways need to
   be specified only for grid sites now.

12)Workflow metrics file in the submit directory

   A workflow metrics file is created by the planner in the submit
   directory that gives a breakdown of various jobs in the executable
   workflow by type.


13) pegasus-plan is always niced

    Starting with this release, pegasus-plan always nice's the
    corresponding java invocation that launches the planner. This is
    helpful in keeping the load on the submit host in check.

14) Dropped support for VORS and MyOSG backends

    pegasus-sc-client now relies only on one backend ( OSGMM ) to
    generate a site catalog for OSG. VORS and MyOSG are no longer
    suppored by OSG.

### Pegasus 3.0.3

**Release Date:** July 21, 2011

This is a minor release, that fixes some bugs and has minor
enhancements.


#### NEW FEATURES

1) job statistics file has a num field now

   The job statistics file has statistics about Condor jobs in the
   workflow across retries. There is now a new field called num added
   that indicates the number of times JOB_TERMINATED event is seen for
   a Condor Job.

2) improvements to pegasus-monitord

   When using MySQL, users no longer are required to create the database
   using the 'latin1' character encoding. Now, pegasus-monitord will
   automatically create all tables using the 'latin1' encoding.

   When using MySQL, the database engine used by Pegasus-monitord is set
   to 'InnoDB'. This prevents certain database errors and allows for
   improved performance.

3) inherited-rc-files option for pegasus-plan

   pegasus-plan has a new option --inherited-rc-files, that is used in
   hierarichal workflows to pass the file locations in the parent
   workflow's DAX to planner instances working on a subdax job. Locations
   passed via this option, have a lower priority than the locations of
   files mentioned in the DAX.

4) improved pegasus-status

   There is a slight change in default behavior of the tool, so please
   read the manpage or short information about it. By default, it will
   look both, into the current Condor Q and a workflow run directory
   (if a valid one is the current one, or was specified.) Each of this
   behavior can be turned on and off separately.

   Improved output includes UTF-8 box drawing characters to show
   dependencies, a color option (for white terminal backgrounds), and
   detection of the current terminal size.

#### BUGS FIXED

1) Fixed a bug in the code handling SIGUSR1 and SIGUSR2 that caused
   pegasus-monitord to abort due to an out-of-bounds condition.

2) Fixed Python 2.4 compatibility issue that caused pegasus-monitord to
   abort when receiving a SIGUSR1 or SIGUSR2 to change its debugging level.

3) pegasus-transfer failed on scp if the destination URL was a file URL
   This is now fixed. More details at

    PM-375 [\#493](https://github.com/pegasus-isi/pegasus/issues/493)

4) pegasus transfer failed on scp if the destination host was not in users
   know_hosts. This is now fixed. More details at

    PM-374 [\#492](https://github.com/pegasus-isi/pegasus/issues/492)

5)  pegasus-plan had a potential stack overflow issue that could occur
    while calling out to transformation selectors that return more than
    one entry.

6)  Destination file url's were not correctly replaced with symlink protocol
    scheme in the case where the destination site had a file server
    ( url prefix file:/// ).

### Pegasus 3.0.2

**Release Date:** March 31, 2011

This is a minor release, that fixes some bugs and has minor
enhancements.


#### NEW FEATURES

1) New Pegasus Properties for pegasus-monitord daemon

   The pegasus-monitord daemon is launched by pegasus-run while
   submitting the workflow, and by default parses the condor logs for
   the workflows and populates them in a sqllite DB in the workflow
   submit directory.

   pegasus.monitord.events - A Boolean Property indicating whether to
   parse and generate log events or not.

   pegasus.monitord.output - This property can be used to specify the
   destination for generated log events in pegasus-monitord


2) Improvements to pegasus-monitord

   pegasus-monitord now does batches evennts before popualating them
   in to the stampede backend.

3) New entries in braindump file

   The braindump file generated in the submit directory has two new
   keys.

   The braindump file has two extra entries now

   properties - path to the properties file
   condor_log -  path to the condor log for the workflow

4) pegasus-transfer supports ftp transfers in unauthenticated mode.

#### BUGS FIXED

1) Failure of rescue dags if submit directory on NFS.

   Pegasus creates a symlink in the submit directory to the condor log
   file for the workflow in /tmp . In case the workflow failed and the
   submit directory was on NFS, pegasus-run on rescue would take a
   backup of the symlink file in the submit directory. This resulted
   in the workflow failing on resubmission, as the condor log now
   pointed to a file in the submit directory that was on NFS.

   pegasus-submit-dag was fixed to copy the symlinked log while
   rotating instead of copying just the symlink.


### Pegasus 3.0.1

**Release Date:** December 24, 2010

This is minor release, that fixes some bugs discovered after 3.0
release and some  enhancements.

#### NEW FEATURES

1) Pegasus VM Tutorial updated

   The Pegasus VM Tutorial has been updated to use a new smaller
   Virtual Image running Debian 6 on x86. The tutorial notes are
   available online at

   http://pegasus.isi.edu/wms/docs/3.0/tutorial_vm.php

2) pegasus-plots improvements

   The worklfow gantt chart produced by pegasus-plots now has better
   legend placement. The legends are placed below the x axis so that
   they dont interfere with the chart in the plotting area. Also the
   gantt chart generated now uses a fixed pallete of colors to assign
   colors to the different compute jobs in the workflows. Earlier,
   random colors were assigned to the compute jobs.


#### Bugs FIXED

1) In the case where symlinking was turned on and the pool attributes
   of the source URL ( discovered in the Replica Catalog ) matched with
   the compute site, the source URL was not converted to a file URL. This
   lead to the symlink job failing when the workflow executed. This is
   fixed now and the planner now converts the source URL's used for
   symlink jobs to a file URL scheme.


2) Metrics displayed by pegasus-statistics were not clear as to what
   the worfklow execution time was, as two metrics with similar meaning
   were displayed ( Workflow Execution Time and Workflow Walltime ). The
   metrics names have been changed and the meaning of the metrics are
   explained in detail in both the output of pegasus-statistics and in
   the online user guide.
   http://pegasus.isi.edu/wms/docs/3.0/monitoring_debugging_stats.php#id2841404

   Additional clarifications have been made to as how the metrics
   calculations happen on hierarchal worklfows.

3) pegasus-transfer when in file copy mode complained if the
   destination file already existed. This is now fixed .

4) pegasus-bug-report was broken in 3.0 release. This is now fixed.

5) Added support in pegasus-monitord for handling SUBDAG EXTERNAL jobs
   without the DIR option.

   More details at This is related to JIRA issue PM-300.

### Pegasus 3.0

**Release Date:** November 29, 2010

This is a major release that attempts to simplify configuration and
running workflows through Pegasus.

Existing users please refer to the Migration Guide available at
http://pegasus.isi.edu/wms/docs/3.0/Migration_From_Pegasus_2_x.php .

A user guide is now available online at
http://pegasus.isi.edu/wms/docs/3.0/index.php

#### NEW FEATURES

1) Support for new DAX Schema Version 3.2

   Pegasus 3.0 by default  parses DAX documents conforming to the DAX
   Schema 3.2 and is explained online at
   http://pegasus.isi.edu/wms/docs/3.0/api.php . The same link has
   documentation about using the JAVA/Python and Perl API's to
   generate DAX3.2 compatible DAX'es. Users are encourage to use the
   API's in their DAX Generators.

2) New Multiline Text Format for Transformation Catalog

   The default format for Transformation Catalog is now Text which is
   a multiline textual format. It is explained in the Catalogs Chapter
   http://pegasus.isi.edu/wms/docs/3.0/catalogs.php#transformation

3) Shell Code Generator

   Pegasus now has support for a Shell Code Generator that generates a
   shell script in the submit directory instead of Condor DAGMan and
   condor submit files. In this mode, all the jobs are run locally on
   the submit host.

   To use this set
      - pegasus.code.generator Shell
      - make sure that --sites option passed to pegasus only has site
        local mentioned.

4) Profiles and Properties Simplification

   Starting with Pegasus 3.0 all profiles can be specified in the
   properties file. Profiles specified in the properties file have the
   lowest priority.

   As a result of this a lot of existing Pegasus Properties were
   replaced by profiles. An exhaustive list of Properties replaced can
   be found at
   http://pegasus.isi.edu/wms/docs/3.0/Migration_From_Pegasus_2_x.php#id2765439

   All Profile Keys are  documented in the Profiles Chapter
   http://pegasus.isi.edu/wms/docs/3.0/advanced_concepts_profiles.php

   The properties guide was broken into two parts and can be found in
   the installation in the etc directory
       - basic.properties   lists only the basic properties that need to
                            be set to use Pegasus. Sufficient for most
   			    users.
       - advanced.properties lists all the properties that can be used
       	 		     to configure Pegasus.

    The properties documentation can be found online at
    http://pegasus.isi.edu/wms/docs/3.0/configuration.php
    http://pegasus.isi.edu/wms/docs/3.0/advanced_concepts_properties.php



5) Transfers Simplification

   Pegasus 3.0 has a new default transfer client pegasus-transfer that
   is invoked by default for first level and second level staging. The
   pegasus-transfer client is a python based wrapper around various
   transfer clients like globus-url-copy, lcg-copy, wget, cp, ln
   . pegasus-transfer looks at source and destination url and figures
   out automatically which underlying client to use. pegasus-transfer
   is distributed with the PEGASUS and can be found in the bin
   subdirectory .

   Also, the Bundle Transfer refiner has been made the default for
   pegasus 3.0. Most of the users no longer need to set any transfer
   related properties. The names of the profiles keys that control the
   Bundle Transfers have been changed .

   To control the clustering granularity of stagein transfer jobs
   following Pegasus Profile Keys can be used

   - stagein.clusters
   - stagein.local.clusters
   - stagein.remote.clusters

   To control the clustering granularity of stageout transfer jobs

   - stageout.clusters
   - stageout.local.clusters
   - stageout.remote.clusters


6) New tools called pegasus-statistics and pegasus-plots

   There are new tools called pegasus-statistics and pegasus-plots that
   can be used to generate statistics and plots about a worklfow run.

   The tools are documented in Monitoring and Debugging Chapter
   http://pegasus.isi.edu/wms/docs/3.0/monitoring_debugging_stats.php#id3083376

7) Example Workflows

   Pegasus distribution comes with canned examples that users can use
   to run after installing Pegasus.

   The examples are documented in the Example Workflows Chapter
   http://pegasus.isi.edu/wms/docs/3.0/example_workflows.php

8) Support for GT5

   Pegasus now has support for GT5.

   Users can use GT5 only if they are using the new site catalog
   schema ( XML3 ) , as only that has a place holder to specify grid
   type with the grid gateways element.
   e.g
   <site handle="isi_viz" arch="x86" os="LINUX" osrelease="" osversion="" glibc="">
               <grid type="gt5"  contact="viz-login.isi.edu/jobmanager-pbs" scheduler="PBS"
   jobtype="compute"/>
               <grid type="gt2" contact="viz-login.isi.edu/jobmanager-fork" scheduler="Fork" jobtype="auxillary"/>
          ....
    </site>

    Users can use pegasus-sc-converter to convert their site catalogs
    to the XML3 format.
    Sample Usage:
    sc-client -I XML -O XML3 -i sites.xml -o sites.xml3


9) pegasus-analyzer can parse kickstart outputs

   pegasus-analyzer now parses kickstart outputs and prints meaningful
   information about the failed jobs.  The following information is
   printed for the failed jobs

   - the executable
   - the arguments
   - the site the job ran on
   - remote the working directory of the job
   - exitcode
   - node the job ran on
   - stdout | stderr

11) Logging improvements

    pegasus-plan now has a -q option to decrease the logging
    verbosity. The logging verbosity can be increased by using the -v
    options. Additionally, there are two new logging levels
    - CONSOLE ( enabled by default - messages like pegasus-run
    invocation are printed to this level )
    - TRACE  ( more verbose that DEBUG level ).

    The INFO level is no longer enabled by default. To see the INFO
    log level messages pass -v to pegasus-plan

    To turn on the TRACE level pass -vvvv . In the TRACE mode, in case
    of exceptions  the full stack trace for exceptions is printed.

12) Default condor priorities to jobs

    Pegasus assigns default Condor priorities to jobs now. The
    priorities come into play when running jobs in Condor vanilla /
    standard or local universe jobs. They dont apply for grid universe
    jobs.

    The default priority for various types of jobs is

    Cleanup     :  1000
    Stage out   :  900
    Dirmanager  :  800
    Stage in    :  700
    Compute jobs:  level * 10 where level is the level of the job in
    	           the workflow as compute from the root of the workflow.

    This priority assignment gives priority to workflows that are
    further along, but also overlap data staging and computation (data
    jobs have higher priority, but the assumptions is
    that there are relatively few data staging jobs compared to compute
    jobs) .


13) Turning off Condor Log Symlinking to /tmp

    By default pegasus has the Condor common log -0.log in the submit
    file as a symlink to a location in /tmp . This is to ensure that
    condor common log does not get written to a shared filesystem. If
    the user knows for sure that the workflow submit directory is not
    on the shared filesystem, then they can opt to turn of the
    symlinking of condor common log file by setting the property
    pegasus.condor.logs.symlink to false.
