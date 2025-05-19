## Pegasus 4.2.x Series

### Pegasus 4.2.2

**Release Date:**  May 1, 2013

Pegasus 4.2.2 is a minor release, that has minor enhancements and
fixes some bugs to Pegasus 4.2.0 release.  Improvements include  

- support for sever side pagination for pegasus-dashboard 
- support for lcg-utils command line clients to retrieve and
  push data to SRM servers 
- installation of Pegasus python libraries  in standard system
  locations 



#### IMPROVEMENTS

1) Rotation of monitord logs

   monitord is automatically launched by pegasus-dagman.  When
   launching monitord, pegasus-dagman sets up the monitord to a log
   file it initializes. However monitord also took a backup of the log
   when it started up as it detected the log file existed. This led to
   two monitord log files in the submit directory which was
   confusing. Now only pegasus-dagman setsup the monitord log. 
 
    More details can be found at
    https://jira.isi.edu/browse/PM-688
 
2) Monitord Recovery in case of SQLLite DB

   If a monitord gets killed on a currently running workflow, then it
   restarts from the start. The information in the recovery file it
   writes out is insufficient to recover gracefully. In case of
   SQLlite DB , monitord does not attempt to expunge the information
   from the database. Instead it takes a backup of the sqlite database
   in the submit directory.  
 
   More details can be found at
   https://jira.isi.edu/browse/PM-689
 
3) Support for lcg-utils for srm transfers 
   
   The pegasus-create-dir, pegasus-cleanup and pegasus-transfer
   clients were updated to include support for lcg utils to do
   operations against a SRM server 
 
    Note that lcg utils takes precedence if both lcg-cp and srm-copy
    are available. 
 
4) Improvements to the dashboard
   
   - Use Content Delivery Networks as source for jQuery, jQueryUI, and
     DataTables plugin. 
   - Most tables in dashboard now have server side pagination, to
      enable large workflows. 
   - Replaced radio buttons with jQuery buttons for a better look and
     feel. 
   - Made Statistics/Charts links more prominent.
   - Added a drop down to filter list of workflows run in last hour,
     day, month, or year. 
 
5) Newer examples added in the examples directory
   
   The release has new examples checked in that highlight 
     - how to use the nonshared fs against a remote staging site that
       has a scp server. 
     - use glite submission to a local PBS cluster using the sharedfs
       data configuration 
     - use the nonsharedfs case, where we use SRM as a staging site
       using CREAMCE submission 
 
6) Pegasus python libraries are installed in standard system locations
   
   The RPM and DEB packages now installs the Python modules in the
   standard system locations. Users should no longer have to set
   PYTHONPATH or add to the include paths in their DAX generators. 
 
7) Condor job logs are no longer in the /tmp directory

   pegasus.condor.logs.symlink now defaults to false. This is to
   ensure compatibility with condor 7.9.4 onwards and ticket
   https://htcondor-wiki.cs.wisc.edu/index.cgi/tktview?tn=1419 DAGMAn
   will fail by default now if it detects that common log is in /tmp  
 
#### BUGS FIXED

1) Externally Accessible URL's for staged executables broken for SRM

   In certain cases, for SRM file servers in the site catalog, the URL
   constructed to a staged executable was incorrect. This is now
   fixed. 
    
   More details can be found at
   https://jira.isi.edu/browse/PM-686
 
2) pegasus-exitcode cluster-summary w/submitted=0
   
   If the output file has a cluster-summary record, and  the number of
   submitted tasks is 0, then the job succeeded. This fixes an error
   SCEC had that was  introduced when the "tasks" and "submitted"
   values in cluster-summary were separated for PMC. 
 
3) Pegasus Lite did not support jobs with stdin file tracked in the DAX

   In the pegasus lite case, support for jobs with their stdin tracked
   in the DAX was broken. This is now fixed. 

   More details can be found at
   https://jira.isi.edu/browse/PM-694
 
4) pegasus-cleanup did not support symlink deletion

   In case where symlinks to the input files are created in the
   scratch directory on the staging-site, the pegasus-cleanup job was
   created with symlink urls to be deleted. This led to the jobs
   failing as pegasus-cleanup did not support deletion of
   symlinks.This is now fixed .

   Additionally, the planner sets up the cleanup jobs to run on the
   remote if the url to b deleted is a file url or a symlink url 

   More details can be found at
   https://jira.isi.edu/browse/PM-696
 
5) pegasus-createdir and pegasus-transfer with S3 buckets

   pegasus-createdir and pegasus-transfer did translate the S3 bucket
   name correctly if it contained a -. This is now fixed. Also the
   clients don't fail if the bucket already exists. 
 
6) Bug fixes to the cleanup algorithm

   The planner exited with an index out of bounds exception when data
   reuse was triggered and an output file that needed to be staged was
   required to be deleted. This is fixed 

   Also, the clustering of the cleanup jobs resulted in not all the
   files to be deleted by the cleanup jobs. 
   
   Improvements were made how excess edges were removed from the
   graph. The edge removal was done per file instead of per cleanup
   job. This fix drastically reduces the runtime for workflows with
   lots of files that need to be cleaned up. 

   More details can be found  at 
   https://jira.isi.edu/browse/PM-699
 
7) pegasus-analyzer detects prescript failures in the DB mode

   Pegasus analyzer in the database mode was not detecting pre script
   failures for dax jobs as the associated job instance was not
   updated with the exitcode. Changed the way how monitord handles
   failures for sub workflows. In case of pre script failures, the
   prescript failure exitcode is recorded in addition to the stdout of
   the planner log. More details at 
 
   https://jira.isi.edu/browse/PM-704

 
8) monitord tracks non kickstarted  files with rotated stdout and stderr files

   monitord did not track the rotated stdout and stderr of jobs that
   were not launched by kickstart. Because of this the stdout and
   stderr was not populated. This is now fixed. More details at  
 
  https://jira.isi.edu/browse/PM-685

9) Planner fails on determining the DN from a proxy file

   The planner uses the Java COG jar to determine the DN from a proxy file. It
   was discovered that for proxies generated from  an X.509 end entity credential, 
   by a GSI-enabled OpenSSH server results in a NPE in the COG jar.

   The planner now catches all the exceptions while trying to determine the DN.
   There is never a FATAL error if unable to determine the DN.

10) pegasus-exitcode checks for the existence of .err file 

   The pegasuslite_failures function did not check for missing stderr files. As a 
   result, if exitcode was called in a scenario where there was no .err file, then
   it failed trying to determine if None is a valid path.  

### Pegasus 4.2.1

**Release Date:** April 27, 2013

Pegasus 4.2.1 was tagged but never officially released. Users are advised to use
4.2.2 instead.  The difference between 4.2.2 and 4.2.1 is that it does not have
the fix for 

- planner fails if there is NullPointerException in the underlying COG code
  when trying to determine the DN.
- pegasus-exitcode checks for existense of .err file before trying to base the
  exitcode on it's contents.

### Pegasus 4.2.0

**Release Date:** January 29, 2013

This a major release of Pegasus which contains

 - several improvements on data management capabilities
 - a new web based monitoring dashboard
 - job submission interfaces supported. CREAM CE is now supported 
 - new replica catalog backends. 
 - support for PMC only workflows and IO forwarding for PMC clustered jobs
 - anonymous usage metrics reporting.

The data management improvements include a new simpler site catalog
schema to describe the site layouts, and enables data to be
transferred to and from staging sites using different protocols. A
driving force behind this change was Open Science Grid, in which it is
common for the compute sites to have Squid caches available to the
jobs.  For example, Pegasus workflows can now be configured to stage
data into a staging site using SRM or GridFTP, and stage data out over
HTTP. This allows the compute jobs to automatically  use the Squid
caching mechanism provided by the sites, when pulling in data to the
worker nodes over HTTP. 

Also, with the release we include a beta version of a web based
monitoring dashboard (built on flask) that users can use to monitor and
debug their running workflows. The dashboard provides workflow overview,
graphs and job status/outputs.

Job submissions to the CREAM job management system has been implemented
and tested. 

New simpler replica catalog backends are included that allow the user
to specify the input directory where the input files reside instead of
specifying a replica catalog file that contains the mappings.

There is prototypical support for setting up Pegasus to generate the
executable workflow as a PMC task workflow instead of a Condor DAGMan
workflow. This is useful for environments, where Condor cannot be
deployed such as Blue Waters. I/O forwarding in PMC enables each task
in a PMC job to write data to an arbitrary number of shared files in a
safe way. This is useful for clustered jobs that contain lots of tasks
and each task only writes out a few KB of output data.

Starting with this release, Pegasus will send anonymous usage statistics 
to the Pegasus development team. Collecting this anonymous information
is mandated by the main Pegasus funding agency, NSF. Please refer to 
http://pegasus.isi.edu/wms/docs/latest/funding_citing_usage.php#usage_statistics 
for more details on our privacy policy and configuration.

#### NEW FEATURES

1) New Site Catalog Schema
   
   Pegasus 4.2 release introduces a version 4 for the site catalog
   schema. The schema represents a simpler way to describing a site
   and organizes the site information by various directories
   accessible on the site for the workflow to use.

   The schema is described in our user guide here
   http://pegasus.isi.edu/wms/docs/latest/creating_workflows.php#sc-XML4
   
   and examples in our distribution have been updated to use the new
   schema. Sample site catalog files in the newer format can also be
   found in the etc directory. 

   With 4.2, Pegasus will autoload the appropriate site catalog schema
   backend by inspecting the version number in the site catalog file
   at runtime. 

   Users can use the client pegasus-sc-converter to convert their
   existing site catalogs in XML3 format to the newer versions. Users
   can choose to specify pegasus.catalog.site as XML or leave it
   undefined. 

   The 4.2 release no longer supports the following old Site Catalog
   Implementations 
   		   - VORS
		   - MYOSG
		   - XML2
		   - Text
   
2) Improved Data Management Capabilities

   Users can now specify different protocols to push data to a
   directory on the staging site and retrieve data from the directory
   using another protocol.

   For example users can use a HTTP file server to retrieve data (
   pull ) data from a staging site to worker node and use another
   protocol say scp to push data back to the staging site after a job
   completes. This is particularly useful when you want to leverage a
   high throughput HTTP deployment backed by SQUID proxies when
   serving input data to compute nodes.

   Users can specify different file servers for a particular directory
   by specifying different operation attribute on the file
   servers. The operation attribute can take enumerated set of values
  
      - put  ( use the server only for put operations to the directory )
      - get ( use the server only for get operations to the directory)
      - all ( use it for both get and put operations)


3) Online Workflow Dashboard
   
   This release includes a beta version of a web based monitoring
   dashboard ( built on flask ) that users can use to monitor and
   debug their running workflows. 

   The dashboard is meant to be run per user, and lists all the
   workflows run by that user. The dashboard gets a list of running
   workflows by looking up a sqlite database in the users home
   directory ~HOME/.pegasus/workflow.db . This database is populated
   by pegasus-monitord everytime a new root workflow is
   executed. Detailed information for each workflow is retrieved from
   the stampede database for the each workflow.

   The workflow dashboard lists all the user workflows on the home
   page and are color coded. Green indicates a successful workflow,
   red indicates a failed workflow while blue indicates a running
   workflows. 

   Users can click on a workflow to drill down and get more
   information about the workflow that leads to a workflow page. The
   workflow page has identifying metadata about the workflow, and has
   a tabbed interface that can be used to traverse through the list of
   sub workflows, failed, running and successful jobs.

   Each job or sub workflow can be clicked to get further details
   about that entity .Clicking on a failed/successful job will lead to
   an invocation details page that will have the contents of the
   associated kickstart record displayed.

   The charts button can be clicked to generate relevant charts about
   the workflow execution such as the 
       - Workflow Gantt Chart
       - Job Distribution by Count/Time
       - Time Chart by Job/Invocation


   The statistics button can be clicked to display a page that lists
   the statistics for a particular workflow. The statistics page
   displays statistics similar to what the command line tool
   pegasus-statistics displays.

   
   The workflow dashboad can be started by a  a command line tool
   called pegasus-dashboard. 

   
4) Usage Statistics Collection

   Pegasus WMS is primarily a NSF funded project as part of the NSF
   SI2 track. The SI2 program focuses on robust, reliable, usable and
   sustainable software infrastructure that is critical to the CIF21
   vision. As part of the requirements of being funded under this
   program, Pegasus WMS is required to gather usage statistics of
   Pegasus WMS and report it back to NSF in annual reports. The
   metrics will also enable us to improve our software as they will
   include errors encountered during the use of our software. 

   More details about our policy and metrics collected can be found
   online at  
   http://pegasus.isi.edu/wms/docs/latest/funding_citing_usage.php#usage_statistics


5) Support for CREAMCE submissions

   CREAM is a webservices based job submission front end for remote
   compute clusters. It can be viewed as a replaced for Globus GRAM
   and is mainly popular in Europe. It widely used in the Italian
   Grid. 

   In order to submit a workflow to compute site using the CREAMCE
   front end, the user needs to specify the following for the site in
   their site catalog 

   	 - pegasus profile style with value set to cream
	 - grid gateway defined for the site with contact attribute
	   set to CREAMCE frontend and scheduler attribute to remote
	   scheduler. 
	 -  a remote queue can be optionally specified using globus
	  profile queue with value set to queue-name 

   More details can be found here

   http://pegasus.isi.edu/wms/docs/latest/execution_environments.php#creamce_submission	  

6) Initial Support for PMC only workflows

   Pegasus can now be configured to generate a workflow in terms of a
   PMC input workflow. This is useful to run on platforms where it not
   feasible to run Condor such as the new XSEDE machines such as Blue
   Waters. In this mode, Pegasus will generate the executable workflow
   as a PMC task workflow and a sample PBS submit script that submits
   this workflow .

   Users can modify the generated PBS script to tailor it to their
   particular cluster.

   To use Pegasus in this mode, set 
   
   pegasus.code.generator PMC

   In this mode, the workflow should be configured to submit to a
   single execution site.
   
6) New options --input-dir and output-dir for  pegasus-plan

   The planner now has --input-dir and --output-dir options. This
   allows the planner to read mappings for input files from an input
   directory and stage the results to an output directory.
 
   If , the output-dir option is set then the planner updates the storage
   directory for the output site specified by the user. If none is
   specified , then the local site entry is updated. 

7) Directory based Replica Catalog

   Users can now setup Pegasus to read the input file mappings from an
   input directory. Details on how to use and configure Pegasus in
   this mode can be found here

   http://pegasus.isi.edu/wms/docs/latest/creating_workflows.php#idp11375504

8) Regular Expressions support in File based Replica Catalog

   Users can now specify a regex expression in a file based replica
   catalog to specify paths for mulitple files/data sets.

   To use it you need to set
   pegasus.catalog.replica to Regex

   More details can be found here

   http://pegasus.isi.edu/wms/docs/latest/creating_workflows.php#idp11375504


9) Support for IO Forwarding in PMC ( pegasus-mpi-cluster )
   
   In workflows that have lots of small tasks it is common for the I/O
   written by those tasks to be very small. For example, a workflow
   may have 10,000 tasks that each write a few KB of data. Typically
   each task writes to its own file, resulting in 10,000 files. This
   I/O pattern is very inefficient on many parallel file systems
   because it requires the file system to handle a large number of
   metadata operations, which are a bottleneck in many parallel file 
   systems.

   In order to address this use case PMC implements a feature that we
   call "I/O Forwarding". I/O forwarding enables each task in a PMC
   job to write data to an arbitrary number of shared files in a safe
   way. It does this by having PMC worker processes collect data
   written by the task and send it over over the high-speed network
   using MPI messaging to the PMC master  process, where it is written
   to the output file. By having one process  (the PMC master process)
   write to the file all of the I/O from many parallel  tasks can be
   synchronized and written out to the files safely.    

   More details on how IO Forwarding works can be found in the manpage
   for PMC under the section I/O Forwarding

   http://pegasus.isi.edu/wms/docs/trunk/cli-pegasus-mpi-cluster.php


10) Clustering of cleanup jobs

    The InPlace cleanup algorithm that adds cleanup jobs to the
    executable workflow , now clusters the cleanup jobs by
    default for each level of the workflow . This keeps in check the
    number of cleanup jobs created for large workflows.

    The number of cleanup jobs added per level can be set by the
    following  property

    pegasus.file.cleanup.clusters.num  

    It defaults to 2.

11) Planner has support for SHIWA Bundles

    The planner can be take in shiwa bundles to execute workflows. For
    this to happen, the bundle need to be created in shiwa gui with
    the appropriate Pegasus Plugins

    More details at
    https://jira.isi.edu/browse/PM-638
    
12) Improvements to pegasus-statistics
    		 
    There is now a single API call executed to get the succeeded and
    failed count for job and sub workflows.

13) Improvements to planner performance

    The performance of the planner has been improved for large
    workflows. 

14) Renamed --output option to --output-site

   The --output option has been deprecated and replaced by a new
   option --output-site

15) Removed support for pegasus.dir.storage

   Pegasus no longer supports pegasus.dir.storage property. The
   storage directory can only be specified in the site catalog for a
   site. 


#### BUGS FIXED

1) Failure in Data Reuse if a deleted job had an output file that had
to be transferred

   There was a bug, where the planner failed in case of data reusue if
   any of the deleted jobs had output files that needed to be
   transferred. 

   More details at 
   https://jira.isi.edu/browse/PM-675

