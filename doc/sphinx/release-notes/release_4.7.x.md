## Pegasus 4.7.x Series

### Pegasus 4.7.5

We are happy to announce the release of Pegasus 4.7.5 . Pegasus 4.7.5
is a minor release, which contains minor enhancements and fixes
bugs. This will most likely be the last release in the 4.7 series, and
unless you have specific reasons to stay with the 4.7.x series, we
recommend to upgrade to 4.8.0. 

The following issues were addressed and more information can be found
in the Pegasus Jira (https://jira.isi.edu/)



#### Improvements

1) [PM-1191] - If available, use GFAL over globus url copy 

   JGlobus is no longer actively supported and is not in compliance
   with RFC 2818
   (https://docs.globus.org/security-bulletins/2015-12-strict-mode). As 
   a result cleanup jobs using pegasus-gridftp client would fail
   against the servers supporting the strict mode. We have removed the
   pegasus-gridftp client and now use gfal clients as globus-url-copy
   does not support removes. If gfal is not available, globus-url-copy
   is used for cleanup by writing out zero bytes files instead of
   removing them. 

2) [PM-1146] - There doesn't seem to be a way to get a persistent URL
   for a workflow in dashboard 

3) [PM-1186] - pegasus-db-admin should list compatibility with latest
   pegasus version if no changes to schema

4) [PM-1187] - make scheduler type case insensitive for grid gateway
   in site catalog 


#### Bugs Fixed

1) [PM-1032] - Handle proper error message for non-standard python
   usage

2) [PM-1171] - Monitord regularly produces empty stderr and stdout
   files

3) [PM-1172] - pegasus-rc-client deletes all entries for a lfn

4) [PM-1173] - cleanup jobs failing against Titan gridftp server due
   to RFC 2818 compliance 

5) [PM-1176] - the job notifications on failure and success should
   have exitcode from kickstart file

6) [PM-1181] - monitord fails to exit if database is locked

7) [PM-1185] - destination in remote file transfers for inter site
   jobs point's to directory 

8) [PM-1193] - "pegasus-rc-client list" modifies rc.txt

9) [PM-1196] - pegasus-statistics is not generating jobs.txt for some
   large workflows 

10) [PM-1207] - Investigate error message: Normalizing '4.8.0dev' to
    '4.8.0.dev0' 

11) [PM-1208] - Improve database is locked error message 

12) [PM-1209] - Analyzer gets confused about retry number in
    hierarchical workflows 

13) [PM-1211] - DAX API should tell which lfn was a dup

14) [PM-1213] - pegasus creates duplicate source URL's for staged
    executables 

15) [PM-1217] - monitord exits prematurely, when in dagman recovery mode


### Pegasus 4.7.4

We are pleased to announce Pegasus 4.7.4. Pegasus 4.7.4 is a minor
release of Pegasus and includes improvements and bug fixes to the
4.7.4 release. 

#### Bugs Fixed

1) [PM-1148] - kickstart should print a more helpful error message if
the executable is missing 

2) [PM-1160] - Dashboard is not recording the hostname correctly

3) [PM-1163] - Confusing error message in pegasus-kickstart

4) [PM-1164] - worker package in submit directory gets deleted during
workflow run  

### Pegasus 4.7.3

We are happy to announce the release of Pegasus 4.7.3. Pegasus 4.7.3
is a minor release of Pegasus and includes improvements and bug fixes
to the 4.7.2 release. It has a bug fix without which monitoring will
break for users running with HTCondor 8.5.8 or higher. 

#### Improvements

1) [PM-1109] – dashboard to display errors if a job is killed instead
   of exiting with non zero exitcode 
   
   pegasus-monitord did not pass signal information from the kickstart
   records to the monitoring database. If a job fails and because of a
   signal, it will now create an error message indicating the signal
   information, and populate it. 

2) [PM-1129] – dashboard should display database and pegasus version 

3) [PM-1138] – Pegasus dashboard pie charts should distinguish between
   running and unsubmitted 

4) [PM-1155] – remote cleanup jobs should have file url’s if possible 

#### Bugs Fixed

1) [PM-1132] – Hashed staging mapper doen’t work correctly with sub
   dax generation jobs 

   For large workflows with dax generation jobs, the planning broke
   for sub workflows if the dax was generated in a hashed directory
   structure. It is now fixed. 

   Note: As a result of this fix, pegasus-plan prescripts for sub
   workflows in all cases, are now invoked by pegasus-lite 

2) [PM-1135] – pegasus.transfer.bypass.input.staging breaks symlinking
   on the local site 

3) [PM-1136] – With bypass input staging some URLs are ending up in
   the wrong site 

4) [PM-1147] – pegasus-transfer should check that files exist before
   trying to transfer them 

   In case where the source file url’s don’t exist, pegasus-transfer
   used to still attempt multiple retries resulting in hard to read
   error messages. This was fixed, whereby pegasus-transfer does not
   attempt retries on a source if a source file does not exist. 

5) [PM-1151] – pegasus-monitord fails to populate stampede DB
   correctly when workflow is run on HTCondor 8.5.8 

6) [PM-1152] – pegasus-analyzer not showing stdout and stderr of
   failed transfer jobs 

   In case of larger stdout+stderr outputted by an application, we
   store only first 64K in the monitoring database combined for a
   single or clustered job. There was a bug whereby if a single task
   outputted more than 64K nothing was populated. This is fixed 

7) [PM-1153] – Pegasus creates extraneous spaces when replacing <file
   name=”something” /> 

   DAX parser was updated to not add extraneous spaces when
   constructing the argument string for jobs 

8) [PM-1154] – regex too narrow for GO names with dashes

9) [PM-1157] – monitord replay should work on submit directories that
   are moved

   pegasus generated submit files have absolute paths. However, for
   debugging purposes where a submit directory might be moved to a
   different host, where the paths don’t exist. monitord now searches
   for files based on relative paths from the top level submit
   directory. This enables users to repopulate their workflow
   databases easily. 

### Pegasus 4.7.2

We are happy to announce the release of Pegasus 4.7.2. Pegasus 4.7.2
is a minor release of Pegasus and includes improvements and bug fixes
to the 4.7.1 release.

#### Improvements in 4.7.2 are

 - [PM-1141] - The commit to allow symlinks in pegasus-transfer broke
               PFN fall through
 - [PM-1142] - Do not set LD_LIBRARY_PATH in job env
 - [PM-1143] - R DAX API
 - [PM-1144] - pegasus lite prints the wrong hostname for non-glidein
               jobs


###  Pegasus 4.7.1

We are happy to announce the release of Pegasus 4.7.1. Pegasus 4.7.1
is a minor release of Pegasus and includes improvements and bug fixes
to the 4.7.0 release.

#### Improvements in 4.7.1 are

- Fix for stage in jobs with repeated portion of LFN [PM-1131]
- Fix for pegasus.transfer.bypass.input.staging breaks symlinking on
  the local site [PM-1135]
- Capture the execution site information in pegasus lite [PM-1134]
]
- Added ability to check CVMFS for worker package

### Pegasus 4.7.0

We are happy to announce the release of Pegasus 4.7.0.  Pegasus 4.7.0
is a major release of Pegasus and includes all the bug fixes and
improvements in the 4.6.2 release 

New features and Improvements in 4.7.0 are

    - automatic submit directory organization
    - improved directory management on staging site in nonsharedfs mode
    - R DAX API
    - pegasus-analyzer reports information about held jobs
    - check for cyclic dependencies in DAG

#### New Features

1) [PM-833] – Pegasus should organize submit files of workflows in
   hierarchal data structure 

   Pegasus now automatically distributes the files in HTCondor submit
   directory for all workflows in 2 level directory structure. This is
   done to prevent having too many workflow and condor submit files in
   one directory for a large workflow.  The behavior of submit
   directory organization can be controlled by the following
   properties 

   pegasus.dir.submit.mapper.hashed.levels         the number of
   directory levels used to accomodate the files. Defaults to 2. 
   pegasus.dir.submit.mapper.hashed.multiplier      the number of
   files associated with a job in the submit directory. defaults to    5. 

   Note that this is enabled by default.  If you want to have pre
   4.7.0 behavior you can 
   	 pegasus.dir.submit.mapper Flat
   
   Submit mapper properties are documented in the user guide here
   https://pegasus.isi.edu/docs/4.7.0/properties.php#site_dir_props 

2) [PM-833] – Pegasus should manage directory structure on the staging
site

    For non sharedfs mode, Pegasus will now automatically manage the
    directory structure on the staging site in a hierarchal directory
    structure via use of staging mappers. The staging mappers
    determine what sub directory on the staging site a job will be
    associated with. Before, the introduction of staging mappers, all
    files associated with the jobs scheduled for a particular site
    landed in the same directory on the staging site. As a result, for
    large workflows this could degrade filesystem performance on the
    staging servers. More information can be found in the
    documentation at
    https://pegasus.isi.edu/docs/4.7.0/ref_staging_mapper.php 

3) [PM-1036] – R DAX API

   Pegasus now includes an R API for generating DAXes of complex and 
   large workflows in R environments. The API follows the Google' R 
   style guide, and all objects and methods are defined using the S3 
   OOP system. The source package can be obtained by running 
   *pegasus-config --r* or from the Pegasus' downloads page. A tutorial
   workflow can be generated using *pegasus-init*, and an example
   workflow is provided in the examples folder. More information can
   be found in the documentation at
   https://pegasus.isi.edu/docs/4.7.0/dax_generator_api.php#api-r

   Related JIRA item: [PM-1074] – Add R example to pegasus-init

4) [PM-1126] – pegasus-analyzer should report information about held jobs

   Pegasus monitoring daemon now populates the reason for held jobs in
   it’s database. Both pegasus-analyzer and dashboard were updated to
   show this information. 
   Related JIRA items:
   	   [PM-1121] – Store reasons for workflow failure in stampede database
	   [PM-1122] – update dashboard to display reasons for workflow state and jobstate
	   [PM-1058] – Create homebrew tap for pegasus

5) Pegasus is now also available via home-brew on MACOSX via a tap
 repository
   github.com/pegasus-isi/homebrew-tools

   It contains formulas for pegasus and htcondor.Users can do:$ brew
   tap pegasus-isi/tools 

$ brew install pegasus htcondor
$ brew tap homebrew/services
$ brew services start htcondor

6) [PM-928] – pegasus-exitcode should write its output to a log file

   pegasus-exticode is now set to write to a workflow global log file
   ending in exitcode.log that captures pegasus-exitcode stdout and
   stderr as json messages.  This allows users to check
   pegasus-exitcode messages, which otherwise would have been set to
   /dev/null by condor dagman. 

7) [PM-1115] – Pegasus to check for cyclic dependencies in the DAG
   Pegasus now explicitly checks for cyclic dependencies and reports
   one of the edges making up the cycle. 

8) [PM-1054] – Add option to ignore files in libinterpose

   kickstart now has support for environment variables
   KICKSTART_TRACE_MATCH and KICKSTART_TRACE_IGNORE that determine
   what file accesses are captured via lib interpose. The MATCH
   version only traces files that match the patterns, and the IGNORE
   version does NOT trace files that match the patterns. Only one of
   the two can be specified. 

9) [PM-915] – modify kickstart to collect and aggregate runtime
metadata

10) [PM-1004] – update metrics server ui to display extra planner configuration metrics

11) [PM-1111] – pegasus planner and api’s should have support for
ppc64 as architecture type

12) [PM-1117] – Support for tutorial via pegasus-init on bluewaters

#### Improvement

1) [PM-1125] – Disable builds for older platforms

2) [PM-1116] – pass task resource requirements as environment
variables for job wrappers to pick up 

3) [PM-1112] – enable variable expansion for regex based replica
catalog

4) [PM-1105] – Mirror job priorities to DAGMan node priorities

   HTCondor ticket 5749 . We can assign DAG priorities only if
   detected condor version is greater than 8.5. 

5) [PM-1094] – pegasus-dashboard file browser should load on demand

6) [PM-1079] – pegasus-statistics should be able to skip over failures
when generating particular type of stats 

7) [PM-1073] – condor_q changes in 8.5.x will affect pegasus-status

8) [PM-1023] – KIckstart stdout/stderr as CDATA

9) [PM-749] – Store job held reasons in stampede database

10) [PM-1088] – Move to relative paths in dagman and condor submit
files

11) [PM-900] – site catalog for XSEDE

12) [PM-901] – site for OSG

#### Bugs Fixed

1) [PM-1061] – pegasus-analyzer should detect and report on failed job
submissions

2) [PM-1118] – database changes to jobstate and workflow state tables

3) [PM-1124] – Hashed Output Mappers throw unable to instantiate error

4) [PM-1127] – Wf adds worker package staging even though it has already placed a worker package in place
 
