## Pegasus 4.6.x Series

### Pegasus 4.6.2

**Release Date:** August 17, 2016


We are happy to announce the release of Pegasus 4.6.2.  Pegasus 4.6.2
is a minor release of Pegasus and includes improvements and bug fixes
to the 4.6.1 release.

New features and Improvements in 4.6.2 are

- support for kickstart wrappers that can setup a user environment
- support for Cobalt and SLURM schedulers via the Glite interfaces
- ability to do local copy of files in PegasusLite to staging site, if
  the compute and staging site is same
- support for setting up Pegasus Tutorial on Bluewaters using
  pegasus-init

#### New Features

1) [PM-1095] - pegasus-service init script [\#1209](https://github.com/pegasus-isi/pegasus/issues/1209)

2) [PM-1101] - Add support for gsiscp transfers [\#1215](https://github.com/pegasus-isi/pegasus/issues/1215)
   These will work like the scp ones, but with x509 auth instead of
   ssh public keys.

3) [PM-1110] - put in support for cobalt scheduler at ALCF [\#1224](https://github.com/pegasus-isi/pegasus/issues/1224)
   Pegasus was updated to use the HTCondor Blahp support. ALCF has a
   cobalt scheduler to schedule jobs to the BlueGene system. The
   documentation has details on how the pegasus task requirement
   profiles map to Cobalt
   parameters. https://pegasus.isi.edu/docs/4.6.2/glite.php#glite_mappings

   To use HTCondor on Mira, please contact the HTCondor team to point
   you to the latest supported HTCondor installation on the system.

4) [PM-1096] - Update Pegasus' glite support to include SLURM [\#1210](https://github.com/pegasus-isi/pegasus/issues/1210)

5) [PM-1115] - Pegasus to check for cyclic dependencies in the DAG [\#1229](https://github.com/pegasus-isi/pegasus/issues/1229)

   Pegasus now checks for cyclic dependencies that may exist in the
   DAX or are as a result of adding edges automatically based on data
   depedencies

6) [PM-1116] - pass task resource requirements as environment [\#1230](https://github.com/pegasus-isi/pegasus/issues/1230)
   variables for job wrappers to pick up
   The task resource requirements are also passed as environment
   variables for the jobs in the GLITE style. This ensures that job
   wrappers can pick up task requirement profiles as environment
   variables.

#### Improvements

1) [PM-1078] - pegasus-statistics should take comma separated list of [\#1192](https://github.com/pegasus-isi/pegasus/issues/1192)
   values for -s option

2) [PM-1105] - Mirror job priorities to DAGMan node priorities [\#1219](https://github.com/pegasus-isi/pegasus/issues/1219)

   The job priorities associated with jobs in the workflow are now
   also associated as DAGMan node priorities, provided that HTCondor
   version is 8.5.7 or higher.

3) [PM-1108] - Ability to do local copy of files in PegasusLite to [\#1222](https://github.com/pegasus-isi/pegasus/issues/1222)
    staging site, if the compute and staging site is same

    The optimization implemented is implemented in the Planner's
    pegasus lite generation code, where when constructing the
    destination URL's for the output site it checks for
    a) symlinking is turned on
    b) compute site for the job and staging site for job are same.

    This means that the shared-scratch directory used on the staging
    site is locally accessible to the compute nodes. So we can go
    directly via the filesystem to copy the file. So instead of
    creating a gsiftp url , will create a file url in pegasuslite
    wrappers for the jobs running on local site.

4) [PM-1112] - enable variable expansion for regex based replica [\#1226](https://github.com/pegasus-isi/pegasus/issues/1226)
   catalog

   Variable expansion for Regex based replica catalogs was not
   supported earlier. This is fixed now.

5) [PM-1117] - Support for tutorial via pegasus-init on Bluewaters [\#1231](https://github.com/pegasus-isi/pegasus/issues/1231)
   pegasus-init was updated to support running tutorial examples on
   Bluewaters. To use this, users need to logon to the bleaters login
   node and run pegasus-init. The assumption is that HTCondor is
   running on the login node either in user space or root.

6) [PM-1111] - pegasus planner and api's should have support for ppc64 [\#1225](https://github.com/pegasus-isi/pegasus/issues/1225)
as architecture type

#### Bugs Fixed

1) [PM-1087] - dashboard and pegasus-metadata don't query for sub [\#1201](https://github.com/pegasus-isi/pegasus/issues/1201)
   workflows

2) [PM-1089] - connect_by_submitdir should seek for braindump.txt in [\#1203](https://github.com/pegasus-isi/pegasus/issues/1203)
   the workflow root folder

3) [PM-1093] - disconnect in site catalog and DAX schema for specifying OSType [\#1207](https://github.com/pegasus-isi/pegasus/issues/1207)

4) [PM-1099] - x509 credentials should be transferred using [\#1213](https://github.com/pegasus-isi/pegasus/issues/1213)
   x509userproxy

5) [PM-1100] - Typo in rsquot, ldquot and rdquot [\#1214](https://github.com/pegasus-isi/pegasus/issues/1214)

6) [PM-1106] - pegasus-init should not allow (or should handle) spaces [\#1220](https://github.com/pegasus-isi/pegasus/issues/1220)
   in site name

7) [PM-1107] - pegasuslite signal handler race condition [\#1221](https://github.com/pegasus-isi/pegasus/issues/1221)

8) [PM-1113] - make planner directory options behavior more consistent [\#1227](https://github.com/pegasus-isi/pegasus/issues/1227)


### Pegasus 4.6.1

**Release Date:** April 21, 2016

We are happy to announce the release of Pegasus 4.6.1.  Pegasus 4.6.1
is a minor release of Pegasus and includes improvements and bug fixes
to the 4.6.0 release

New features and Improvements in 4.6.1 are

- support for MOAB submissions via glite. A new tool called
  pegasus-configure-glite helps users setup their HTCondor GLite
  directory for use with Pegasus

- pegasus-s3 now allows for downloading and uploading folders to
  and from S3

- initial support for globus online in pegasus-transfer
    planner automatically copies the user catalog files into a
    directory called catalogs in the submit directory.

- changes to how worker package staging occurs for compute jobs.

#### New Features

1) [PM-1045] – There is a new command line tool [\#1159](https://github.com/pegasus-isi/pegasus/issues/1159)
   pegasus-configure-glite that automatically installs the Pegasus
   shipped glite local attributes script to the condor glite
   installation directory

2) [PM-1044] – Added glite scripts for moab submissions via the Glite [\#1158](https://github.com/pegasus-isi/pegasus/issues/1158)
   interface

3) [PM-1054] – kickstart has an option to ignore files in lib [\#1168](https://github.com/pegasus-isi/pegasus/issues/1168)
   interpose.

   This is triggered by setting the environment variables
   KICKSTART_TRACE_MATCH and KICKSTART_TRACE_IGNORE. The MATCH version
   only traces files that match the patterns, and the IGNORE version
   does NOT trace files that match the patterns. Only one of the two
   can be specified.

4) [PM-1058] -pegasus can be now installed via homebrew on MACOSX [\#1172](https://github.com/pegasus-isi/pegasus/issues/1172)
   For details refer to documentation at
   https://pegasus.isi.edu/documentation/macosx.php

5) [PM-1075] – pegasus-s3 to be able to download all files in a folder [\#1189](https://github.com/pegasus-isi/pegasus/issues/1189)

   pegasus-s3 has a –recursive option to allow users to download all
   files from a folder in S3 or upload all files from a local
   directory to S3 bucket.

6) [PM-680] – Add support for GlobusOnline to pegasus-transfer [\#798](https://github.com/pegasus-isi/pegasus/issues/798)
   Details on how to configure can be found at
   https://pegasus.isi.edu/docs/4.6.1/transfer.php#transfer_globus_online

7) [PM-1043] – Improve CSV file read for Storage Constraints algorithm [\#1157](https://github.com/pegasus-isi/pegasus/issues/1157)

8) [PM-1047] – Pegasus saves all the catalog files in submit dir in a [\#1161](https://github.com/pegasus-isi/pegasus/issues/1161)
   directory named catalogs. This enables for easier debugging later on
   as everything is saved in the submit directory.

#### Improvements

1) [PM-1043] – Improve CSV file read for Storage Constraints algorithm [\#1157](https://github.com/pegasus-isi/pegasus/issues/1157)

2) [PM-1057] – PegasusLite worker package download improvements [\#1171](https://github.com/pegasus-isi/pegasus/issues/1171)
   Pegasus exposes two additional properties to control behavior of
   worker package staging for jobs. Users can use these to control
   whether a PegasusLite job downloads a worker package from the
   pegasus website or not , in case the shipped worker package does
   not match the node architecture.

   pegasus.transfer.worker.package.strict – enforce strict checks
   against provided worker package. if a job comes with worker package
   and it does not match fully with worker node architecture , it
   falls down to pegasus download website. Default value is true.
   pegasus.transfer.worker.package.autodownload – a boolean property
   to indicate whether a pegasus lite job is allowed to download from
   pegasus website. Defaults to true.

3) [PM-1059] – Implement backup for MySQL databases [\#1173](https://github.com/pegasus-isi/pegasus/issues/1173)

4) [PM-1060] – expose a way to turn off kickstart stat options [\#1174](https://github.com/pegasus-isi/pegasus/issues/1174)

5) [PM-1063] – improve performance for inserts into database replica catalog [\#1177](https://github.com/pegasus-isi/pegasus/issues/1177)

6) [PM-1067] – pegasus-cluster -R should report the finish time and [\#1181](https://github.com/pegasus-isi/pegasus/issues/1181)
   duration, not the start time and duration

7) [PM-1078] – pegasus-statistics should take comma separated list of [\#1192](https://github.com/pegasus-isi/pegasus/issues/1192)
   values for -s option

8) [PM-1073] – condor_q changes in 8.5.x will affect pegasus-status [\#1187](https://github.com/pegasus-isi/pegasus/issues/1187)

   pegasus-status was updated to account for changes in the condor_q
   output in the 8.5 series

#### Bugs Fixed

1) [PM-1077] – pegasus-remove on hierarchal workflows results in jobs [\#1191](https://github.com/pegasus-isi/pegasus/issues/1191)
   from the sub workflows still in the condor queue

   DAGMan no longer condor_rm jobs in a workflow itself. Instead it
   relies on condor schedd to do it. Pegasus generated sub workflow
   description files did not trigger this . As a result,
   pegasus-remove on a top level workflow still resulted in jobs from
   the sub workflows to be in the condor queue. This is now
   fixed. Pegasus generated dagman submit files have the right
   expressions specified.

2) [PM-997] – pyOpenSSL v0.13 does not work with new version of [\#1114](https://github.com/pegasus-isi/pegasus/issues/1114)
   openssl (1.0.2d) and El Captain

3) [PM-1048] – PegasusLite should do a full version check for [\#1162](https://github.com/pegasus-isi/pegasus/issues/1162)
   pre-installed worker packages

   PegasusLite does a full check ( including the patch version) with
   the pegasus version installed on the node, when determining whether
   to use the preinstalled version on the node or not.

4) [PM-1050] – pegasus-plan should not fail if -D arguments don’t [\#1164](https://github.com/pegasus-isi/pegasus/issues/1164)
   appear first

5) [PM-1051] – Error missing when nodes, cores, and ppn are all [\#1165](https://github.com/pegasus-isi/pegasus/issues/1165)
   specified

   In 4.6.0 release, there was a bug where the error message thrown (
   when user specified an invalid combination of task requirements)
   was incorrect. This is fixed, and error messages have been improved
   to also indicate a reason

6) [PM-1053] – pegasus-cluster does not know about new Kickstart [\#1167](https://github.com/pegasus-isi/pegasus/issues/1167)
   arguments

7) [PM-1055] – Interleaved libinterpose records [\#1169](https://github.com/pegasus-isi/pegasus/issues/1169)

8) [PM-1061] – pegasus-analyzer should detect and report on failed job [\#1175](https://github.com/pegasus-isi/pegasus/issues/1175)
   submissions

   pegasus-monitord did not populate the stampede workflow database
   with information about job submission failures. As a result,
   pegasus-analyzer for the cases where a job failed because of job
   submission errors did not report any helpful information as to why
   the job failed. This is now fixed.

9) [PM-1062] – pegasus dashboard shows some workflows twice [\#1176](https://github.com/pegasus-isi/pegasus/issues/1176)

   In the case where HTCondor crashes on a submit node, DAGMan logs
   may miss a workflow end event. When monitord detects consecutive
   start events, it creates and inserts a workflow end event. The end
   event had the same timestamp as the new start event, because of
   which underlying dashboard query retrieved multiple rows.  This was
   fixed by setting the timestamp for the artificial end event to be
   one second less than the second start event.

10) [PM-1064] – pegasus-transfer prepends to PATH [\#1178](https://github.com/pegasus-isi/pegasus/issues/1178)

    pegasus-transfer used to prepend the system path with other
    internal determined lookup directories based on environment
    variables such as GLOBUS_LOCATION. As a result, in some cases,
    user preferred copy of executables were not picked up. This is now
    fixed.

11) [PM-1066] – wget errors because of network issues [\#1180](https://github.com/pegasus-isi/pegasus/issues/1180)
    pegasus-transfer now sets the OSG_SQUID_LOCATION/http_proxy
    setting only for the first wget attempt

12) [PM-1068] – monitord fails when trying to open a job error file in [\#1182](https://github.com/pegasus-isi/pegasus/issues/1182)
    a workflow with condor recovery

    monitord parses the job submit file whenever it notices job
    submission log by DAGMan. This is done to avoid the case, where
    because of HTCondor recovery a job may not have a ULOG_ job
    submission event, because of which the internal state of the job
    maybe uninitialized.

13) [PM-1069] – Dashboard invocation page gives an error if the task [\#1183](https://github.com/pegasus-isi/pegasus/issues/1183)
    has no invocation record

    Dashboard did not display invocation records for Pegasus added
    auxiliary jobs in the workflow. This was due to a bug in the query
    that is now fixed.

14) [PM-1070] – monitord should handle case where jobs have missing [\#1184](https://github.com/pegasus-isi/pegasus/issues/1184)
    JOB_FAILURE/JOB_TERMINATED events

15) [PM-1072] – Worker package staging issues on OSX [\#1186](https://github.com/pegasus-isi/pegasus/issues/1186)

16) [PM-1081] – pegasus-plan complains if output dir is set but site [\#1195](https://github.com/pegasus-isi/pegasus/issues/1195)
     catalog entry for local site does not storage directory specified

     pegasus-plan complained if a storage directory was not specified
     in the site catalog entry for site “local”, even if a user
     specified a –output-dir option. This is now fixed. The planner
     will create a default file server based entry for this case.


17) [PM-1082] – transfer jobs don’t have symlink destination URL even [\#1196](https://github.com/pegasus-isi/pegasus/issues/1196)
    though symlink is enabled

    In the case, where there are multiple candidate replica locations
    ( some on preferred site and some on other sites), the destination
    URL for the transfer jobs did not have a symlink URL. As a result
    the data was never symlinked even though it was available locally
    on the preferred site.

18) [PM-1083] – dashboard user home page requires a trailing / [\#1197](https://github.com/pegasus-isi/pegasus/issues/1197)

    To access a user home page on the dashboard, a trailing / needs to
    be specified after the username in the URL. Dashboard was updated
    to handle URL’s without trailing names.

19) [PM-1084] – credential handling for glite jobs [\#1198](https://github.com/pegasus-isi/pegasus/issues/1198)

    As part of credential handling the environment variable for the
    staged credential was as environment key instead of the
    +remote_environment classed key.  As a result transfer jobs
    running via Glite submission failed as the appropriate environment
    variable was not set. This is fixed now.

20) [PM-1085] – -p 0 options for condor_dagman sub dax jobs result in [\#1199](https://github.com/pegasus-isi/pegasus/issues/1199)
    dagman ( 8.2.8) dying

    Pegasus updated to generate the dagman submit files for sub
    workflows to be compatible with 8.5.x series.  However, the new
    arguments added resulted in breaking workflows running with old
    HTCondor versions. The offending argument is now set only if
    condor version is more than 8.3.6

21) [PM-1086] – Never symlink executables [\#1200](https://github.com/pegasus-isi/pegasus/issues/1200)

    Pegasus adds chmod jobs to explicitly set the x bit of the
    executables staged. If the executable is a symlinked executable,
    then chmod fails. Symlinking is never triggered for staged
    executables now.


### Pegasus 4.6.0

**Release Date:** January 27, 2016

 We are happy to announce the release of Pegasus 4.6.0.  Pegasus 4.6.0
 is a major release of Pegasus and includes all the bug fixes and
 improvements in the 4.5.4 release

 New features and Improvements in 4.6.0  are

 - metadata support
 - support for variable substitution
 - constraints based cleanup algorithm
 - common pegasus profiles to specify task requirements
 - new command line client pegasus-init to configure pegasus and
pegasus-metadata to query workflow database for metadata
 - support for fallback PFN's

 Migration guide available at
 http://pegasus.isi.edu/wms/docs/4.6.0dev/useful_tips.php#migrating_from_leq45

 Debian and Ubuntu users: Please note that the Apt repository GPG key
 has changed. To continue to get automatic updates, please follow the
 instructions on the download page on how to install the new key.

#### New Features

1)  Metadata support in Pegasus
    Pegasus allows users to associate metadata at
     - Workflow Level in the DAX
     - Task level in the DAX and the Transformation Catalog
     - File level in the DAX and Replica Catalog

    Metadata is specified as a key value tuple, where both key and
    values are of type String.

    All the metadata ( user specified and auto-generated) gets
    populated into the workflow database ( usually in the workflow
    submit directory) by pegasus-monitord. The metadata in this
    database can be be queried for using the pegasus-metadata command
    line tool, or is also shown in the Pegasus Dashboard.

    Documentation: https://pegasus.isi.edu/wms/docs/4.6.0/metadata.php

    Relevant JIRA items
    - [PM-917] - modify the workflow database to associate metadata with [\#1035](https://github.com/pegasus-isi/pegasus/issues/1035)
    workflow, job and files
    - [PM-918] - modify pegasus-monitord to populate metadata into [\#1036](https://github.com/pegasus-isi/pegasus/issues/1036)
    stampede database
    - [PM-919] - pegasus-metadata command line tool [\#1037](https://github.com/pegasus-isi/pegasus/issues/1037)
    - [PM-916] - identify and generate the BP events for metadata [\#1034](https://github.com/pegasus-isi/pegasus/issues/1034)
    - [PM-913] - kickstart support for stat command line options [\#1031](https://github.com/pegasus-isi/pegasus/issues/1031)
    - [PM-1025] - Document the metadata capability for 4.6 [\#1140](https://github.com/pegasus-isi/pegasus/issues/1140)
    - [PM-992] - automatically capture file metadata from kickstart and record it [\#1109](https://github.com/pegasus-isi/pegasus/issues/1109)
    - [PM-892] - Add metadata to DAX schema [\#1010](https://github.com/pegasus-isi/pegasus/issues/1010)
    - [PM-893] - Add metadata to Python DAX API [\#1011](https://github.com/pegasus-isi/pegasus/issues/1011)
    - [PM-894] - Add metadata to site catalog schema [\#1012](https://github.com/pegasus-isi/pegasus/issues/1012)
    - [PM-895] - Add metadata to transformation catalog text format [\#1013](https://github.com/pegasus-isi/pegasus/issues/1013)
    - [PM-902] - support for metadata to JAVA DAX API [\#1020](https://github.com/pegasus-isi/pegasus/issues/1020)
    - [PM-903] - add metadata to perl dax api [\#1021](https://github.com/pegasus-isi/pegasus/issues/1021)
    - [PM-904] - support for parsing DAX 3.6 documents [\#1022](https://github.com/pegasus-isi/pegasus/issues/1022)
    - [PM-978] - Update JDBCRC with the new schema [\#1095](https://github.com/pegasus-isi/pegasus/issues/1095)
    - [PM-925] - support for 4.1 new site catalog schema with metadata extensions [\#1043](https://github.com/pegasus-isi/pegasus/issues/1043)
    - [PM-991] - pegasus dashboard to display metadata stored in workflow database [\#1108](https://github.com/pegasus-isi/pegasus/issues/1108)

2) Support for Variable Substitution

   Pegasus Planner supports notion of variable expansions in the DAX
   and the catalog files along the same lines as bash variable
   expansion works. This is often useful, when you want paths in your
   catalogs or profile values in the DAX to be picked up from the
   environment. An error is thrown if a variable cannot be expanded.

   Variable substitution is supported in the DAX, File Based Replica
   Catalog, Transformation Catalog and the  Site Catalog.

   Documentation: https://pegasus.isi.edu/wms/docs/4.6.0/variable_expansion.php

   Relevant JIRA items
   - [PM-831] - Add better support for variables [\#949](https://github.com/pegasus-isi/pegasus/issues/949)

3) Constraints based Cleanup Algorithm

   The planner now has support for a new cleanup algorithm called
   constraint. The algoirthm adds cleanup nodes to constraint the
   amount of storage space used by a workflow. The nodes remove files
   no longer required during execution. The added cleanup node
   guarantees limits on disk usage. The leaf cleanup nodes are also
   added when this is selected.

   - [PM-850] - Integrate Sudarshan's cleanup algorithm [\#968](https://github.com/pegasus-isi/pegasus/issues/968)

4) Common Pegasus Profiles to indicate Resource Requirements for jobs

   Users can now specify Pegasus profiles to indicate resource
   requirements for jobs. Pegasus will automatically, translate these
   to the approprate condor, globus or batch system keys based on how
   the job is executed.

   The profiles are documented in the configuration chapter at ask
   requirement profiles are documented here
   https://pegasus.isi.edu/wms/docs/4.6.0/profiles.php#pegasus_profiles

   - [PM-962] - common pegasus profiles to indicate resource requirements for job [\#1079](https://github.com/pegasus-isi/pegasus/issues/1079)


5) New client pegasus-init

   A new command line client called "pegasus-init" that generates a
   new workflow configuration based by asking the user a series of
   questions. Based on the responses to these questions,
   *pegasus-init* generates a workflow configuration including a DAX
   generator, site catalog, properties file, and other artifacts that
   can be edited to meet the user's needs.

   - [PM-1019] - pegasus-init client to setup pegasus on a machine [\#1134](https://github.com/pegasus-isi/pegasus/issues/1134)

6) Support for automatic fallover to fallback file locations

   Pegasus, now during Replica Selection orders all the candidate
   replica's instead of selecting the best replica. This replica's are
   ordered based on the strategy selected, and the ordered list is
   passed to pegasus-transfer invocation. This allows users to specify
   failover, or preferred location for discovering the input files.

   By default, planner employs the following logic for the ordering of replicas
      - valid file URL's . That is URL's that have the site attribute
   matching the site where the executable pegasus-transfer is
   executed.
      - all URL's from preferred site (usually the compute site)
      - all other remotely accessible ( non file) URL's

   If a user wants to specify their own order preference , then they
   should use the Regex Replica Selector and specify a ranked order
   list of regular expressions in the properties.

   Documentation:
   https://pegasus.isi.edu/wms/docs/4.6.0/data_management.php#replica_selection

   Relevant JIRA items:
   - [PM-1002] - Support symlinking against compute site datasets in [\#1119](https://github.com/pegasus-isi/pegasus/issues/1119)
   nonsharedfs mode with bypass of input file staging
   - [PM-1014] - Support for Fallback PFN while transferring raw input files [\#1130](https://github.com/pegasus-isi/pegasus/issues/1130)

7) Support SGE via the HTCondor Glite/Batch GAHP support

   Pegasus now has support for submitting to a local SGE cluster via
   the HTCondor Glite/Blahp interfaces. More details can be found in
   the documentation at
   https://pegasus.isi.edu/wms/docs/4.6.0/glite.php

   - [PM-955] - Support for direct submission through SGE using [\#1072](https://github.com/pegasus-isi/pegasus/issues/1072)
   Condor/Glite/Blahp layer

8) Glite Style improvements

   Users don't need to set extra pegasus profiles to enable jobs to
   run correctly on glite style sites. By default, condor quoting for
   jobs  on glite style sites is disabled. Also, the -w option to
   kickstart is always as batch gahp does not support specification of
   a remote execution directory directly.

   If the user knows that a compute site shares a file system with the
   submit host, then they can get Pegasus to run the auxillary jobs in
   local universe. This is especially helpful , when submitting to
   local campus clusters using Glite and users don't want the pegasus
   auxillary jobs to run through the cluster PBS|SGE queue.

   Relevant JIRA items
   - [PM-934] - changed how environment is set for jobs submitted via [\#1051](https://github.com/pegasus-isi/pegasus/issues/1051)
   HTCondor Glite / Blahp layer
   - [PM-1024] - Use local universe for auxiliary jobs in glite/blahp mode [\#1139](https://github.com/pegasus-isi/pegasus/issues/1139)
   - [PM-1037] - Disable Condor Quoting for jobs run on glite style [\#1151](https://github.com/pegasus-isi/pegasus/issues/1151)
   execution sites
   - [PM-960] - Set default working dir to scratch dir for glite style jobs [\#1077](https://github.com/pegasus-isi/pegasus/issues/1077)


9)  Support for PAPI CPU counters in kickstart
    - [PM-967] - Add support for PAPI CPU counters in Kickstart [\#1084](https://github.com/pegasus-isi/pegasus/issues/1084)

10) Changes to worker package staging

    Pegasus now by default, attempts to use the worker package out of
    the Pegasus submit host installation unless a user has specified
    finer grained attributes for the compute sites in the site catalog
    or an entry is specified in the transformation catalog.

    Relevant JIRA items
    - [PM-888] - Guess which worker package to use based on the submit host [\#1006](https://github.com/pegasus-isi/pegasus/issues/1006)

11) [PM-953] - PMC now has the ability to set CPU affinity for multicore tasks. [\#1070](https://github.com/pegasus-isi/pegasus/issues/1070)

12) [PM-954] - Add useful environment variables to PMC [\#1071](https://github.com/pegasus-isi/pegasus/issues/1071)

13) [PM-985] - separate input and output replica catalogs [\#1102](https://github.com/pegasus-isi/pegasus/issues/1102)

    Users can specify a different output replica catalog optionally by
    specifying the property with prefix pegasus.catalog.replica.output

    This is useful when users want to separate the replica catalog
    that they use for discovery of input files and the catalog where
    the output files generated are registered. For example use a
    Directory backed replica catalog backend to discover file
    locations, and a file based replica catalog to catalog the
    locations of the output files.

14) [PM-986] - input-dir option to pegasus-plan should be a comma [\#1103](https://github.com/pegasus-isi/pegasus/issues/1103)
separated list

15) [PM-1031] - pegasus-db-admin should have an upgrade/dowgrade [\#1145](https://github.com/pegasus-isi/pegasus/issues/1145)
option to update all databases from the dashboard database to current
pegasus version

16) [PM-882] - Create prototype integration between Pegasus and Aspen [\#1000](https://github.com/pegasus-isi/pegasus/issues/1000)

17) [PM-964] - Add tips on how to use CPU affinity on condor [\#1081](https://github.com/pegasus-isi/pegasus/issues/1081)

#### Improvements

1) [PM-924] - Merge transfer/cleanup/create-dir into one client [\#1042](https://github.com/pegasus-isi/pegasus/issues/1042)

2) [PM-610] - Batch scp transfers in pegasus-transfer [\#728](https://github.com/pegasus-isi/pegasus/issues/728)

   pegasus-transfer now batches 70 transfers in a single scp
   invocation against the same host.

3) [PM-611] - Batch rm commands in scp cleanup implementation [\#729](https://github.com/pegasus-isi/pegasus/issues/729)

   scp rm are now batched together at a level of 70 per group so that
   we can keep the command lines short enough.

4) [PM-856] - pegasus-cleanup should use pegasus-s3's bulk delete [\#974](https://github.com/pegasus-isi/pegasus/issues/974)
feature

    s3 removes are now batched and passed in a temp file to pegasus-s3

5) [PM-890] - pegasus-version should include a Git hash [\#1008](https://github.com/pegasus-isi/pegasus/issues/1008)

6) [PM-899] - Handling of database update versions from different branches [\#1017](https://github.com/pegasus-isi/pegasus/issues/1017)

7) [PM-911] - Use ssh to call rm for sshftp URL cleanup [\#1029](https://github.com/pegasus-isi/pegasus/issues/1029)

8) [PM-929] - Use make to build externals to make python development easier [\#1046](https://github.com/pegasus-isi/pegasus/issues/1046)

9) [PM-937] - Discontinue support for Python 2.4 and 2.5 [\#1054](https://github.com/pegasus-isi/pegasus/issues/1054)

10) [PM-938] - Pegasus DAXParser always validates against latest supported DAX version [\#1055](https://github.com/pegasus-isi/pegasus/issues/1055)

11) [PM-958] - Deprecate "gridstart" names in Kickstart [\#1075](https://github.com/pegasus-isi/pegasus/issues/1075)

12) [PM-963] - Add support for wrappers in Kickstart [\#1080](https://github.com/pegasus-isi/pegasus/issues/1080)

     Kickstart supports an environment variable, KICKSTART_WRAPPER
     that contains a set of command-line arguments to insert between
     Kickstart and the application

13) [PM-965] - monitord amqp population [\#1082](https://github.com/pegasus-isi/pegasus/issues/1082)

14) [PM-979] - Update documentation for new DB schema [\#1096](https://github.com/pegasus-isi/pegasus/issues/1096)

15) [PM-984] - condor_rm on a pegasus-kickstart wrapped job does not [\#1101](https://github.com/pegasus-isi/pegasus/issues/1101)
return stdout back

      When a user condor_rm's their job, Condor sends the job a
      SIGTERM. Previously this would cause Kickstart to die. This
      commit changes Kickstart so that it catches the SIGTERM and
      passes it on to the child instead. That way the child dies, but
      not Kickstart, and Kickstart can report an invocation record
      forthe job to provide the user with useful debugging info. This
      same logic is also applied to SIGINT and SIGQUIT.

16) [PM-1018] - defaults for pegasus-plan to pick up properties and [\#1133](https://github.com/pegasus-isi/pegasus/issues/1133)
other catalogs

      pegasus will default the --conf option to pegasus-plan to
      pegasus.properties in the current working directory.
      In addition, the default locations for the various catalog files
      now point to current working directory ( rc.txt, tc.txt,
      sites.xml )


17) [PM-1038] - Update tutorial to reflect the defaults for Pegasus 4.6 release [\#1152](https://github.com/pegasus-isi/pegasus/issues/1152)

#### Bugs Fixed

1) [PM-653] - pegasus.dagman.nofity should be removed in favor of [\#771](https://github.com/pegasus-isi/pegasus/issues/771)
Pegasus level notifcaitons

2) [PM-897] - kickstart is reporting misleading permission error when [\#1015](https://github.com/pegasus-isi/pegasus/issues/1015)
it is really a file not found

3) [PM-906] - Add Ubuntu apt repository [\#1024](https://github.com/pegasus-isi/pegasus/issues/1024)

4) [PM-910] - Cleanup jobs should ignore "file not found" errors, but [\#1028](https://github.com/pegasus-isi/pegasus/issues/1028)
not other errors

5) [PM-920] - Bamboo / title.xml problems [\#1038](https://github.com/pegasus-isi/pegasus/issues/1038)

6) [PM-922] - Dashboard and monitoring interface contain Python that [\#1040](https://github.com/pegasus-isi/pegasus/issues/1040)
is not valid for RHEL5

7) [PM-923] - Debian packages rebuild documentation [\#1041](https://github.com/pegasus-isi/pegasus/issues/1041)

8) [PM-931] - For Subworkflows Monitord populates host.wf_id to be [\#1048](https://github.com/pegasus-isi/pegasus/issues/1048)
wf_id of root_wf and not wf_id of sub workflow

9) [PM-944] - Make it possible to build Pegasus on SuSE (openSUSE and SLES) [\#1061](https://github.com/pegasus-isi/pegasus/issues/1061)

10) [PM-1029] - Planner should ensure that local aux jobs run with the same Pegasus install as the planner [\#1143](https://github.com/pegasus-isi/pegasus/issues/1143)

11) [PM-1035] - pegasus-analyzer fails when workflow db has no [\#1149](https://github.com/pegasus-isi/pegasus/issues/1149)
entries
