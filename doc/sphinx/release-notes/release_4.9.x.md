## Pegasus 4.9.x Series

### Pegasus 4.9.3


We are happy to announce the release of Pegasus 4.9.3. Pegasus 4.9.3 is a
minor bug fix release after Pegasus 4.9.2.   

This release has support for Singularity Library as a source for pulling
singularity containers, in lieu of Singularity HUB.  It also has support
for non authenticated ftp to file transfers for pulling in input datasets. 

The tutorial utility pegasus-init was updated to support to execute on
Summit from a Kubernetes cluster at OLCF. 

The release also improvement to workflow monitoring events that can be
sent to an AMQP endpoint, that in turn in can be used to populate
ElasticSearch and build a dashboard in Grafana. More details can be
found in the guide at https://pegasus.isi.edu/docs/monitoring_amqp.php. 

#### New Features


1) [PM-1398] - include machine information in job_instance.composite event

2) [PM-1402] - pegasus-init to support summit as execution env for tutorial

3) [PM-1397] - Support ftp to file transfers in pegasus-transfer

4) [PM-1416] - add data collection setup instructions to docs under
   	       section 6.7.1.1. Monitord, RabbitMQ, ElasticSearch Example
	       
5) [PM-1403] - Support POWER9 nodes in PMC

6) [PM-1404] - pegasus tutorial for summit from Kubernetes

#### Bugs Fixed


1) [PM-1346] - Pegasus job checkpointing is incompatible with condorio

2) [PM-1380] - Support for Singularity Library

3) [PM-1388] - PegasusLite cannot locate osname and version SLES 15

4) [PM-1389] - pegasus.cores causes issues on Summit

5) [PM-1395] - GLite LSF scripts don't work as intended on OLCF's DTNs

6) [PM-1405] - Is Pegasus supposed to build on 32-bit x86 (Debian i386 Stretch)?


### Pegasus 4.9.2


We are happy to announce the release of Pegasus 4.9.2 . Pegasus 4.9.2
is a minor bug fix release after Pegasus 4.9.1. This release has
support for retrieval of datasets from HPSS, and improved events that
can be sent to AMQP endpoints for users to build custom
dashboards. Additionally, all PegasusLite jobs will now chirp
durations of the major steps such as data staging, task execution and
data stage-out. 

Also there is a new integrity dial (nosymlink) that allows 
you to disable integrity checking on symlinked files.

Binary packages for Ubuntu 17 Zesty and Debian 8 Jessie have been
discontinued. Packages for Debian 10 Buster have been added.

#### New Features and Improvements


1)  [PM-1355] – composite records when sending events to AMQP 

2)  [PM-1357] – In lite jobs, chirp durations for stage in, stage out
    	         of data

3)  [PM-1367] – Support for retrieval from HPSS tape store using
   	       commands htar and his

4)  [PM-1376] – Add LSF local attributes

5)  [PM-1378] – Handle (copy) HPSS credentials when an environment
   	       variable is set 

6)  [PM-1337] – pegasus should run monitord-replay at the end of a
   	      workflow 

7)  [PM-1356] – Replace Google CDN with a different CDN as China block
    	      	it

8)  [PM-1363] – Condor Configuration MOUNT_UNDER_SCRATCH causes
    	      	pegasus auxiliary jobs to fail

9)  [PM-1373] – Debian Buster no longer provides openjdk-8-jdk

10) [PM-1374] – make monitord resilient to dagman logging the debug
    	      	level in dagman.out


11) [PM-1375] – Do not run integrity checks on symlinked files 

12) [PM-1365] – remove __ from event keys wherever possible

13) [PM-1381] – Associated planner changes to handle LSF sites

14) [PM-1387] – make the netlogger events consistent with the
    	        documentation 


#### Bugs Fixed


1) [PM-1358] – HTCondor 8.8.0/8.8.1 remaps /tmp, and can break access
   	       to x509 credentials

2) [PM-1360] – planner drops transfer_(in|out)put_files if NoGridStart
   	       is used

3) [PM-1364] – Container Name Collision

4) [PM-1366] – Pegasus Cluster Label – Job Env Not Picked Up in
   	       Containers 

5) [PM-1369] – Chirp related changes causing jobs to get held.

6) [PM-1370] – submit error

7) [PM-1377] – A + in a tc name breaks pegasus-plan

8) [PM-1379] – Stage out job fails – wrong src location

9) [PM-1384] – .sig Singularity images (naming issue?)

### Pegasus 4.9.1

We are happy to announce the release of Pegasus 4.9.1 . Pegasus 4.9.1
is a minor bug fix release and includes a major change to how
transfers are handled for containerized jobs. Pegasus 4.9.1 also
includes support for Shifter.

Until Pegasus 4.9.0 the transfers for the jobs were conducted on the
host node in Pegasus Lite job before the application container was
launched. However, this limited the ability for users to use transfer
protocols that were not installed on the host OS.  Starting 4.9.1 the
transfers happen within the application container. 

More details at
https://pegasus.isi.edu/docs/4.9.1dev/container-exec-model.php

#### New Features and Improvements

1)  [PM-1339] – construct a default entry for local site if not
    	        present in site catalog

2)  [PM-1345] – Support for Shifter at Nersc

3)  [PM-1354] - pegasus-init to support titan tutorial

4)  [PM-1352] – Build failure on Debian 10 due to mariadb/MySQL-Python
   	        incompatibility 

5)  [PM-1216] – pegasus-transfer should follow redirects to file urls

6)  [PM-1321] – Move transfer staging into the container rather than
   	        the host OS

7)  [PM-1323] – pegasus transfer should not try and transfer a file
        	that does not exist

8)  [PM-1329] – pegasus integrity causes LIGO workflows to fail

9)  [PM-1338] – Add support for TACC wrangler to pegasus-init

10)  [PM-1328] – support sharedfs on the compute site as staging site

11) [PM-1340] – make planner os releases consistent with builds

#### Bugs Fixed

1)  [PM-1182] – registration jobs fail if a file based RC has variables
   	        defined

2)  [PM-1320] – pegasus-plan doesn’t plan with container in sharedfs
               mode

3)  [PM-1322] – DB Admin failing when run within planner

4)  [PM-1325] – Debian build with incorrect dependencies

5)  [PM-1326] – singularity suffix computed incorrectly

6)  [PM-1327] – bypass input file staging broken for container
               execution 

7)  [PM-1330] – .meta files created even when integrity checking is
   	       disabled. 

8)  [PM-1332] – monitord is failing on a dagman.out file

9)  [PM-1333] – amqp endpoint errors should not disable database
    	        population for multiplexed sinks 

10) [PM-1334] – pegasus dagman is not exiting cleanly

11) [PM-1336] – pegasus-submitdir is broken

12) [PM-1350] – pegasus is ignoring when_to_transfer_output


###  Pegasus 4.9.0
We are pleased to announce release of Pegasus 4.9.0

Pegasus 4.9.0 is be a major release of Pegasus. Highlights of new
features: 

1) Integrity Checking 

   Pegasus now performs integrity checking on files in a workflow for
   non shared filesystem deployments. More details can be found in the
   documentation at 
   https://pegasus.isi.edu/documentation/integrity_checking.php . This
   work is the result of the Scientific Workflow Integration with
   Pegasus  project (NSF awards 1642070, 1642053, and 1642090). More
   information  on SWIP is available at
   https://cacr.iu.edu/projects/swip/  

2) AWS Batch 

    Pegasus now provides a way to execute horizontally clustered jobs
    on Amazon AWS Batch Service using a new command line tool
    pegasus-aws-batch. In other words, you can get Pegasus to cluster
    each level of your workflow into a bag of tasks and run those
    clustered jobs on Amazon Cloud using AWS Batch Service. InMore
    details can be found in documentation
    at https://pegasus.isi.edu/docs/4.9.0/aws-batch.php . 

3) Pegasus Tutorial With Containers 

    We have a version of the tutorial for prospective new users to try
    out at  https://pegasus.isi.edu/tutorial/isi/ . This tutorial
    walks through users on how to bundle their application code in
    containers and use them for execution with Pegasus. 

    The release can be downloaded from
    https://pegasus.isi.edu/downloads

Please note that the Perl DAX API is deprecated starting 4.9.0 Release
and will be removed in the 5.0 Release. 

Exhaustive list of features, improvements and bug fixes can be found below

#### New Features


1)  [PM-1179] - Integrity checking in Pegasus

2)  [PM-1228] - Integrate Pegasus with AWS Batch

3)  [PM-1167] - Dashboard should explain why condor submission failed

4)  [PM-1233] - update pyopen ssl to 0.14 or higher

5)  [PM-1265] - monitord to send events from job stdout

6)  [PM-1277] - pegasus workflows on OSG should appear in OSG gratia

7)  [PM-1280] - incorporate container based example in pegasus-init

8)  [PM-1283] - tutorial vm should be updated to be able to run the
   	       containers tutorial 

9)  [PM-1289] - ability to filter events sent to AMQP endpoint.

10) [PM-1298] - symlinking when running with containers

13) [PM-1310] - integrity checking documentation

#### Improvements


1)  [PM-1288] - pegasus.project profile key is not set for SLURM
   	       submissions

2)  [PM-898] - Modify monitord so that it can send AMQP messages AND
   	      populate a DB.

3)  [PM-1243] - Clusters of size 1 should be allowed when using AWS
	      Batch and Horizontal Clustering 

4)  [PM-1244] - analyzer is not showing location of submit file

5)  [PM-1248] - Planning fails in shared-fs mode with cryptic message.

6)  [PM-1258] - Update AMQP support for Panorama

7)  [PM-1270] - transformation selectors should preferred the site
   	       assigned by site selector 

8)  [PM-1285] - Exporting environmental variables in containers with
   	       the same technique.

9)  [PM-1294] - update flask dependency

10) [PM-1299] - Consider capturing which API was used to generate the
    	        DAX in metrics. 
11) [PM-1308] - changes to database connection code

12) [PM-1313] - allow for direct use of singularity containers on
    	      	CVMFS on compute site 
#### Tasks


1)  [PM-1284] - pegasus-mpi-cluster's test PM848 fails on Debian 10 

2)  [PM-901] - site for OSG

3)  [PM-1200] - update pegasus lite mode to support singulartiy 

4)  [PM-1231] - support pegasus-aws-batch as clustering executable

5)  [PM-1246] - add manpage for pegasus-aws-batch

6)  [PM-1250] - update handling for raw input files for integrity
    	        checking

7)  [PM-1251] - pegasus-transfer to checksum files

8)  [PM-1252] - enforce integrity checking for stage-out jobs

9)  [PM-1254] - handling of checkpoint files for integrity checking 

10) [PM-1257] - propagate checksums in hierarchal workflows

11) [PM-1260] - Integrity data in Stampede / statistics

12) [PM-1271] - update database schema and pegasus-db-admin to include
    	        integrity meta table 

13) [PM-1272] - pegasus-integrity-check to log integrity data in
    	        monitord parseable event 

14) [PM-1273] - pegasus-integrity-check to check for multiple files

15) [PM-1274] - update monitord to populate integrity events

16) [PM-1279] - Show integrity metrics in dashboard statistics page

17) [PM-1295] - pegasus should be able to report integrity errors

18) [PM-1296] - update database schema with tag table

19) [PM-1302] - pegasus-transfer should expose option for verify
    	        source existent for symlinks 

20) [PM-1303] - update jupiter tc api for container mount keyword

21) [PM-1304] - pegasus-transfer should support docker->singularity
    	        pulls

22) [PM-1305] - Integrity checking breaks for symlinks in containers
    	        with different mount points 

23) [PM-1306] - dials for integrity checking

24) [PM-1311] - Update DAX generators to generate workflow metadata
    	      	key dax.api 

25) [PM-1312] - track the dax_api key in the normalized database
    	        schema for metrics server 


#### Bugs Fixed


1)  [PM-1219] - Fix Foreign key constraint in workflow_files table

2)  [PM-1221] - source tar balls have .git files

3)  [PM-1222] - condor dagman does not allow . in job names

4)  [PM-1223] - Python DAX API available via PIP

5)  [PM-1224] - sub workflow planning pegasus lite prescript does not
    	        associate credentials 

6)  [PM-1225] - hierarchal workflows planning in sharedfs fails with
    	      	worker package staging set 

7)  [PM-1226] - hierarchal workflow with worker package staging fails

8)  [PM-1234] - bucket creation for S3 as staging site fails 

9)  [PM-1245] - Blank space in remote_environment variable generates
    	        blank export command 

10) [PM-1249] - Build fails against newer PostgreSQL

11) [PM-1253] - Planner should complain for same file designated as
    	      	input and output 

12) [PM-1255] - Singularity 2.4.(2?) pull cli has changed

13) [PM-1256] - rc-client does not strip quotes from PFN while
    	      	populating

14) [PM-1261] - PMC .in files are not generated into the 00/00 pattern
    	        folder.

15) [PM-1263] - Invalid raise statement in Python

16) [PM-1266] - Jupyter API does not only plans the workflow without
    	      	submitting it 

17) [PM-1275] - kickstart build fails on arm64

18) [PM-1276] - unbounded recursion in database loader for dashboard
    	      	in monitord 

19) [PM-1281] - pegasus-analyzer does not show task stdout/stderr for
     	       held jobs 

20) [PM-1282] - pegasus.runtime doesn't affect walltime

21) [PM-1287] - python3 added as a rpm dep, even though it is not a
    	        true dep 

22) [PM-1290] - Queue is not handled correctly in Glite

23) [PM-1291] - DB integrity errors in 4.9.0dev

24) [PM-1292] - incomplete kickstart output

25) [PM-1293] - stage-in jobs always have generate_checksum set to
    	        true if checksums are not present in RC 

26) [PM-1297] - monitord does not prescript log in case of planning
    	      	failure for sub workflows 

27) [PM-1300] - Planning error when image is of type singularity and
    	        source is docker hub 

28) [PM-1301] - Python exception raised when using MySQL for stampede
    	      	DB

29) [PM-1307] - some of the newly generated events are missing
    	      	timestamp 

30) [PM-1314] - incorrect way of computing suffix for singularity
    	      	images

31) [PM-1315] - Exception when data reuse removes all jobs.

32) [PM-1316] - Transformation that uses a regular file, but defines
    	      	it in RC raises an exception, unlike in older versions 

33) [PM-1317] - Pegasus plan fails on container task clustering

