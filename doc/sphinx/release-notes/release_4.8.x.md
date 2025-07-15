## Pegasus 4.8.x Series

### Pegasus 4.8.5

**Release Date:** October 31, 2018

We are happy to announce the release of Pegasus 4.8.5 . Pegasus 4.8.5
is a minor bug fix release and the last release in the 4.8 series.

#### Improvements


1) [PM-1248] – Planning fails in shared-fs mode with cryptic message. [\#1362](https://github.com/pegasus-isi/pegasus/issues/1362)

#### Bugs Fixed


1) [PM-1314] – incorrect way of computing suffix for singularity [\#1428](https://github.com/pegasus-isi/pegasus/issues/1428)
   	       images

2) [PM-1315] – Exception when data reuse removes all jobs. [\#1429](https://github.com/pegasus-isi/pegasus/issues/1429)

3) [PM-1316] – Transformation that uses a regular file, but defines it [\#1430](https://github.com/pegasus-isi/pegasus/issues/1430)
   	       in RC raises an exception, unlike in older versions

4) [PM-1317] – Pegasus plan fails on container task clustering [\#1431](https://github.com/pegasus-isi/pegasus/issues/1431)

### Pegasus 4.8.4

**Release Date:** August 31, 2018

We are happy to announce the release of Pegasus 4.8.4 . Pegasus 4.8.4
is a minor bug fix release.

#### Improvements

1) [PM-1294] – update flask dependency [\#1408](https://github.com/pegasus-isi/pegasus/issues/1408)

2) [PM-1277] – pegasus workflows on OSG should appear in OSG gratia [\#1391](https://github.com/pegasus-isi/pegasus/issues/1391)

3) [PM-1288] – pegasus.project profile key is not set for SLURM [\#1402](https://github.com/pegasus-isi/pegasus/issues/1402)
submissions

#### Bugs Fixed


1) [PM-1282] – pegasus.runtime doesn’t affect walltime [\#1396](https://github.com/pegasus-isi/pegasus/issues/1396)

2) [PM-1287] – python3 added as a rpm dep, even though it is not a [\#1401](https://github.com/pegasus-isi/pegasus/issues/1401)
   	       true depedency

3) [PM-1290] – Queue is not handled correctly in Glite [\#1404](https://github.com/pegasus-isi/pegasus/issues/1404)

4) [PM-1297] – monitord does not prescript log in case of planning [\#1411](https://github.com/pegasus-isi/pegasus/issues/1411)
   	       failure for sub workflows

### Pegasus 4.8.3

**Release Date:** August 6, 2018

We are happy to announce the release of Pegasus 4.8.3 . Pegasus 4.8.3
is a minor bug fix release

#### New Features

1) [PM-1280] – incorporate container based example in pegasus-init [\#1394](https://github.com/pegasus-isi/pegasus/issues/1394)
   	        pegasus-init was updated to include a population
   	        modeling example using containers. Updated tutorial
   	        instructions using that example can be found at
   	        http://pegasus.isi.edu/tutorial/isi/index.php

2)   [PM-1283] – tutorial vm should be updated to be able to run the [\#1397](https://github.com/pegasus-isi/pegasus/issues/1397)
     	         containers tutorial

#### Improvements

1) [PM-1285] – Exporting environmental variables in containers with the [\#1399](https://github.com/pegasus-isi/pegasus/issues/1399)
same technique

#### Bugs Fixed

1) [PM-1275] – kickstart build fails on arm64 [\#1389](https://github.com/pegasus-isi/pegasus/issues/1389)

2) [PM-1276] – unbounded recursion in database loader for dashboard in [\#1390](https://github.com/pegasus-isi/pegasus/issues/1390)
   	       monitord

3) [PM-1281] – pegasus-analyzer does not show task stdout/stderr for [\#1395](https://github.com/pegasus-isi/pegasus/issues/1395)
   	       held jobs

4) [PM-1200] – updated singularity mode to not mount /tmp and /var/tmp [\#1314](https://github.com/pegasus-isi/pegasus/issues/1314)
   	       as scratch.  Newer singularity versions do that
   	       automatically and complain if re-mounting

### Pegasus 4.8.2

**Release Date:** May 3, 2018

We are happy to announce the release of Pegasus 4.8.2 . Pegasus 4.8.2
is a minor bug fix release.

#### Improvements

1)  [PM-1244] – analyzer is not showing location of submit file [\#1358](https://github.com/pegasus-isi/pegasus/issues/1358)

2)  [PM-1262] – Condor DagMan no longer allows . in job names [\#1376](https://github.com/pegasus-isi/pegasus/issues/1376)

3)  [PM-1264] – update pegasus-init and usc tutorial to account for [\#1378](https://github.com/pegasus-isi/pegasus/issues/1378)
    	         the SLURM upgrade

#### Bugs Fixed

1) [PM-1245] – Blank space in remote_environment variable generates blank [\#1359](https://github.com/pegasus-isi/pegasus/issues/1359)
   	       export command

2) [PM-1249] – Build fails against newer PostgreSQL [\#1363](https://github.com/pegasus-isi/pegasus/issues/1363)

3) [PM-1253] – Planner should complain for same file designated as [\#1367](https://github.com/pegasus-isi/pegasus/issues/1367)
   	       input and output

4) [PM-1255] – Singularity 2.4.(2?) pull cli has changed [\#1369](https://github.com/pegasus-isi/pegasus/issues/1369)

5) [PM-1256] – rc-client does not strip quotes from PFN while [\#1370](https://github.com/pegasus-isi/pegasus/issues/1370)
   	       populating

6) [PM-1259] – unable to change encoding for events via properties [\#1373](https://github.com/pegasus-isi/pegasus/issues/1373)

7) [PM-1261] – PMC .in files are not generated into the 00/00 pattern [\#1375](https://github.com/pegasus-isi/pegasus/issues/1375)
   	       folder

8) [PM-1263] – Invalid raise statement in Python [\#1377](https://github.com/pegasus-isi/pegasus/issues/1377)

9) [PM-1266] – Jupyter API does not only plans the workflow without [\#1380](https://github.com/pegasus-isi/pegasus/issues/1380)
   	       submitting it

### Pegasus 4.8.1

**Release Date:** January 16, 2018

We are pleased to announce release of Pegasus 4.8.1

Pegasus 4.8.1 is be a minor bug fix release

#### Improvements

1) [PM-1233] – update pyopen ssl to 0.14 or higher [\#1347](https://github.com/pegasus-isi/pegasus/issues/1347)

#### Bugs Fixed

1) [PM-1221] - source tar balls have .git files [\#1335](https://github.com/pegasus-isi/pegasus/issues/1335)

2) [PM-1222] - condor dagman does not allow . in job names [\#1336](https://github.com/pegasus-isi/pegasus/issues/1336)

3) [PM-1224] - sub workflow planning pegasus lite prescript does not [\#1338](https://github.com/pegasus-isi/pegasus/issues/1338)
   	       associate credentials

4) [PM-1225] - hierarchal workflows planning in sharedfs fails with [\#1339](https://github.com/pegasus-isi/pegasus/issues/1339)
   	       worker package staging set

5) [PM-1226] - hierarchal workflow with worker package staging fails [\#1340](https://github.com/pegasus-isi/pegasus/issues/1340)

6) [PM-1230] – The pegasus python install fails on CentOS 7 [\#1344](https://github.com/pegasus-isi/pegasus/issues/1344)

### Pegasus 4.8.0

**Release Date:** September 5, 2017

We are pleased to announce release of Pegasus 4.8.0

Pegasus 4.8.0 is be a major release of Pegasus and includes
improvements and bug fixes to the 4.7.4 release.

Pegasus 4.8.0 Release has support for
 a) Application Containers - Pegasus now supports containers for user
    applications. Both Docker and Singularity are supported. More details
    can be found in the documentation
    at https://pegasus.isi.edu/docs/4.8.0/containers.php

 b) Jupyter Support -  Pegasus now provides a Python API to declare and
    manage workflows via Jupyter, which allows workflow creation,
    execution, and monitoring. The API also provides mechanisms to
    create Pegasus catalogs (sites, replica, and transformation). More
    details can be found in the documentation
    at https://pegasus.isi.edu/docs/4.8.0/jupyter.php

 c) Tuning of Transfer and Cleanup jobs - Pegasus now computes the
    number of transfer and cleanup jobs to be added for a workflow for
    a particular level, according to number of jobs on that
    level https://pegasus.isi.edu/docs/4.8.0/data_transfers.php

The following issues were addressed and more information can be found
in the Pegasus Jira 

#### New Features and Improvements

1)  [PM-1159] – Support for containers [\#1273](https://github.com/pegasus-isi/pegasus/issues/1273)

2)  [PM-1177] – API for running Pegasus workflows via Jupyter [\#1291](https://github.com/pegasus-isi/pegasus/issues/1291)

3)  [PM-1191] – If available, use GFAL over globus url copy [\#1305](https://github.com/pegasus-isi/pegasus/issues/1305)

    JGlobus is no longer actively supported and is not in compliance
    with RFC 2818
    (https://docs.globus.org/security-bulletins/2015-12-strict-mode). As
    a result cleanup jobs using pegasus-gridftp client would fail
    against the servers supporting the strict mode. We have removed the
    pegasus-gridftp client and now use gfal clients as globus-url-copy
    does not support removes. If gfal is not available,
    globus-url-copy is used for cleanup by writing out zero bytes
    files instead of removing them.

4)  [PM-1212] – new defaults for number of transfer and inplace jobs [\#1326](https://github.com/pegasus-isi/pegasus/issues/1326)
   	       created

5)  [PM-1134] – Capture the execution site information in pegasus lite [\#1248](https://github.com/pegasus-isi/pegasus/issues/1248)

6)  [PM-1109] – dashboard to display errors if a job is killed instead of [\#1223](https://github.com/pegasus-isi/pegasus/issues/1223)
    	        exiting with non zero exitcode

7)  [PM-1146] – There doesn’t seem to be a way to get a persistent URL [\#1260](https://github.com/pegasus-isi/pegasus/issues/1260)
    	        for a workflow in dashboard

8)  [PM-1155] – remote cleanup jobs should have file url’s if possible [\#1269](https://github.com/pegasus-isi/pegasus/issues/1269)

9)  [PM-1158] – Make DAX3 API compatible with Python 2.6+ and Python3+ [\#1272](https://github.com/pegasus-isi/pegasus/issues/1272)

10) [PM-1161] – Update documentation of large databases for handling [\#1275](https://github.com/pegasus-isi/pegasus/issues/1275)
    	      	mysqldump: Error 2013

11) [PM-1187] – make scheduler type case insensitive for grid gateway in [\#1301](https://github.com/pegasus-isi/pegasus/issues/1301)
    	        site catalog

12) [PM-1165] – Update Transformation Catalog format to support [\#1279](https://github.com/pegasus-isi/pegasus/issues/1279)
    	        containers

13) [PM-1166] – pegasus-transfer to support transfers from docker hub [\#1280](https://github.com/pegasus-isi/pegasus/issues/1280)

14) [PM-1180] – update monitord to populate checksums [\#1294](https://github.com/pegasus-isi/pegasus/issues/1294)

15) [PM-1183] – monitord plumbing for putting hooks for integrity [\#1297](https://github.com/pegasus-isi/pegasus/issues/1297)
    	       checking

16) [PM-1188] – Add tool to integrity check transferred files [\#1302](https://github.com/pegasus-isi/pegasus/issues/1302)

17) [PM-1190] – planner changes to enable integrity checking [\#1304](https://github.com/pegasus-isi/pegasus/issues/1304)

18) [PM-1194] – update planner pegasus lite mode to support for docker [\#1308](https://github.com/pegasus-isi/pegasus/issues/1308)
    	      	container wrapper

19) [PM-1195] – update site selection to handle containers [\#1309](https://github.com/pegasus-isi/pegasus/issues/1309)

20) [PM-1197] – handle symlinks for input files when launching job via [\#1311](https://github.com/pegasus-isi/pegasus/issues/1311)
    	        container

21) [PM-1200] – update pegasus lite mode to support singulartiy [\#1314](https://github.com/pegasus-isi/pegasus/issues/1314)

22) [PM-1201] – Transformation Catalog API should support the container [\#1315](https://github.com/pegasus-isi/pegasus/issues/1315)
    	      	keywords

23) [PM-1202] – Move catalog APIs into Pegasus.catalogs and develop [\#1316](https://github.com/pegasus-isi/pegasus/issues/1316)
    	       standalone test cases independent from Jupyter

24) [PM-1210] – update pegasus-transfer to support transfers from [\#1324](https://github.com/pegasus-isi/pegasus/issues/1324)
    	       singularity hub

25) [PM-1214] – Specifying environment for sites and containers [\#1328](https://github.com/pegasus-isi/pegasus/issues/1328)

26) [PM-1215] – Document support for containers for 4.8 [\#1329](https://github.com/pegasus-isi/pegasus/issues/1329)

27) [PM-1178] – kickstart to checksum output files [\#1292](https://github.com/pegasus-isi/pegasus/issues/1292)

28) [PM-1220] – default app name for metrics server based on dax label [\#1334](https://github.com/pegasus-isi/pegasus/issues/1334)

29) The documentation also lists on how to setup pyglidein ( a resouce
    provisioner framwork) from the IceCube to provision resources for your
    workflow. More details can be found at
    https://pegasus.isi.edu/docs/4.8.0/pyglidein.php

#### Bugs Fixed


1)  [PM-1032] – Handle proper error message for non-standard python [\#1146](https://github.com/pegasus-isi/pegasus/issues/1146)
    	      	usage

2)  [PM-1162] – Running pegasus-monitord replay created an unreadable [\#1276](https://github.com/pegasus-isi/pegasus/issues/1276)
    	        database

3)  [PM-1171] – Monitord regularly produces empty stderr and stdout [\#1285](https://github.com/pegasus-isi/pegasus/issues/1285)
    	       files

4)  [PM-1172] – pegasus-rc-client deletes all entries for a lfn [\#1286](https://github.com/pegasus-isi/pegasus/issues/1286)

5)  [PM-1173] – cleanup jobs failing against Titan gridftp server due to [\#1287](https://github.com/pegasus-isi/pegasus/issues/1287)
    	      	RFC 2818 compliance

6)  [PM-1174] – monitord should maintain the permissions on [\#1288](https://github.com/pegasus-isi/pegasus/issues/1288)
    	      	~/.pegasus/workflow.db

7)  [PM-1176] – the job notifications on failure and success should [\#1290](https://github.com/pegasus-isi/pegasus/issues/1290)
    	      	have exitcode from kickstart file

8)  [PM-1181] – monitord fails to exit if database is locked [\#1295](https://github.com/pegasus-isi/pegasus/issues/1295)

9)  [PM-1185] – destination in remote file transfers for inter site [\#1299](https://github.com/pegasus-isi/pegasus/issues/1299)
    	       jobs point’s to directory

10) [PM-1189] – Making X86_64 the default arch in the site catalog [\#1303](https://github.com/pegasus-isi/pegasus/issues/1303)

11) [PM-1193] – “pegasus-rc-client list” modifies rc.txt [\#1307](https://github.com/pegasus-isi/pegasus/issues/1307)

12) [PM-1196] – pegasus-statistics is not generating jobs.txt for some [\#1310](https://github.com/pegasus-isi/pegasus/issues/1310)
    	       large workflows

13) [PM-1207] – Investigate error message: Normalizing ‘4.8.0dev’ to [\#1321](https://github.com/pegasus-isi/pegasus/issues/1321)
    	        ‘4.8.0.dev0’

14) [PM-1208] – Improve database is locked error message [\#1322](https://github.com/pegasus-isi/pegasus/issues/1322)

15) [PM-1209] – Analyzer gets confused about retry number in hierarchical [\#1323](https://github.com/pegasus-isi/pegasus/issues/1323)
    	      	workflows

16) [PM-1211] – DAX API should tell which lfn was a dup [\#1325](https://github.com/pegasus-isi/pegasus/issues/1325)

17) [PM-1213] – pegasus creates duplicate source URL’s for staged [\#1327](https://github.com/pegasus-isi/pegasus/issues/1327)
    	       executables

18) [PM-1217] – monitord exits prematurely, when in dagman recovery [\#1331](https://github.com/pegasus-isi/pegasus/issues/1331)
    	        mode

19) [PM-1218] – monitord replay against mysql with registration jobs [\#1332](https://github.com/pegasus-isi/pegasus/issues/1332)

