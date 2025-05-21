## Pegasus 4.9.x Series

### Pegasus 4.9.3

**Release Date:**  January 31, 2020


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


1) [PM-1398] - include machine information in job_instance.composite event [\#1512](https://github.com/pegasus-isi/pegasus/issues/1512)

2) [PM-1402] - pegasus-init to support summit as execution env for tutorial [\#1516](https://github.com/pegasus-isi/pegasus/issues/1516)

3) [PM-1397] - Support ftp to file transfers in pegasus-transfer [\#1511](https://github.com/pegasus-isi/pegasus/issues/1511)

4) [PM-1416] - add data collection setup instructions to docs under [\#1530](https://github.com/pegasus-isi/pegasus/issues/1530)
   	       section 6.7.1.1. Monitord, RabbitMQ, ElasticSearch Example

5) [PM-1403] - Support POWER9 nodes in PMC [\#1517](https://github.com/pegasus-isi/pegasus/issues/1517)

6) [PM-1404] - pegasus tutorial for summit from Kubernetes [\#1518](https://github.com/pegasus-isi/pegasus/issues/1518)

#### Bugs Fixed


1) [PM-1346] - Pegasus job checkpointing is incompatible with condorio [\#1460](https://github.com/pegasus-isi/pegasus/issues/1460)

2) [PM-1380] - Support for Singularity Library [\#1494](https://github.com/pegasus-isi/pegasus/issues/1494)

3) [PM-1388] - PegasusLite cannot locate osname and version SLES 15 [\#1502](https://github.com/pegasus-isi/pegasus/issues/1502)

4) [PM-1389] - pegasus.cores causes issues on Summit [\#1503](https://github.com/pegasus-isi/pegasus/issues/1503)

5) [PM-1395] - GLite LSF scripts don't work as intended on OLCF's DTNs [\#1509](https://github.com/pegasus-isi/pegasus/issues/1509)

6) [PM-1405] - Is Pegasus supposed to build on 32-bit x86 (Debian i386 Stretch)? [\#1519](https://github.com/pegasus-isi/pegasus/issues/1519)


### Pegasus 4.9.2

**Release Date:** August 7, 2019


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


1)  [PM-1355] – composite records when sending events to AMQP [\#1469](https://github.com/pegasus-isi/pegasus/issues/1469)

2)  [PM-1357] – In lite jobs, chirp durations for stage in, stage out [\#1471](https://github.com/pegasus-isi/pegasus/issues/1471)
    	         of data

3)  [PM-1367] – Support for retrieval from HPSS tape store using [\#1481](https://github.com/pegasus-isi/pegasus/issues/1481)
   	       commands htar and his

4)  [PM-1376] – Add LSF local attributes [\#1490](https://github.com/pegasus-isi/pegasus/issues/1490)

5)  [PM-1378] – Handle (copy) HPSS credentials when an environment [\#1492](https://github.com/pegasus-isi/pegasus/issues/1492)
   	       variable is set

6)  [PM-1337] – pegasus should run monitord-replay at the end of a [\#1451](https://github.com/pegasus-isi/pegasus/issues/1451)
   	      workflow

7)  [PM-1356] – Replace Google CDN with a different CDN as China block [\#1470](https://github.com/pegasus-isi/pegasus/issues/1470)
    	      	it

8)  [PM-1363] – Condor Configuration MOUNT_UNDER_SCRATCH causes [\#1477](https://github.com/pegasus-isi/pegasus/issues/1477)
    	      	pegasus auxiliary jobs to fail

9)  [PM-1373] – Debian Buster no longer provides openjdk-8-jdk [\#1487](https://github.com/pegasus-isi/pegasus/issues/1487)

10) [PM-1374] – make monitord resilient to dagman logging the debug [\#1488](https://github.com/pegasus-isi/pegasus/issues/1488)
    	      	level in dagman.out


11) [PM-1375] – Do not run integrity checks on symlinked files [\#1489](https://github.com/pegasus-isi/pegasus/issues/1489)

12) [PM-1365] – remove __ from event keys wherever possible [\#1479](https://github.com/pegasus-isi/pegasus/issues/1479)

13) [PM-1381] – Associated planner changes to handle LSF sites [\#1495](https://github.com/pegasus-isi/pegasus/issues/1495)

14) [PM-1387] – make the netlogger events consistent with the [\#1501](https://github.com/pegasus-isi/pegasus/issues/1501)
    	        documentation


#### Bugs Fixed


1) [PM-1358] – HTCondor 8.8.0/8.8.1 remaps /tmp, and can break access [\#1472](https://github.com/pegasus-isi/pegasus/issues/1472)
   	       to x509 credentials

2) [PM-1360] – planner drops transfer_(in|out)put_files if NoGridStart [\#1474](https://github.com/pegasus-isi/pegasus/issues/1474)
   	       is used

3) [PM-1364] – Container Name Collision [\#1478](https://github.com/pegasus-isi/pegasus/issues/1478)

4) [PM-1366] – Pegasus Cluster Label – Job Env Not Picked Up in [\#1480](https://github.com/pegasus-isi/pegasus/issues/1480)
   	       Containers

5) [PM-1369] – Chirp related changes causing jobs to get held. [\#1483](https://github.com/pegasus-isi/pegasus/issues/1483)

6) [PM-1370] – submit error [\#1484](https://github.com/pegasus-isi/pegasus/issues/1484)

7) [PM-1377] – A + in a tc name breaks pegasus-plan [\#1491](https://github.com/pegasus-isi/pegasus/issues/1491)

8) [PM-1379] – Stage out job fails – wrong src location [\#1493](https://github.com/pegasus-isi/pegasus/issues/1493)

9) [PM-1384] – .sig Singularity images (naming issue?) [\#1498](https://github.com/pegasus-isi/pegasus/issues/1498)

### Pegasus 4.9.1

**Release Date:** March 6, 2019

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

1)  [PM-1339] – construct a default entry for local site if not [\#1453](https://github.com/pegasus-isi/pegasus/issues/1453)
    	        present in site catalog

2)  [PM-1345] – Support for Shifter at Nersc [\#1459](https://github.com/pegasus-isi/pegasus/issues/1459)

3)  [PM-1354] - pegasus-init to support titan tutorial [\#1468](https://github.com/pegasus-isi/pegasus/issues/1468)

4)  [PM-1352] – Build failure on Debian 10 due to mariadb/MySQL-Python [\#1466](https://github.com/pegasus-isi/pegasus/issues/1466)
   	        incompatibility

5)  [PM-1216] – pegasus-transfer should follow redirects to file urls [\#1330](https://github.com/pegasus-isi/pegasus/issues/1330)

6)  [PM-1321] – Move transfer staging into the container rather than [\#1435](https://github.com/pegasus-isi/pegasus/issues/1435)
   	        the host OS

7)  [PM-1323] – pegasus transfer should not try and transfer a file [\#1437](https://github.com/pegasus-isi/pegasus/issues/1437)
        	that does not exist

8)  [PM-1329] – pegasus integrity causes LIGO workflows to fail [\#1443](https://github.com/pegasus-isi/pegasus/issues/1443)

9)  [PM-1338] – Add support for TACC wrangler to pegasus-init [\#1452](https://github.com/pegasus-isi/pegasus/issues/1452)

10)  [PM-1328] – support sharedfs on the compute site as staging site [\#1442](https://github.com/pegasus-isi/pegasus/issues/1442)

11) [PM-1340] – make planner os releases consistent with builds [\#1454](https://github.com/pegasus-isi/pegasus/issues/1454)

#### Bugs Fixed

1)  [PM-1182] – registration jobs fail if a file based RC has variables [\#1296](https://github.com/pegasus-isi/pegasus/issues/1296)
   	        defined

2)  [PM-1320] – pegasus-plan doesn’t plan with container in sharedfs [\#1434](https://github.com/pegasus-isi/pegasus/issues/1434)
               mode

3)  [PM-1322] – DB Admin failing when run within planner [\#1436](https://github.com/pegasus-isi/pegasus/issues/1436)

4)  [PM-1325] – Debian build with incorrect dependencies [\#1439](https://github.com/pegasus-isi/pegasus/issues/1439)

5)  [PM-1326] – singularity suffix computed incorrectly [\#1440](https://github.com/pegasus-isi/pegasus/issues/1440)

6)  [PM-1327] – bypass input file staging broken for container [\#1441](https://github.com/pegasus-isi/pegasus/issues/1441)
               execution

7)  [PM-1330] – .meta files created even when integrity checking is [\#1444](https://github.com/pegasus-isi/pegasus/issues/1444)
   	       disabled.

8)  [PM-1332] – monitord is failing on a dagman.out file [\#1446](https://github.com/pegasus-isi/pegasus/issues/1446)

9)  [PM-1333] – amqp endpoint errors should not disable database [\#1447](https://github.com/pegasus-isi/pegasus/issues/1447)
    	        population for multiplexed sinks

10) [PM-1334] – pegasus dagman is not exiting cleanly [\#1448](https://github.com/pegasus-isi/pegasus/issues/1448)

11) [PM-1336] – pegasus-submitdir is broken [\#1450](https://github.com/pegasus-isi/pegasus/issues/1450)

12) [PM-1350] – pegasus is ignoring when_to_transfer_output [\#1464](https://github.com/pegasus-isi/pegasus/issues/1464)


###  Pegasus 4.9.0

**Release Date:** October 31, 2018

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


1)  [PM-1179] - Integrity checking in Pegasus [\#1293](https://github.com/pegasus-isi/pegasus/issues/1293)

2)  [PM-1228] - Integrate Pegasus with AWS Batch [\#1342](https://github.com/pegasus-isi/pegasus/issues/1342)

3)  [PM-1167] - Dashboard should explain why condor submission failed [\#1281](https://github.com/pegasus-isi/pegasus/issues/1281)

4)  [PM-1233] - update pyopen ssl to 0.14 or higher [\#1347](https://github.com/pegasus-isi/pegasus/issues/1347)

5)  [PM-1265] - monitord to send events from job stdout [\#1379](https://github.com/pegasus-isi/pegasus/issues/1379)

6)  [PM-1277] - pegasus workflows on OSG should appear in OSG gratia [\#1391](https://github.com/pegasus-isi/pegasus/issues/1391)

7)  [PM-1280] - incorporate container based example in pegasus-init [\#1394](https://github.com/pegasus-isi/pegasus/issues/1394)

8)  [PM-1283] - tutorial vm should be updated to be able to run the [\#1397](https://github.com/pegasus-isi/pegasus/issues/1397)
   	       containers tutorial

9)  [PM-1289] - ability to filter events sent to AMQP endpoint. [\#1403](https://github.com/pegasus-isi/pegasus/issues/1403)

10) [PM-1298] - symlinking when running with containers [\#1412](https://github.com/pegasus-isi/pegasus/issues/1412)

13) [PM-1310] - integrity checking documentation [\#1424](https://github.com/pegasus-isi/pegasus/issues/1424)

#### Improvements


1)  [PM-1288] - pegasus.project profile key is not set for SLURM [\#1402](https://github.com/pegasus-isi/pegasus/issues/1402)
   	       submissions

2)  [PM-898] - Modify monitord so that it can send AMQP messages AND [\#1016](https://github.com/pegasus-isi/pegasus/issues/1016)
   	      populate a DB.

3)  [PM-1243] - Clusters of size 1 should be allowed when using AWS [\#1357](https://github.com/pegasus-isi/pegasus/issues/1357)
	      Batch and Horizontal Clustering

4)  [PM-1244] - analyzer is not showing location of submit file [\#1358](https://github.com/pegasus-isi/pegasus/issues/1358)

5)  [PM-1248] - Planning fails in shared-fs mode with cryptic message. [\#1362](https://github.com/pegasus-isi/pegasus/issues/1362)

6)  [PM-1258] - Update AMQP support for Panorama [\#1372](https://github.com/pegasus-isi/pegasus/issues/1372)

7)  [PM-1270] - transformation selectors should preferred the site [\#1384](https://github.com/pegasus-isi/pegasus/issues/1384)
   	       assigned by site selector

8)  [PM-1285] - Exporting environmental variables in containers with [\#1399](https://github.com/pegasus-isi/pegasus/issues/1399)
   	       the same technique.

9)  [PM-1294] - update flask dependency [\#1408](https://github.com/pegasus-isi/pegasus/issues/1408)

10) [PM-1299] - Consider capturing which API was used to generate the [\#1413](https://github.com/pegasus-isi/pegasus/issues/1413)
    	        DAX in metrics.
11) [PM-1308] - changes to database connection code [\#1422](https://github.com/pegasus-isi/pegasus/issues/1422)

12) [PM-1313] - allow for direct use of singularity containers on [\#1427](https://github.com/pegasus-isi/pegasus/issues/1427)
    	      	CVMFS on compute site
#### Tasks


1)  [PM-1284] - pegasus-mpi-cluster's test PM848 fails on Debian 10 [\#1398](https://github.com/pegasus-isi/pegasus/issues/1398)

2)  [PM-901] - site for OSG [\#1019](https://github.com/pegasus-isi/pegasus/issues/1019)

3)  [PM-1200] - update pegasus lite mode to support singulartiy [\#1314](https://github.com/pegasus-isi/pegasus/issues/1314)

4)  [PM-1231] - support pegasus-aws-batch as clustering executable [\#1345](https://github.com/pegasus-isi/pegasus/issues/1345)

5)  [PM-1246] - add manpage for pegasus-aws-batch [\#1360](https://github.com/pegasus-isi/pegasus/issues/1360)

6)  [PM-1250] - update handling for raw input files for integrity [\#1364](https://github.com/pegasus-isi/pegasus/issues/1364)
    	        checking

7)  [PM-1251] - pegasus-transfer to checksum files [\#1365](https://github.com/pegasus-isi/pegasus/issues/1365)

8)  [PM-1252] - enforce integrity checking for stage-out jobs [\#1366](https://github.com/pegasus-isi/pegasus/issues/1366)

9)  [PM-1254] - handling of checkpoint files for integrity checking [\#1368](https://github.com/pegasus-isi/pegasus/issues/1368)

10) [PM-1257] - propagate checksums in hierarchal workflows [\#1371](https://github.com/pegasus-isi/pegasus/issues/1371)

11) [PM-1260] - Integrity data in Stampede / statistics [\#1374](https://github.com/pegasus-isi/pegasus/issues/1374)

12) [PM-1271] - update database schema and pegasus-db-admin to include [\#1385](https://github.com/pegasus-isi/pegasus/issues/1385)
    	        integrity meta table

13) [PM-1272] - pegasus-integrity-check to log integrity data in [\#1386](https://github.com/pegasus-isi/pegasus/issues/1386)
    	        monitord parseable event

14) [PM-1273] - pegasus-integrity-check to check for multiple files [\#1387](https://github.com/pegasus-isi/pegasus/issues/1387)

15) [PM-1274] - update monitord to populate integrity events [\#1388](https://github.com/pegasus-isi/pegasus/issues/1388)

16) [PM-1279] - Show integrity metrics in dashboard statistics page [\#1393](https://github.com/pegasus-isi/pegasus/issues/1393)

17) [PM-1295] - pegasus should be able to report integrity errors [\#1409](https://github.com/pegasus-isi/pegasus/issues/1409)

18) [PM-1296] - update database schema with tag table [\#1410](https://github.com/pegasus-isi/pegasus/issues/1410)

19) [PM-1302] - pegasus-transfer should expose option for verify [\#1416](https://github.com/pegasus-isi/pegasus/issues/1416)
    	        source existent for symlinks

20) [PM-1303] - update jupiter tc api for container mount keyword [\#1417](https://github.com/pegasus-isi/pegasus/issues/1417)

21) [PM-1304] - pegasus-transfer should support docker->singularity [\#1418](https://github.com/pegasus-isi/pegasus/issues/1418)
    	        pulls

22) [PM-1305] - Integrity checking breaks for symlinks in containers [\#1419](https://github.com/pegasus-isi/pegasus/issues/1419)
    	        with different mount points

23) [PM-1306] - dials for integrity checking [\#1420](https://github.com/pegasus-isi/pegasus/issues/1420)

24) [PM-1311] - Update DAX generators to generate workflow metadata [\#1425](https://github.com/pegasus-isi/pegasus/issues/1425)
    	      	key dax.api

25) [PM-1312] - track the dax_api key in the normalized database [\#1426](https://github.com/pegasus-isi/pegasus/issues/1426)
    	        schema for metrics server


#### Bugs Fixed


1)  [PM-1219] - Fix Foreign key constraint in workflow_files table [\#1333](https://github.com/pegasus-isi/pegasus/issues/1333)

2)  [PM-1221] - source tar balls have .git files [\#1335](https://github.com/pegasus-isi/pegasus/issues/1335)

3)  [PM-1222] - condor dagman does not allow . in job names [\#1336](https://github.com/pegasus-isi/pegasus/issues/1336)

4)  [PM-1223] - Python DAX API available via PIP [\#1337](https://github.com/pegasus-isi/pegasus/issues/1337)

5)  [PM-1224] - sub workflow planning pegasus lite prescript does not [\#1338](https://github.com/pegasus-isi/pegasus/issues/1338)
    	        associate credentials

6)  [PM-1225] - hierarchal workflows planning in sharedfs fails with [\#1339](https://github.com/pegasus-isi/pegasus/issues/1339)
    	      	worker package staging set

7)  [PM-1226] - hierarchal workflow with worker package staging fails [\#1340](https://github.com/pegasus-isi/pegasus/issues/1340)

8)  [PM-1234] - bucket creation for S3 as staging site fails [\#1348](https://github.com/pegasus-isi/pegasus/issues/1348)

9)  [PM-1245] - Blank space in remote_environment variable generates [\#1359](https://github.com/pegasus-isi/pegasus/issues/1359)
    	        blank export command

10) [PM-1249] - Build fails against newer PostgreSQL [\#1363](https://github.com/pegasus-isi/pegasus/issues/1363)

11) [PM-1253] - Planner should complain for same file designated as [\#1367](https://github.com/pegasus-isi/pegasus/issues/1367)
    	      	input and output

12) [PM-1255] - Singularity 2.4.(2?) pull cli has changed [\#1369](https://github.com/pegasus-isi/pegasus/issues/1369)

13) [PM-1256] - rc-client does not strip quotes from PFN while [\#1370](https://github.com/pegasus-isi/pegasus/issues/1370)
    	      	populating

14) [PM-1261] - PMC .in files are not generated into the 00/00 pattern [\#1375](https://github.com/pegasus-isi/pegasus/issues/1375)
    	        folder.

15) [PM-1263] - Invalid raise statement in Python [\#1377](https://github.com/pegasus-isi/pegasus/issues/1377)

16) [PM-1266] - Jupyter API does not only plans the workflow without [\#1380](https://github.com/pegasus-isi/pegasus/issues/1380)
    	      	submitting it

17) [PM-1275] - kickstart build fails on arm64 [\#1389](https://github.com/pegasus-isi/pegasus/issues/1389)

18) [PM-1276] - unbounded recursion in database loader for dashboard [\#1390](https://github.com/pegasus-isi/pegasus/issues/1390)
    	      	in monitord

19) [PM-1281] - pegasus-analyzer does not show task stdout/stderr for [\#1395](https://github.com/pegasus-isi/pegasus/issues/1395)
     	       held jobs

20) [PM-1282] - pegasus.runtime doesn't affect walltime [\#1396](https://github.com/pegasus-isi/pegasus/issues/1396)

21) [PM-1287] - python3 added as a rpm dep, even though it is not a [\#1401](https://github.com/pegasus-isi/pegasus/issues/1401)
    	        true dep

22) [PM-1290] - Queue is not handled correctly in Glite [\#1404](https://github.com/pegasus-isi/pegasus/issues/1404)

23) [PM-1291] - DB integrity errors in 4.9.0dev [\#1405](https://github.com/pegasus-isi/pegasus/issues/1405)

24) [PM-1292] - incomplete kickstart output [\#1406](https://github.com/pegasus-isi/pegasus/issues/1406)

25) [PM-1293] - stage-in jobs always have generate_checksum set to [\#1407](https://github.com/pegasus-isi/pegasus/issues/1407)
    	        true if checksums are not present in RC

26) [PM-1297] - monitord does not prescript log in case of planning [\#1411](https://github.com/pegasus-isi/pegasus/issues/1411)
    	      	failure for sub workflows

27) [PM-1300] - Planning error when image is of type singularity and [\#1414](https://github.com/pegasus-isi/pegasus/issues/1414)
    	        source is docker hub

28) [PM-1301] - Python exception raised when using MySQL for stampede [\#1415](https://github.com/pegasus-isi/pegasus/issues/1415)
    	      	DB

29) [PM-1307] - some of the newly generated events are missing [\#1421](https://github.com/pegasus-isi/pegasus/issues/1421)
    	      	timestamp

30) [PM-1314] - incorrect way of computing suffix for singularity [\#1428](https://github.com/pegasus-isi/pegasus/issues/1428)
    	      	images

31) [PM-1315] - Exception when data reuse removes all jobs. [\#1429](https://github.com/pegasus-isi/pegasus/issues/1429)

32) [PM-1316] - Transformation that uses a regular file, but defines [\#1430](https://github.com/pegasus-isi/pegasus/issues/1430)
    	      	it in RC raises an exception, unlike in older versions

33) [PM-1317] - Pegasus plan fails on container task clustering [\#1431](https://github.com/pegasus-isi/pegasus/issues/1431)

