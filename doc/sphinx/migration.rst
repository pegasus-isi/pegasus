.. _useful-tips:

===============
Migration Notes
===============

.. _migrating-from-lt47:

Migrating From Pegasus 4.5.X to Pegasus current version
=======================================================

Most of the migrations from one version to another are related to
database upgrades, that is addressed by running the tool
**pegasus-db-admin**.

Database Upgrades From Pegasus 4.5.X to Pegasus current version
---------------------------------------------------------------

Since Pegasus 4.5 all databases are managed by a single tool:
**pegasus-db-admin**. Databases will be automatically updated when
**pegasus-plan** is invoked, but WORKFLOW databases from past runs may
not be updated accordingly. Since Pegasus 4.6.0, the
**pegasus-db-admin** tool provides an option to automatically update all
databases from completed workflows in the MASTER database. To enable
this option, run the following command:

::

   $ pegasus-db-admin update -a
   Your database has been updated.
   Your database is compatible with Pegasus version: 4.7.0

   Verifying and updating workflow databases:
   21/21

   Summary:
   Verified/Updated: 21/21
   Failed: 0/21
   Unable to connect: 0/21
   Unable to update (active workflows): 0/21

   Log files:
   20161006T134415-dbadmin.out (Succeeded operations)
   20161006T134415-dbadmin.err (Failed operations)


This option generates a log file for succeeded operations, and a log
file for failed operations. Each file contains the list of URLs of the
succeeded/failed databases.

Note that, if no URL is provided, the tool will create/use a SQLite
database in the user's home directory: *${HOME}/.pegasus/workflow.db*.

For complete description of pegasus-db-admin, see the `man
page <#cli-pegasus-db-admin>`__.

Migration from Pegasus 4.6 to 4.7
---------------------------------

In addition to the database changes, in Pegasus 4.7 the default submit
directory layout was changed from a flat structure where all submit
files independent of the number of jobs in the workflow appeared in a
single directory. For 4.7, the default is a hierarchal directory
structure two levels deep. To use the earlier layout, set the following
property

::

   pegasus.dir.submit.mapper     Flat

.. _migrating-from-leq44:

Migrating From Pegasus <4.5 to Pegasus 4.5.X
============================================

Since Pegasus 4.5 all databases are managed by a single tool:
**pegasus-db-admin**. Databases will be automatically updated when
**pegasus-plan** is invoked, but it may require manually invocation of
the **pegasus-db-admin** for other Pegasus tools.

The **check** command verifies if the database is compatible with the
Pegasus' latest version. If the database is not compatible, it will
print the following message:

::

   $ pegasus-db-admin check
   Your database is NOT compatible with version 4.5.0


If you are running the **check** command for the first time, the tool
will prompt the following message:

::

   Missing database tables or tables are not updated:
       dbversion
   Run 'pegasus-db-admin update <path_to_database>' to create/update your database.


To update the database, run the following command:

::

   $ pegasus-db-admin update
   Your database has been updated.
   Your database is compatible with Pegasus version: 4.5.0


The **pegasus-db-admin** tool can operate directly over a database URL,
or can read configuration parameters from the properties file or a
submit directory. In the later case, a database type should be provided
to indicate which properties should be used to connect to the database.
For example, the tool will seek for *pegasus.catalog.replica.db.\**
properties to connect to the JDBCRC database; or seek for
*pegasus.catalog.master.url* (or *pegasus.dashboard.output*, which is
deprecated) property to connect to the MASTER database; or seek for the
*pegasus.catalog.workflow.url* (or *pegasus.monitord.output*, which is
deprecated) property to connect to the WORKFLOW database. If none of
these properties are found, the tool will connect to the default
database in the user's home directory
(sqlite:///${HOME}/.pegasus/workflow.db).

Example: connection by providing the URL to the database:

::

   $ pegasus-db-admin create sqlite:///${HOME}/.pegasus/workflow.db
   $ pegasus-db-admin update sqlite:///${HOME}/.pegasus/workflow.db


Example: connection by providing a properties file that contains the
information to connect to the database. Note that a database type
(MASTER, WORKFLOW, or JDBCRC) should be provided:

::

   $ pegasus-db-admin update -c pegasus.properties -t MASTER
   $ pegasus-db-admin update -c pegasus.properties -t JDBCRC
   $ pegasus-db-admin update -c pegasus.properties -t WORKFLOW


Example: connection by providing the path to the submit directory
containning the *braindump.txt* file, where information to connect to
the database can be extracted. Note that a database type (MASTER,
WORKFLOW, or JDBCRC) should also be provided:

::

   $ pegasus-db-admin update -s /path/to/submitdir -t WORKFLOW
   $ pegasus-db-admin update -s /path/to/submitdir -t MASTER
   $ pegasus-db-admin update -s /path/to/submitdir -t JDBCRC


Note that, if no URL is provided, the tool will create/use a SQLite
database in the user's home directory: *${HOME}/.pegasus/workflow.db*.

For complete description of pegasus-db-admin, see the `man
page <#cli-pegasus-db-admin>`__.

.. _migrating-from-3x:

Migrating From Pegasus 3.1 to Pegasus 4.X
=========================================

With Pegasus 4.0 effort has been made to move the Pegasus installation
to be FHS compliant, and to make workflows run better in Cloud
environments and distributed grid environments. This chapter is for
existing users of Pegasus who use Pegasus 3.1 to run their workflows and
walks through the steps to move to using Pegasus 4.0

Move to FHS layout
------------------

Pegasus 4.0 is the first release of Pegasus which is `Filesystem
Hierarchy Standard (FHS) <http://www.pathname.com/fhs/>`__ compliant.
The native packages no longer installs under /opt. Instead, pegasus-\*
binaries are in /usr/bin/ and example workflows can be found under
/usr/share/pegasus/examples/.

To find Pegasus system components, a pegasus-config tool is provided.
pegasus-config supports setting up the environment for

-  Python

-  Perl

-  Java

-  Shell

For example, to find the PYTHONPATH for the DAX API, run:

::

   export PYTHONPATH=`pegasus-config --python`

For complete description of pegasus-config, see the `man
page <#cli-pegasus-config>`__.

Stampede Schema Upgrade Tool
----------------------------

Starting Pegasus 4.x the monitoring and statistics database schema has
changed. If you want to use the pegasus-statistics, pegasus-analyzer and
pegasus-plots against a 3.x database you will need to upgrade the schema
first using the schema upgrade tool
/usr/share/pegasus/sql/schema_tool.py or
/path/to/pegasus-4.x/share/pegasus/sql/schema_tool.py

Upgrading the schema is required for people using the MySQL database for
storing their monitoring information if it was setup with 3.x monitoring
tools.

If your setup uses the default SQLite database then the new databases
run with Pegasus 4.x are automatically created with the correct schema.
In this case you only need to upgrade the SQLite database from older
runs if you wish to query them with the newer clients.

To upgrade the database

::

   For SQLite Database

   cd /to/the/workflow/directory/with/3.x.monitord.db

   Check the db version

   /usr/share/pegasus/sql/schema_tool.py -c connString=sqlite:////to/the/workflow/directory/with/workflow.stampede.db
   2012-02-29T01:29:43.330476Z INFO   netlogger.analysis.schema.schema_check.SchemaCheck.init |
   2012-02-29T01:29:43.330708Z INFO   netlogger.analysis.schema.schema_check.SchemaCheck.check_schema.start |
   2012-02-29T01:29:43.348995Z INFO   netlogger.analysis.schema.schema_check.SchemaCheck.check_schema
                                      | Current version set to: 3.1.
   2012-02-29T01:29:43.349133Z ERROR  netlogger.analysis.schema.schema_check.SchemaCheck.check_schema
                                      | Schema version 3.1 found - expecting 4.0 - database admin will need to run upgrade tool.


   Convert the Database to be version 4.x compliant

   /usr/share/pegasus/sql/schema_tool.py -u connString=sqlite:////to/the/workflow/directory/with/workflow.stampede.db
   2012-02-29T01:35:35.046317Z INFO   netlogger.analysis.schema.schema_check.SchemaCheck.init |
   2012-02-29T01:35:35.046554Z INFO   netlogger.analysis.schema.schema_check.SchemaCheck.check_schema.start |
   2012-02-29T01:35:35.064762Z INFO   netlogger.analysis.schema.schema_check.SchemaCheck.check_schema
                                     | Current version set to: 3.1.
   2012-02-29T01:35:35.064902Z ERROR  netlogger.analysis.schema.schema_check.SchemaCheck.check_schema
                                     | Schema version 3.1 found - expecting 4.0 - database admin will need to run upgrade tool.
   2012-02-29T01:35:35.065001Z INFO   netlogger.analysis.schema.schema_check.SchemaCheck.upgrade_to_4_0
                                     | Upgrading to schema version 4.0.

   Verify if the database has been converted to Version 4.x

   /usr/share/pegasus/sql/schema_tool.py -c connString=sqlite:////to/the/workflow/directory/with/workflow.stampede.db
   2012-02-29T01:39:17.218902Z INFO   netlogger.analysis.schema.schema_check.SchemaCheck.init |
   2012-02-29T01:39:17.219141Z INFO   netlogger.analysis.schema.schema_check.SchemaCheck.check_schema.start |
   2012-02-29T01:39:17.237492Z INFO   netlogger.analysis.schema.schema_check.SchemaCheck.check_schema | Current version set to: 4.0.
   2012-02-29T01:39:17.237624Z INFO   netlogger.analysis.schema.schema_check.SchemaCheck.check_schema | Schema up to date.

   For upgrading a MySQL database the steps remain the same. The only thing that changes is the connection String to the database
   E.g.

   /usr/share/pegasus/sql/schema_tool.py -u connString=mysql://username:password@server:port/dbname

After the database has been upgraded you can use either 3.x or 4.x
clients to query the database with **pegasus-statistics**, as well as
**pegasus-plots**\ and **pegasus-analyzer.**

Existing users running in a condor pool with a non shared filesystem setup
--------------------------------------------------------------------------

Existing users that are running workflows in a cloud environment with a
non shared filesystem setup have to do some trickery in the site catalog
to include placeholders for local/submit host paths for execution sites
when using CondorIO. In Pegasus 4.0, this has been rectified.

For example, for a 3.1 user, to run on a local-condor pool without a
shared filesystem and use Condor file IO for file transfers, the site
entry looks something like this

::

    <site  handle="local-condor" arch="x86" os="LINUX">
           <grid  type="gt2" contact="localhost/jobmanager-fork" scheduler="Fork" jobtype="auxillary"/>
           <grid  type="gt2" contact="localhost/jobmanager-condor" scheduler="unknown" jobtype="compute"/>
           <head-fs>

             <!-- the paths for scratch filesystem are the paths on local site as we execute create dir job
                  on local site. Improvements planned for 4.0 release.-->
               <scratch>
                   <shared>
                       <file-server protocol="file" url="file:///" mount-point="/submit-host/scratch"/>
                       <internal-mount-point mount-point="/submit-host/scratch"/>
                   </shared>
               </scratch>
               <storage>
                   <shared>
                       <file-server protocol="file" url="file:///" mount-point="/glusterfs/scratch"/>
                       <internal-mount-point mount-point="/glusterfs/scratch"/>
                   </shared>
               </storage>
           </head-fs>
           <replica-catalog  type="LRC" url="rlsn://dummyValue.url.edu" />
           <profile namespace="env" key="PEGASUS_HOME" >/cluster-software/pegasus/2.4.1</profile>
           <profile namespace="env" key="GLOBUS_LOCATION" >/cluster-software/globus/5.0.1</profile>

           <!-- profies for site to be treated as condor pool -->
           <profile namespace="pegasus" key="style" >condor</profile>
           <profile namespace="condor" key="universe" >vanilla</profile>


           <!-- to enable kickstart staging from local site-->
           <profile namespace="condor" key="transfer_executable">true</profile>


       </site>

With Pegasus 4.0 the site entry for a local-condor pool can be as
concise as the following

::

    <site  handle="condorpool" arch="x86" os="LINUX">
           <head-fs>
               <scratch />
               <storage />
           </head-fs>
           <profile namespace="pegasus" key="style" >condor</profile>
           <profile namespace="condor" key="universe" >vanilla</profile>
       </site>

The planner in 4.0 correctly picks up the paths from the local site
entry to determine the staging location for the condor io on the submit
host.

Users should read pegasus data staging configuration
`chapter <#data_staging_configuration>`__ and also look in the examples
directory ( share/pegasus/examples).

.. _migrating-from-2x:

Migrating From Pegasus 2.X to Pegasus 3.X
=========================================

With Pegasus 3.0 effort has been made to simplify configuration. This
chapter is for existing users of Pegasus who use Pegasus 2.x to run
their workflows and walks through the steps to move to using Pegasus 3.0

PEGASUS_HOME and Setup Scripts
------------------------------

Earlier versions of Pegasus required users to have the environment
variable PEGASUS_HOME set and to source a setup file
$PEGASUS_HOME/setup.sh \| $PEGASUS_HOME/setup.csh before running Pegasus
to setup CLASSPATH and other variables.

Starting with Pegasus 3.0 this is no longer required. The above paths
are automaticallly determined by the Pegasus tools when they are
invoked.

All the users need to do is to set the PATH variable to pick up the
pegasus executables from the bin directory.

::

   $ export PATH=/some/install/pegasus-3.0.0/bin:$PATH

Changes to Schemas and Catalog Formats
--------------------------------------

DAX Schema
~~~~~~~~~~

Pegasus 3.0 by default now parses DAX documents conforming to the DAX
Schema 3.2 available `here <schemas/dax-3.2/dax-3.2.xsd>`__ and is
explained in detail in the chapter on API references.

Starting Pegasus 3.0 , DAX generation API's are provided in Java/Python
and Perl for users to use in their DAX Generators. The use of API's is
highly encouraged. Support for the old DAX schema's has been deprecated
and will be removed in a future version.

For users, who still want to run using the old DAX formats i.e 3.0 or
earlier, can for the time being set the following property in the
properties and point it to dax-3.0 xsd of the installation.

::

   pegasus.schema.dax  /some/install/pegasus-3.0/etc/dax-3.0.xsd

Site Catalog Format
~~~~~~~~~~~~~~~~~~~

Pegasus 3.0 by default now parses Site Catalog format conforming to the
SC schema 3.0 ( XML3 ) available `here <schemas/dax-3.2/dax-3.2.xsd>`__
and is explained in detail in the chapter on Catalogs.

Pegasus 3.0 comes with a pegasus-sc-converter that will convert users
old site catalog ( XML ) to the XML3 format. Sample usage is given
below.

::

   $ pegasus-sc-converter -i sample.sites.xml -I XML -o sample.sites.xml3 -O XML3

   2010.11.22 12:55:14.169 PST:   Written out the converted file to sample.sites.xml3

To use the converted site catalog, in the properties do the following

1. unset pegasus.catalog.site or set pegasus.catalog.site to XML3

2. point pegasus.catalog.site.file to the converted site catalog

Transformation Catalog Format
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Pegasus 3.0 by default now parses a file based multiline textual format
of a Transformation Catalog. The new Text format is explained in detail
in the chapter on Catalogs.

Pegasus 3.0 comes with a pegasus-tc-converter that will convert users
old transformation catalog ( File ) to the Text format. Sample usage is
given below.

::

   $ pegasus-tc-converter -i sample.tc.data -I File -o sample.tc.text -O Text

   2010.11.22 12:53:16.661 PST:   Successfully converted Transformation Catalog from File to Text
   2010.11.22 12:53:16.666 PST:   The output transfomation catalog is in file  /lfs1/software/install/pegasus/pegasus-3.0.0cvs/etc/sample.tc.text

To use the converted transformation catalog, in the properties do the
following

1. unset pegasus.catalog.transformation or set
   pegasus.catalog.transformation to Text

2. point pegasus.catalog.transformation.file to the converted
   transformation catalog

Properties and Profiles Simplification
--------------------------------------

Starting with Pegasus 3.0 all profiles can be specified in the
properties file. Profiles specified in the properties file have the
lowest priority. Profiles are explained in the detail in
the\ `configuration <#configuration>`__ chapter. As a result of this a
lot of existing Pegasus Properties were replaced by profiles. The table
below lists the properties removed and the new profile based names.

.. table:: Property Keys removed and their Profile based replacement

   ======================================== =======================================================================
   **Old Property Key**                     **New Property Key**
   pegasus.local.env                        no replacement. Specify env profiles for local site in the site catalog
   pegasus.condor.release                   condor.periodic_release
   pegasus.condor.remove                    condor.periodic_remove
   pegasus.job.priority                     condor.priority
   pegasus.condor.output.stream             pegasus.condor.output.stream
   pegasus.condor.error.stream              condor.stream_error
   pegasus.dagman.retry                     dagman.retry
   pegasus.exitcode.impl                    dagman.post
   pegasus.exitcode.scope                   dagman.post.scope
   pegasus.exitcode.arguments               dagman.post.arguments
   pegasus.exitcode.path.\*                 dagman.post.path.\*
   pegasus.dagman.maxpre                    dagman.maxpre
   pegasus.dagman.maxpost                   dagman.maxpost
   pegasus.dagman.maxidle                   dagman.maxidle
   pegasus.dagman.maxjobs                   dagman.maxjobs
   pegasus.remote.scheduler.min.maxwalltime globus.maxwalltime
   pegasus.remote.scheduler.min.maxtime     globus.maxtime
   pegasus.remote.scheduler.min.maxcputime  globus.maxcputime
   pegasus.remote.scheduler.queues          globus.queue
   ======================================== =======================================================================

Profile Keys for Clustering
~~~~~~~~~~~~~~~~~~~~~~~~~~~

The pegasus profile keys for job clustering were **renamed**. The
following table lists the old and the new names for the profile keys.

.. table:: Old and New Names For Job Clustering Profile Keys

   =========================== ===========================
   **Old Pegasus Profile Key** **New Pegasus Profile Key**
   collapse                    clusters.size
   bundle                      clusters.num
   =========================== ===========================

Transfers Simplification
------------------------

Pegasus 3.0 has a new default transfer client pegasus-transfer that is
invoked by default for first level and second level staging. The
pegasus-transfer client is a python based wrapper around various
transfer clients like globus-url-copy, lcg-copy, wget, cp, ln .
pegasus-transfer looks at source and destination url and figures out
automatically which underlying client to use. pegasus-transfer is
distributed with the PEGASUS and can be found in the bin subdirectory .

Also, the Bundle Transfer refiner has been made the default for pegasus
3.0. Most of the users no longer need to set any transfer related
properties. The names of the profiles keys that control the Bundle
Transfers have been changed . The following table lists the old and the
new names for the Pegasus Profile Keys and are explained in details in
the Profiles Chapter.

.. table:: Old and New Names For Transfer Bundling Profile Keys

   =========================== ========================================================================
   **Old Pegasus Profile Key** **New Pegasus Profile Keys**
   bundle.stagein              stagein.clusters \| stagein.local.clusters \| stagein.remote.clusters
   bundle.stageout             stageout.clusters \| stageout.local.clusters \| stageout.remote.clusters
   =========================== ========================================================================

Worker Package Staging
~~~~~~~~~~~~~~~~~~~~~~

Starting Pegasus 3.0 there is a separate boolean property
**pegasus.transfer.worker.package** to enable worker package staging to
the remote compute sites. Earlier it was bundled with user executables
staging i.e if **pegasus.catalog.transformation.mapper** property was
set to Staged .

Clients in bin directory
------------------------

Starting with Pegasus 3.0 the pegasus clients in the bin directory have
a pegasus prefix. The table below lists the old client names and new
names for the clients that replaced them

.. table:: Old Client Names and their New Names

   =============================== ====================
   **Old Client**                  **New Client**
   rc-client                       pegasus-rc-client
   tc-client                       pegasus-tc-client
   pegasus-get-sites               pegasus-sc-client
   sc-client                       pegasus-sc-converter
   tailstatd                       pegasus-monitord
   genstats and genstats-breakdown pegasus-statistics
   show-job                        pegasus-plots
   dirmanager                      pegasus-dirmanager
   exitcode                        pegasus-exitcode
   rank-dax                        pegasus-rank-dax
   transfer                        pegasus-transfer
   =============================== ====================


