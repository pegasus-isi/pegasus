.. _basic-properties:

Basic Properties
================

Properties are primarily used to configure the behavior of the Pegasus
Workflow Planner at a global level. The properties file is actually a
java properties file and follows the same conventions as that to specify
the properties.

Please note that the values rely on proper capitalization, unless
explicitly noted otherwise.

Some properties rely with their default on the value of other
properties. As a notation, the curly braces refer to the value of the
named property. For instance, ${pegasus.home} means that the value
depends on the value of the pegasus.home property plus any noted
additions. You can use this notation to refer to other properties,
though the extent of the subsitutions are limited. Usually, you want to
refer to a set of the standard system properties. Nesting is not
allowed. Substitutions will only be done once.

There is a priority to the order of reading and evaluating properties.
Usually one does not need to worry about the priorities. However, it is
good to know the details of when which property applies, and how one
property is able to overwrite another. The following is a mutually
exclusive list ( highest priority first ) of property file locations.

1. --conf option to the tools. Almost all of the clients that use
   properties have a --conf option to specify the property file to pick
   up.
2. submit-dir/pegasus.xxxxxxx.properties file. All tools that work on
   the submit directory ( i.e after pegasus has planned a workflow) pick
   up the pegasus.xxxxx.properties file from the submit directory. The
   location for the pegasus.xxxxxxx.propertiesis picked up from the
   braindump file.
3. The properties defined in the user property file
   ${user.home}/.pegasusrc
   have lowest priority.

Commandline properties have the highest priority. These override any
property loaded from a property file. Each commandline property is
introduced by a -D argument. Note that these arguments are parsed by the
shell wrapper, and thus the -D arguments must be the first arguments to
any command. Commandline properties are useful for debugging purposes.

From Pegasus 3.1 release onwards, support has been dropped for the
following properties that were used to signify the location of the
properties file

-  pegasus.properties
-  pegasus.user.properties

The following example provides a sensible set of properties to be set by
the user property file. These properties use mostly non-default
settings. It is an example only, and will not work for you:

::

   pegasus.catalog.replica              File
   pegasus.catalog.replica.file         ${pegasus.home}/etc/sample.rc.data
   pegasus.catalog.transformation       Text
   pegasus.catalog.transformation.file  ${pegasus.home}/etc/sample.tc.text
   pegasus.catalog.site.file            ${pegasus.home}/etc/sample.sites.xml

If you are in doubt which properties are actually visible, pegasus
during the planning of the workflow dumps all properties after reading
and prioritizing in the submit directory in a file with the suffix
properties.

.. _basic-properties-pegasus.home:

pegasus.home
------------

======== =========================
Systems: all
Type:    directory location string
Default: "$PEGASUS_HOME"
======== =========================

The property pegasus.home cannot be set in the property file. This
property is automatically set up by the pegasus clients internally by
determining the installation directory of pegasus. Knowledge about this
property is important for developers who want to invoke PEGASUS JAVA
classes without the shell wrappers.

.. _basic-catalog-props:

Catalog Related Properties
--------------------------

.. table:: Replica Catalog Properties

   ======================================================================================================================================== ==============================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================
   **Key Attributes**                                                                                                                       **Description**
   **Property Key:**\ pegasus.catalog.replica\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 2.0 **Default :** File             Pegasus queries a Replica Catalog to discover the physical filenames (PFN) for input files specified in the DAX. Pegasus can interface with various types of Replica Catalogs. This property specifies which type of Replica Catalog to use during the planning process.

                                                                                                                                            JDBCRC
                                                                                                                                               In this mode, Pegasus queries a SQL based replica catalog that is accessed via JDBC. The sql schema's for this catalog can be found at $PEGASUS_HOME/sql directory. To use JDBCRC, the user additionally needs to set the following properties

                                                                                                                                               1. pegasus.catalog.replica.db.driver = mysql
                                                                                                                                               2. pegasus.catalog.replica.db.url = jdbc url to database e.g jdbc:mysql://database-host.isi.edu/database-name
                                                                                                                                               3. pegasus.catalog.replica.db.user = database-user
                                                                                                                                               4. pegasus.catalog.replica.db.password = database-password

                                                                                                                                            File
                                                                                                                                               In this mode, Pegasus queries a file based replica catalog. It is neither transactionally safe, nor advised to use for production purposes in any way. Multiple concurrent instances *will clobber* each other!. The site attribute should be specified whenever possible. The attribute key for the site attribute is "site".

                                                                                                                                               The LFN may or may not be quoted. If it contains linear whitespace, quotes, backslash or an equality sign, it must be quoted and escaped. Ditto for the PFN. The attribute key-value pairs are separated by an equality sign without any whitespaces. The value may be in quoted. The LFN sentiments about quoting apply.

                                                                                                                                               ::

                                                                                                                                                  LFN PFN
                                                                                                                                                  LFN PFN a=b [..]
                                                                                                                                                  LFN PFN a="b" [..]
                                                                                                                                                  "LFN w/LWS" "PFN w/LWS" [..]

                                                                                                                                               To use File, the user additionally needs to specify pegasus.catalog.replica.file property to specify the path to the file based RC.

                                                                                                                                            Regex
                                                                                                                                               In this mode, Pegasus queries a file based replica catalog. It is neither transactionally safe, nor advised to use for production purposes in any way. Multiple concurrent access to the File will end up clobbering the contents of the file. The site attribute should be specified whenever possible. The attribute key for the site attribute is "site".

                                                                                                                                               The LFN may or may not be quoted. If it contains linear whitespace, quotes, backslash or an equality sign, it must be quoted and escaped. Ditto for the PFN. The attribute key-value pairs are separated by an equality sign without any whitespaces. The value may be in quoted. The LFN sentiments about quoting apply.

                                                                                                                                               In addition users can specifiy regular expression based LFN's. A regular expression based entry should be qualified with an attribute named 'regex'. The attribute regex when set to true identifies the catalog entry as a regular expression based entry. Regular expressions should follow Java regular expression syntax.

                                                                                                                                               For example, consider a replica catalog as shown below.

                                                                                                                                               Entry 1 refers to an entry which does not use a resular expressions. This entry would only match a file named 'f.a', and nothing else. Entry 2 referes to an entry which uses a regular expression. In this entry f.a referes to files having name as f[any-character]a i.e. faa, f.a, f0a, etc.

                                                                                                                                               ::

                                                                                                                                                  f.a file:///Vol/input/f.a site="local"
                                                                                                                                                  f.a file:///Vol/input/f.a site="local" regex="true"

                                                                                                                                               Regular expression based entries also support substitutions. For example, consider the regular expression based entry shown below.

                                                                                                                                               Entry 3 will match files with name alpha.csv, alpha.txt, alpha.xml. In addition, values matched in the expression can be used to generate a PFN.

                                                                                                                                               For the entry below if the file being looked up is alpha.csv, the PFN for the file would be generated as file:///Volumes/data/input/csv/alpha.csv. Similary if the file being lookedup was alpha.csv, the PFN for the file would be generated as file:///Volumes/data/input/xml/alpha.xml i.e. The section [0], [1] will be replaced. Section [0] refers to the entire string i.e. alpha.csv. Section [1] refers to a partial match in the input i.e. csv, or txt, or xml. Users can utilize as many sections as they wish.

                                                                                                                                               ::

                                                                                                                                                  alpha\.(csv|txt|xml) file:///Vol/input/[1]/[0] site="local" regex="true"

                                                                                                                                               To use File, the user additionally needs to specify pegasus.catalog.replica.file property to specify the path to the file based RC.

                                                                                                                                            Directory
                                                                                                                                               In this mode, Pegasus does a directory listing on an input directory to create the LFN to PFN mappings. The directory listing is performed recursively, resulting in deep LFN mappings. For example, if an input directory $input is specified with the following structure

                                                                                                                                               ::

                                                                                                                                                  $input
                                                                                                                                                  $input/f.1
                                                                                                                                                  $input/f.2
                                                                                                                                                  $input/D1
                                                                                                                                                  $input/D1/f.3

                                                                                                                                               Pegasus will create the mappings the following LFN PFN mappings internally

                                                                                                                                               ::

                                                                                                                                                  f.1 file://$input/f.1  site="local"
                                                                                                                                                  f.2 file://$input/f.2  site="local"
                                                                                                                                                  D1/f.3 file://$input/D2/f.3 site="local"

                                                                                                                                               If you don't want the deep lfn's to be created then, you can set pegasus.catalog.replica.directory.flat.lfn to true In that case, for the previous example, Pegasus will create the following LFN PFN mappings internally.

                                                                                                                                               ::

                                                                                                                                                  f.1 file://$input/f.1  site="local"
                                                                                                                                                  f.2 file://$input/f.2  site="local"
                                                                                                                                                  f.3 file://$input/D2/f.3 site="local"

                                                                                                                                               pegasus-plan has --input-dir option that can be used to specify an input directory.

                                                                                                                                               Users can optionally specify additional properties to configure the behvavior of this implementation.

                                                                                                                                               pegasus.catalog.replica.directory.site to specify a site attribute other than local to associate with the mappings.

                                                                                                                                               pegasus.catalog.replica.directory.url.prefix to associate a URL prefix for the PFN's constructed. If not specified, the URL defaults to file://

                                                                                                                                            MRC
                                                                                                                                               In this mode, Pegasus queries multiple replica catalogs to discover the file locations on the grid. To use it set

                                                                                                                                               ::

                                                                                                                                                  pegasus.catalog.replica MRC

                                                                                                                                               Each associated replica catalog can be configured via properties as follows.

                                                                                                                                               The user associates a variable name referred to as [value] for each of the catalogs, where [value] is any legal identifier (concretely [A-Za-z][_A-Za-z0-9]*) For each associated replica catalogs the user specifies the following properties.

                                                                                                                                               ::

                                                                                                                                                  pegasus.catalog.replica.mrc.[value]       specifies the type of \
                                                                                                                                                                                            replica catalog.
                                                                                                                                                  pegasus.catalog.replica.mrc.[value].key   specifies a property name\
                                                                                                                                                                                            key for a particular catalog

                                                                                                                                               ::

                                                                                                                                                  pegasus.catalog.replica.mrc.directory1 LRC
                                                                                                                                                  pegasus.catalog.replica.mrc.directory1.url /input/dir1
                                                                                                                                                  pegasus.catalog.replica.mrc.directory2 LRC
                                                                                                                                                  pegasus.catalog.replica.mrc.directory2.url /input/dir2

                                                                                                                                               In the above example, directory1, directory2 are any valid identifier names and url is the property key that needed to be specified.
   **Property Key:**\ pegasus.catalog.replica.url\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 2.0 **Default :** (no default) When using the modern RLS replica catalog, the URI to the Replica catalog must be provided to Pegasus to enable it to look up filenames. There is no default.
   ======================================================================================================================================== ==============================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================

.. table:: Site Catalog Properties

   =============================================================================================================================================================== =================================================================================================================================================================================================================================
   **Key Attributes**                                                                                                                                              **Description**
   **Property Key:**\ pegasus.catalog.site\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 2.0 **Default :** XML                                        Pegasus supports two different types of site catalogs in XML format conforming to sc-3.0.xsd and sc-4.0.xsd. Pegasus is able to auto-detect what schema a user site catalog refers to. Hence, this property may no longer be set.
   **Property Key:**\ pegasus.catalog.site.file\ **Profile Key :**\ N/A\ **Scope :** Properties **Since :** 2.0 **Default :** ${pegasus.home.sysconfdir}/sites.xml The path to the site catalog file, that describes the various sites and their layouts to Pegasus.
   =============================================================================================================================================================== =================================================================================================================================================================================================================================

.. table:: Transformation Catalog Properties

   ================================================================================================================================================================== =======================================================================================================================================================================================================================================================================
   **Key Attributes**                                                                                                                                                 **Description**
   **Property Key:**\ pegasus.catalog.transformation\ **Profile Key:**\ N/A\ **Scope :**\ Properties **Since :** 2.0 **Default :** Text                               The only recommended and supported version of Transformation Catalog for Pegasus is Text. For the old File based formats, users should use pegasus-tc-converter to convert File format to Text Format.

                                                                                                                                                                      Text
                                                                                                                                                                         In this mode, a multiline file based format is understood. The file is read and cached in memory. Any modifications, as adding or deleting, causes an update of the memory and hence to the file underneath. All queries are done against the memory representation.

                                                                                                                                                                         The file sample.tc.text in the etc directory contains an example

                                                                                                                                                                         Here is a sample textual format for transfomation catalog containing one transformation on two sites

                                                                                                                                                                         ::

                                                                                                                                                                            tr example::keg:1.0 {
                                                                                                                                                                            #specify profiles that apply for all the sites for the transformation
                                                                                                                                                                            #in each site entry the profile can be overriden
                                                                                                                                                                            profile env "APP_HOME" "/tmp/karan"
                                                                                                                                                                            profile env "JAVA_HOME" "/bin/app"
                                                                                                                                                                            site isi {
                                                                                                                                                                            profile env "me" "with"
                                                                                                                                                                            profile condor "more" "test"
                                                                                                                                                                            profile env "JAVA_HOME" "/bin/java.1.6"
                                                                                                                                                                            pfn "/path/to/keg"
                                                                                                                                                                            arch  "x86"
                                                                                                                                                                            os    "linux"
                                                                                                                                                                            osrelease "fc"
                                                                                                                                                                            osversion "4"
                                                                                                                                                                            type "INSTALLED"
                                                                                                                                                                            site wind {
                                                                                                                                                                            profile env "me" "with"
                                                                                                                                                                            profile condor "more" "test"
                                                                                                                                                                            pfn "/path/to/keg"
                                                                                                                                                                            arch  "x86"
                                                                                                                                                                            os    "linux"
                                                                                                                                                                            osrelease "fc"
                                                                                                                                                                            osversion "4"
                                                                                                                                                                            type "STAGEABLE"
   **Property Key:**\ pegasus.catalog.transformation\ **Profile Key :**\ N/A\ **Scope :** Properties **Since :** 2.0 **Default :** ${pegasus.home.sysconfdir}/tc.text The path to the transformation catalog file, that describes the locations of the executables.
   ================================================================================================================================================================== =======================================================================================================================================================================================================================================================================

.. _basic-data-conf-props:

Data Staging Configuration Properties
-------------------------------------

.. table:: Data Configuration Properties

   ====================================================================================================================================================================================================================================== ===========================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================
   **Key Attributes**                                                                                                                                                                                                                     **Description**
   **Property Key:**\ pegasus.data.configuration\ **Profile Key:**\ N/A\ **Scope :** Properties **Since :** 4.0.0 **Values** : sharedfs|nonsharedfs|condorio **Default :** sharedfs\ **See Also :** pegasus.transfer.bypass.input.staging This property sets up Pegasus to run in different environments.

                                                                                                                                                                                                                                          sharedfs
                                                                                                                                                                                                                                             If this is set, Pegasus will be setup to execute jobs on the shared filesystem on the execution site. This assumes, that the head node of a cluster and the worker nodes share a filesystem. The staging site in this case is the same as the execution site. Pegasus adds a create dir job to the executable workflow that creates a workflow specific directory on the shared filesystem . The data transfer jobs in the executable workflow ( stage_in\_ , stage_inter\_ , stage_out\_ ) transfer the data to this directory.The compute jobs in the executable workflow are launched in the directory on the shared filesystem. Internally, if this is set the following properties are set.
                                                                                                                                                                                                                                             ::

                                                                                                                                                                                                                                                pegasus.execute.*.filesystem.local   false

                                                                                                                                                                                                                                          condorio
                                                                                                                                                                                                                                             If this is set, Pegasus will be setup to run jobs in a pure condor pool, with the nodes not sharing a filesystem. Data is staged to the compute nodes from the submit host using Condor File IO. The planner is automatically setup to use the submit host ( site local ) as the staging site. All the auxillary jobs added by the planner to the executable workflow ( create dir, data stagein and stage-out, cleanup ) jobs refer to the workflow specific directory on the local site. The data transfer jobs in the executable workflow ( stage_in\_ , stage_inter\_ , stage_out\_ ) transfer the data to this directory. When the compute jobs start, the input data for each job is shipped from the workflow specific directory on the submit host to compute/worker node using Condor file IO. The output data for each job is similarly shipped back to the submit host from the compute/worker node. This setup is particularly helpful when running workflows in the cloud environment where setting up a shared filesystem across the VM's may be tricky. On loading this property, internally the following properies are set
                                                                                                                                                                                                                                             ::

                                                                                                                                                                                                                                                pegasus.transfer.lite.*.impl          Condor
                                                                                                                                                                                                                                                pegasus.execute.*.filesystem.local   true
                                                                                                                                                                                                                                                pegasus.gridstart          PegasusLite
                                                                                                                                                                                                                                                pegasus.transfer.worker.package      true

                                                                                                                                                                                                                                          nonsharedfs
                                                                                                                                                                                                                                             If this is set, Pegasus will be setup to execute jobs on an execution site without relying on a shared filesystem between the head node and the worker nodes. You can specify staging site ( using --staging-site option to pegasus-plan) to indicate the site to use as a central storage location for a workflow. The staging site is independant of the execution sites on which a workflow executes. All the auxillary jobs added by the planner to the executable workflow ( create dir, data stagein and stage-out, cleanup ) jobs refer to the workflow specific directory on the staging site. The data transfer jobs in the executable workflow ( stage_in\_ , stage_inter\_ , stage_out\_ ) transfer the data to this directory. When the compute jobs start, the input data for each job is shipped from the workflow specific directory on the submit host to compute/worker node using pegasus-transfer. The output data for each job is similarly shipped back to the submit host from the compute/worker node. The protocols supported are at this time SRM, GridFTP, iRods, S3. This setup is particularly helpful when running workflows on OSG where most of the execution sites don't have enough data storage. Only a few sites have large amounts of data storage exposed that can be used to place data during a workflow run. This setup is also helpful when running workflows in the cloud environment where setting up a shared filesystem across the VM's may be tricky. On loading this property, internally the following properies are set
                                                                                                                                                                                                                                             ::

                                                                                                                                                                                                                                                pegasus.execute.*.filesystem.local   true
                                                                                                                                                                                                                                                pegasus.gridstart          PegasusLite
                                                                                                                                                                                                                                                pegasus.transfer.worker.package      true
   ====================================================================================================================================================================================================================================== ===========================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================
