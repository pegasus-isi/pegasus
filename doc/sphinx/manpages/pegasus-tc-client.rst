=================
pegasus-tc-client
=================

1
pegasus-tc-client
A full featured generic client to handle adds, deletes and queries to
the Transformation Catalog (TC).
   ::

      pegasus-tc-client [-Dproperty=value…] [-h] [-v] [-V]
                        OPERATION TRIGGERS [OPTIONS]



Description
===========

The **pegasus-tc-client** command is a generic client that performs the
three basic operation of adding, deleting and querying of any
Transformation Catalog implemented to the TC API. The client implements
all the operations supported by the TC API. It is up to the TC
implementation whether they support all operations or modes.

The following 3 operations are supported by the **pegasus-tc-client**.
One of these operations have to be specified to run the client.

**ADD**
   This operation allows the client to add or update entries in the
   Transformation Catalog. Entries can be added one by one on the
   command line or in bulk by using the *BULK* Trigger and providing a
   file with the necessary entries. Also Profiles can be added to either
   the logical transformation or the physical transformation.

**DELETE**
   This operation allows the client to delete entries from the
   Transformation Catalog. Entries can be deleted based on logical
   transformation, by resource, by transformation type as well as the
   transformation system information. Also Profiles associated with the
   logical or physical transformation can be deleted.

**QUERY**
   This operation allows the client to query for entries from the
   Transformation Catalog. Queries can be made for printing all the
   contents of the Catalog or for specific entries, for all the logical
   transformations or resources etc.

See the `TRIGGERS <#TRIGGERS>`__ and `VALID
COMBINATIONS <#VALID_COMBINATIONS>`__ section for more details.



Operations
==========

To select one of the 3 operations.

**-a, --add**
   Perform addition operations on the TC.

**-d, --delete**
   Perform delete operations on the TC.

**-q, --query**
   Perform query operations on the TC.

.. _TRIGGERS:

Triggers
========

Triggers modify the behavior of an **OPERATION**. For example, if you
want to perform a bulk operation you would use a *BULK* Trigger or if
you want to perform an operation on a Logical Transformation then you
would use the *LFN* Trigger.

The following 7 Triggers are available. See the `VALID
COMBINATIONS <#VALID_COMBINATIONS>`__ section for the correct grouping
and usage.

**-B**
   Triggers a bulk operation.

**-L**
   Triggers an operation on a logical transformation.

**-P**
   Triggers an operation on a physical transformation

**-R**
   Triggers an operation on a resource.

**-E**
   Triggers an operation on a Profile.

**-T**
   Triggers an operation on a Type.

**-S**
   Triggers an operation on a System information.



Options
=======

The following options are applicable for all the operations.

**-D**\ *property=value*
   The -D options allows an experienced user to override certain
   properties which influence the program execution, among them the
   default location of the user’s properties file and the PEGASUS home
   location. One may set several CLI properties by giving this option
   multiple times. The **-D** option(s) must be the first option on the
   command line. A CLI property take precedence over the properties file
   property of the same key.

**-l, --lfn** *logical*
   The logical transformation to be added. The format is:
   **NAMESPACE::NAME:VERSION**. The name is always required, namespace
   and version are optional.

**-p, --pfn** *physical*
   The physical transformation to be added. For INSTALLED executables
   its a local file path, for all others its a url.

**-t, --type** *type*
   The type of physical transformation. Valid values are: INSTALLED,
   STATIC_BINARY, DYNAMIC_BINARY, SCRIPT, SOURCE, PACMAN_PACKAGE.

**-r, --resource** *resource*
   The resourceID where the transformation is located.

**-e, --profile** *profiles*
   The profiles for the transformation. Multiple profiles of same
   namespace can be added simultaneously by separating them with a comma
   **","**. Each profile section is written as
   **NAMESPACE::KEY=VALUE,KEY2=VALUE2** e.g.
   ``ENV::JAVA_HOME=/usr/bin/java2,PEGASUS_HOME=/usr/local/pegasus``. To
   add multiple namespaces you need to repeat the -e option for each
   namespace. e.g.
   ``-e ENV::JAVA_HOME=/usr/bin/java -e GLOBUS::JobType=MPI,COUNT=10``

**-s, --system** *systeminfo*
   The architecture, os, osversion and glibc if any for the executable.
   Each system info is written in the form **ARCH::OS:OSVER:GLIBC**

**-v, --verbose**
   Displays the output in verbose mode (Lots of Debugging info).

**-V, --version**
   Displays the Pegasus version.

**-h, --help**
   Generates help



Other Options
=============

**-o, --oldformat**
   Generates the output in the old single line format

**-c, --conf**
   path to property file

.. _VALID_COMBINATIONS:

Valid Combinations
==================

The following are valid combinations of **OPERATIONS, TRIGGERS,
OPTIONS** for the **pegasus-tc-client**.



ADD
---

**Add TC Entry**
   -a -l *lfn* -p *pfn* -t *type* -r *resource* -s *system* [-e
   *profiles*\ …]

   Adds a single entry into the transformation catalog.

**Add PFN Profile**
   -a -P -E -p *pfn* -t *type* -r *resource* -e *profiles* …

   Adds profiles to a specified physical transformation on a given
   resource and of a given type.

**Add LFN Profile**
   -a -L -E -l *lfn* -e *profiles* …

   Adds profiles to a specified logical transformation.

**Add Bulk Entries**
   -a -B -f *file*

   Adds entries in bulk mode by supplying a file containing the entries.
   The format of the file contains 6 columns. E.g.

   ::

      #RESOURCE   LFN         PFN      TYPE      SYSINFO      PROFILES
      #
      isi NS::NAME:VER  /bin/date  INSTALLED  ARCH::OS:OSVERS:GLIBC  NS::KEY=VALUE,KEY=VALUE;NS2::KEY=VALUE,KEY=VALUE



DELETE
------

**Delete all TC**
   -d -BPRELST

   Deletes the entire contents of the TC.

   **WARNING : USE WITH CAUTION.**

**Delete by LFN**
   -d -L -l *lfn* [-r *resource*] [-t *type*]

   Deletes entries from the TC for a particular logical transformation
   and additionally a resource and or type.

**Delete by PFN**
   -d -P -l *lfn* -p *pfn* [-r *resource*] [-t *type*]

   Deletes entries from the TC for a given logical and physical
   transformation and additionally on a particular resource and or of a
   particular type.

**Delete by Type**
   -d -T -t *type* [-r *resource*]

   Deletes entries from TC of a specific type and/or on a specific
   resource.

**Delete by Resource**
   -d -R -r *resource*

   Deletes the entries from the TC on a particular resource.

**Delete by SysInfo**
   -d -S -s *sysinfo*

   Deletes the entries from the TC for a particular system information
   type.

**Delete Pfn Profile**
   -d -P -E -p *pfn* -r *resource* -t *type* [-e *profiles* ..]

   Deletes all or specific profiles associated with a physical
   transformation.

**Delete Lfn Profile**
   -d -L -E -l *lfn* -e *profiles* ….

   Deletes all or specific profiles associated with a logical
   transformation.



QUERY
-----

**Query Bulk**
   -q -B

   Queries for all the contents of the TC. It produces a file format TC
   which can be added to another TC using the bulk option.

**Query LFN**
   -q -L [-r *resource*] [-t *type*]

   Queries the TC for logical transformation and/or on a particular
   resource and/or of a particular type.

**Query PFN**
   -q -P -l *lfn* [-r *resource*] [-t *type*]

   Queries the TC for physical transformations for a give logical
   transformation and/or on a particular resource and/or of a particular
   type.

**Query Resource**
   -q -R -l *lfn* [-t *type*]

   Queries the TC for resources that are registered and/or resources
   registered for a specific type of transformation.

**Query LFN Profile**
   -q -L -E -l *lfn*

   Queries for profiles associated with a particular logical
   transformation

**Query Pfn Profile**
   -q -P -E -p *pfn* -r *resource* -t *type*

   Queries for profiles associated with a particular physical
   transformation



Properties
==========

These are the properties you will need to set to use either the **File**
or **Database** TC.

For more details please check the
**$PEGASUS_HOME/etc/sample.properties** file.

**pegasus.catalog.transformation**
   Identifies what impelemntation of TC will be used. If relative name
   is used then the path org.griphyn.cPlanner.tc is prefixed to the name
   and used as the class name to load. The default value if **Text**.
   Other supported mode is **File**

**pegasus.catalog.transformation.file**
   The file path where the text based TC is located. By default the path
   **$PEGASUS_HOME/var/tc.data** is used.



Files
=====

**$PEGASUS_HOME/var/tc.data**
   is the suggested location for the file corresponding to the
   Transformation Catalog

**$PEGASUS_HOME/etc/properties**
   is the location to specify properties to change what Transformation
   Catalog Implementation to use and the implementation related
   **PROPERTIES**.

**pegasus.jar**
   contains all compiled Java bytecode to run the Pegasus planner.



Environment Variables
=====================

**PEGASUS_HOME**
   Path to the PEGASUS installation directory.

**JAVA_HOME**
   Path to the JAVA 1.4.x installation directory.

**CLASSPATH**
   The classpath should be set to contain all necessary PEGASUS files
   for the execution environment. To automatically add the *CLASSPATH*
   to you environment, in the *$PEGASUS_HOME* directory run the script
   *source setup-user-env.csh* or *source setup-user-env.sh*.



Authors
=======

Gaurang Mehta ``<gmehta at isi dot edu>``

Karan Vahi ``<vahi at isi dot edu>``

Pegasus Team http://pegasus.isi.edu
