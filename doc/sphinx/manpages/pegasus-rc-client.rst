=================
pegasus-rc-client
=================

1
pegasus-rc-client
shell client for replica implementations
   ::

      pegasus-rc-client [-Dproperty=value[…]] [-V]
                        [-c fn] [-p k=v]
                        [[-f fn]|[-i|-d fn]|[cmd [args]]

.. __description:

Description
===========

The shell interface to replica catalog implementations is a prototype.
It determines from various property setting which class implements the
replica manager interface, and loads that driver at run-time. Some
commands depend on the implementation.

.. __options:

Options
=======

Any option will be displayed with its long options synonym(s).

**-D**\ *property=value*
   The **-D** option allows an experienced user to override certain
   properties which influence the program execution, among them the
   default location of the user’s properties file and the PEGASUS home
   location. One may set several CLI properties by giving this option
   multiple times. The **-D** option(s) must be the first option on the
   command line. A CLI property take precedence over the properties file
   property of the same key.

**-c** *fn*; \ **--conf** *fn*
   Path to the property file

**-f** *fn*; \ **--file** *fn*
   The optional input file argument permits to enter non-interactive
   bulk mode. If this option is not present, replica manager specific
   commands should be issued on the command-line. The special filename
   hyphen (-) can be used to read from pipes.

   Default is to use an interactive interface reading from *stdin*.

**-i** *fn*; \ **--insert** *fn*
   The optional input file argument permits insertion of entries from
   the Replica Catalog in a bulk mode, wherever supported by the
   underlying implementation.

   Each line in the file denotes one mapping of the format **<lfn> <pfn>
   [k=v [..]]**

**-d** *fn*; \ **--delete** *fn*
   The optional input file argument permits deletion of entries from the
   Replica Catalog in a bulk mode, wherever supported by the underlying
   implementation.

   Each line in the file denotes one mapping of the format: **<lfn>
   <pfn> [k=v [..]]**

**-p** *k=v*; \ **--pref** *k=v*
   This option may be specified multiple times. Each specification
   populates instance preferences. Preferences control the extend of log
   information, or the output format string to use in listings.

   The keys **format** and **level** are recognized as of this writing.

   There are no defaults.

*cmd [args]*
   If not in file-driven mode, a single command can be specified with
   its arguments.

   Default is to use interactive mode.

**-V**; \ **--version**
   displays the version of Pegasus you are using.

.. __return_value:

Return Value
============

Regular and planned program terminations will result in an exit code of
0. Abnormal termination will result in a non-zero exit code.

.. __files:

Files
=====

**$PEGASUS_HOME/etc/properties**
   contains the basic properties with all configurable options.

**$HOME/.pegasusrc**
   contains the basic properties with all configurable options.

**pegasus.jar**
   contains all compiled Java bytecode to run the replica manager.

.. __environment_variables:

Environment Variables
=====================

**PEGASUS_HOME**
   is the suggested base directory of your the execution environment.

**JAVA_HOME**
   should be set and point to a valid location to start the intended
   Java virtual machine as *$JAVA_HOME/bin/java*.

**CLASSPATH**
   should be set to contain all necessary files for the execution
   environment. Please make sure that your *CLASSPATH* includes pointer
   to the replica implementation required jar files.

.. __properties:

Properties
==========

The complete branch of properties *pegasus.catalog.replica* including
itself are interpreted by the prototype. While the
*pegasus.catalog.replica* property itself steers the backend to connect
to, any meaning of branched keys is dependent on the backend. The same
key may have different meanings for different backends.

**pegasus.catalog.replica**
   determines the name of the implementing class to load at run-time. If
   the class resides in *org.griphyn.common.catalog.replica* no prefix
   is required. Otherwise, the fully qualified class name must be
   specified.

**pegasus.catalog.replica.file**
   is used by the SimpleFile implementation. It specifies the path to
   the file to use as the backend for the catalog.

**pegasus.catalog.replica.db.driver**
   is used by a simple rDBMs implementation. The string is the
   fully-qualified class name of the JDBC driver used by the RDBMS
   implementer.

**pegasus.catalog.replica.db.url**
   is the JDBC URL to use to connect to the database.

**pegasus.catalog.replica.db.user**
   is used by a simple rDBMS implementation. It constitutes the database
   user account that contains the *RC_LFN* and *RC_ATTR* tables.

**pegasus.catalog.replica.db.password**
   is used by a simple RDBMS implementation. It constitutes the database
   user account that contains the *RC_LFN* and *RC_ATTR* tables.

**pegasus.catalog.replica.chunk.size**
   is used by **the pegasus-rc-client** for the bulk insert and delete
   operations. The value determines the number of lines that are read in
   at a time, and worked upon at together.

.. __commands:

Commands
========

The command line tool provides a simplified shell-wrappable interface to
manage a replica catalog backend. The commands can either be specified
in a file in bulk mode, in a pipe, or as additional arguments to the
invocation.

Note that you must escape special characters from the shell.

**help**
   displays a small resume of the commands.

**exit**; \ **quit**
   should only be used in interactive mode to exit the interactive mode.

**clear**
   drops all contents from the backend. Use with special care!

**insert <lfn> <pfn> [k=v […]]**
   inserts a given **lfn** and **pfn**, and an optional **site** string
   into the backend. If the site is not specified, a *null* value is
   inserted for the **site**.

**delete <lfn> <pfn> [k=v […]]**
   removes a triple of **lfn**, **pfn** and, optionally, **site** from
   the replica backend. If the site was not specified, all matches of
   the **lfn** **pfn** pairs will be removed, regardless of the
   **site**.

**lookup <lfn> [<lfn> […]]**
   retrieves one or more mappings for a given **lfn** from the replica
   backend.

**remove <lfn> [<lfn> […]]**
   removes all mappings for each **lfn** from the replica backend.

**list [lfn <pat>] [pfn <pat>] [<name> <pat>]**
   obtains all matches from the replica backend. If no arguments were
   specified, all contents of the replica backend are matched. You must
   use the word **lfn**, **pfn** or **<name>** before specifying a
   pattern. The pattern is meaningful only to the implementation. Thus,
   a SQL implementation may chose to permit SQL wild-card characters. A
   memory-resident service may chose to interpret the pattern as regular
   expression.

**set [var [value]]**
   sets an internal variable that controls the behavior of the
   front-end. With no arguments, all possible behaviors are displayed.
   With one argument, just the matching behavior is listed. With two
   arguments, the matching behavior is set to the value.

.. __database_schema:

Database Schema
===============

The tables are set up as part of the PEGASUS database setup. The files
concerned with the database have a suffix *-rc.sql*.

.. __authors:

Authors
=======

Karan Vahi ``<vahi at isi dot edu>``

Gaurang Mehta ``<gmetha at isi dot edu>``

Jens-S. Vöckler ``<voeckler at isi dot dot edu>``

Pegasus Team http://pegasus.isi.edu/
