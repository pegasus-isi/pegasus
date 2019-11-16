===============
pegasus-version
===============

1
pegasus-version
print or match the version of the toolkit.
   ::

      pegasus-version [-Dproperty=value] [-m [-q]] [-V] [-f] [-l]

.. __description:

Description
===========

This program prints the version string of the currently active Pegasus
toolkit on *stdout*.

pegasus-version is a simple command-line tool that reports the version
number of the Pegasus distribution being used. In its most basic
invocation, it will show the current version of the Pegasus software you
have installed:

::

   $ pegasus-version
   3.1.0cvs

If you want to know more details about the installed version, i.e. which
system it was compiled for and when, use the long or full mode:

::

   $ pegasus-version -f
   3.1.0cvs-x86_64_cent_5.6-20110706191019Z

.. __options:

Options
=======

**-D**\ *property=value*
   The **-D** option allows an experienced user to override certain
   properties which influence the program execution, among them the
   default location of the user’s properties file and the
   **PEGASUS_HOME** location. One may set several CLI properties by
   giving this option multiple times.

   The **-D** option(s) must be the first option on the command line.
   CLI properties take precedence over the file-based properties of the
   same key.

**-f**; \ **--full**
   The **--full** mode displays internal build metrics, like OS type and
   libc version, addition to the version number. It appends the build
   time as time stamp to the version. The time stamp uses ISO 8601
   format, and is a UTC stamp.

**-l**; \ **--long**
   This option is an alias for **--full**.

**-V**; \ **--version**
   Displays the version of the Pegasus planner you are using.

**--verbose**
   is ignored in this tool. However, to provide a uniform interface for
   all tools, the option is recognized and will not trigger an error.

.. __return_value:

Return Value
============

The program will usually return with success (0). In match mode, if the
internal version does not match the external installation, an exit code
of 1 is returned. If run-time errors are detected, an exit code of 2 is
returned, 3 for fatal errors.

.. __environment_variables:

Environment Variables
=====================

**JAVA_HOME**
   should be set and point to a valid location to start the intended
   Java virtual machine as *$JAVA_HOME/bin/java*.

.. __example:

Example
=======

::

   $ pegasus-version
   3.1.0cvs

   $ pegasus-version -f
   3.1.0cvs-x86_64_cent_5.6-20110706191019Z

.. __authors:

Authors
=======

Jens-S. Vöckler ``<voeckler at isi dot edu>``

Pegasus Team http://pegasus.isi.edu
