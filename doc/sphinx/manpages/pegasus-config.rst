.. _cli-pegasus-config:

==============
pegasus-config
==============

Can be used to find installed Pegasus tools and libraries.
::

      pegasus-config [-h] [--help] [-V] [--version] [--noeoln]
                     [--perl-dump] [--perl-hash] [--python-dump] [--sh-dump]
                     [--bin] [--conf] [--java] [--perl] [--python]
                     [--python-externals] [--r] [--schema] [--classpath]



Description
===========

**pegasus-config** is used to find locations of Pegasus system
components. The tool is used internally in Pegasus and by users who need
to find paths for DAX generator libraries and schemas.



Options
=======

**-h**; \ **--help**
   Prints help and exits.

**-V**; \ **--version**
   Prints Pegasus version information

**--perl-dump**
   Dumps all settings in perl format as separate variables.

**--perl-hash**
   Dumps all settings in perl format as single perl hash.

**--python-dump**
   Dumps all settings in python format.

**--sh-dump**
   Dumps all settings in shell format.

**--bin**
   Print the directory containing Pegasus binaries.

**--conf**
   Print the directory containing configuration files.

**--java**
   Print the directory containing the jars.

**--perl**
   Print the directory to include into your PERL5LIB.

**--python**
   Print the directory to include into your PYTHONLIB.

**--python-externals**
   Print the directory to the external Python libraries.

**--r**
   Print the path to the R DAX API source package.

**--schema**
   Print the directory containing schemas.

**--classpath**
   Builds a classpath containing the Pegasus jars.

**--noeoln**
   Do not produce a end-of-line after output. This is useful when being
   called from non-shell backticks in scripts. However, order is
   important for this option: If you intend to use it, specify it first.



Example
=======

To set the PYTHONPATH variable in your shell for using the Python DAX
API:

::

   export PYTHONPATH=`pegasus-config --python`

To set the same path inside Python:

::

   config = subprocess.Popen("pegasus-config --python-dump", stdout=subprocess.PIPE, shell=True).communicate()[0]
   exec config

To set the PERL5LIB variable in your shell for using the Perl DAX API:

::

   export PERL5LIB=`pegasus-config --perl`

To set the same path inside Perl:

.. code:: perl

   eval `pegasus-config --perl-dump`;
   die("Unable to eval pegasus-config output: $@") if $@;

will set variables a number of lexically local-scoped **my** variables
with prefix "pegasus\_" and expand Perl’s search path for this script.

Alternatively, you can fail early and collect all Pegasus-related
variables into a single global ``%pegasus`` variable for convenience:

.. code:: perl

   BEGIN {
       eval `pegasus-config --perl-hash`;
       die("Unable to eval pegasus-config output: $@") if $@;
   }



Author
======

Pegasus Team http://pegasus.isi.edu
