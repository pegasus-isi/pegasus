====================
pegasus-tc-converter
====================

1
pegasus-tc-converter
A client to convert transformation catalog from one format to another
format.
   ::

      pegasus-tc-converter [-Dproperty=value…] [-v] [-q] [-V] [-h]
                           [-I fmt] [-O fmt]
                           [-N dbusername] [-P dbpassword] [-U dburl] [-H dbhost]
                           -i infile[,infile,…] -o outfile



Description
===========

The tc-convert program is used to convert the transformation catalog
from one format to another.

Currently, the following formats of transformation catalog exist:

**Text**
   This is a easy to read multi line textual format.

   A sample entry in this format looks as follows:

   ::

      tr example::keg:1.0 {
              site isi {
              profile env "JAVA_HOME" "/bin/java.1.6"
              pfn "/path/to/keg"
              arch  "x86"
              os    "linux"
              osrelease "fc"
              osversion "4"
              type "installed"
          }
      }

**File**
   This is a tuple based format which contains 6 columns.

   ::

      RESOURCE  LFN  PFN  TYPE  SYSINFO  PROFILES

   A sample entry in this format looks as follows

   ::

      isi  example::keg:1.0  /path/to/keg  INSTALLED  INTEL32::LINUX:fc_4:  env::JAVA_HOME="/bin/java.1.6"

**Database**
   Only MySQL is supported for the time being.



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

**-I** *fmt*; \ **--iformat** *fmt*
   The input format of the input files. Valid values for the input
   format are: **File**, **Text**, and **Database**.

**-O** *fmt* **--oformat** *fmt* The output format of the output file.
Valid values for the output format are: **File**, **Text**, and
**Database**.

**-i** *infile*\ [,*infile*,…] **--input** *infile*\ [,*infile*,…] The
comma separated list of input files that need to be converted to a file
in the format specified by the **--oformat** option.

**-o** *outfile*; \ **--output** *outfile*
   The output file to which the output needs to be written out to.



Other Options
-------------

**-N** *dbusername*; \ **--db-user-name** *dbusername*
   The database user name.

**-P** *dbpassword*; \ **--db-user-pwd** *dbpassword*
   The database user password.

**-U** *dburl*; \ **--db-url** *dburl*
   The database url.

**-H** *dbhost*; \ **--db-host** *dbhost*
   The database host.

**-v**; \ **--verbose**
   Increases the verbosity of messages about what is going on. By
   default, all FATAL ERROR, ERROR , CONSOLE and WARNINGS messages are
   logged.

**-q**; \ **--quiet**
   Decreases the verbosity of messages about what is going on. By
   default, all FATAL ERROR, ERROR , CONSOLE and WARNINGS messages are
   logged.

**-V**; \ **--version**
   Displays the current version number of the Pegasus Workflow Planner
   Software.

**-h**; \ **--help**
   Displays all the options to the **pegasus-tc-converter** command.



Example
=======

Text to file format conversion

::

   pegasus-tc-converter -i tc.data -I File -o tc.txt  -O Text -v

File to Database(new) format conversion

::

   pegasus-tc-converter -i tc.data -I File -N mysql_user -P mysql_pwd -U jdbc:mysql://localhost:3306/tc -H localhost -O Database -v

Database (username, password, host, url specified in properties file) to text format conversion

::

   pegasus-tc-converter -I Database -o tc.txt -O Text -vvvvv



Authors
=======

Prasanth Thomas

Pegasus Team http://pegasus.isi.edu
