.. _cli-pegasus-tc-converter:

====================
pegasus-tc-converter
====================

A client to parse the transformation catalogs in given input format (Text or YAML) and generates transformation catalog into given output format (Text or YAML).

   ::

      pegasus-tc-converter [-Dprop [..]]  [--iformat <input format>] [--oformat <output format>]
                           [--input <list of input files>] [--output <output file to write>]
                           [--conf <path to property file>] [--verbose] [--quiet] [--Version] [--help]


Description
===========

The tc-converter program is used to convert the transformation catalog
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
              type "installed"
          }
      }

**YAML**
   This is YAML-based format. A sample entry in this format looks as follows:

   ::

      pegasus: "5.0"
      transformations:
       -
        name: "keg"
        sites:
         -
          name: "condorpool"
          type: "installed"
          pfn: "/path/to/keg"
          arch: "x86_64"
          os.type: "linux"

Options
=======

**-D**\ *prop=value*
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
   format are: **Text** or **YAML**.

**-O** *fmt* **--oformat** *fmt* The output format of the output file.
Valid values for the output format are: **Text** or **YAML**

**-i** *infile*\ [,*infile*,…] **--input** *infile*\ [,*infile*,…] The
comma separated list of input files that need to be converted to a file
in the format specified by the **--oformat** option.

**-o** *outfile*; \ **--output** *outfile*
   The output file to which the output needs to be written out to.



Other Options
-------------
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

Text to YAML format conversion

::

   pegasus-tc-converter -i tc.text -I Text -o tc.yml -v


YAML to Text format conversion

::

   pegasus-tc-converter  -i tc.yml -I YAML -O Text -o tc.text -v

Authors
=======

Prasanth Thomas

Pegasus Team http://pegasus.isi.edu

