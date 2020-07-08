====================
pegasus-rc-converter
====================

pegasus-rc-converter
A client to convert replica catalog from one format to another format.
   ::

      pegasus-rc-converter [-Dprop  [..]]
                           [--iformat <input format>]
                           [--oformat <output format>]
                           [--input <list of input files>]
                           [--output <output file to write>]
                           [--conf <path to property file>]
                           [--verbose] [--quiet][--Version]
                           [--help]



Description
===========

The **pegasus-rc-converter** program parses the replica catalogs in
given input format (File, File, Database) and generates replica catalogs
into given output format (File, File, Database).


Options
=======

**-I**; **–iformat**
    The input format for the files . Can be [File, YAML].

**-O**; **–oformat**
    The output format of the file. Can be [File, YAML].

**-i**; **–input**
    Comma separated list of input files to convert. This option
    is mandatory when input format is File or file.

**-o**; **–output**
    The output file to which the output needs to be written to. This
    option is mandatory when output format is File or file.


Other Options
-------------

**-c**; **–conf**
    Path to the property file.

**-e**; **–expand**
    Sets variable expansion on. Any variables in input files will
    be expanded and their values will be written out to output replica
    catalog.

**-v**; **–verbose**
    Increases the verbosity of messages about what is going on.

**-q**; **–quiet**
    Decreases the verbosity of messages about what is going on.

**-V**; **–version**
    Displays the version of the Pegasus Workflow Planner.

**-h**; **–help**
    Prints help and exits.


Example
=======

::

   # File to file format conversion
   $ pegasus-rc-converter -i cc.txt -I File -o rc.yml -v


Authors
=======

Pegasus Team http://pegasus.isi.edu
