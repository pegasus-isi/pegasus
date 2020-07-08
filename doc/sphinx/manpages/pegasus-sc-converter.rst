us-sc-converter
====================

A client to parse the site catalogs in old format (XML and generates site catalog in new format (YAML).
   ::

      pegasus-sc-converter [-Dprop  [..]]  --input <list of input files> --output <output file to write>
                           [--iformat input format] [--oformat <output format>] [--conf <path to property file>] [--verbose]
                           [--quiet] [--Version] [--help]



Description
===========

The **pegasus-sc-converter** program is used to convert the site catalog
from XML to YAML.

Currently, the following formats of site catalog exist.

**XML**
   This format is the old format used in Pegasus version <= 4.9.4. All information about
   a site that can be described about a site can be described in this
   format. In addition, the user has finer grained control over the
   specification of directories and FTP servers that are accessible at
   the **head node** and the **worker node**. The user can also specify
   which different file-servers for read/write operations

   A sample entry in this format looks as follows

   ::

      <site handle="osg" arch="x86" os="LINUX" osrelease="" osversion="" glibc="">
              <grid  type="gt2" contact="viz-login.isi.edu/jobmanager-pbs" scheduler="PBS" jobtype="compute"/>
              <grid  type="gt2" contact="viz-login.isi.edu/jobmanager-fork" scheduler="Fork" jobtype="auxillary"/>

              <directory path="/tmp" type="local-scratch">
                      <file-server operation="put" url="file:///tmp"/>
              </directory>

              <profile namespace="pegasus" key="style">condor</profile>
              <profile namespace="condor" key="universe">vanilla</profile>
      </site>

   This format conforms to the XML schema found at
   http://pegasus.isi.edu/schema/sc-4.0.xsd.

**YAML**
   This format is the new format since Pegasus 5.0. This format is identical to the XML in terms of information but translated into YAML.
   A sample entry in this format looks as follows

   ::

      pegasus: "5.0"
      sites:
       -
        name: "osg"
        arch: "x86_64"
        os.type: "linux"
        directories:
         -
          type: "local-scratch"
          path: "/tmp"
          fileServers:
           -
            operation: "all"
            url: "file:///tmp"
        profiles:
          condor:
            universe: "vanilla"
          pegasus:
            style: "condor"


   This format conforms to the XML schema found at
   http://pegasus.isi.edu/schema/sc-5.0.yml.

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

**-i** *infile*\ [,*infile*,…]; \ **--input** *infile*\ [,*infile*,…]
   The comma separated list of input files that need to be converted to
   a file in the format specified by **--oformat** option.

**-o** *outfile*; \ **--output** *outfile*
   The output file to which the output needs to be written out to.


Other Options
-------------

**-O** *fmt*; \ **--oformat** *fmt*
   The output format of the output file.

   Valid values for the output format is **YAML**

**-c** *path*; \ **--conf** *path*
   path to  property file.

**-e**; \ **--expand**
   sets variable expansion on. Any variables in input files
   will be expanded and their values will be written out to
   output site catalog.

**-v**; \ **--verbose**
   Increases the verbosity of messages about what is going on.

   By default, all FATAL ERROR, ERROR , WARNINGS and INFO messages are
   logged.

**-V**; \ **--version**
   Displays the current version number of the Pegasus Workflow Planner
   Software.

**-h**; \ **--help**
   Displays all the options to the **pegasus-plan** command.



Example
=======

::

   pegasus-sc-converter -i sites.xml -o sites.yml  -O YAML -v

Authors
=======

Karan Vahi ``<vahi at isi dot edu>``

Gaurang Mehta ``<gmehta at isi dot edu>``

Pegasus Team http://pegasus.isi.edu

