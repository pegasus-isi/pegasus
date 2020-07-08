====================
pegasus-sc-converter
====================

1
pegasus-sc-converter
A client to convert site catalog from one format to another format.
   ::

      pegasus-sc-converter [-v] [-V] [-h] [-Dproperty=value…]
                           [-I fmt] [-O fmt]
                           -i infile[,infile,…] -o outfile



Description
===========

The **pegasus-sc-converter** program is used to convert the site catalog
from one format to another.

Currently, the following formats of site catalog exist.

**XML4**
   This format is a superset of previous formats. All information about
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

**XML3**
   This format is a superset of previous formats. All information about
   a site that can be described about a site can be described in this
   format. In addition, the user has finer grained control over the
   specification of directories and FTP servers that are accessible at
   the **head node** and the **worker node**.

   A sample entry in this format looks as follows

   ::

      <site  handle="local" arch="x86" os="LINUX">
        <grid  type="gt2" contact="viz-login.isi.edu/jobmanager-pbs" scheduler="PBS" jobtype="compute"/>
        <grid  type="gt2" contact="viz-login.isi.edu/jobmanager-fork" scheduler="Fork" jobtype="auxillary"/>
        <head-fs>
          <scratch>
            <shared>
              <file-server protocol="gsiftp" url="gsiftp://viz-login.isi.edu" mount-point="/scratch">
              </file-server>
              <internal-mount-point mount-point="/scratch" free-size="null" total-size="null"/>
            </shared>
          </scratch>
          <storage>
            <shared>
              <file-server protocol="gsiftp" url="gsiftp://viz-login.isi.edu" mount-point="/scratch">
              </file-server>
              <internal-mount-point mount-point="/scratch" free-size="null" total-size="null"/>
            </shared>
          </storage>
        </head-fs>
        <replica-catalog  type="LRC" url="rlsn://smarty.isi.edu">
        </replica-catalog>
        <profile namespace="env" key="GLOBUS_LOCATION" >/nfs/software/globus/default</profile>
        <profile namespace="env" key="LD_LIBRARY_PATH" >/nfs/software/globus/default/lib</profile>
        <profile namespace="env" key="PEGASUS_HOME" >/nfs/software/pegasus/default</profile>
      </site>

   This format conforms to the XML schema found at
   http://pegasus.isi.edu/schema/sc-3.0.xsd.



Options
=======

**-i** *infile*\ [,*infile*,…]; \ **--input** *infile*\ [,*infile*,…]
   The comma separated list of input files that need to be converted to
   a file in the format specified by **--oformat** option.

**-o** *outfile*; \ **--output** *outfile*
   The output file to which the output needs to be written out to.



Other Options
-------------

**-O** *fmt*; \ **--oformat** *fmt*
   The output format of the output file.

   Valid values for the output format is **XML3**, **XML4**.

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

   pegasus-sc-converter -i sites.xml -o sites.xml.new -O XML3 -vvvvv



Authors
=======

Karan Vahi ``<vahi at isi dot edu>``

Gaurang Mehta ``<gmehta at isi dot edu>``

Pegasus Team http://pegasus.isi.edu
