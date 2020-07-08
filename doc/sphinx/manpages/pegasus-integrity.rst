=================
pegasus-integrity
=================

Generates and verifies data integrity with checksums
   ::

      pegasus-integrity [--help]
                        [--generate file]
                        [--generate-yaml file]
                        [--generate-fullstat-yaml file]
                        [--generate-xmls file]
                        [--generate-fullstat-xmls file]                        
                        [--verify file]
                        [--print-timings]
                        [--debug]



Description
===========

**pegasus-integrity** either generates a file checksum (usually called
from **pegasus-kickstart**) or verifies a checksum for a file using
metadata in the current working directory (usually from PegasusLite).

Note that pegasus-integrity is a tool mostly used internally in Pegasus
workflows, but the tool can be used stand alone as well.



Options
=======

**-h**; \ **--help**
   Prints a usage summary with all the available command-line options.

**--generate** *files*
   Generates a sha256 checksum for a set of files, separated by :

**--generate-yaml** *files*
   Generate hashes for the given files, output to kickstart yaml.

**--generate-fullstat-yaml** *files*
   Generate hashes for the given file, output to kickstart yaml,
   with file stat records.

**--generate-xmls** *files*
   Generate hashes for the given file, output to kickstart xml.

**--generate-fullstat-xmls** files
   Generate hashes for the given file, output to kickstart xml
   with file stat records.

**--verify** *file*
   Verifies a file checksum as compared to what is provided in metadata.

**--print-timings**
   Display timing data after verifying files

**-d**; \ **--debug**
   Enables debugging output.




Author
======

Pegasus Team http://pegasus.isi.edu
