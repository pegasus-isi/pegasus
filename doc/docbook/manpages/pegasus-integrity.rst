=================
pegasus-integrity
=================

1
pegasus-integrity
Generates and verifies data integrity with checksums
   ::

      pegasus-integrity [-h]
                          [--generate-sha256 file]
                          [--verify file]
                          [--debug]

.. __description:

Description
===========

**pegasus-integrity** either generates a file checksum (usually called
from **pegasus-kickstart**) or verifies a checksum for a file using
metadata in the current working directory.

Note that pegasus-integrity is a tool mostly used internally in Pegasus
workflows, but the tool can be used stand alone as well.

.. __options:

Options
=======

**-h**; \ **--help**
   Prints a usage summary with all the available command-line options.

**--generate-sha256** *file*
   Generates a sha256 checksum for a file.

**--verify** *file*
   Verifies a file checksum as compared to what is provided in metadata.

**-d**; \ **--debug**
   Enables debugging output.

.. __author:

Author
======

Pegasus Team http://pegasus.isi.edu
