=====================
pegasus-dax-validator
=====================
1
pegasus-dax-validator
determines if a given DAX file is valid.
   ::

      pegasus-dax-validator daxfile [verbose]

.. __description:

Description
===========

The **pegasus-dax-validator** is a simple application that determines,
if a given DAX file is valid XML. For this, it parses the file with as
many XML validity checks that the Apache Xerces XML parser framework
supports.

.. __options:

Options
=======

*daxfile*
   The location of the file containing the DAX.

*verbose*
   If any kind of second argument was specified, not limited to the
   string *verbose*, the verbose output mode is switched on.

.. __return_value:

Return Value
============

If the DAX was parsed successfully, or only *warning’s were issued, the
exit code is 0. Any 'error* or *fatal error* will result in an exit code
of 1.

Additionally, a summary statistics with counts of warnings, errors, and
fatal errors will be displayed.

.. __example:

Example
=======

The following shows the parsing of a DAX file that uses the wrong kind
of value for certain enumerations. The output shows the errors with the
respective line number and column number of the input DAX file, so that
one can find and fix them more easily. (The lines in the example were
broken to fit the manpage format.)

::

   $ pegasus-dax-validator bd.dax
   ERROR in line 14, col 110: cvc-enumeration-valid: Value 'i386' is not
    facet-valid with respect to enumeration '[x86, x86_64, ppc, ppc_64,
    ia64, sparcv7, sparcv9, amd64]'. It must be a value from the
    enumeration.
   ERROR in line 14, col 110: cvc-attribute.3: The value 'i386' of
    attribute 'arch' on element 'executable' is not valid with respect to
    its type, 'ArchitectureType'.
   ERROR in line 14, col 110: cvc-enumeration-valid: Value 'darwin' is
    not facet-valid with respect to enumeration '[aix, sunos, linux, macosx,
    windows]'. It must be a value from the enumeration.
   ERROR in line 14, col 110: cvc-attribute.3: The value 'darwin' of
    attribute 'os' on element 'executable' is not valid with respect to
    its type, 'OSType'.

   0 warnings, 4 errors, and 0 fatal errors detected.

.. __see_also:

See Also
========

Apache Xerces-J http://xerces.apache.org/xerces2-j/

.. __authors:

Authors
=======

Jens-S. Vöckler ``<voeckler at isi dot edu>``

Pegasus Team http://pegasus.isi.edu/
