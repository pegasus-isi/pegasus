==============
pegasus-dagman
==============

1
pegasus-dagman
Wrapper around \*condor_dagman*. Not to be run by user.
.. __description:

Description
===========

The **pegasus-dagman** is a python wrapper that invokes
**pegasus-monitord** and **condor_dagman** both. This is started
automatically by **pegasus-submit-dag** and ultimately
**condor_submit_dag**. **DO NOT USE DIRECTLY**

.. __return_value:

Return Value
============

If the **condor_dagman** and **pegasus-monitord** exit successfully,
**pegasus-dagman** exits with 0, else exits with non-zero.

.. __environment_variables:

Environment Variables
=====================

**PATH**
   The path variable is used to locate binary for **condor_dagman** and
   **pegasus-monitord**

.. __see_also:

See Also
========

pegasus-run(1) pegasus-monitord(1) pegasus-submit-dag(1)

.. __authors:

Authors
=======

Gaurang Mehta ``<gmehta at isi dot edu>``

Pegasus Team http://pegasus.isi.edu
