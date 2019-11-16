==================
pegasus-submit-dag
==================

1
pegasus-submit-dag
Wrapper around \*condor_submit_dag*. Not to be run by user.
.. __description:

Description
===========

The **pegasus-submit-dag** is a wrapper that invokes
**condor_submit_dag**. This is started automatically by **pegasus-run**.
**DO NOT USE DIRECTLY**

.. __return_value:

Return Value
============

If the workflow is submitted succesfully **pegasus-submit-dag** exits
with 0, else exits with non-zero.

.. __environment_variables:

Environment Variables
=====================

**PATH**
   The path variable is used to locate binary for **condor_submit_dag**
   and **pegasus-dagman**

.. __see_also:

See Also
========

pegasus-run(1) pegasus-dagman(1)

.. __authors:

Authors
=======

Gaurang Mehta ``<gmehta at isi dot edu>``

Pegasus Team http://pegasus.isi.edu
