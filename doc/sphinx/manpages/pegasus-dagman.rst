==============
pegasus-dagman
==============

1
pegasus-dagman
Wrapper around \*condor_dagman*. Not to be run by user.


Description
===========

The **pegasus-dagman** is a python wrapper that invokes
**pegasus-monitord** and **condor_dagman** both. This is started
automatically by **pegasus-submit-dag** and ultimately
**condor_submit_dag**. **DO NOT USE DIRECTLY**



Return Value
============

If the **condor_dagman** and **pegasus-monitord** exit successfully,
**pegasus-dagman** exits with 0, else exits with non-zero.



Environment Variables
=====================

**PATH**
   The path variable is used to locate binary for **condor_dagman** and
   **pegasus-monitord**



See Also
========

pegasus-run(1) pegasus-monitord(1) pegasus-submit-dag(1)



Authors
=======

Gaurang Mehta ``<gmehta at isi dot edu>``

Pegasus Team http://pegasus.isi.edu
