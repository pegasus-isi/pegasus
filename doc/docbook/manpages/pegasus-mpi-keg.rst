===============
pegasus-mpi-keg
===============

1
pegasus-mpi-keg
MPI version of KEG
   ::

      pegasus-mpi-keg [-a appname] [-t interval |-T interval] [-l logname]
                  [-P prefix] [-o fn [..]] [-i fn [..]] [-G sz] [-m memory]
                  [-r root_memory_allocation] [-C] [-e env [..]] [-p parm [..]]

.. __description:

Description
===========

The parallel version of kanonical executable is a stand-in for parallel
binaries in a DAG - but not for their arguments. It allows to trace the
shape of the execution of a DAG, and thus is an aid to debugging DAG
related issues.

It works in the same way as the sequential version of **pegasus-keg**
but it is intended to be executed as an MPI task. **pegasus-mpi-keg**
accepts the same parameters as **pegasus-keg**, so please refer to the
**pegasus-keg** manual page for more details.

.. __arguments:

Arguments
=========

The same as **pegasus-keg**. But there are some MPI-specific arguments.

**-r root_memory_allocation_only**
   Works use only with the **-m** option. When set, the memory
   allocation will take place in the root MPI process only. By default,
   each MPI processe allocates the amount of memory set by the **-m**
   option.

.. __return_value:

Return Value
============

The same as **pegasus-keg**.

.. __example:

Example
=======

The example shows the bracketing of an input file, and the copy produced
on the output file. For illustration purposes, the output file is
connected to *stdout* :

::

   $ date > xx
   $ mpiexec -n 2 ./pegasus-mpi-keg -i xx -p a b c -o -
   --- start xx ----
     Tue Dec  2 17:35:39 PST 2014
   --- final xx ----
   Timestamp Today: 20141202T173553.184-08:00 (1417570553.184;0.001)
   Applicationname: pegasus-mpi-keg [36116e11c0735993bf54264953194e626fe4ab7e 2014-11-25] @ 138.25.147.42 (myc-2.local)
   Current Workdir: /opt/pegasus/default/bin/pegasus-mpi-keg
   Systemenvironm.: x86_64-Darwin 14.0.0
   Processor Info.: 4 x Intel(R) Core(TM) i5-4278U CPU @ 2.60GHz
   Load Averages  : 1.240 1.354 1.434
   Memory Usage MB: 8192 total, 161 avail, 3599 active, 2496 inactive, 1077 wired
   Swap Usage   MB: 2048 total, 1256 free
   Filesystem Info: /                        hfs   232GB total,    66GB avail
   Output Filename: -
   Input Filenames: xx
   Other Arguments: a b c

.. __restrictions:

Restrictions
============

The same as **pegasus-keg**.

.. __authors:

Authors
=======

Pegasus - http://pegasus.isi.edu/
