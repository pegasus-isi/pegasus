.. _cli-pegasus-keg:

===========
pegasus-keg
===========

kanonical executable for grids

   ::

      pegasus-keg [-a appname] [ -t interval | -T interval] [-s interval]
                  [-l logname] [-P prefix] [-o fn [..]] [-i fn [..]]
                  [-G sz [..]] [-m memory] [-C] [-e env [..]]
                  [-p param [..]] [-u data_unit]



Description
===========

The kanonical executable is a stand-in for regular binaries in a DAG -
but not for their arguments. It allows to trace the shape of the
execution of a DAG, and thus is an aid to debugging DAG related issues.

Key feature of **pegasus-keg** is that it can copy any number of input
files, including the *generator* case, to any number of output files,
including the *datasink* case. In addition, it records the IPv4 address
and hostname of the host it ran upon at the end of each output file.

The workflow of the Keg tool is as follows: - if **-m** - allocate a
memory buffer of the specified amount - if **-i** - read all input files
into the memory buffer - if **-o** - write either the input files
content (or a generated content if **-G**) to output files - if **-T** -
generate CPU load for the specified time period decreased by the time
period spent on IO stuff; if the IO stuff time period exceeds the time
period specified here the program exits with code status 3 - if **-t** -
wait/sleep for the specified time period decreased by time periods spent
on IO stuff (and CPU load generating if any); if the time period spent
on previous activities exceeds the amount specified here the program
exits with code status 3 - if **-s** - sleep for the remaining **-s**
time (see below) - if **-l** - write info to the specified log file.



Arguments
=========

The **-e**, **-i**, **-o**, **-p** and **-G** arguments allow lists with
arbitrary number of arguments. These options may also occur repeatedly
on the command line. The file options may be provided with the special
filename - to indicate *stdout* in append mode for writing, or *stdin*
for reading. The **-a**, **-l** , **-P** , **-T** and **-t** arguments
should only occur a single time with a single argument.

If **pegasus-keg** is called without any arguments, it will display its
usage and exit with success.

**-a appname**
   This option allows **pegasus-keg** to display a different name as its
   applications. This mode of operation is useful in make-believe mode.
   The default is the basename of *argv[0]*.

**-e env [..]**
   Accepted for backward compatibility; the values are not reported.

**-i infile [..]**
   The **pegasus-keg** binary can work on any number of input files. For
   each output file, every input file will be opened, and its content
   copied to the output file. The input file content is bracketed
   between an start and end section, see below. Input files are copied
   byte for byte; a directory reads as empty. By default, no input files
   are read.

**-l logfile**
   The *logfile* is the name of a file to append atomically the
   identification line, see below. The atomic write guarantees that the
   information will not interleave with other processes that
   simultaneously write to the same file. The default is not to use any
   log file.

**-o outfile [..]**
   The **pegasus-keg** can work on any number of output files. For each
   output file, every input file will be opened, and its content copied
   to the output file, preceded by the **-P** prefix. The input file
   content is bracketed between an start and end section, see the
   example. After all input
   files are copied, the data dump from this instance of **pegasus-keg**
   is appended to the output file. Without output files, **pegasus-keg**
   operates in *data sink* mode. Accept also
   *<filename>=<filesize>[<data_unit>]* form, where the optional
   <data_unit> is a character supported by the **-u** switch (default B).
   Parent directories of a plain *<filename>* are created as needed.

**-G size [..]**
   If you want **pegasus-keg** to generate a lot of output, the
   generator option will do that for you. Just specify how much, in
   bytes (but you can change it with **-u** switch), you want. You can
   specify more than 1 value here if you specify more than 1 output
   file. Subsequent values specified here will correspond to sizes of
   subsequent output files. This option is off by default.

**-u data_unit**
   By default, the output data generator (the **-G** switch) generates
   the specified amount of data in Bytes. You can alter this behavior
   with this switch. It accepts one of the following characters as
   *data_unit* value: B for Bytes, K for KiloBytes, M for MegaBytes, and
   G for GigaBytes.

**-C**
   Accepted for backward compatibility; has no effect.

**-p string [..]**
   Accepted for backward compatibility; the strings are not reported.

**-P prefix**
   A prefix string written once at the start of each output file, before
   the copied input files. It is not written to generated outputs
   (**-G** or *<filename>=<filesize>*). By default, two spaces are used as
   prefix string.

**-t interval**
   The interval is an amount of sleep time that the **pegasus-keg**
   executable is to sleep in seconds. This can be used to emulate light
   work without straining the pool resources. The interval is measured
   from the start of **pegasus-keg**, so it includes the time spent on
   I/O and on the **-T** spin, which comes first. The default is no
   sleep time. If that time already exceeds the interval,
   **pegasus-keg** exits with code 3.

**-T interval**
   The interval is an amount of busy spin time that the **pegasus-keg**
   executable is to simulate intense computation in seconds. The
   simulation is done by random julia set calculations. This option can
   be used to emulate an intense work to strain pool resources. The
   interval is measured from the start of **pegasus-keg**, so it
   includes the time spent on I/O. The spin comes before the **-t**
   sleep. The default is no spin time. If the I/O time already exceeds
   the interval, **pegasus-keg** exits with code 3.

**-s interval**
   The interval is an amount of sleep time that the **pegasus-keg**
   executable is to sleep in seconds after performing any I/Os.
   With this option **pegasus-keg** will perform I/Os and then sleep
   for the amount of seconds specified (i.e, **pegasus-keg**
   will run for I/O time + interval). When combined with **-t** and/or
   **-T**, the extra sleep is the absolute difference between this
   interval and each of them, and is dropped entirely if **-t** or
   **-T** is larger than the result.


**-m memory**
   The amount of memory ([MB]) the Keg process should use. This option
   can be used to emulated application’s memory requirements. The
   default is not to allocate anything.



Return Value
============

Execution as planned will return 0. The failure to create the parent
directory of an output file will return 1, the failure to open an input
or output file, or to write an output file (e.g. a full disk), will
return with exit code 2. A log file that cannot be opened only produces
a warning, and a failed **-m** allocation only reports
*Memory allocation failure* and continues. If the time spent on IO exceeds the
specified CPU load period with **-T** or the time spent on IO and CPU
load exceeds the specified wall time with **-t** the return code will
be 3.



Example
=======

The example shows the bracketing of an input file, and the copy produced
on the output file. For illustration purposes, the output file is
connected to *stdout* :

::

   $ date > xx
   $ pegasus-keg -i xx -o -
     --- start xx ----
   Thu May  5 10:55:45 PDT 2011
   --- final xx ----
   IP addr and hostname: 128.9.xxx.xxx (xxx.isi.edu)



Restrictions
============

The host address is determined from the primary interface. If there is
no active interface besides loopback, the host address will default to
0.0.0.0. If the host address is within a *virtual private network*
address range, the address is followed by *(VPN)* instead of a hostname,
and no reverse address lookup will be attempted.
