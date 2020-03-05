==============
pegasus-invoke
==============

1
pegasus-invoke
invokes a command from a file
   ::

      pegasus-invoke ( app | @fn ) [ arg | *@fn [..]]

.. __description:

Description
===========

The **pegasus-invoke** tool invokes a single application with as many
arguments as your Unix permits (128k characters for Linux). Arguments
are come from two places, either the command-line as regular arguments,
or from a special file, which contains one argument per line.

The **pegasus-invoke** tool became necessary to work around the 4k
argument length limit in Condor. It also permits to use arguments inside
argument files without worry about shell, Condor or Globus escape
necessities. All argument file contents are passed as is, one line per
argument entry.

.. __arguments:

Arguments
=========

**-d**
   This option increases the debug level. Currently, only debugging or
   no debugging is distinguished. Debug message are generated on
   *stdout* . By default, debugging is disabled.

**-h**
   This option prints the help message and exits the program.

**--**
   This option stops any option processing. It may only be necessary, if
   the application is stated on the command-line, and starts with a
   hyphen itself.The first argument must either be the application to
   run as fully-specified location (either absolute, or relative to
   current wd), or a file containing one argument per line. The *PATH*
   environment variables is **not** used to locate an application.
   Subsequent arguments may either be specified explicitely on the
   commandline. Any argument that starts with an at (@) sign is taken to
   introduce a filename, which contains one argument per line. The
   textual file may contain long arguments and filenames. However,
   Unices still impose limits on the maximum length of a directory name,
   and the maximum length of a file name. These lengths are not checked,
   because **pegasus-invoke** is oblivious of the application (e.g. what
   argument is a filename, and what argument is a mere string resembling
   a filename).

.. __return_value:

Return Value
============

The **pegasus-invoke** tool returns 127, if it was unable to find the
application. It returns 126, if there was a problem parsing the file.
All other exit status, including 126 and 127, come from the application.

.. __see_also:

See Also
========

**pegasus-kickstart(1)**

.. __example:

Example
=======

::

   $ echo "/bin/date" > X
   $ echo "-Isec" >> X
   $ pegasus-invoke @X
   2005-11-03T15:07:01-0600

Recursion is also possible. Please mind not to use circular inclusions.
Also note how duplicating the initial at (@) sign will escape its
meaning as inclusion symbol.

::

   $ cat test.3
   This is test 3

   $ cat test.2
   /bin/echo
   @test.3
   @@test.3

   $ pegasus-invoke @test.2
   This is test 3 @test.3

.. __restrictions:

Restrictions
============

While the arguments themselves may contain files with arguments to
parse, starting with an at (@) sign as before, the maximum recursion
limit is 32 levels of inclusions. It is not possible (yet) to use
*stdin* as source of inclusion.

.. __history:

History
=======

As you may have noticed, **pegasus-invoke** had the name **invoke** in
previous incantations. We are slowly moving to the new name to avoid
clashes in a larger OS installation setting. However, there is no
pertinent need to change the internal name, too, as no name clashes are
expected.

.. __authors:

Authors
=======

Mike Wilde <wilde at mcs dot anl dot gov>

Jens-S. Vöckler <voeckler at isi dot edu>

Pegasus **http://pegasus.isi.edu/**
