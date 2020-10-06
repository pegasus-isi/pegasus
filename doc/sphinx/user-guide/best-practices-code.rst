.. _best-practices-code:

===========================================
Best Practices For Developing Portable Code
===========================================

This document lists out issues for application developers to keep in mind while
developing code that will be run by Pegasus in a distributed computing environment.

Applications cannot specify the directory in which they should be run
---------------------------------------------------------------------

Application codes are either installed in some standard location at the compute
sites or staged on demand. When they are invoked, they are not invoked from the
directories where they are installed. Therefore, they should work when invoked
from any directory.

No hard-coded paths
-------------------

The applications should not hard-code directory paths as these hard coded paths
may become unusable when the application runs on different sites. Rather, these
paths should be passed via command line arguments to the job or picked up from
environment variables to increase portability.

Propogating back the right exitcode
-----------------------------------

A job in the workflow is only released for execution if its parents have
executed successfully. Hence, **it is very important that the applications
exit with the correct error code in case of success and failure**.
The application should exit with a status of 0 indicating a successful execution,
or a non zero status indicating an error has occurred. Failure to do so will result in
erroneous workflow execution where jobs might be released for execution
even though their parents had exited with an error.

Successful execution of the application code can only be
determined by an exitcode of 0. The application code should not rely upon
something being written to ``stdout`` to designate success. For example, if
the application writes to ``stdout``: ``SUCCESS``, and exits with a non
zero status the job will still be marked as ``FAILED``.

In \*nix, a quick way to see if a code is exiting with the correct code
is to execute the code and then execute echo $?.

::

   $ component-x input-file.lisp
   ... some output ...
   $ echo $?
   0

If the code is not exiting correctly, it is necessary to wrap the code
in a script that tests some final condition (such as the presence or
format of a result file) and uses exit to return correctly.

Static vs. Dynamically Linked Libraries
---------------------------------------

Since there is no way to know the profile of the machine that will be
executing the code, it is important that **dynamically linked libraries
are avoided or that reliance on them is kept to a minimum**. For example,
a component that requires ``libc 2.5`` may or may not run on a machine that
uses ``libc 2.3``. On \*nix, you can use the ``ldd`` command to see what
libraries a binary depends on.

If for some reason you install an application specific library in a non
standard location, make sure to set the ``LD_LIBRARY_PATH`` for the application
in the transformation catalog for each site.

