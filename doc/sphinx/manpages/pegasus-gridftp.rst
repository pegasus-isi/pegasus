===============
pegasus-gridftp
===============

1
pegasus-gridftp
Perform file and directory operations on remote GridFTP servers
   ::

      pegasus-gridftp ls [options] [URL…]
      pegasus-gridftp mkdir [options] [URL…]
      pegasus-gridftp rm [options] [URL…]



Description
===========

**pegasus-gridftp** is a client for Globus GridFTP servers. It enables
remote operations on files and directories via the GridFTP protocol.
This tool was created to enable more efficient remote directory creation
and file cleanup tasks in Pegasus.



Options
=======



Global Options
--------------

**-v**
   Turn on verbose output. Verbosity can be increased by specifying
   multiple -v arguments.

**-i** *FILE*
   Read a list of URLs to operate on from FILE.



rm Options
----------

**-f**
   If the URL does not exist, then ignore the error.

**-r**
   Recursively delete files and directories.



ls Options
----------

**-a**
   List files beginning with a ".".

**-l**
   Create a long-format listing with file size, creation date, type,
   permissions, etc.



mkdir Options
-------------

**-p**
   Create intermediate directories as necessary.

**-f**
   Ignore error if directory already exists



Subcommands
===========

**pegasus-gridftp** has several subcommands to implement different
operations.

**ls**
   The **ls** subcommand lists the details of a file, or the contents of
   a directory on the remote server.

**mkdir**
   The **mkdir** subcommand creates one or more directories on the
   remote server.

**rm**
   The **rm** subcommand deletes one or more files and directories from
   the remote server.



URL Format
==========

All URLs supplied to **pegasus-gridftp** should begin with "gsiftp://".



Configuration
=============

**pegasus-gridftp** uses the CoG JGlobus API to communicate with remote
GridFTP servers. Refer to the CoG JGlobus documentation for information
about configuring the API, such as how to specify the user’s proxy, etc.



Return Value
============

**pegasus-gridftp** returns a zero exist status if the operation is
successful. A non-zero exit status is returned in case of failure.



Author
======

Gideon Juve ``<gideon@isi.edu>``

Pegasus Team http://pegasus.isi.edu
