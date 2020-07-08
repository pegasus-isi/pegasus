================
pegasus-monitord
================

tracks a workflow progress, mining information

   ::

      pegasus-monitord [--help|-help] [--verbose|-v]
                       [--adjust|-a i] [--condor-daemon|-N]
                       [--job|-j jobstate.log file]
                       [--conf properties file]
                       [--no-recursive] [--no-database | --no-events]
                       [--replay|-r] [--no-notifications]
                       [--notifications-max max_notifications]
                       [--notifications-timeout timeout]
                       [--sim|-s millisleep] [--db-stats]
                       [--skip-stdout] [--force|-f]
                       [--output-dir | -o dir]
                       [--dest|-d PATH or URL] [--encoding|-e bp | bson]
                       DAGMan output file



Description
===========

This program follows a workflow, parsing the output of DAGMAN’s
dagman.out file. In addition to generating the jobstate.log file,
**pegasus-monitord** can also be used mine information from the workflow
dag file and jobs' submit and output files, and either populate a
database or write a NetLogger events file with that information.
**pegasus-monitord** can also perform notifications when tracking a
workflow’s progress in real-time.



Options
=======

**-h**; \ **--help**
   Prints a usage summary with all the available command-line options.

**-v**; \ **--verbose**
   Sets the log level for **pegasus-monitord**. If omitted, the default
   *level* will be set to *WARNING*. When this option is given, the log
   level is changed to *INFO*. If this option is repeated, the log level
   will be changed to *DEBUG*.

   The log level in **pegasus-monitord** can also be adjusted
   interactively, by sending the *USR1* and *USR2* signals to the
   process, respectively for incrementing and decrementing the log
   level.

**-a** *i*; \ **--adjust** *i*
   For adjusting time zone differences by *i* seconds, default is 0.

**-N**; \ **--condor-daemon**
   Condor daemon mode. This is used when monitord is invoked by
   pegasus-dagman. It just causes monitord to create a new process
   group.

**-j** *jobstate.log file*; \ **--job** *jobstate.log file*
   Alternative location for the *jobstate.log* file. The default is to
   write a *jobstate.log* in the workflow directory. An absolute file
   name should only be used if the workflow does not have any
   sub-workflows, as each sub-workflow will generate its own
   *jobstate.log* file. If an alternative, non-absolute, filename is
   given with this option, **pegasus-monitord** will create one file in
   each workflow (and sub-workflow) directory with the filename provided
   by the user with this option. If an absolute filename is provided and
   sub-workflows are found, a warning message will be printed and
   **pegasus-monitord** will not track any sub-workflows.

**--conf** *properties_file*
   is an alternative file containing properties in the *key=value*
   format, and allows users to override values read from the
   *braindump.txt* file. This option has precedence over the properties
   file specified in the *braindump.txt* file. Please note that these
   properties will apply not only to the main workflow, but also to all
   sub-workflows found.

**--no-recursive**
   This options disables **pegasus-monitord** to automatically follow
   any sub-workflows that are found.

**--nodatabase**; \ **--no-database**; \ **--no-events**
   Turns off generating events (when this option is given,
   **pegasus-monitord** will only generate the jobstate.log file). The
   default is to automatically log information to a SQLite database (see
   the **--dest** option below for more details). This option overrides
   any parameter given by the **--dest** option.

**-r**; \ **--replay**
   This option is used to replay the output of an already finished
   workflow. It should only be used after the workflow is finished (not
   necessarily successfully). If a *jobstate.log* file is found, it will
   be rotated. However, when using a database, all previous references
   to that workflow (and all its sub-workflows) will be erased from it.
   When outputing to a bp file, the file will be deleted. When running
   in replay mode, **pegasus-monitord** will always run with the
   **--no-daemon** option, and any errors will be output directly to the
   terminal. Also, **pegasus-monitord** will not process any
   notifications while in replay mode.

**--no-notifications**
   This options disables notifications completely, making
   **pegasus-monitord** ignore all the .notify files for all workflows
   it tracks.

**--notifications-max** *max_notifications*
   This option sets the maximum number of concurrent notifications that
   **pegasus-monitord** will start. When the *max_notifications* limit
   is reached, **pegasus-monitord** will queue notifications and wait
   for a pending notification script to finish before starting a new
   one. If *max_notifications* is set to 0, notifications will be
   disabled.

**--notifications-timeout** *timeout*
   Normally, **pegasus-monitord** will start a notification script and
   wait indefinitely for it to finish. This option allows users to set
   up a maximum *timeout* that **pegasus-monitord** will wait for a
   notification script to finish before terminating it. If notification
   scripts do not finish in a reasonable amount of time, it can cause
   other notification scripts to be queued due to the maximum number of
   concurrent scripts allowed by **pegasus-monitord**. Additionally,
   until all notification scripts finish, **pegasus-monitord** will not
   terminate.

**-s** *millisleep*; \ **--sim** *millisleep*
   This option simulates delays between reads, by sleeping *millisleep*
   milliseconds. This option is mainly used by developers.

**--db-stats**
   This option causes the database module to collect and print database
   statistics at the end of the execution. It has no effect if the
   **--no-database** option is given.

**--skip-stdout**
   This option causes **pegasus-monitord** not to populate jobs' stdout
   and stderr into the BP file or the Stampede database. It should be
   used to avoid increasing the database size substantially in cases
   where jobs are very verbose in their output.

**-f**; \ **--force**
   This option causes **pegasus-monitord** to skip checking for another
   instance of itself already running on the same workflow directory.
   The default behavior prevents two or more **pegasus-monitord**
   instances from starting and running simultaneously (which would cause
   the bp file and database to be left in an unstable state). This
   option should noly be used when the user knows the previous instance
   of **pegasus-monitord** is **NOT** running anymore.

**-o** *dir*; \ **--ouput-dir** *dir*
   When this option is given, **pegasus-monitord** will create all its
   output files in the directory specified by *dir.* This option is
   useful for allowing a user to debug a workflow in a directory the
   user does not have write permissions. In this case, all files
   generated by **pegasus-monitord** will have the workflow *wf_uuid* as
   a prefix so that files from multiple sub-workflows can be placed in
   the same directory. This option is mainly used by
   **pegasus-analyzer**. It is important to note that the location for
   the output BP file or database is not changed by this option and
   should be set via the **--dest** option.

**-d** *URL* *params*; \ **--dest** *URL* *params*
   This option allows users to specify the destination for the log
   events generated by **pegasus-monitord**. If this option is omitted,
   **pegasus-monitord** will create a SQLite database in the workflow’s
   run directory with the same name as the workflow, but with a
   *.stampede.db* prefix. For an *empty* scheme, *params* are a file
   path with **-** meaning standard output. For a *x-tcp* scheme,
   *params* are *TCP_host[:port=14380]*. For a database scheme, *params*
   are a *SQLAlchemy engine URL* with a database connection string that
   can be used to specify different database engines. Please see the
   examples section below for more information on how to use this
   option. Note that when using a database engine other than **sqlite**,
   the necessary Python database drivers will need to be installed.

**-e** *encoding*; \ **--encoding** *encoding*
   This option specifies how to encode log events. The two available
   possibilities are *bp* and *bson*. If this option is not specified,
   events will be generated in the *bp* format.

*DAGMan_output_file*
   The *DAGMan_output_file* is the only requires command-line argument
   in **pegasus-monitord** and must have the *.dag.dagman.out*
   extension.



Return Value
============

If the plan could be constructed, **pegasus-monitord** returns with an
exit code of 0. However, in case of error, a non-zero exit code
indicates problems. In that case, the *logfile* should contain
additional information about the error condition.



Environment Variables
=====================

**pegasus-monitord** does not require that any environmental variables
be set. It locates its required Python modules based on its own
location, and therefore should not be moved outside of Pegasus' bin
directory.



Examples
========

Usually, **pegasus-monitord** is invoked automatically by
**pegasus-run** and tracks the workflow progress in real-time, producing
the *jobstate.log* file and a corresponding SQLite database. When a
workflow fails, and is re-submitted with a rescue DAG,
**pegasus-monitord** will automatically pick up from where it left
previously and continue the *jobstate.log* file and the database.

If users need to create the *jobstate.log* file after a workflow is
already finished, the **--replay \| -r** option should be used when
running **pegasus-monitord** manually. For example:

::

   $ pegasus_monitord -r diamond-0.dag.dagman.out

will launch **pegasus-monitord** in replay mode. In this case, if a
*jobstate.log* file already exists, it will be rotated and a new file
will be created. If a *diamond-0.stampede.db* SQLite database already
exists, **pegasus-monitord** will purge all references to the workflow
id specified in the *braindump.txt* file, including all sub-workflows
associated with that workflow id.

::

   $ pegasus_monitord -r --no-database diamond-0.dag.dagman.out

will do the same thing, but without generating any log events.

::

   $ pegasus_monitord -r --dest `pwd`/diamond-0.bp diamond-0.dag.dagman.out

will create the file *diamond-0.bp* in the current directory, containing
NetLogger events with all the workflow data. This is in addition to the
*jobstate.log* file.

For using a database, users should provide a database connection string
in the format of:

::

   dialect://username:password@host:port/database

Where *dialect* is the name of the underlying driver (*mysql*, *sqlite*,
*oracle*, *postgres*) and *database* is the name of the database running
on the server at the *host* computer.

If users want to use a different *SQLite* database, **pegasus-monitord**
requires them to specify the absolute path of the alternate file. For
example:

::

   $ pegasus_monitord -r --dest sqlite:////home/user/diamond_database.db diamond-0.dag.dagman.out

Here are docs with details for all of the supported drivers:
http://www.sqlalchemy.org/docs/05/reference/dialects/index.html

Additional per-database options that work into the connection strings
are outlined there.

It is important to note that one will need to have the appropriate db
interface library installed. Which is to say, *SQLAlchemy* is a wrapper
around the mysql interface library (for instance), it does not provide a
*MySQL* driver itself. The **Pegasus** distribution includes both
**SQLAlchemy** and the **SQLite** Python driver.

As a final note, it is important to mention that unlike when using
*SQLite* databases, using **SQLAlchemy** with other database servers,
e.g. *MySQL* or *Postgres*, the target database needs to exist. So, if a
user wanted to connect to:

::

   mysql://pegasus-user:supersecret@localhost:localport/diamond

it would need to first connect to the server at *localhost* and issue
the appropriate create database command before running
**pegasus-monitord** as **SQLAlchemy** will take care of creating the
tables and indexes if they do not already exist.



See Also
========

pegasus-run(1)



Authors
=======

Gaurang Mehta ``<gmehta at isi dot edu>``

Fabio Silva ``<fabio at isi dot edu>``

Karan Vahi ``<vahi at isi dot edu>``

Jens-S. Vöckler ``<voeckler at isi dot edu>``

Pegasus Team http://pegasus.isi.edu
