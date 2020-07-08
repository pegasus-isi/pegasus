==============
pegasus-status
==============

1
pegasus-status
Pegasus workflow- and run-time status
   ::

      pegasus-status [-h|--help]
                     [-V|--version] [-v|--verbose] [-d|--debug]
                     [-w|--watch [s]]
                     [-L|--[no]legend] [-c|--[no]color] [-U|--[no]utf8]
                     [-Q|--[no]queue] [-i|--[no]idle] [--[no]held]
                     [--[no]heavy] [-S|--[no]success]
                     [-j|--jobtype jt] [-s|--site sid]
                     [-u|--user name]
                     { [-l|--long] | [-r|--rows] }
                     [rundir]



Description
===========

**pegasus-status** shows the current state of the Condor Q and a
workflow, depending on settings. If no valid run directory could be
determined, including the current directory, **pegasus-status** will
show all jobs of the current user and no workflows. If a run directory
was specified, or the current directory is a valid run directory, status
about the workflow will also be shown.

Many options will modify the behavior of this program, not withstanding
a proper UTF-8 capable terminal, watch mode, the presence of jobs in the
queue, progress in the workflow directory, etc.



Options
=======

**-h**; \ **--help**
   Prints a concise help and exits.

**-V**; \ **--version**
   Prints the version information and exits.

**-w** [*sec*]; \ **--watch** [*sec*]
   This option enables the *watch mode*. In watch mode, the program
   repeatedly polls the status sources and shows them in an updating
   window. The optional argument *sec* to this option determines how
   often these sources are polled.

   We *strongly* recommend to set this interval not too low, as frequent
   polling will degrade the scheduler performance and increase the host
   load. In watch mode, the terminal size is the limiting factor, and
   parts of the output may be truncated to fit it onto the given
   terminal.

   Watch mode is disabled by default. The *sec* argument defaults to 60
   seconds.

**-L**; \ **--legend**; \ **--nolegend**
   This option shows a legend explaining the columns in the output, or
   turns off legends.

   By default, legends are turned off to save terminal real estate.

**-c**; \ **--color**; \ **--nocolor**
   This option turns on (or off) ANSI color escape sequences in the
   output. The single letter option can only switch on colors.

   By default, colors are turned off, as they will not display well on a
   terminal with black background.

**-U**; \ **--utf8**; \ **--noutf8**
   This option turns on (or off) the output of Unicode box drawing
   characters as UTF-8 encoded sequences. The single option can only
   turn on box drawing characters.

   The defaults for this setting depend on the *LANG* environment
   variable. If the variable contains a value ending in something
   indicating UTF-8 capabilities, the option is turned on by default. It
   is off otherwise.

**-Q**; \ **--queue**; \ **--noqueue**
   This option turns on (or off) the output from parsing Condor Q.

   By default, Condor Q will be parsed for jobs of the current user. If
   a workflow run directory is specified, it will furthermore be limited
   to jobs only belonging to the workflow.

**-v**; \ **--verbose**
   This option increases the expert level, showing more information
   about the condor_q state. Being an incremental option, two increases
   are supported.

   Additionally, the signals *SIGUSR1* and *SIGUSR2* will increase and
   decrease the expert level respectively during run-time.

   By default, the simplest queue view is enabled.

**-d**; \ **--debug**
   This is an internal debugging tool and should not be used outside the
   development team. As incremental option, it will show
   Pegasus-specific ClassAd tuples for each job, more in the second
   level.

   By default, debug mode is off.

**-u** *name*; \ **--user** *name*
   This option permits to query the queue for a different user than the
   current one. This may be of interest, if you are debugging the
   workflow of another user.

   By default, the current user is assumed.

**-i**; \ **--idle**; \ **--noidle**
   With this option, jobs in Condor state *idle* are omitted from the
   queue output.

   By default, *idle* jobs are shown.

**--held**; \ **--noheld**
   This option enables or disabled showing of the reason a job entered
   Condor’s *held* state. The reason will somewhat destroy the screen
   layout.

   By default, the reason is shown.

**--heavy**; \ **--noheavy**
   If the terminal is UTF-8 capable, and output is to a terminal, this
   option decides whether to use heavyweight or lightweight line drawing
   characters.

   By default, heavy lines connect the jobs to workflows.

**-j** *jt*; \ **--jobtype** *jt*
   This option filters the Condor jobs shown only to the Pegasus
   jobtypes given as argument or arguments to this option. It is a
   multi-option, and may be specified multiple times, and may use
   comma-separated lists. Use this option with an argument *help* to see
   all valid and recognized jobtypes.

   By default, all Pegasus jobtypes are shown.

**-s** *site*; \ **--site** *site*
   This option limits the Condor jobs shown to only those pertaining to
   the (remote) site *site*. This is an multi-option, and may be
   specified multiple times, and may use comma-separated lists.

   By default, all sites are shown.

**-l**; \ **--long**
   This option will show one line per sub-DAG, including one line for
   the workflow. If there is only a single DAG pertaining to the
   *rundir*, only total will be shown.

   This option is mutually exclusive with the **--rows** option. If both
   are specified, the **--long** option takes precedence.

   By default, only DAG totals (sums) are shown.

**-r**; \ **--rows**; \ **--norows**
   This option is shows the workflow summary statistics in rows instead
   of columns. This option is useful for sending the statistics in email
   and later viewing them in a proportional font.

   This option is mutually exclusive with the **--long** option. If both
   are specified, the **--long** option takes precedence.

   By default, the summary is shown in columns.

**-S**; \ **--success**; \ **--nosuccess**
   This option modifies the previous **--long** option. It will omit (or
   show) fully successful sub-DAGs from the output.

   By default, all DAGs are shown.

*rundir*
   This option show statistics about the given DAG that runs in
   *rundir*. To gather proper statistics, **pegasus-status** needs to
   traverse the directory and all sub-directories. This can become an
   expensive operation on shared filesystems.

   By default, the *rundir* is assumed to be the current directory. If
   the current directory is not a valid *rundir*, no DAG statistics will
   be shown.



Return Value
============

**pegasus-status** will typically return success in regular mode, and
the termination signal in watch mode. Abnormal behavior will result in a
non-zero exit code.



Example
=======

**pegasus-status**
   This invocation will parse the Condor Q for the current user and show
   all her jobs. Additionally, if the current directory is a valid
   Pegasus workflow directory, totals about the DAG in that directory
   are displayed.

**pegasus-status -l rundir**
   As above, but providing a specific Pegasus workflow directory in
   argument *rundir* and requesting to itemize sub-DAGs.

**pegasus-status -j help**
   This option will show all permissible job types and exit.

**pegasus-status -vvw 300 -Ll**
   This invocation will parse the queue, print it in high-expert mode,
   show legends, itemize DAG statistics of the current working
   directory, and redraw the terminal every five minutes with updated
   statistics.



Restrictions
============

Currently only supports a single (optional) run directory. If you want
to watch multiple run directories, I suggest to open multiple terminals
and watch them separately. If that is not an option, or deemed too
expensive, you can ask *pegasus-support at isi dot edu* to extend the
program.



See Also
========

condor_q(1), pegasus-statistics(1)



Authors
=======

Jens-S. Vöckler ``<voeckler at isi dot edu>``

Gaurang Mehta ``<gmehta at isi dot edu>``

Pegasus Team http://pegasus.isi.edu/
