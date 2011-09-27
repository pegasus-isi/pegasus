This directory contains an example of multi-level recursive workflows.

The workflows are simplistic, run on the local Condor (assumed), and
each workflow runs a 'pegasus-keg' in sleep mode plus its sub-workflow
(except for the bottom leaf, of course).

To create, plan and run the workflow, please make sure that you have
a valid Condor on your system that you can submit jobs into. 

Simply run the shell script "likethis.sh". As optional argument you can
specify the number of levels depth (suggest to stick between 2 and
16). If you do not specify anything, the depth is 3. The script will
create a 'work' directory for the workflows, a 'conf' directory with
the dynamically generated Pegasus catalogs and configuration file, and
an 'output' directory. 

You can watch the progress of the workflow using 'pegasus-status', i.e.

  pegasus-status -vvl work/<pathtotoplevelwf> -w

Once the workflow is done, you are free to run more, or poke into the
directories. It will give you an idea how multilple levels of workflows
can be nested. The abstract workflow descriptions are in the top-level
directory ($PWD). 

To clean up everything, run the "clean.sh" shell script. 

