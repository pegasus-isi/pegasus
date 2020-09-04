.. _cli-pegasus-graphviz:

================
pegasus-graphviz
================

Convert a Pegasus workflow YAML or DAX file, or Condor DAGMan file into a 
graphviz dot file

   ::

      pegasus-graphviz [options] FILE



Description
===========

pegasus-graphviz is a tool that generates a graphviz DOT file based on a
Pegasus DAX file or DAGMan DAG file. **FILE** in ``pegasus-graphviz [options] FILE``
must have the extension ``.yml``, ``.dax``, ``.xml``, or ``.dag``.



Options
=======

**-h**; \ **--help**
   Show the help message

**-s**; \ **--nosimplify**
   Do not simplify the graph by removing redundant edges. [default:
   False]

**-l** *LABEL*; \ **--label** *LABEL*
   What attribute to use for labels. One of *label*,\ *xform*, or *id*.
   For *label*, the transformation is used for jobs that have no
   node-label. [default: label]

**-o** *FILE*; \ **--output** *FILE*
   Write output to FILE [default: stdout]

**-r** *XFORM*; \ **--remove** *XFORM*
   Remove jobs from the workflow by transformation name

**-W** *WIDTH*; \ **--width** *WIDTH*
   Width of the digraph.

**-H** *HEIGHT*; \ **--height** *HEIGHT*
   Height of the digraph.

**-f**; \ **--files**
   Include files. This option is only valid for DAX files. [default:
   false]

Example
=======

::

   pegasus-graphviz workflow.yml

::

   pegasus-graphviz workflow.dax

:: 

   pegasus-graphviz workflow.yml --label=xform-id --output=wf.dot

Author
======

Gideon Juve ``<gideon@isi.edu>``

Pegasus Team http://pegasus.isi.edu
