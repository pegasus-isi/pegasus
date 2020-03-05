================
pegasus-graphviz
================
1
pegasus-graphviz
Convert a DAX or DAG into a graphviz dot file
   ::

      pegasus-graphviz [options] FILE

.. __description:

Description
===========

pegasus-graphviz is a tool that generates a graphviz DOT file based on a
Pegasus DAX file or DAGMan DAG file.

.. __options:

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

.. __author:

Author
======

Gideon Juve ``<gideon@isi.edu>``

Pegasus Team http://pegasus.isi.edu
