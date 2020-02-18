=====================
pegasus-cwl-converter
=====================

pegasus-cwl-converter - converts a Common Workflow Language (CWL) workflow into
a Pegasus workflow. Note that this utility is currently a **work in progress**
and we are working towards providing more comprehensive support of the CWL 
specification. ::
    pegasus-cwl-converter [-h] [-d]
                          cwl_workflow_file_path input_file_spec_path
                          output_file_path

Description
===========
**pegasus-cwl-converter** is a command-line utility for converting CWL workflows
into Pegasus workflows.

Additional Python Dependencies
==============================
Pegasus does not ship with the **cwl-utils** package used by this utility,
therefore you must install it prior: ``pip install cwl-utils``

Positional Arguments
====================
**cwl_workflow_file_path**
    Path to the file containing the CWL workflow class.

**input_file_spec_path**
    YAML file describing initial inputs to the CWL workflow.

**output_file_path**
    Desired path of the generated Pegasus workflow file. 

Optional Arguments
==================
**-h**
    Prints a concise help and exits.

**-d**
    Enables debug mode in the logger.  

Example
=======
Given the following directory, ``my_cwl_workflow``, that contains a main workflow CWL file,
job CWL files, input specifications, input files, and scripts or executables
used by the jobs: ::
        my_cwl_workflow
        ├── tar.cwl            <-- CWL Job class that invokes /usr/bin/tar
        ├── compile_1.cwl      <-- CWL Job class that invokes /usr/bin/gcc
        ├── compile_2.cwl      <-- CWL Job class that invokes /usr/bin/gcc
        ├── get_file_sizes.cwl <-- CWL Job class that invokes ./get_file_sizes.sh
        ├── get_file_sizes.sh  <-- stageable script
        ├── input.yml          <-- workflow input specs
        ├── src_tarball        <-- initial input file
        └── workflow.cwl       <-- CWL Workflow class that describes steps

``pegasus-cwl-converter`` can be invoked to convert ``workflow.cwl`` into a format 
native to Pegasus by calling: ::
        pegasus-cwl-converter workflow.cwl input.yml pegasus_workflow 

Authors
=======
Ryan Tanaka ``<tanaka at isi dot edu>``

Pegasus Team http://pegasus.isi.edu
    
