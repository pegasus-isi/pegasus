=====================
pegasus-cwl-converter
=====================

pegasus-cwl-converter - converts a Common Workflow Language (CWL) workflow into
a Pegasus workflow. Note that this utility is currently a **work in progress**
and we are working towards providing more comprehensive support of the CWL 
specification. ::
    pegasus-cwl-converter [-h] [-d]
                          cwl_workflow_file_path
                          workflow_inputs_file_path
                          transformation_spec_file_path
                          output_file_path

Description
===========
**pegasus-cwl-converter** is a command-line utility for converting CWL workflows
into Pegasus workflows.

Additional Python Dependencies
==============================
Pegasus does not ship with the **cwl-utils v0.3** and **jsonschema v3.2.0** 
packages used by this utility, and must be installed prior: 
``pip install pegasus-wms[cwl]``

Positional Arguments
====================
**cwl_workflow_file_path**
    Path to the file containing the CWL workflow class.

**workflow_inputs_file_path**
    YAML file specifying initial inputs to the CWL workflow.

**transformation_spec_file_path**
    YAML file specifying Pegasus specific transformation parameters. This file
    must be formatted as: ::
        <executable name0>:
            site: <str>
            is_stageable: <bool>
        .
        .
        .
        <executable nameN>:
            site: <str>
            is_stageable: <bool>

    Where each <executable name> is the name of the executable specified in the
    ``baseCommand`` field of a CWL ``CommandLineTool``. Note that if 
    ``baseCommand`` has something like ``/usr/bin/gcc``, the corresponding
    executable name that you would place in this file is just ``gcc``. The ``site``
    field specifies the site on which this transformation resides (must be one
    of the sites specified in your Pegasus **site catalog**). 

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
Say that you have the following directory, ``my_cwl_workflow``, that contains a 
main workflow CWL file, job CWL files, input specifications, input files, 
and scripts or executables used by the jobs: ::
        my_cwl_workflow
        ├── tar.cwl            <-- CWL Job class that invokes /usr/bin/tar
        ├── compile_1.cwl      <-- CWL Job class that invokes /usr/bin/gcc
        ├── compile_2.cwl      <-- CWL Job class that invokes /usr/bin/gcc
        ├── get_file_sizes.cwl <-- CWL Job class that invokes ./get_file_sizes.sh
        ├── get_file_sizes.sh  <-- stageable script
        ├── src_tarball        <-- initial input file
        ├── input.yml          <-- workflow input specs
        └── workflow.cwl       <-- CWL Workflow class that describes steps

In order to use the converter, you must create a **transformation spec file**.
This file specifies Pegasus specific properties about each CWL CommandLineTool
which are required for the conversion. In this example, the CommandLineTool files
have the following ``baseCommand`` fields:

+--------------------+-----------------------------------------------+
| file               | baseCommand                                   |
+====================+===============================================+
| tar.cwl            | /usr/bin/tar                                  |
+--------------------+-----------------------------------------------+
| compile_1.cwl      | /usr/bin/gcc                                  |
+--------------------+-----------------------------------------------+
| compile_2.cwl      | /usr/bin/gcc                                  |
+--------------------+-----------------------------------------------+
| get_file_sizes.cwl | /Users/ryan/my_cwl_workflow/get_file_sizes.sh |
+--------------------+-----------------------------------------------+

Assuming that your Pegasus **site catalog** has two sites ``local`` and 
``condorpool``, the transformation spec file would look something like: ::
    tar:
        site: condorpool
        is_stageable: False
    gcc:
        site: condorpool
        is_stageable: False
    get_file_sizes.sh:
        site: local
        is_stageable: True

Note that, ``gcc`` is only referenced once. Now that the transformation spec file
has been created (let's call this ``tr_specs.yml``), ``pegasus-cwl-converter`` can be used to convert ``workflow.cwl`` 
into Pegasus's native format by calling: ::
        pegasus-cwl-converter workflow.cwl input.yml tr_specs.yml  pegasus_workflow.yml

Authors
=======
Ryan Tanaka ``<tanaka at isi dot edu>``

Pegasus Team http://pegasus.isi.edu
    
