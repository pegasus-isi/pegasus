.. _cwl-support:

===========
CWL Support
===========

The `Common Workflow Language (CWL) <https://www.commonwl.org/>`__ is a standardized
way of describing workflow pipelines and tools. Users with existing CWL workflows
can use the :ref:`pegasus-cwl-converter <cli-pegasus-cwl-converter>` to convert
their workflows from CWL into Pegasus's native format. Once converted, 
:ref:`pegasus-plan <cli-pegasus-plan>` can be used to plan, and submit the workflow
for execution. 

.. Do not want to create a subsection here, using a plain html header.

.. raw:: html

    <h2>A Simple Example</h2>

.. figure:: ../images/examples-diamond-clear.png
   :name: cwl-diamond-example
   :align: center
   :scale: 70%

Consider the workflow depicted above. The *CWL* tab below illustrates what this
workflow might look like when represented using CWL. The *Pegasus* tab illustrates
the resulting translation from CWL to Pegasus's native format. 

While there are inherent similarities between the two formats, this is not a one to
one mapping. As such, some information needs to be provided to the
:ref:`pegasus-cwl-converter <cli-pegasus-cwl-converter>` in order for the translation
to be done correctly (see highlighted lines in the Pegasus tab). 


This additional information is as follows:

- For each transformation:
    - What site does it reside on? (e.g. ``local``, ``condorpool``, etc)
    - Can this transformation be staged to another site?

.. tabs::

    .. tab:: CWL

        .. code-block:: yaml
            :linenos:

            cwlVersion: v1.1
            class: Workflow
            inputs:
                f.a: File
            outputs:
                final_output:
                    type: File
                    outputSource: analyze/of
            steps:
                preprocess:
                    run:
                        cwlVersion: v1.1
                        class: CommandLineTool
                        baseCommand: $PEGASUS_LOCAL_BIN_DIR/pegasus-keg
                        arguments: ["-a", "preprocess", "-T60", "-o", "f.b1", "-o", "f.b2"]
                        requirements:
                            DockerRequirement:
                                dockerPull: opensciencegrid/osgvo-el7
                        inputs:
                            if:
                                type: File
                                inputBinding:
                                    prefix: -i
                                    separate: true
                                    position: 0
                        outputs:
                            of1:
                                type: File
                                outputBinding:
                                    glob: f.b1
                            
                            of2:
                                type: File
                                outputBinding:
                                    glob: f.b2

                    in:
                        if: f.a
                    out: [of1, of2]

                findrange1:
                    run:
                        cwlVersion: v1.1
                        class: CommandLineTool
                        baseCommand: $PEGASUS_LOCAL_BIN_DIR/pegasus-keg
                        arguments: ["-a", "findrange", "-T60", "-o", "f.c1"]
                        requirements:
                            DockerRequirement:
                                dockerPull: opensciencegrid/osgvo-el7
                        inputs:
                            if:
                                type: File
                                inputBinding:
                                    prefix: -i
                                    separate: true
                                    position: 0

                        outputs:
                            of:
                                type: File
                                outputBinding:
                                    glob: f.c1
                    in:
                        if: preprocess/of1
                    out: [of]

                findrange2:
                    run:
                        cwlVersion: v1.1
                        class: CommandLineTool
                        baseCommand: $PEGASUS_LOCAL_BIN_DIR/pegasus-keg
                        arguments: ["-a", "findrange", "-T60", "-o", "f.c2"]
                        requirements:
                            DockerRequirement:
                                dockerPull: opensciencegrid/osgvo-el7
                        inputs:
                            if:
                                type: File
                                inputBinding:
                                    prefix: -i
                                    separate: true
                                    position: 0

                        outputs:
                            of:
                                type: File
                                outputBinding:
                                    glob: f.c2
                    in:
                        if: preprocess/of2
                    out: [of]

                analyze:
                    run:
                        cwlVersion: v1.1
                        class: CommandLineTool
                        baseCommand: $PEGASUS_LOCAL_BIN_DIR/pegasus-keg
                        arguments: ["-a", "analyze", "-T60", "-o", "f.d"]
                        requirements:
                            DockerRequirement:
                                dockerPull: opensciencegrid/osgvo-el7
                        inputs:
                            if1:
                                type: File
                                inputBinding:
                                    prefix: -i
                                    separate: true
                                    position: 0
                            
                            if2:
                                type: File
                                inputBinding:
                                    position: 1

                        outputs:
                            of:
                                type: File
                                outputBinding:
                                    glob: f.d  
                    in:
                        if1: findrange1/of
                        if2: findrange2/of
                    out: [of] 

    .. tab:: Pegasus

        .. code-block:: yaml
            :linenos:
            :emphasize-lines: 17,19

            x-pegasus:
            apiLang: python
            createdBy: ryantanaka
            createdOn: 11-11-20T16:45:46Z
            pegasus: '5.0'
            name: cwl-converted-pegasus-workflow
            replicaCatalog:
            replicas:
            - lfn: f.a
                pfns:
                - site: local
                pfn: /Users/ryantanaka/symlinks/isip/test/core/047-cwl-docker-black-diamond/cwl/f.a
            transformationCatalog:
            transformations:
            - name: pegasus-keg
                sites:
                - name: local
                pfn: /Users/ryantanaka/ISI/pegasus/dist/pegasus-5.0.0dev/bin/pegasus-keg
                type: stageable
                container: opensciencegrid_osgvo-el7
            containers:
            - name: opensciencegrid_osgvo-el7
                type: docker
                image: docker://opensciencegrid/osgvo-el7
                image.site: local
            jobs:
            - type: job
            name: pegasus-keg
            id: analyze
            arguments:
            - -a
            - analyze
            - -T60
            - -o
            - f.d
            - -i f.c1
            - f.c2
            uses:
            - lfn: f.c1
                type: input
            - lfn: f.c2
                type: input
            - lfn: f.d
                type: output
                stageOut: true
                registerReplica: true
            - type: job
            name: pegasus-keg
            id: findrange1
            arguments:
            - -a
            - findrange
            - -T60
            - -o
            - f.c1
            - -i f.b1
            uses:
            - lfn: f.c1
                type: output
                stageOut: true
                registerReplica: true
            - lfn: f.b1
                type: input
            - type: job
            name: pegasus-keg
            id: findrange2
            arguments:
            - -a
            - findrange
            - -T60
            - -o
            - f.c2
            - -i f.b2
            uses:
            - lfn: f.b2
                type: input
            - lfn: f.c2
                type: output
                stageOut: true
                registerReplica: true
            - type: job
            name: pegasus-keg
            id: preprocess
            arguments:
            - -a
            - preprocess
            - -T60
            - -o
            - f.b1
            - -o
            - f.b2
            - -i f.a
            uses:
            - lfn: f.b2
                type: output
                stageOut: true
                registerReplica: true
            - lfn: f.a
                type: input
            - lfn: f.b1
                type: output
                stageOut: true
                registerReplica: true
            jobDependencies:
            - id: findrange1
            children:
            - analyze
            - id: findrange2
            children:
            - analyze
            - id: preprocess
            children:
            - findrange1
            - findrange2

.. raw:: html

    <h2>Using pegasus-cwl-converter</h2>

Using the :ref:`pegasus-cwl-converter <cli-pegasus-cwl-converter>`, how can we
convert the above CWL workflow into Pegasus's native format?

First, we need to create a file ``tr_spec.yml``, which contains Pegasus specific
information about the transformations (executables) used in the workflow.

.. code-block:: yaml

    pegasus-keg:
        site: local
        is_stageable: True

Next, we need a file that specifies where the initial input files to the workflow
are physically located. For CWL workflows, input file specifications are typically
specified in a separate YAML file. For the CWL workflow above, lets say that 
this file is called ``input_file_specs.yml`` and contains the following contents:

.. code-block:: yaml

    f.a:
        class: File
        path: /data/f.a

Using the following three files:

1. CWL workflow file (``wf.cwl``)
2. Workflow inputs file (``input_file_specs.yml``)
3. Pegasus specific information about the executables (``tr_specs.yml``)

The converter can be invoked as:

.. code-block::

    pegasus-cwl-converter workflow.cwl input.yml tr_specs.yml  pegasus_workflow.yml

The resulting Pegasus workflow file ``pegasus_workflow.yml`` is depicted above in 
the *Pegasus* tab. 


.. Note::

    :ref:`pegasus-cwl-converter <cli-pegasus-cwl-converter>` works on a subset of
    the CWL specification. If the conversion does not work for your workflow, 
    reach out to us and we can assist you in getting your workflow up and running.