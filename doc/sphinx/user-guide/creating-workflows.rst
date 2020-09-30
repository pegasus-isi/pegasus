.. _creating-workflows:

==================
Creating Workflows
==================

.. _abstract-workflows:

Abstract Workflows
==================

The Abstract Workflow is a description of an user workflow, usually in
**YAML** format (before 5.0 release, it was a XML based format called the DAX)
that is used as the primary input into Pegasus. The workflow schema is
described using JSON schemas in
`wf-5.0.yml <schemas/5.0/wf-5.0.yml>`__ .
We recommend that users  use the Workflow API to generate the abstract
workflows. The documentation of the API's can be found at
:ref:`api-reference` . The Workflow API is available for users to use in
Python, Java and R format.


The sample workflow below incorporates some of the elementary graph
structures used in all abstract workflows.

-  **fan-out**, **scatter**, and **diverge** all describe the fact that
   multiple siblings are dependent on fewer parents.

   The example shows how the **Job 2 and 3** nodes depend on **Job 1**
   node.

-  **fan-in**, **gather**, **join**, and **converge** describe how
   multiple siblings are merged into fewer dependent child nodes.

   The example shows how the **Job 4** node depends on both **Job 2 and
   Job 3** nodes.

-  **serial execution** implies that nodes are dependent on one another,
   like pearls on a string.

-  **parallel execution** implies that nodes can be executed in parallel

The example diamond workflow consists of four nodes representing jobs,
and are linked by six files.

-  Required input files must be registered with the Replica catalog in
   order for Pegasus to find it and integrate it into the workflow.

-  Leaf files are a product or output of a workflow. Output files can be
   collected at a location.

-  The remaining files all have lines leading to them and originating
   from them. These files are products of some job steps (lines leading
   to them), and consumed by other job steps (lines leading out of
   them). Often, these files represent intermediary results that can be
   cleaned.

The example workflow representation in form of an abstract requires external
catalogs, such as

* :ref:`replica catalog (RC) <replica>`  to resolve the input file ``f.a``.

* :ref:`transformation catalog (TC) <transformation>` to resolve the logical job
names (such as diamond::preprocess:2.0) and

* :ref:`site catalog (SC) <site>` to resolve on what compute resources will
  the jobs execute on.

The  workflow below defines the four jobs just like the example picture,
and the files that flow between the jobs.The intermediary files are neither
registered nor staged out, and can be considered transient.
Only the final result file ``f.d`` is staged out.


There are two main ways of generating the abstract workfow
1. Using a Workflow generating API in
   * :ref:`Python <api-python>`,
   * :ref:`Java <api-java>`, or
   * :ref:`R <api-r>`.

   **Note:** We recommend this option.

2. Generating YAML directly from your script.

   **Note:** This option should only be considered by advanced users who
   can also read YAML schema definitions. This process can be error
   prone considering YAML's sensitivity towards indenting and whitespaces.

One example for the Abstract Workflow representing the example workflow
can look like the following:

 .. tabs::

    .. code-tab:: python Pegasus.api

      #! /usr/bin/env python3
      import logging

      from pathlib import Path

      from Pegasus.api import *

      logging.basicConfig(level=logging.DEBUG)

      # --- Raw input file -----------------------------------------------------------------

      fa = File("f.a").add_metadata(creator="ryan")

      # --- Workflow -----------------------------------------------------------------
      '''
                              [f.b1] - (findrange) - [f.c1]
                              /                             \
      [f.a] - (preprocess)                               (analyze) - [f.d]
                              \                             /
                              [f.b2] - (findrange) - [f.c2]

      '''
      wf = Workflow("diamond")

      wf.add_shell_hook(EventType.START, "/pegasus/libexec/notification/email -t notify@example.com")
      wf.add_shell_hook(EventType.END, "/pegasus/libexec/notification/email -t notify@example.com")

      fb1 = File("f.b1")
      fb2 = File("f.b2")
      job_preprocess = Job("preprocess")\
                              .add_args("-a", "preprocess", "-T", "3", "-i", fa, "-o", fb1, fb2)\
                              .add_inputs(fa)\
                              .add_outputs(fb1, fb2)\
                              .add_metadata(time=60)\
                              .add_shell_hook(EventType.START, "/pegasus/libexec/notification/email -t notify@example.com")\
                              .add_shell_hook(EventType.END, "/pegasus/libexec/notification/email -t notify@example.com")


      fc1 = File("f.c1")
      job_findrange_1 = Job("findrange")\
                              .add_args("-a", "findrange", "-T", "3", "-i", fb1, "-o", fc1)\
                              .add_inputs(fb1)\
                              .add_outputs(fc1)\
                              .add_metadata(time=60)\
                              .add_shell_hook(EventType.START, "/pegasus/libexec/notification/email -t notify@example.com")\
                              .add_shell_hook(EventType.END, "/pegasus/libexec/notification/email -t notify@example.com")

      fc2 = File("f.c2")
      job_findrange_2 = Job("findrange")\
                              .add_args("-a", "findrange", "-T", "3", "-i", fb2, "-o", fc2)\
                              .add_inputs(fb2)\
                              .add_outputs(fc2)\
                              .add_metadata(time=60)\
                              .add_shell_hook(EventType.START, "/pegasus/libexec/notification/email -t notify@example.com")\
                              .add_shell_hook(EventType.END, "/pegasus/libexec/notification/email -t notify@example.com")

      fd = File("f.d").add_metadata(final_output="true")
      job_analyze = Job("analyze")\
                     .add_args("-a", "analyze", "-T", "3", "-i", fc1, fc2, "-o", fd)\
                     .add_inputs(fc1, fc2)\
                     .add_outputs(fd)\
                     .add_metadata(time=60)\
                     .add_shell_hook(EventType.START, "/pegasus/libexec/notification/email -t notify@example.com")\
                     .add_shell_hook(EventType.END, "/pegasus/libexec/notification/email -t notify@example.com")

      wf.add_jobs(job_preprocess, job_findrange_1, job_findrange_2, job_analyze)
      wf.write()

    .. code-tab:: yaml YAML

      x-pegasus:
      apiLang: python
      createdBy: ryantanaka
      createdOn: 07-24-20T10:08:48Z
      pegasus: "5.0"
      name: diamond
      hooks:
      shell:
         - _on: start
            cmd: /pegasus/libexec/notification/email -t notify@example.com
         - _on: end
            cmd: /pegasus/libexec/notification/email -t notify@example.com
      jobs:
      - type: job
         name: preprocess
         id: ID0000001
         arguments: [-a, preprocess, -T, "3", -i, f.a, -o, f.b1, f.b2]
         uses:
            - lfn: f.a
            metadata:
               creator: ryan
            type: input
            - lfn: f.b1
              type: output
              stageOut: true
              registerReplica: true
            - lfn: f.b2
              type: output
              stageOut: true
              registerReplica: true
         metadata:
            time: "60"
         hooks:
            shell:
            - _on: start
               cmd: /pegasus/libexec/notification/email -t notify@example.com
            - _on: end
               cmd: /pegasus/libexec/notification/email -t notify@example.com
      - type: job
         name: findrange
         id: ID0000002
         arguments: [-a, findrange, -T, "3", -i, f.b1, -o, f.c1]
         uses:
            - lfn: f.b1
            type: input
            - lfn: f.c1
            type: output
            stageOut: true
            registerReplica: true
         metadata:
            time: "60"
         hooks:
            shell:
            - _on: start
               cmd: /pegasus/libexec/notification/email -t notify@example.com
            - _on: end
               cmd: /pegasus/libexec/notification/email -t notify@example.com
      - type: job
         name: findrange
         id: ID0000003
         arguments: [-a, findrange, -T, "3", -i, f.b2, -o, f.c2]
         uses:
            - lfn: f.c2
            type: output
            stageOut: true
            registerReplica: true
            - lfn: f.b2
            type: input
         metadata:
            time: "60"
         hooks:
            shell:
            - _on: start
               cmd: /pegasus/libexec/notification/email -t notify@example.com
            - _on: end
               cmd: /pegasus/libexec/notification/email -t notify@example.com
      - type: job
         name: analyze
         id: ID0000004
         arguments: [-a, analyze, -T, "3", -i, f.c1, f.c2, -o, f.d]
         uses:
            - lfn: f.d
            metadata:
               final_output: "true"
            type: output
            stageOut: true
            registerReplica: true
            - lfn: f.c2
            type: input
            - lfn: f.c1
            type: input
         metadata:
            time: "60"
         hooks:
            shell:
            - _on: start
               cmd: /pegasus/libexec/notification/email -t notify@example.com
            - _on: end
               cmd: /pegasus/libexec/notification/email -t notify@example.com
      jobDependencies:
      - id: ID0000001
         children:
            - ID0000002
            - ID0000003
      - id: ID0000002
         children:
            - ID0000004
      - id: ID0000003
         children:
            - ID0000004

    .. code-tab:: xml XML

       <?xml version="1.0" encoding="UTF-8"?>
       <!-- generated on: 2016-01-21T10:36:39-08:00 -->
       <!-- generated by: vahi [ ?? ] -->
       <adag xmlns="http://pegasus.isi.edu/schema/DAX" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://pegasus.isi.edu/schema/DAX http://pegasus.isi.edu/schema/dax-3.6.xsd" version="3.6" name="diamond" index="0" count="1">

       <!-- Section 1: Metadata attributes for the workflow (can be empty)  -->

          <metadata key="name">diamond</metadata>
          <metadata key="createdBy">Karan Vahi</metadata>

       <!-- Section 2: Invokes - Adds notifications for a workflow (can be empty) -->

          <invoke when="start">/pegasus/libexec/notification/email -t notify@example.com</invoke>
          <invoke when="at_end">/pegasus/libexec/notification/email -t notify@example.com</invoke>

       <!-- Section 3: Files - Acts as a Replica Catalog (can be empty) -->

          <file name="f.a">
             <metadata key="size">1024</metadata>
             <pfn url="file:///Volumes/Work/lfs1/work/pegasus-features/PM-902/f.a" site="local"/>
          </file>

       <!-- Section 4: Executables - Acts as a Transformation Catalog (can be empty) -->

          <executable namespace="pegasus" name="preprocess" version="4.0" installed="true" arch="x86" os="linux">
             <metadata key="size">2048</metadata>
             <pfn url="file:///usr/bin/keg" site="TestCluster"/>
          </executable>
          <executable namespace="pegasus" name="findrange" version="4.0" installed="true" arch="x86" os="linux">
             <pfn url="file:///usr/bin/keg" site="TestCluster"/>
          </executable>
          <executable namespace="pegasus" name="analyze" version="4.0" installed="true" arch="x86" os="linux">
             <pfn url="file:///usr/bin/keg" site="TestCluster"/>
          </executable>

       <!-- Section 5: Transformations - Aggregates executables and Files (can be empty) -->


       <!-- Section 6: Job's, DAX's or Dag's - Defines a JOB or DAX or DAG (Atleast 1 required) -->

          <job id="j1" namespace="pegasus" name="preprocess" version="4.0">
             <metadata key="time">60</metadata>
             <argument>-a preprocess -T 60 -i  <file name="f.a"/> -o  <file name="f.b1"/>   <file name="f.b2"/></argument>
             <uses name="f.a" link="input">
                <metadata key="size">1024</metadata>
             </uses>
             <uses name="f.b1" link="output" transfer="true" register="true"/>
             <uses name="f.b2" link="output" transfer="true" register="true"/>
             <invoke when="start">/pegasus/libexec/notification/email -t notify@example.com</invoke>
             <invoke when="at_end">/pegasus/libexec/notification/email -t notify@example.com</invoke>
          </job>
          <job id="j2" namespace="pegasus" name="findrange" version="4.0">
             <metadata key="time">60</metadata>
             <argument>-a findrange -T 60 -i  <file name="f.b1"/> -o  <file name="f.c1"/></argument>
             <uses name="f.b1" link="input"/>
             <uses name="f.c1" link="output" transfer="true" register="true"/>
             <invoke when="start">/pegasus/libexec/notification/email -t notify@example.com</invoke>
             <invoke when="at_end">/pegasus/libexec/notification/email -t notify@example.com</invoke>
          </job>
          <job id="j3" namespace="pegasus" name="findrange" version="4.0">
             <metadata key="time">60</metadata>
             <argument>-a findrange -T 60 -i  <file name="f.b2"/> -o  <file name="f.c2"/></argument>
             <uses name="f.b2" link="input"/>
             <uses name="f.c2" link="output" transfer="true" register="true"/>
             <invoke when="start">/pegasus/libexec/notification/email -t notify@example.com</invoke>
             <invoke when="at_end">/pegasus/libexec/notification/email -t notify@example.com</invoke>
          </job>
          <job id="j4" namespace="pegasus" name="analyze" version="4.0">
             <metadata key="time">60</metadata>
             <argument>-a analyze -T 60 -i  <file name="f.c1"/>   <file name="f.c2"/> -o  <file name="f.d"/></argument>
             <uses name="f.c1" link="input"/>
             <uses name="f.c2" link="input"/>
             <uses name="f.d" link="output" transfer="true" register="true"/>
             <invoke when="start">/pegasus/libexec/notification/email -t notify@example.com</invoke>
             <invoke when="at_end">/pegasus/libexec/notification/email -t notify@example.com</invoke>
          </job>

       <!-- Section 7: Dependencies - Parent Child relationships (can be empty) -->

          <child ref="j2">
             <parent ref="j1"/>
          </child>
          <child ref="j3">
             <parent ref="j1"/>
          </child>
          <child ref="j4">
             <parent ref="j2"/>
             <parent ref="j3"/>
          </child>
       </adag>

.. _catalogs:

TODO: Catalogs
==============



