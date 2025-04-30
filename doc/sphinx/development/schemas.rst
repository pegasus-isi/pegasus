.. _schemas:

=======
Schemas
=======

Workflow YAML Schema
====================

The workflow (erstwhile called DAX) format is described by the YAML
schema instance document
:download:`wf-5.0.yml <../../schemas/5.0/wf-5.0.yml>`.

Workflow YAML Schema In Detail
------------------------------

The abstract workflow file format has four major sections, with the second section
divided into more sub-sections. The abstract workflow format works on the abstract or
logical level, letting you focus on the shape of the workflows, what to
do and what to work upon.

1. Workflow level Metadata

   Metadata that is associated with the whole workflow. These are
   defined in the Metadata section.

2. Workflow-level Notifications

   Very simple workflow-level notifications. These are defined in the
   `Notification <#notifications>`__ section.

3. Catalogs

   The first section deals with included catalogs. While we do recommend
   to use external replica- and transformation catalogs, it is possible
   to include some replicas and transformations into the abstract workflow file
   itself. Any workflow-included entry takes precedence over regular replica
   catalog (RC) and transformation catalog (TC) entries.

   The first section (and any of its sub-sections) is completely
   optional.

   1. The first sub-section deals with included replica descriptions.

   2. The second sub-section deals with included transformation
      descriptions.

   3. The third sub-section declares multi-item executables.

4. Job List

   The jobs section defines the job- or task descriptions. For each task
   to conduct, a three-part logical name declares the task and aides
   identifying it in the transformation catalog or one of the
   *executable* section above. During planning, the logical name is
   translated into the physical executable location on the chosen target
   site. By declaring jobs abstractly, physical layout consideration of
   the target sites do not matter. The job's *id* uniquley identifies
   the job within this workflow.

   The arguments declare what command-line arguments to pass to the job.
   If you are passing filenames, you should refer to the logical
   filename using the *file* element in the argument list.

   Important for properly planning the task is the list of files
   consumed by the task, its input files, and the files produced by the
   task, its output files. Each file is described with a *uses* element
   inside the task.

   Elements exist to link a logical file to any of the stdio file
   descriptors. The *profile* element is Pegasus's way to abstract
   site-specific data.

   Jobs are nodes in the workflow graph. Other nodes include unplanned
   workflows (DAX), which are planned and then run when the node runs,
   and planned workflows (DAG), which are simply executed.

5. Control-flow Dependencies

   The third section lists the dependencies between the tasks. The
   relationships are defined as child parent relationships, and thus
   impacts the order in which tasks are run. No cyclic dependencies are
   permitted.

   Dependencies are directed edges in the workflow graph.

YAML Intro
~~~~~~~~~~

The Workflow API generated abstract workflows always list the following
optional attributes under the x-pegasus extensions.

.. code-block:: yaml

   x-pegasus:
     apiLang: python
     createdBy: vahi
     createdOn: 07-24-20T10:08:48Z
     pegasus: "5.0"
     name: diamond

The following attributes are supported for the root element *adag*.

.. table:: Root element attributes

   ========== ========= ================== ======================================================
   attribute  optional? type               meaning
   ========== ========= ================== ======================================================
   pegasus    required  *VersionPattern*   Version number of YAML instance document. Must be 5.0.
   name       required  string             name of this abstract workflow
   apiLang    optional  string             the language of the workflow api used to generate this
   createdBy  optional  string             the user who created this
   createdOn  optional  string             the timestamp when it was created
   ========== ========= ================== ======================================================

The *version* attribute is restricted to the regular expression
``\d+(\.\d+(\.\d+)?)?``.This expression represents the *VersionPattern*
type that is used in other places, too. It is a more restrictive
expression than before, but allows us to compute comparable version
number using the following formula:

=================================== ===================================
version1: a.b.c                     version2: d.e.f
n = a \* 1,000,000 + b \* 1,000 + c m = d \* 1,000,000 + e \* 1,000 + f
version1 > version2 if n > m
=================================== ===================================

Workflow-level Metadata
~~~~~~~~~~~~~~~~~~~~~~~

Metadata associated with the whole workflow.

.. code-block:: yaml

   metadata:
     creator: vahi

The workflow level metadata maybe used to control the Pegasus Mapper
behaviour at planning time or maybe propogated to external services
while querying for job characteristics.

Workflow-level Notifications
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Notifications that are generated when workflow level events happened.

.. code-block:: yaml

   hooks:
   shell:
     - _on: start
        cmd: /pegasus/libexec/notification/email -t notify@example.com
     - _on: end
        cmd: /pegasus/libexec/notification/email -t notify@example.com

The above snippet will append the current time to a log file in the
current directory. This is with regards to the pegasus-monitord instance
acting on the `notification <#notifications>`__.

The Catalogs Section
~~~~~~~~~~~~~~~~~~~~

The initial section features three sub-sections:

1. a catalog of files used,

2. a catalog of transformations used, and

3. compound transformation declarations.

.. _dax-replica-catalog:

The Replica Catalog Section
^^^^^^^^^^^^^^^^^^^^^^^^^^^

The file section acts as in in-file replica catalog (RC). Any files
declared in this section take precedence over files in external replica
catalogs during planning.

.. code-block:: yaml

     replicaCatalog:
        replicas:
          - lfn: input.txt
            pfns:
              - {site: local, pfn: 'http://example.com/pegasus/input/input.txt'}
            checksum: {sha256: 66a42b4be204c824a7533d2c677ff7cc5c44526300ecd6b450602e06128063f9}

The first *replicas* entry above is an example of a data file with single
replica. The *lfn* attribute signifies logical file *name*. Each
logical filename may have additional information associated with it,
enumerated by *profile* elements. Each entry in the *replicas* section
may have 0 or more *metadata* associated with it.

Each entry in the *replicas* section can provide 0 or more *pfn* locations,
taking precedence over the replica catalog. Multiple locations constitute
replicas of the same file, and are assumed to be usable interchangably.
The *pfn* attribute is mandatory, and typically would use a file schema URL.
The *site* attribute is optional, and defaults to value *local* if missing.
A *pfns* entri may have *profile* children-elements, which refer to attributes
of the physical file. The file-level profiles refer to attributes of the
logical file.

.. _dax-transformation-catalog:

The Transformation Catalog Section
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The executable section acts as an in-file transformation catalog (TC).
Any transformations declared in this section take precedence over the
external transformation catalog during planning.

.. code-block:: yaml

     transformationCatalog
       transformations:
          - name: keg
            sites:
            - {name: condorpool, pfn: /usr/bin/pegasus-keg, type: installed}
            profiles:
              env: {APP_HOME: /tmp/myscratch, JAVA_HOME: /opt/java/1.6}

Logical filenames pertaining to a single executables in the
transformation catalog use the *transformations* element. Any *transformation*
entry features the optional *namespace* attribute, a mandatory *name*
attribute, and an optional *version* attribute. The *version* attribute
defaults to "1.0" when absent. An executable typically needs additional
attributes to describe it properly, like the architecture, OS release
and other flags typically seen with transformations. They are described
in the *sites* entries under each *transformations* entry.

.. table:: transformations entry attributes

   ========= ========= ============== =============================================================
   attribute optional? type           meaning
   ========= ========= ============== =============================================================
   name      required  string         logical transformation name
   namespace optional  string         namespace of logical transformation, default to *null* value.
   version   optional  VersionPattern version of logical transformation, defaults to "1.0".
   sites     required  yaml array     details about where the transformation resides on 1 or more
                                      sites.
   profiles  optional  yaml array     details about the profiles associated with the transformation.
   hooks     optional  yaml array     details about the shell hooks to be invoked.
   requires  optional  yaml array     details about dependent transformations required.
   ========= ========= ============== =============================================================


.. table:: sites entry attributes

   ========== ========= ============== =============================================================
   attribute  optional? type           meaning
   ========== ========= ============== =============================================================
   name       required  string         the site on which the transformation resides.
   type       required  string         whether the executable is installed or stageable.
   pfn        required  string         the pfn of where it is.
   arch       optional  Architecture   restricted set of tokens such as x86, x86_64 etc.
   os.type    optional  OSType         restricted set of tokens, such as linux, macosx etc.
   os.release optional  string         the os release such deb, rhel etc.
   os.version optional  VersionPattern os version.
   bypass     optional  boolean        boolean attribute indicate whether to bypass staging.
   profiles   optional  yaml           details about the profiles associated with the sites entry
   metadata   optional  yaml           details about the profiles associated with the sites entry
   container  optional  string         the name of the container in which the job should run.
   ========== ========= ============== =============================================================


Similar to the replica catalog, each *sites* entry may have 0 or
more *profile* elements abstracting away site-specific details, zero or
more *metadata* elements, and a required *pfn* entry. If there are
no *sites* entry, the transformation must still be searched for in the
external transformation catalog.

Each *transformations* entry may also feature *hooks* entry. These
enable notifications at the appropriate point when every job that uses
this executable reaches the point of notification. Please refer to the
`notification section <#notifications>`__ for details and caveats.

The last example above comes from the black diamond example workflow,
and presents the kind and extend of attributes you are most likely to
see and use in your own workflows.

The Compound Transformation Section
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The compound transformation section declares a transformation that
comprises multiple plain transformation. You can think of a compound
transformation like a script interpreter and the script itself. In order
to properly run the application, you must start both, the script
interpreter and the script passed to it. The compound transformation
helps Pegasus to properly deal with this case, especially when it needs
to stage executables.

.. code-block:: yaml

     transformationCatalog
       transformations:
          - name: mDiffFit
            namespace: montage
            version: 2.0
            requires: [mDiff, mFitplane]


A *transformations* entry declares a set of purely logical entities,
executables and config (data) files, that are all required together for
the same job. Being purely logical entities, the lookup happens only
when the transformation element is referenced (or instantiated) by a job
element later on.

The *namespace* and *version* attributes of the transformation element
are optional, and provide the defaults for the inner uses elements. They
are also essential for matching the transformation with a job.

The *transformations* entry can have a *requires* element indicating
0 or more required executable. Each entry in *requires* list a string
of the format Namespace::Name:Version (Namespace:: and :Version may
be omitted). The *name* is a mandatory attribute.

.. _api-graph-nodes:

Graph Nodes
~~~~~~~~~~~

The nodes in the DAX comprise regular job nodes, already instantiated
sub-workflows as dag nodes, and still to be instantiated dax nodes. Each
of the graph nodes can has a mandatory *id* attribute. The *id*
attribute is currently a restriction of type *NodeIdentifierPattern*
type, which is a restriction of the ``xs:NMTOKEN`` type to letters,
digits, hyphen and underscore.

The *level* attribute is deprecated, as the planner will trust its own
re-computation more than user input. Please do not use nor produce any
*level* attribute.

The *node-label* attribute is optional. It applies to the use-case when
every transformation has the same name, but its arguments determine what
it really does. In the presence of a *node-label* value, a workflow
grapher could use the label value to show graph nodes to the user. It
may also come in handy while debugging.

Any job-like graph node has the following set of children elements, as
defined in the *AbstractJobType* declaration in the schema definition:

-  0 or 1 *argument* element to declare the command-line of the job's
   invocation.

-  0 or more *profile* elements to abstract away site-specific or
   job-specific details.

-  0 or 1 *stdin* element to link a logical file the the job's standard
   input.

-  0 or 1 *stdout* element to link a logical file to the job's standard
   output.

-  0 or 1 *stderr* element to link a logical file to the job's standard
   error.

-  0 or more *uses* elements to declare consumed data files and produced
   data files.

-  0 or more *invoke* elements to solicit
   `notifications <#notifications>`__ whence a job reaches a certain
   state in its life-cycle.

.. _api-job-nodes:

Job Nodes
^^^^^^^^^

A job element has a number of attributes. In addition to the *id* and
*node-label* described in (Graph Nodes)above, the optional *namespace*,
mandatory *name* and optional *version* identify the transformation, and
provide the look-up handle: first in the DAX's *transformation*
elements, then in the *executable* elements, and finally in an external
transformation catalog.

::

     <!-- part 2: definition of all jobs (at least one) -->
     <job id="ID000001" namespace="example" name="mDiffFit" version="1.0"
          node-label="preprocess" >
       <argument>-a top -T 6  -i <file name="f.a"/>  -o <file name="f.b1"/></argument>

       <!-- profiles are optional -->
       <profile namespace="execution" key="site">isi_viz</profile>
       <profile namespace="condor" key="getenv">true</profile>

        <uses name="f.a" link="input" transfer="true" register="true">
            <metadata key="size">1024</metadata>
         </uses>
       <uses name="f.b" link="output" register="false" transfer="true" type="data" />

       <!-- 'WHEN' enumeration: never, start, on_error, on_success, at_end, all -->
       <!-- PEGASUS_* env-vars: event, status, submit dir, wf/job id, stdout, stderr -->
       <invoke when="start">/path/to arg arg</invoke>
       <invoke when="on_success"><![CDATA[/path/to arg arg]]></invoke>
       <invoke when="at_end"><![CDATA[/path/to arg arg]]></invoke>
     </job>

The *argument* element contains the complete command-line that is needed
to invoke the executable. The only variable components are logical
filenames, as included *file* elements.

The *profile* argument lets you encapsulate site-specific knowledge .

The *stdin*, *stdout* and *stderr* element permits you to connect a
stdio file descriptor to a logical filename. Note that you will still
have to declare these files in the *uses* section below.

The *uses* element enumerates all the files that the task consumes or
produces. While it is not necessary nor required to have all files
appear on the command-line, it is imperative that you declare even
hidden files that your task requires in this section, so that the proper
ancilliary staging- and clean-up tasks can be generated during planning.

The *invoke* element may be specified multiple times, as needed. It has
a mandatory when attribute with the following value set:

.. table:: invoke element attributes

   ========== ==================== =====================================================================================================
   keyword    job life-cycle state meaning
   ========== ==================== =====================================================================================================
   never      never                (default). Never notify of anything. This is useful to temporarily disable an existing notifications.
   start      submit               create a notification when the job is submitted.
   on_error   end                  after a job finishes with failure (exitcode != 0).
   on_success end                  after a job finishes with success (exitcode == 0).
   at_end     end                  after a job finishes, regardless of exitcode.
   all        always               like start and at_end combined.
   ========== ==================== =====================================================================================================

..

   **Warning**

   In clustered jobs, a notification can only be sent at the start or
   end of the clustered job, not for each member.

Each *invoke* is a simple local invocation of an executable or script
with the specified arguments. The executable inside the invoke body will
see the following environment variables:

.. table:: invoke/executable environment variables

   ================== ==================== =========================================================================================================================================================
   variable           job life-cycle state meaning
   ================== ==================== =========================================================================================================================================================
   PEGASUS_EVENT      always               The value of the ``when`` attribute
   PEGASUS_STATUS     end                  The exit status of the graph node. Only available for end notifications.
   PEGASUS_SUBMIT_DIR always               In which directory to find the job (or workflow).
   PEGASUS_JOBID      always               The job (or workflow) identifier. This is potentially more than merely the value of the *id* attribute.
   PEGASUS_STDOUT     always               The filename where *stdout* goes. Empty and possibly non-existent at submit time (though we still have the filename). The kickstart record for job nodes.
   PEGASUS_STDERR     always               The filename where *stderr* goes. Empty and possibly non-existent at submit time (though we still have the filename).
   ================== ==================== =========================================================================================================================================================

Generators should use CDATA encapsulated values to the invoke element to
minimize interference. Unfortunately, CDATA cannot be nested, so if the
user invocation contains a CDATA section, we suggest that they use
careful XML-entity escaped strings. The `notifications
section <#notifications>`__ describes these in further detail.

DAG Nodes
^^^^^^^^^

A workflow that has already been concretized, either by an earlier run
of Pegasus, or otherwise constructed for DAGMan execution, can be
included into the current workflow using the *dag* element.

::

     <dag id="ID000003" name="black.dag" node-label="foo" >
       <profile namespace="dagman" key="DIR">/dag-dir/test</profile>
       <invoke> <!-- optional, should be possible --> </invoke>
       <uses file="sites.xml" link="input" register="false" transfer="true" type="data"/>
     </dag>

The *id* and *node-label* attributes were described
`previously <#api-graph-nodes>`__. The *name* attribute refers to a file
from the File Catalog that provides the actual DAGMan DAG as data
content. The *dag* element features optional *profile* elements. These
would most likely pertain to the ``dagman`` and ``env`` profile
namespaces. It should be possible to have the optional *notify* element
in the same manner as for jobs.

A graph node that is a dag instead of a job would just use a different
submit file generator to create a DAGMan invocation. There can be an
*argument* element to modify the command-line passed to DAGMan.

DAX Nodes
^^^^^^^^^

A still to be planned workflow incurs an invocation of the Pegasus
planner as part of the workflow. This still abstract sub-workflow uses
the *dax* element.

::

     <dax id="ID000002" name="black.dax" node-label="bar" >
       <profile namespace="env" key="foo">bar</profile>
       <argument>-Xmx1024 -Xms512 -Dpegasus.dir.storage=storagedir  -Dpegasus.dir.exec=execdir -o local --dir ./datafind -vvvvv --force -s dax_site </argument>
       <invoke> <!-- optional, may not be possible here --> </invoke>
       <uses file="sites.xml" link="input" register="false" transfer="true" type="data" />
     </dax>

In addition to the *id* and *node-label* attributes, See `Graph
Nodes <#api-graph-nodes>`__. The *name* attribute refers to a file from
the File Catalog that provides the to be planned DAX as external file
data content. The *dax* element features optional *profile* elements.
These would most likely pertain to the ``pegasus``, ``dagman`` and
``env`` profile namespaces. It may be possible to have the optional
*notify* element in the same manner as for jobs.

A graph node that is a *dax* instead of a job would just use yet another
submit file and pre-script generator to create a DAGMan invocation. The
*argument* string pertains to the command line of the to-be-generated
DAGMan invocation.

Inner ADAG Nodes
^^^^^^^^^^^^^^^^

While completeness would argue to have a recursive nesting of *adag*
elements, such recursive nestings are currently not supported, not even
in the schema. If you need to nest workflows, please use the *dax* or
*dag* element to achieve the same goal.

The Dependency Section
~~~~~~~~~~~~~~~~~~~~~~

This section describes the dependencies between the jobs.

::

     <!-- part 3: list of control-flow dependencies -->
     <child ref="ID000002">
       <parent ref="ID000001" edge-label="edge1" />
     </child>
     <child ref="ID000003">
       <parent ref="ID000001" edge-label="edge2" />
     </child>
     <child ref="ID000004">
       <parent ref="ID000002" edge-label="edge3" />
       <parent ref="ID000003" edge-label="edge4" />
     </child>

Each *child* element contains one or more *parent* element. Either
element refers to a *job*, *dag* or *dax* element id attribute using the
*ref* attribute. In this version, we relaxed the ``xs:IDREF`` constraint
in favor of a restriction on the ``xs:NMTOKEN`` type to permit a larger
set of identifiers.

The *parent* element has an optional *edge-label* attribute.

   **Warning**

   The *edge-label* attribute is currently unused.

Its goal is to annotate edges when drawing workflow graphs.

Closing
~~~~~~~

As any XML element, the root element needs to be closed.

::

   </adag>

DAX XML Schema Example
----------------------

The following code example shows the XML instance document representing
the diamond workflow.

::

   <?xml version="1.0" encoding="UTF-8"?>
   <adag xmlns="http://pegasus.isi.edu/schema/DAX"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://pegasus.isi.edu/schema/DAX http://pegasus.isi.edu/schema/dax-3.6.xsd"
    version="3.6" name="diamond" index="0" count="1">
     <!-- part 1.1: invocations -->
     <invoke when="on_error">/bin/mailx -s &apos;diamond failed&apos; use@some.domain</invoke>

     <!-- part 1.2: included replica catalog -->
     <file name="f.a">
       <pfn url="file:///lfs/voeckler/src/svn/pegasus/trunk/examples/grid-blackdiamond-perl/f.a" site="local" />
     </file>

     <!-- part 1.3: included transformation catalog -->
     <executable namespace="diamond" name="preprocess" version="2.0" arch="x86_64" os="linux" installed="false">
       <profile namespace="globus" key="maxtime">2</profile>
       <profile namespace="dagman" key="RETRY">3</profile>
       <pfn url="file:///opt/pegasus/latest/bin/keg" site="local" />
     </executable>
     <executable namespace="diamond" name="analyze" version="2.0" arch="x86_64" os="linux" installed="false">
       <profile namespace="globus" key="maxtime">2</profile>
       <profile namespace="dagman" key="RETRY">3</profile>
       <pfn url="file:///opt/pegasus/latest/bin/keg" site="local" />
     </executable>
     <executable namespace="diamond" name="findrange" version="2.0" arch="x86_64" os="linux" installed="false">
       <profile namespace="globus" key="maxtime">2</profile>
       <profile namespace="dagman" key="RETRY">3</profile>
       <pfn url="file:///opt/pegasus/latest/bin/keg" site="local" />
     </executable>

     <!-- part 2: definition of all jobs (at least one) -->
     <job namespace="diamond" name="preprocess" version="2.0" id="ID000001">
       <argument>-a preprocess -T60 -i <file name="f.a" /> -o <file name="f.b1" /> <file name="f.b2" /></argument>
       <uses name="f.b2" link="output" register="false" transfer="true" />
       <uses name="f.b1" link="output" register="false" transfer="true" />
       <uses name="f.a" link="input" />
     </job>
     <job namespace="diamond" name="findrange" version="2.0" id="ID000002">
       <argument>-a findrange -T60 -i <file name="f.b1" /> -o <file name="f.c1" /></argument>
       <uses name="f.b1" link="input" register="false" transfer="true" />
       <uses name="f.c1" link="output" register="false" transfer="true" />
     </job>
     <job namespace="diamond" name="findrange" version="2.0" id="ID000003">
       <argument>-a findrange -T60 -i <file name="f.b2" /> -o <file name="f.c2" /></argument>
       <uses name="f.b2" link="input" register="false" transfer="true" />
       <uses name="f.c2" link="output" register="false" transfer="true" />
     </job>
     <job namespace="diamond" name="analyze" version="2.0" id="ID000004">
       <argument>-a analyze -T60 -i <file name="f.c1" /> <file name="f.c2" /> -o <file name="f.d" /></argument>
       <uses name="f.c2" link="input" register="false" transfer="true" />
       <uses name="f.d" link="output" register="false" transfer="true" />
       <uses name="f.c1" link="input" register="false" transfer="true" />
     </job>

     <!-- part 3: list of control-flow dependencies -->
     <child ref="ID000002">
       <parent ref="ID000001" />
     </child>
     <child ref="ID000003">
       <parent ref="ID000001" />
     </child>
     <child ref="ID000004">
       <parent ref="ID000002" />
       <parent ref="ID000003" />
     </child>
   </adag>

The above workflow defines the black diamond from the abstract workflow
section of the `Introduction <#about>`__ chapter. It will require
minimal configuration, because the catalog sections include all
necessary declarations.

The file element defines the location of the required input file in
terms of the local machine. Please note that

-  The **file** element declares the required input file "f.a" in terms
   of the local machine. Please note that if you plan the workflow for a
   remote site, the has to be some way for the file to be staged from
   the local site to the remote site. While Pegasus will augment the
   workflow with such ancillary jobs, the site catalog as well as local
   and remote site have to be set up properlyl. For a locally run
   workflow you don't need to do anything.

-  The **executable** elements declare the same executable keg that is
   to be run for each the logical transformation in terms of the remote
   site *futuregrid*. To declare it for a local site, you would have to
   adjust the *site* attribute's value to ``local``. This section also
   shows that the same executable may come in different guises as
   transformation.

-  The **job** elements define the workflow's logical constituents, the
   way to invoke the ``keg`` command, where to put filenames on the
   commandline, and what files are consumed or produced. In addition to
   the direction of files, further attributes determine whether to
   register the file with a replica catalog and whether to transfer it
   to the output site in case of a product. We are only interested in
   the final data product "f.d" in this workflow, and not any
   intermediary files. Typically, you would also want to register the
   data products in the replica catalog, especially in larger scenarios.

-  The **child** elements define the control flow between the jobs.


