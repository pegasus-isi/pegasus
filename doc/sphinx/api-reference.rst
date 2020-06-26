.. _api-reference:

=============
API Reference
=============

DAX XML Schema
==============

The DAX format is described by the XML schema instance document
`dax-3.6.xsd <schemas/dax-3.6/dax-3.6.xsd>`__. A local copy of the
schema definition is provided in the “etc” directory. The documentation
of the XML schema and its elements can be found in
`dax-3.6.html <schemas/dax-3.6/dax-3.6.html>`__ as well as locally in
``doc/schemas/dax-3.6/dax-3.6.html`` in your Pegasus distribution.

DAX XML Schema In Detail
------------------------

The DAX file format has four major sections, with the second section
divided into more sub-sections. The DAX format works on the abstract or
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
   to include some replicas and transformations into the DAX file
   itself. Any DAX-included entry takes precedence over regular replica
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

XML Intro
~~~~~~~~~

If you have seen the DAX schema before, not a lot of new items in the
root element. *However*, we did retire the (old) attributes ending in
*Count*.

::

   <?xml version="1.0" encoding="UTF-8"?>
   <!-- generated: 2011-07-28T18:29:57Z -->
   <adag xmlns="http://pegasus.isi.edu/schema/DAX"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://pegasus.isi.edu/schema/DAX http://pegasus.isi.edu/schema/dax-3.6.xsd"
         version="3.6"
         name="diamond"
         index="0"
         count="1">

The following attributes are supported for the root element *adag*.

.. table:: Root element attributes

   ========== ========= ================== ======================================================
   attribute  optional? type               meaning
   ========== ========= ================== ======================================================
   version    required  *VersionPattern*   Version number of DAX instance document. Must be 3.6.
   name       required  string             name of this DAX (or set of DAXes).
   count      optional  positiveInteger    size of list of DAXes with this *name*. Defaults to 1.
   index      optional  nonNegativeInteger current index of DAX with same *name*. Defaults to 0.
   fileCount  removed   nonNegativeInteger Old 2.1 attribute, removed, do not use.
   jobCount   removed   positiveInteger    Old 2.1 attribute, removed, do not use.
   childCount removed   nonNegativeInteger Old 2.1 attribute, removed, do not use.
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

::

      <metadata key="name">diamond</metadata>
      <metadata key="createdBy">Karan Vahi</metadata>

The workflow level metadata maybe used to control the Pegasus Mapper
behaviour at planning time or maybe propogated to external services
while querying for job characteristics.

Workflow-level Notifications
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Notifications that are generated when workflow level events happened.

::

     <!-- part 1.1: invocations -->
     <invoke when="at_end">/bin/date -Ins &gt;&gt; my.log</invoke>

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

::

     <!-- part 1.2: included replica catalog -->
     <file name="example.a" >
       <!-- profiles are optional -->
       <!-- The "stat" namespace is ONLY AN EXAMPLE -->
       <profile namespace="stat" key="size">/* integer to be defined */</profile>
       <profile namespace="stat" key="md5sum">/* 32 char hex string */</profile>
       <profile namespace="stat" key="mtime">/* ISO-8601 timestamp */</profile>

       <!-- Metadata will be supported 4.6 onwards-->
       <metadata key="timestamp" >/* ISO-8601 *or* 20100417134523:int */</metadata>
       <metadata key="origin" >ocean</metadata>

       <!-- PFN to by-pass replica catalog -->
       <!-- The "site attribute is optional -->
       <pfn url="file:///tmp/example.a" site="local">
         <profile namespace="stat" key="owner">voeckler</profile>
       </pfn>
       <pfn url="file:///storage/funky.a" site="local"/>
     </file>

     <!-- a more typical example from the black diamond -->
     <file name="f.a">
       <pfn url="file:///Users/voeckler/f.a" site="local"/>
     </file>

The first *file* entry above is an example of a data file with two
replicas. The *file* element requires a logical file *name*. Each
logical filename may have additional information associated with it,
enumerated by *profile* elements. Each file entry may have 0 or more
*metadata* associated with it. Each piece of metadata has a *key* string
and *type* attribute describing the element's value.

   **Warning**

   The *metadata* element is not support as of this writing! Details may
   change in the future.

The *file* element can provide 0 or more *pfn* locations, taking
precedence over the replica catalog. A *file* element that does not name
any *pfn* children-elements will still require look-ups in external
replica catalogs. Each *pfn* element names a concrete location of a
file. Multiple locations constitute replicas of the same file, and are
assumed to be usable interchangably. The *url* attribute is mandatory,
and typically would use a file schema URL. The *site* attribute is
optional, and defaults to value *local* if missing. A *pfn* element may
have *profile* children-elements, which refer to attributes of the
physical file. The file-level profiles refer to attributes of the
logical file.

   **Note**

   The ``stat`` profile namespace is ony an example, and details about
   stat are not yet implemented. The proper namespaces ``pegasus``,
   ``condor``, ``dagman``, ``env``, ``hints``, ``globus`` and
   ``selector`` enjoy full support.

The second *file* entry above shows a usage example from the
black-diamond example workflow that you are more likely to encouter or
write.

The presence of an in-file replica catalog lets you declare a couple of
interesting advanced features. The DAG and DAX file declarations are
just files for all practical purposes. For deferred planning, the
location of the site catalog (SC) can be captured in a file, too, that
is passed to the job dealing with the deferred planning as logical
filename.

::

     <file name="black.dax" >
       <!-- specify the location of the DAX file -->
       <pfn url="file:///Users/vahi/Pegasus/work/dax-3.0/blackdiamond_dax.xml" site="local"/>
     </file>

     <file name="black.dag" >
       <!-- specify the location of the DAG file -->
       <pfn url="file:///Users/vahi/Pegasus/work/dax-3.0/blackdiamond.dag" site="local"/>
     </file>

     <file name="sites.xml" >
       <!-- specify the location of a site catalog to use for deferred planning -->
       <pfn url="file:///Users/vahi/Pegasus/work/dax-3.0/conf/sites.xml" site="local"/>
     </file>

.. _dax-transformation-catalog:

The Transformation Catalog Section
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The executable section acts as an in-file transformation catalog (TC).
Any transformations declared in this section take precedence over the
external transformation catalog during planning.

::

     <!-- part 1.3: included transformation catalog -->
     <executable namespace="example" name="mDiffFit" version="1.0"
                 arch="x86_64" os="linux" installed="true" >
       <!-- profiles are optional -->
       <!-- The "stat" namespace is ONLY AN EXAMPLE! -->
       <profile namespace="stat" key="size">5000</profile>
       <profile namespace="stat" key="md5sum">AB454DSSDA4646DS</profile>
       <profile namespace="stat" key="mtime">2010-11-22T10:05:55.470606000-0800</profile>

       <!-- metadata will be supported in 4.6 -->
       <metadata key="timestamp" >/* see above */</metadata>
       <metadata key="origin">ocean</metadata>

       <!-- PFN to by-pass transformation catalog -->
       <!-- The "site" attribute is optional -->
       <pfn url="file:///tmp/mDiffFit"          site="local"/>
       <pfn url="file:///tmp/storage/mDiffFit"  site="local"/>
     </executable>

     <!-- to be used in compound transformation later -->
     <executable namespace="example" name="mDiff" version="1.0"
                 arch="x86_64" os="linux" installed="true" >
       <pfn url="file:///tmp/mDiff" site="local"/>
     </executable>

     <!-- to be used in compound transformation later -->
     <executable namespace="example" name="mFitplane" version="1.0"
                 arch="x86_64" os="linux" installed="true" >
       <pfn url="file:///tmp/mDiffFitplane"  site="local">
         <profile namespace="stat" key="md5sum">0a9c38b919c7809cb645fc09011588a6</profile>
       </pfn>
       <invoke when="at_end">/path/to/my_send_email some args</invoke>
     </executable>

     <!-- a more likely example from the black diamond -->
     <executable namespace="diamond" name="preprocess" version="2.0"
                 arch="x86_64"
                 os="linux"
                 osversion="2.6.18">
       <pfn url="file:///opt/pegasus/default/bin/keg" site="local" />
     </executable>

Logical filenames pertaining to a single executables in the
transformation catalog use the *executable* element. Any *executable*
element features the optional *namespace* attribute, a mandatory *name*
attribute, and an optional *version* attribute. The *version* attribute
defaults to "1.0" when absent. An executable typically needs additional
attributes to describe it properly, like the architecture, OS release
and other flags typically seen with transformations, or found in the
transformation catalog.

.. table:: executable element attributes

   ========= ========= ============== =============================================================
   attribute optional? type           meaning
   ========= ========= ============== =============================================================
   name      required  string         logical transformation name
   namespace optional  string         namespace of logical transformation, default to *null* value.
   version   optional  VersionPattern version of logical transformation, defaults to "1.0".
   installed optional  boolean        whether to stage the file (false), or not (true, default).
   arch      optional  Architecture   restricted set of tokens, see schema definition file.
   os        optional  OSType         restricted set of tokens, see schema definition file.
   osversion optional  VersionPattern kernel version as beginning of \`uname -r`.
   glibc     optional  VersionPattern version of libc.
   ========= ========= ============== =============================================================

The rationale for giving these flags in the *executable* element header
is that PFNs are just identical replicas or instances of a given LFN. If
you need a different 32/64 bit-ed-ness or OS release, the underlying PFN
would be different, and thus the LFN for it should be different, too.

   **Note**

   We are still discussing some details and implications of this
   decision.

The initial examples come with the same caveats as for the included
replica catalog.

   **Warning**

   The *metadata* element is not support as of this writing! Details may
   change in the future.

Similar to the replica catalog, each *executable* element may have 0 or
more *profile* elements abstracting away site-specific details, zero or
more *metadata* elements, and zero or more *pfn* elements. If there are
no *pfn* elements, the transformation must still be searched for in the
external transformation catalog. As before, the *pfn* element may have
*profile* children-elements, referring to attributes of the physical
filename itself.

Each *executable* element may also feature *invoke* elements. These
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

::

     <transformation namespace="example" version="1.0" name="mDiffFit" >
       <uses name="mDiffFit" />
       <uses name="mDiff" namespace="example" version="2.0" />
       <uses name="mFitPlane" />
       <uses name="mDiffFit.config" executable="false" />
     </transformation>

A *transformation* element declares a set of purely logical entities,
executables and config (data) files, that are all required together for
the same job. Being purely logical entities, the lookup happens only
when the transformation element is referenced (or instantiated) by a job
element later on.

The *namespace* and *version* attributes of the transformation element
are optional, and provide the defaults for the inner uses elements. They
are also essential for matching the transformation with a job.

The *transformation* is made up of 1 or more *uses* element. Each *uses*
has a boolean attribute *executable*, ``true`` by default, or ``false``
to indicate a data file. The *name* is a mandatory attribute, refering
to an LFN declared previously in the File Catalog (*executable* is
``false``), Executable Catalog (*executable* is ``true``), or to be
looked up as necessary at instantiation time. The lookup catalog is
determined by the *executable* attribute.

After *uses* elements, any number of *invoke* elements may occur to add
a `notification <#notifications>`__ each whenever this transformation is
instantiated.

The *namespace* and *version* attributes' default values inside *uses*
elements are inherited from the *transformation* attributes of the same
name. There is no such inheritance for *uses* elements with *executable*
attribute of ``false``.

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

.. _dax-generator-api:

DAX Generator API
=================

The DAX generating APIs support Python, Java, and R. This section
will show in each language the necessary code, using Pegasus-provided
libraries, to generate the diamond DAX example above. There may be minor
differences in details, e.g. to show-case certain features, but
effectively all generate the same basic diamond.

.. _api-java:

The Java DAX Generator API
--------------------------

The Java DAX API provided with the Pegasus distribution allows easy
creation of complex and huge workflows. This API is used by several
applications to generate their abstract DAX. SCEC, which is Southern
California Earthquake Center, uses this API in their CyberShake workflow
generator to generate huge DAX containing 10RSQUORs of thousands of
tasks with 100RSQUORs of thousands of input and output files. The `Java
API <javadoc/index.html>`__ is well documented using `Javadoc for
ADAGs <javadoc/edu/isi/pegasus/planner/dax/ADAG.html>`__ .

The steps involved in creating a DAX using the API are

1.  Create a new *ADAG* object

2.  Add any metadata attributes associated with the whole workflow.

3.  Add any Workflow notification elements

4.  Create *File* objects as necessary. You can augment the files with
    physical information, if you want to include them into your DAX.
    Otherwise, the physical information is determined from the replica
    catalog.

5.  (Optional) Create *Executable* objects, if you want to include your
    transformation catalog into your DAX. Otherwise, the translation of
    a job/task into executable location happens with the transformation
    catalog.

6.  Create a new *Job* object.

7.  Add arguments, files, profiles, notifications and other information
    to the *Job* object

8.  Add the job object to the *ADAG* object

9.  Repeat step 4-6 as necessary.

10. Add all dependencies to the *ADAG* object.

11. Call the *writeToFile()* method on the *ADAG* object to render the
    XML DAX file.

An example Java code that generates the diamond dax show above is listed
below. This same code can be found in the Pegasus distribution in the
``examples/grid-blackdiamond-java`` directory as
``BlackDiamonDAX.java``:

.. code-block:: java

   /**
    *  Copyright 2007-2008 University Of Southern California
    *
    *  Licensed under the Apache License, Version 2.0 (the "License");
    *  you may not use this file except in compliance with the License.
    *  You may obtain a copy of the License at
    *
    *  http://www.apache.org/licenses/LICENSE-2.0
    *
    *  Unless required by applicable law or agreed to in writing,
    *  software distributed under the License is distributed on an "AS IS" BASIS,
    *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    *  See the License for the specific language governing permissions and
    *  limitations under the License.
    */

   import edu.isi.pegasus.planner.dax.*;


   /**
    * An example class to highlight how to use the JAVA DAX API to generate a diamond
    * DAX.
    *
    */
   public class Diamond {



       public class Diamond {



       public ADAG generate(String site_handle, String pegasus_location) throws Exception {

           java.io.File cwdFile = new java.io.File (".");
           String cwd = cwdFile.getCanonicalPath();

           ADAG dax = new ADAG("diamond");
           dax.addNotification(Invoke.WHEN.start,"/pegasus/libexec/notification/email -t notify@example.com");
           dax.addNotification(Invoke.WHEN.at_end,"/pegasus/libexec/notification/email -t notify@example.com");
           dax.addMetadata( "name", "diamond");
           dax.addMetadata( "createdBy", "Karan Vahi");

           File fa = new File("f.a");
           fa.addPhysicalFile("file://" + cwd + "/f.a", "local");
           fa.addMetaData( "size", "1024" );
           dax.addFile(fa);

           File fb1 = new File("f.b1");
           File fb2 = new File("f.b2");
           File fc1 = new File("f.c1");
           File fc2 = new File("f.c2");
           File fd = new File("f.d");
           fd.setRegister(true);

           Executable preprocess = new Executable("pegasus", "preprocess", "4.0");
           preprocess.setArchitecture(Executable.ARCH.X86).setOS(Executable.OS.LINUX);
           preprocess.setInstalled(true);
           preprocess.addPhysicalFile("file://" + pegasus_location + "/bin/keg", site_handle);
           preprocess.addMetaData( "size", "2048" );

           Executable findrange = new Executable("pegasus", "findrange", "4.0");
           findrange.setArchitecture(Executable.ARCH.X86).setOS(Executable.OS.LINUX);
           findrange.setInstalled(true);
           findrange.addPhysicalFile("file://" + pegasus_location + "/bin/keg", site_handle);

           Executable analyze = new Executable("pegasus", "analyze", "4.0");
           analyze.setArchitecture(Executable.ARCH.X86).setOS(Executable.OS.LINUX);
           analyze.setInstalled(true);
           analyze.addPhysicalFile("file://" + pegasus_location + "/bin/keg", site_handle);

           dax.addExecutable(preprocess).addExecutable(findrange).addExecutable(analyze);

           // Add a preprocess job
           Job j1 = new Job("j1", "pegasus", "preprocess", "4.0");
           j1.addArgument("-a preprocess -T 60 -i ").addArgument(fa);
           j1.addArgument("-o ").addArgument(fb1);
           j1.addArgument(" ").addArgument(fb2);
           j1.addMetadata( "time", "60" );
           j1.uses(fa, File.LINK.INPUT);
           j1.uses(fb1, File.LINK.OUTPUT);
           j1.uses(fb2, File.LINK.OUTPUT);
           j1.addNotification(Invoke.WHEN.start,"/pegasus/libexec/notification/email -t notify@example.com");
           j1.addNotification(Invoke.WHEN.at_end,"/pegasus/libexec/notification/email -t notify@example.com");
           dax.addJob(j1);

           // Add left Findrange job
           Job j2 = new Job("j2", "pegasus", "findrange", "4.0");
           j2.addArgument("-a findrange -T 60 -i ").addArgument(fb1);
           j2.addArgument("-o ").addArgument(fc1);
           j2.addMetadata( "time", "60" );
           j2.uses(fb1, File.LINK.INPUT);
           j2.uses(fc1, File.LINK.OUTPUT);
           j2.addNotification(Invoke.WHEN.start,"/pegasus/libexec/notification/email -t notify@example.com");
           j2.addNotification(Invoke.WHEN.at_end,"/pegasus/libexec/notification/email -t notify@example.com");
           dax.addJob(j2);

           // Add right Findrange job
           Job j3 = new Job("j3", "pegasus", "findrange", "4.0");
           j3.addArgument("-a findrange -T 60 -i ").addArgument(fb2);
           j3.addArgument("-o ").addArgument(fc2);
           j3.addMetadata( "time", "60" );
           j3.uses(fb2, File.LINK.INPUT);
           j3.uses(fc2, File.LINK.OUTPUT);
           j3.addNotification(Invoke.WHEN.start,"/pegasus/libexec/notification/email -t notify@example.com");
           j3.addNotification(Invoke.WHEN.at_end,"/pegasus/libexec/notification/email -t notify@example.com");
           dax.addJob(j3);

           // Add analyze job
           Job j4 = new Job("j4", "pegasus", "analyze", "4.0");
           j4.addArgument("-a analyze -T 60 -i ").addArgument(fc1);
           j4.addArgument(" ").addArgument(fc2);
           j4.addArgument("-o ").addArgument(fd);
           j4.addMetadata( "time", "60" );
           j4.uses(fc1, File.LINK.INPUT);
           j4.uses(fc2, File.LINK.INPUT);
           j4.uses(fd, File.LINK.OUTPUT);
           j4.addNotification(Invoke.WHEN.start,"/pegasus/libexec/notification/email -t notify@example.com");
           j4.addNotification(Invoke.WHEN.at_end,"/pegasus/libexec/notification/email -t notify@example.com");
           dax.addJob(j4);

           dax.addDependency("j1", "j2");
           dax.addDependency("j1", "j3");
           dax.addDependency("j2", "j4");
           dax.addDependency("j3", "j4");
           return dax;
       }

       /**
        * Create an example DIAMOND DAX
        * @param args
        */
       public static void main(String[] args) {
           if (args.length != 1) {
               System.out.println("Usage: java GenerateDiamondDAX  <pegasus_location> ");
               System.exit(1);
           }

           try {
               Diamond diamond = new Diamond();
               String pegasusHome = args[0];
               String site = "TestCluster";
               ADAG dag = diamond.generate( site, pegasusHome );
               dag.writeToSTDOUT();
               //generate(args[0], args[1]).writeToFile(args[2]);
           }
           catch (Exception e) {
               e.printStackTrace();
           }

       }
   }

Of course, you will have to set up some catalogs and properties to run
this example. The details are catpured in the examples directory
``examples/grid-blackdiamond-java``.

.. _api-python:

The Python DAX Generator API
----------------------------

Refer to :ref:`python-api` for documentation and usage.

.. code-block:: python

   #!/usr/bin/env python

   from Pegasus.DAX3 import *

   # Create a DAX
   diamond = ADAG("diamond")

   # Add some metadata
   diamond.metadata("name", "diamond")
   diamond.metadata("createdby", "Gideon Juve")

   # Add input file to the DAX-level replica catalog
   a = File("f.a")
   a.addPFN(PFN("gsiftp://site.com/inputs/f.a","site"))
   a.metadata("size", "1024")
   diamond.addFile(a)

   # Add executables to the DAX-level replica catalog
   e_preprocess = Executable(namespace="diamond", name="preprocess", version="4.0", os="linux", arch="x86_64")
   e_preprocess.metadata("size", "2048")
   e_preprocess.addPFN(PFN("gsiftp://site.com/bin/preprocess","site"))
   diamond.addExecutable(e_preprocess)

   e_findrange = Executable(namespace="diamond", name="findrange", version="4.0", os="linux", arch="x86_64")
   e_findrange.addPFN(PFN("gsiftp://site.com/bin/findrange","site"))
   diamond.addExecutable(e_findrange)

   e_analyze = Executable(namespace="diamond", name="analyze", version="4.0", os="linux", arch="x86_64")
   e_analyze.addPFN(PFN("gsiftp://site.com/bin/analyze","site"))
   diamond.addExecutable(e_analyze)

   # Add a preprocess job
   preprocess = Job(e_preprocess)
   preprocess.metadata("time", "60")
   b1 = File("f.b1")
   b2 = File("f.b2")
   preprocess.addArguments("-a preprocess","-T60","-i",a,"-o",b1,b2)
   preprocess.uses(a, link=Link.INPUT)
   preprocess.uses(b1, link=Link.OUTPUT, transfer=True)
   preprocess.uses(b2, link=Link.OUTPUT, transfer=True)
   diamond.addJob(preprocess)

   # Add left Findrange job
   frl = Job(e_findrange)
   frl.metadata("time", "60")
   c1 = File("f.c1")
   frl.addArguments("-a findrange","-T60","-i",b1,"-o",c1)
   frl.uses(b1, link=Link.INPUT)
   frl.uses(c1, link=Link.OUTPUT, transfer=True)
   diamond.addJob(frl)

   # Add right Findrange job
   frr = Job(e_findrange)
   frr.metadata("time", "60")
   c2 = File("f.c2")
   frr.addArguments("-a findrange","-T60","-i",b2,"-o",c2)
   frr.uses(b2, link=Link.INPUT)
   frr.uses(c2, link=Link.OUTPUT, transfer=True)
   diamond.addJob(frr)

   # Add Analyze job
   analyze = Job(e_analyze)
   analyze.metadata("time", "60")
   d = File("f.d")
   analyze.addArguments("-a analyze","-T60","-i",c1,c2,"-o",d)
   analyze.uses(c1, link=Link.INPUT)
   analyze.uses(c2, link=Link.INPUT)
   analyze.uses(d, link=Link.OUTPUT, transfer=True, register=True)
   diamond.addJob(analyze)

   # Add dependencies
   diamond.depends(parent=preprocess, child=frl)
   diamond.depends(parent=preprocess, child=frr)
   diamond.depends(parent=frl, child=analyze)
   diamond.depends(parent=frr, child=analyze)

   # Write the DAX to stdout
   import sys
   diamond.writeXML(sys.stdout)

   # Write the DAX to a file
   f = open("diamond.dax","w")
   diamond.writeXML(f)
   f.close()


.. _api-r:

The R DAX Generator API
-----------------------

The R DAX API provided with the Pegasus distribution allows easy
creation of complex and large workflows in R environments. The API
follows the `Google' R style
guide <http://google.github.io/styleguide/Rguide.xml>`__, and all
objects and methods are defined using the *S3* OOP system.

The API can be installed as follows:

1. Installing from source package (.tar.gz) in an R environment:

   ::

      install.packages("/path/to/source/package.tar.gz", repo=NULL)

   The source package can be obtained using ``pegasus-config --r`` or from the `Pegasus' downloads <http://pegasus.isi.edu/downloads>`__ page.

The R API is well documented using
`Roxygen <http://http://roxygen.org>`__. In an R environment, it can be
accessed using ``help(package=dax3)``. A `PDF
manual <r/dax3-manual.pdf>`__ is also available.

The steps involved in creating a DAX using the API are

1.  Create a new *ADAG* object

2.  Add any metadata attributes associated with the whole workflow.

3.  Add any Workflow notification elements.

4.  Create *File* objects as necessary. You can augment the files with
    physical information, if you want to include them into your DAX.
    Otherwise, the physical information is determined from the replica
    catalog.

5.  (Optional) Create *Executable* objects, if you want to include your
    transformation catalog into your DAX. Otherwise, the translation of
    a job/task into executable location happens with the transformation
    catalog.

6.  Create a new *Job* object.

7.  Add arguments, files, profiles, notifications and other information
    to the *Job* object

8.  Add the job object to the *ADAG* object

9.  Repeat step 4-6 as necessary.

10. Add all dependencies to the *ADAG* object.

11. Call the ``WriteXML()`` method on the *ADAG* object to render the
    XML DAX file.

An example R code that generates the diamond dax show previously is
listed below. A workflow example code can be found in the Pegasus
distribution in the ``examples/grid-blackdiamond-r`` directory as
``blackdiamond.R``:

.. code-block:: R

   #!/usr/bin/Rscript

   library(dax3)

   # Create a DAX
   diamond <- ADAG("diamond")

   # Add some metadata
   diamond <- Metadata(diamond, "name", "diamond")
   diamond <- Metadata(diamond, "createdby", "Rafael Ferreira da Silva")

   # Add input file to the DAX-level replica catalog
   a <- File("f.a")
   a <- AddPFN(a, PFN("gsiftp://site.com/inputs/f.a","site"))
   a <- Metadata(a, "size", "1024")
   diamond <- AddFile(diamond, a)

   # Add executables to the DAX-level replica catalog
   e_preprocess <- Executable(namespace="diamond", name="preprocess", version="4.0", os="linux", arch="x86_64")
   e_preprocess <- Metadata(e_preprocess, "size", "2048")
   e_preprocess <- AddPFN(e_preprocess, PFN("gsiftp://site.com/bin/preprocess","site"))
   diamond <- AddExecutable(diamond, e_preprocess)

   e_findrange <- Executable(namespace="diamond", name="findrange", version="4.0", os="linux", arch="x86_64")
   e_findrange <- AddPFN(e_findrange, PFN("gsiftp://site.com/bin/findrange","site"))
   diamond <- AddExecutable(diamond, e_findrange)

   e_analyze <- Executable(namespace="diamond", name="analyze", version="4.0", os="linux", arch="x86_64")
   e_analyze <- AddPFN(e_analyze, PFN("gsiftp://site.com/bin/analyze","site"))
   diamond <- AddExecutable(diamond, e_analyze)

   # Add a preprocess job
   preprocess <- Job(e_preprocess)
   preprocess <- Metadata(preprocess, "time", "60")
   b1 <- File("f.b1")
   b2 <- File("f.b2")
   preprocess <- AddArguments(preprocess, list("-a preprocess","-T60","-i",a,"-o",b1,b2))
   preprocess <- Uses(preprocess, a, link=DAX3.Link$INPUT)
   preprocess <- Uses(preprocess, b1, link=DAX3.Link$OUTPUT, transfer=TRUE)
   preprocess <- Uses(preprocess, b2, link=DAX3.Link$OUTPUT, transfer=TRUE)
   diamond <- AddJob(diamond, preprocess)

   # Add left Findrange job
   frl <- Job(e_findrange)
   frl <- Metadata(frl, "time", "60")
   c1 <- File("f.c1")
   frl <- AddArguments(frl, list("-a findrange","-T60","-i",b1,"-o",c1))
   frl <- Uses(frl, b1, link=DAX3.Link$INPUT)
   frl <- Uses(frl, c1, link=DAX3.Link$OUTPUT, transfer=TRUE)
   diamond <- AddJob(diamond, frl)

   # Add right Findrange job
   frr <- Job(e_findrange)
   frr <- Metadata(frr, "time", "60")
   c2 <- File("f.c2")
   frr <- AddArguments(frr, list("-a findrange","-T60","-i",b2,"-o",c2))
   frr <- Uses(frr, b2, link=DAX3.Link$INPUT)
   frr <- Uses(frr, c2, link=DAX3.Link$OUTPUT, transfer=TRUE)
   diamond <- AddJob(diamond, frr)

   # Add Analyze job
   analyze <- Job(e_analyze)
   analyze <- Metadata(analyze, "time", "60")
   d <- File("f.d")
   analyze <- AddArguments(analyze, list("-a analyze","-T60","-i",c1,c2,"-o",d))
   analyze <- Uses(analyze, c1, link=DAX3.Link$INPUT)
   analyze <- Uses(analyze, c2, link=DAX3.Link$INPUT)
   analyze <- Uses(analyze, d, link=DAX3.Link$OUTPUT, transfer=TRUE)
   diamond <- AddJob(diamond, analyze)

   # Add dependencies
   diamond <- Depends(diamond, parent=preprocess, child=frl)
   diamond <- Depends(diamond, parent=preprocess, child=frr)
   diamond <- Depends(diamond, parent=frl, child=analyze)
   diamond <- Depends(diamond, parent=frr, child=analyze)

   # Get generated diamond dax
   WriteXML(diamond, stdout())

.. _dax-no-api:

DAX Generator without a Pegasus DAX API
=======================================

If you are using some other scripting or programming environment, you
can directly write out the DAX format using the provided schema using
any language. For instance, LIGO, the Laser Interferometer Gravitational
Wave Observatory, generate their DAX files as XML using their own Python
code, not using our provided API.

.. _rest-api-monitoring:

Monitoring
==========

Monitoring REST API allows developers to query a Pegasus workflow's
STAMPEDE database.

Resource Definition
-------------------

.. _resource-root-workflow:

Root Workflow
~~~~~~~~~~~~~

::

   {
       "wf_id"             : <int:wf_id>,
       "wf_uuid"           : <string:wf_uuid>,
       "submit_hostname"   : <string:submit_hostname>,
       "submit_dir"        : <string:submit_dir>,
       "planner_arguments" : <string:planner_arguments>,
       "planner_version"   : <string:planner_version>,
       "user"              : <string:user>,
       "grid_dn"           : <string:grid_dn>,
       "dax_label"         : <string:dax_label>,
       "dax_version"       : <string:dax_version>,
       "dax_file"          : <string:dax_file>,
       "dag_file_name"     : <string:dag_file_name>,
       "timestamp"         : <int:timestamp>,
       "workflow_state"    : <object:workflow_state>,
   }

.. _resource-workflow:

Workflow
~~~~~~~~

::

   {
       "wf_id"             : <int:wf_id>,
       "root_wf_id"        : <int:root_wf_id>,
       "parent_wf_id"      : <int:parent_wf_id>,
       "wf_uuid"           : <string:wf_uuid>,
       "submit_hostname"   : <string:submit_hostname>,
       "submit_dir"        : <string:submit_dir>,
       "planner_arguments" : <string:planner_arguments>,
       "planner_version"   : <string:planner_version>,
       "user"              : <string:user>,
       "grid_dn"           : <string:grid_dn>,
       "dax_label"         : <string:dax_label>,
       "dax_version"       : <string:dax_version>,
       "dax_file"          : <string:dax_file>,
       "dag_file_name"     : <string:dag_file_name>,
       "timestamp"         : <int:timestamp>,
   }

.. _resource-workflow-metadata:

Workflow Metadata
~~~~~~~~~~~~~~~~~

::

   {
       "key"    : <string:key>,
       "value"  : <string:value>,
   }

.. _resource-workflow-files:

Workflow Files
~~~~~~~~~~~~~~

::

   {
       "wf_id"  : <int:wf_id>,
       "lfn_id" : <string:lfn_id>,
       "lfn"    : <string:lfn>,
       "pfns"   : [
            {
               "pfn_id" : <int:pfn_id>,
               "pfn"    : <string:pfn>,
               "site"   : <string:site>
            }
       ],
       "meta" : [
            {
               "meta_id" : <int:meta_id>,
               "key"     : <string:key>,
               "value"   : <string:value>
            }
       ],
   }

.. _resource-workflow-state:

Workflow State
~~~~~~~~~~~~~~

::

   {
       "wf_id"         : int:wf_id,
       "state"         : <string:state>,
       "status"        : <int:status>,
       "restart_count" : <int:restart_count>,
       "timestamp"     : <datetime:timestamp>,
   }

.. _resource-job:

Job
~~~

::

   {
       "job_id"      : <int: job_id>,
       "exec_job_id" : <string: exec_job_id>,
       "submit_file" : <string: submit_file>,
       "type_desc"   : <string: type_desc>,
       "max_retries" : <int: max_retries>,
       "clustered"   : <bool: clustered>,
       "task_count"  : <int: task_count>,
       "executable"  : <string: executable>,
       "argv"        : <string: argv>,
       "task_count"  : <int:task_count>,
   }

.. _resource-host:

Host
~~~~

::

   {
       "host_id"      : <int:host_id>,
       "site_name"    : <string:site_name>,
       "hostname"     : <string:hostname>,
       "ip"           : <string:ip>,
       "uname"        : <string:uname>,
       "total_memory" : <string:total_memory>,
   }

.. _resource-job-state:

Job State
~~~~~~~~~

::

   {
       "job_instance_id"     : <int:job_instance_id>,
       "state"               : <string:state>,
       "jobstate_submit_seq" : <int:jobstate_submit_seq>,
       "timestamp"           : <int:timestamp>,
   }

.. _resource-task:

Task
~~~~

::

   {
       "task_id"        : <int:task_id>,
       "abs_task_id"    : <string:abs_task_id>,
       "type_desc"      : <string: type_desc>,
       "transformation" : <string:transformation>,
       "argv"           : <string:argv>,
   }

.. _resource-task-metadata:

Task Metadata
~~~~~~~~~~~~~

::

   {
       "key"    : <string:key>,
       "value"  : <string:value>,
   }

.. _resource-job-instance:

Job Instance
~~~~~~~~~~~~

::

   {
       "job_instance_id"   : <int:job_instance_id>,
       "host_id"           : <int:host_id>,
       "job_submit_seq"    : <int:job_submit_seq>,
       "sched_id"          : <string:sched_id>,
       "site_name"         : <string:site_name>,
       "user"              : <string:user>,
       "work_dir"          : <string:work_dir>,
       "cluster_start"     : <int:cluster_start>,
       "cluster_duration"  : <int:cluster_duration>,
       "local_duration"    : <int:local_duration>,
       "subwf_id"          : <int:subwf_id>,
       "stdout_text"       : <string:stdout_text>,
       "stderr_text"       : <string:stderr_text>,
       "stdin_file"        : <string:stdin_file>,
       "stdout_file"       : <string:stdout_file>,
       "stderr_file"       : <string:stderr_file>,
       "multiplier_factor" : <int:multiplier_factor>,
       "exitcode"          : <int:exitcode>,
   }

.. _resource-invocation:

Invocation
~~~~~~~~~~

::

   {
       "invocation_id"   : <int:invocation_id>,
       "job_instance_id" : <int:job_instance_id>,
       "abs_task_id"     : <string:abs_task_id>,
       "task_submit_seq" : <int:task_submit_seq>,
       "start_time"      : <int:start_time>,
       "remote_duration" : <int:remote_duration>,
       "remote_cpu_time" : <int:remote_cpu_time>,
       "exitcode"        : <int:exitcode>,
       "transformation"  : <string:transformation>,
       "executable"      : <string:executable>,
       "argv"            : <string:argv>,
   }

RC LFN
~~~~~~

::

   {
       "lfn_id" : <int:pfn_id>,
       "lfn"    : <string:pfn>
   }

RC PFN
~~~~~~

::

   {
       "pfn_id" : <int:pfn_id>,
       "pfn"    : <string:pfn>,
       "site"   : <string:site>
   }

RC Metadata
^^^^^^^^^^^

::

   {
       "meta_id" : <int:meta_id>,
       "key"     : <string:key>,
       "value"   : <string:value>
   }

.. _section-endpoints:

Endpoints
---------

All URIs are prefixed by\ **/api/v1/user/<string:user>**.

All endpoints return response with content-type as application/json.

All endpoints support **pretty-print** query argument, to return a
formatted JSON response.

All endpoints return status code **401** for\ **Authentication
failure**.

All endpoints return status code **403** for\ **Authorization failure**.

.. openapi:: ../../packages/pegasus-python/src/Pegasus/service/monitoring/swagger.yml

Querying
--------

Querying is supported through query string argument **query**.

Querying is supported only on endpoints returning collections.

Syntax
~~~~~~

Query clauses are rudimentary and must follow some rules.

-  Supported comparators are **==**, **!=**, **<**, **<=**, **>**, **>=**, **in**.

-  Supported operators are **and**, **or**, **not**.

-  Supported functions **like**, **ilike**.

-  Comparision clauses must have the form <FIELDNAME> SPACE <COMPARATOR>
   SPACE <STRING LITERAL OR INTEGER LITERAL OR FLOAT LITERAL>

-  <FIELDNAME> must be prefixed with resource query prefix Example:
   **r.wf_id** is valid, but **wf_id** is not.

-  <FIELDNAMES> which can be used in a query caluse depends on the
   resource being queries. Example: For endpoint
   /api/v1/user/user-a/root/1/**workflow**/1/**job**/2/**state** query
   clause can only contain fields from the `Job
   State <#resource-job-state>`__ resource.

-  Only exceptions for the previous rules are

   Querying Root Workflow where fields from both `Root
   Workflow <#resource-root-workflow>`__ and `Workflow
   State <#resource-workflow-state>`__ can be included.

   Querying the /api/v1/user/user-a/root/1/workflow/1/files where fields
   from RC LFN, RC PFN, an RC Metadata can be included.

   Views endpoint
   /api/v1/user/user-a/root/1/workflow/1/job/<[running|successful|failed|failing]>
   where fields from `Job <#resource-job>`__ and
   `JobInstance <#resource-job-instance>`__ resource can be included.

**Example**

::

   For Root Workflow https://www.domain.com/api/v1/user/user-a/root?query<QUERY>

   Where QUERY can be( r.wf_id >= 5 AND r.planner_version.like( '4.5%' ) OR ( r.wf_id in ( 1, 2 ) )

Resource - Query Prefix
~~~~~~~~~~~~~~~~~~~~~~~

.. table:: Query Prefix

   ===================== ============ =============
   Resource              Query Prefix Example
   ===================== ============ =============
   **Root Workflow**     r            r.wf_id
   **Workflow**          w            w.wf_uuid
   **Workflow Metadata** wm           wm.key
   **Workflow Files**    wf           wf.lfn
   **Workflow State**    ws           ws.state
   **Job**               j            j.type_desc
   **Host**              h            h.site
   **Job State**         js           js.state
   **Task**              t            t.abs_task_id
   **Task Metadata**     tm           tm.value
   **Job Instance**      ji           ji.exitcode
   **Job**               i            i.argv
   **RC LFN**            l            l.lfn
   **RC PFN**            p            p.pfn
   **RC Metadata**       rm           rm.key
   ===================== ============ =============

Recent
~~~~~~

Job Instance resources have historical records.

For use cases where developers need to get the most recent record, we
set **query** argument **recent** to **true**.

Ordering
--------

Ordering is supported through query string argument **order**.

Ordering is supported only on endpoints returning collections.

Order clause can only contain fields which are part of the resource
being returned. Fields must be prefixed by the Resource Query Prefix

**Example:** Order clause for an endpoint returning a
`Workflow <#resource-workflow>`__ resource can only contain fields that
are part of the `Workflow <#resource-workflow>`__ resource.

Syntax
~~~~~~

Order clause consists of one or more field names optionally prefixed by
a "+" to denote ascending or "-" to denote sorting direction, separated
by commas.

::

   https://www.domain.com/api/v1/user/user-a/root?order=r.submit_hostname,-r.wf_id

Examples
--------

Resource - Single
~~~~~~~~~~~~~~~~~

::

   $ curl --request GET \
          --user user-a:user-a-password \
          https://www.domain.com/api/v1/user/user-a/root/1/workflow/1?pretty-print=true


   HTTP/1.1 200 OK

   {
       "wf_id"             : 1,
       "root_wf_id"        : 1,
       "parent_wf_id"      : null,
       "wf_uuid"           : "7193de8c-a28d-4eca-b576-1b1c3c4f668b",
       "submit_hostname"   : "isis.isi.edu",
       "submit_dir"        : "/home/tutorial/submit/",
       "planner_arguments" : "--conf pegasusrc --sites condorpool --output-site local --dir dags --dax dax.xml --submit",
       "planner_version"   : "4.5.0dev",
       "user"              : "user-a",
       "grid_dn"           : null,
       "dax_label"         : "hello_world",
       "dax_version"       : "3.5",
       "dax_file"          : "/home/tutorial/hello-world.xml",
       "dag_file_name"     : "hello_world-0.dag",
       "timestamp"         : 1421432530.0
   }

Resource - Collection
~~~~~~~~~~~~~~~~~~~~~

::

   $ curl --request GET \
          --user user-a:user-a-password \
          https://www.domain.com/api/v1/user/user-a/root/1/workflow?pretty-print=true

   HTTP/1.1 200 OK

   {
       "records" : [
           {
               "wf_id"             : 1,
               "root_wf_id"        : 1,
               "parent_wf_id"      : null,
               "wf_uuid"           : "7193de8c-a28d-4eca-b576-1b1c3c4f668b",
               "submit_hostname"   : "isis.isi.edu",
               "submit_dir"        : "/home/tutorial/dags/20150116T102210-0800",
               "planner_arguments" : "--conf pegasusrc --sites condorpool --output-site local --dir dags  --dax dax.xml --submit",
               "planner_version"   : "4.5.0dev",
               "user"              : "user-a",
               "grid_dn"           : null,
               "dax_label"         : "hello_world",
               "dax_version"       : "3.5",
               "dax_file"          : "/home/tutorial/hello-world.xml",
               "dag_file_name"     : "hello_world-0.dag",
               "timestamp"         : 1421432530.0
           },
           {
               "wf_id"             : 2,
               "root_wf_id"        : 2,
               "parent_wf_id"      : null,
               "wf_uuid"           : "41920a57-7882-4990-854e-658b7a797745",
               "submit_hostname"   : "isis.isi.edu",
               "submit_dir"        : "/home/tutorial/dags/20150330T165231-0700",
               "planner_arguments" : "--conf pegasusrc --sites condorpool --output-site local --dir dags --dax dax.xml --submit",
               "planner_version"   : "4.5.0dev",
               "user"              : "user-a",
               "grid_dn"           : null,
               "dax_label"         : "hello_world",
               "dax_version"       : "3.5",
               "dax_file"          : "/home/tutorial/hello-world.xml",
               "dag_file_name"     : "hello_world-0.dag",
               "timestamp"         : 1427759551.0
           },
           {
               "wf_id"             : 3,
               "root_wf_id"        : 3,
               "parent_wf_id"      : null,
               "wf_uuid"           : "fce67b41-df67-4b3c-8fa4-d77e6e2b9769",
               "submit_hostname"   : "isis.isi.edu",
               "submit_dir"        : "/home/tutorial/dags/20150330T170228-0700",
               "planner_arguments" : "--conf pegasusrc --sites condorpool --output-site local --dir dags --dax dax.xml --submit",
               "planner_version"   : "4.5.0dev",
               "user"              : "user-a",
               "grid_dn"           : null,
               "dax_label"         : "hello_world",
               "dax_version"       : "3.5",
               "dax_file"          : "/home/tutorial/hello-world.xml",
               "dag_file_name"     : "hello_world-0.dag",
               "timestamp"         : 1427760148.0
           }
       ],
       "_meta"   : {
           "records_total"    : 3,
           "records_filtered" : 3
       }
   }

Querying
~~~~~~~~

::

   $ curl --request GET \
          --get \
          --data-urlencode "pretty-print=true" \
          --data-urlencode "query=w.wf_uuid == '41920a57-7882-4990-854e-658b7a797745'" \
          --user user-a:user-a-password \
          https://www.domain.com/api/v1/user/user-a/root/1/workflow

   HTTP/1.1 200 OK

   {
       "records" : [
           {
               "wf_id"             : 2,
               "root_wf_id"        : 2,
               "parent_wf_id"      : null,
               "wf_uuid"           : "41920a57-7882-4990-854e-658b7a797745",
               "submit_hostname"   : "isis.isi.edu",
               "submit_dir"        : "/home/tutorial/dags/20150330T165231-0700",
               "planner_arguments" : "--conf pegasusrc --sites condorpool --output-site local --dir dags --dax dax.xml --submit",
               "planner_version"   : "4.5.0dev",
               "user"              : "user-a",
               "grid_dn"           : null,
               "dax_label"         : "hello_world",
               "dax_version"       : "3.5",
               "dax_file"          : "/home/tutorial/hello-world.xml",
               "dag_file_name"     : "hello_world-0.dag",
               "timestamp"         : 1427759551.0
           }
       ],
       "_meta"   : {
           "records_total"    : 3,
           "records_filtered" : 1
       }
   }

Ordering
~~~~~~~~

::

   $ curl --request GET \
          --user user-a:user-a-password \
          https://www.domain.com/api/v1/user/user-a/root/1/workflow?pretty-print=true&order=-w.wf_id

   HTTP/1.1 200 OK

   {
       "records" : [
           {
               "wf_id"             : 3,
               "root_wf_id"        : 3,
               "parent_wf_id"      : null,
               "wf_uuid"           : "fce67b41-df67-4b3c-8fa4-d77e6e2b9769",
               "submit_hostname"   : "isis.isi.edu",
               "submit_dir"        : "/home/tutorial/dags/20150330T170228-0700",
               "planner_arguments" : "--conf pegasusrc --sites condorpool --output-site local --dir dags --dax dax.xml --submit",
               "planner_version"   : "4.5.0dev",
               "user"              : "user-a",
               "grid_dn"           : null,
               "dax_label"         : "hello_world",
               "dax_version"       : "3.5",
               "dax_file"          : "/home/tutorial/hello-world.xml",
               "dag_file_name"     : "hello_world-0.dag",
               "timestamp"         : 1427760148.0
           },
           {
               "wf_id"             : 2,
               "root_wf_id"        : 2,
               "parent_wf_id"      : null,
               "wf_uuid"           : "41920a57-7882-4990-854e-658b7a797745",
               "submit_hostname"   : "isis.isi.edu",
               "submit_dir"        : "/home/tutorial/dags/20150330T165231-0700",
               "planner_arguments" : "--conf pegasusrc --sites condorpool --output-site local --dir dags --dax dax.xml --submit",
               "planner_version"   : "4.5.0dev",
               "user"              : "user-a",
               "grid_dn"           : null,
               "dax_label"         : "hello_world",
               "dax_version"       : "3.5",
               "dax_file"          : "/home/tutorial/hello-world.xml",
               "dag_file_name"     : "hello_world-0.dag",
               "timestamp"         : 1427759551.0
           },
           {
               "wf_id"             : 1,
               "root_wf_id"        : 1,
               "parent_wf_id"      : null,
               "wf_uuid"           : "7193de8c-a28d-4eca-b576-1b1c3c4f668b",
               "submit_hostname"   : "isis.isi.edu",
               "submit_dir"        : "/home/tutorial/dags/20150116T102210-0800",
               "planner_arguments" : "--conf pegasusrc --sites condorpool --output-site local --dir dags  --dax dax.xml --submit",
               "planner_version"   : "4.5.0dev",
               "user"              : "user-a",
               "grid_dn"           : null,
               "dax_label"         : "hello_world",
               "dax_version"       : "3.5",
               "dax_file"          : "/home/tutorial/hello-world.xml",
               "dag_file_name"     : "hello_world-0.dag",
               "timestamp"         : 1421432530.0
           }
       ],
       "_meta"   : {
           "records_total"    : 3,
           "records_filtered" : 3
       }
   }
