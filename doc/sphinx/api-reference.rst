.. _api-reference:

============
Workflow API
============

.. _api-python:

Python 
======

The Python API is the most powerful way of creating Pegasus workflows.

.. toctree::

    python/Pegasus.api.rst

Example workflow generator using the Python API:

.. code-block:: python

   import logging

   from pathlib import Path

   from Pegasus.api import *

   logging.basicConfig(level=logging.DEBUG)

   # --- Replicas -----------------------------------------------------------------
   with open("f.a", "w") as f:
      f.write("This is sample input to KEG")

   fa = File("f.a").add_metadata(creator="ryan")
   rc = ReplicaCatalog().add_replica("local", fa, Path(".").resolve() / "f.a")

   # --- Transformations ----------------------------------------------------------
   preprocess = Transformation(
                  "preprocess",
                  site="condorpool",
                  pfn="/usr/bin/pegasus-keg",
                  is_stageable=False,
                  arch=Arch.X86_64,
                  os_type=OS.LINUX
               )

   findrange = Transformation(
                  "findrange",
                  site="condorpool",
                  pfn="/usr/bin/pegasus-keg",
                  is_stageable=False,
                  arch=Arch.X86_64,
                  os_type=OS.LINUX
               )

   analyze = Transformation(
                  "analyze",
                  site="condorpool",
                  pfn="/usr/bin/pegasus-keg",
                  is_stageable=False,
                  arch=Arch.X86_64,
                  os_type=OS.LINUX
               )

   tc = TransformationCatalog().add_transformations(preprocess, findrange, analyze)

   # --- Workflow -----------------------------------------------------------------
   '''
                        [f.b1] - (findrange) - [f.c1] 
                        /                             \
   [f.a] - (preprocess)                               (analyze) - [f.d]
                        \                             /
                        [f.b2] - (findrange) - [f.c2]

   '''
   wf = Workflow("blackdiamond")

   fb1 = File("f.b1")
   fb2 = File("f.b2")
   job_preprocess = Job(preprocess)\
                        .add_args("-a", "preprocess", "-T", "3", "-i", fa, "-o", fb1, fb2)\
                        .add_inputs(fa)\
                        .add_outputs(fb1, fb2)

   fc1 = File("f.c1")
   job_findrange_1 = Job(findrange)\
                        .add_args("-a", "findrange", "-T", "3", "-i", fb1, "-o", fc1)\
                        .add_inputs(fb1)\
                        .add_outputs(fc1)

   fc2 = File("f.c2")
   job_findrange_2 = Job(findrange)\
                        .add_args("-a", "findrange", "-T", "3", "-i", fb2, "-o", fc2)\
                        .add_inputs(fb2)\
                        .add_outputs(fc2)

   fd = File("f.d")
   job_analyze = Job(analyze)\
                  .add_args("-a", "analyze", "-T", "3", "-i", fc1, fc2, "-o", fd)\
                  .add_inputs(fc1, fc2)\
                  .add_outputs(fd)

   wf.add_jobs(job_preprocess, job_findrange_1, job_findrange_2, job_analyze)
   wf.add_replica_catalog(rc)
   wf.add_transformation_catalog(tc)

   try:
      wf.plan(submit=True)\
            .wait()\
            .analyze()\
            .statistics()
   except PegasusClientError as e:
      print(e.output)

.. _api-java:

Java
====

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
this example. 


.. _api-r:

R
=

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

.. _rest-api-monitoring:

Monitoring REST API
===================

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
