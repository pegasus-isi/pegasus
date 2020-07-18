============
pegasus-plan
============

runs Pegasus to generate the executable workflow
   ::

      pegasus-plan [-Dprop=value…]] [-b prefix]
                   [-v] [-q] [-V] [-h]
                   [--conf propsfile]
                   [-c cachefile[,cachefile…]] [--cleanup cleanup strategy ]
                   [-C style[,style…]]
                   [--dir dir]
                   [--force] [--force-replan]
                   [--inherited-rc-files file1[,file2…]] [-j prefix]
                   [-n][-I input-dir1[,input-dir2…]][-O output-dir]
                   [-o site1[,site2…]]
                   [-s site1[,site2…]]
                   [--staging-site s1=ss1[,s2=ss2[..]]
                   [--randomdir[=dirname]]
                   [--relative-dir dir]
                   [--relative-submit-dir dir]
                   [-X[non standard jvm option]]
                   [abstract-workflow]



Description
===========

The **pegasus-plan** command takes in as input the abstract workflow and
generates an executable workflow usually in form of **HTCondor** submit files,
which can be submitted to an *execution* site for execution.

As part of generating an executable workflow, the planner needs to
discover:

**data**
   The Pegasus Workflow Planner ensures that all the data required for
   the execution of the executable workflow is transferred to the
   execution site by adding transfer nodes at appropriate points in the
   DAG. This is done by looking up an appropriate **Replica Catalog** to
   determine the locations of the input files for the various jobs. By
   default, a file based replica catalog is used.

   The Pegasus Workflow Planner also tries to reduce the workflow,
   unless specified otherwise. This is done by deleting the jobs whose
   output files have been found in some location in the Replica Catalog.
   At present no cost metrics are used. However preference is given to a
   location corresponding to the execution site.

   The default location from where the replica catalog file is picked up
   is *replicas.yml* in the current working directory. To specify a
   replica catalog residing in a different location you can specify
   **pegasus.catalog.replica.file** property value.

   The planner can also add nodes to transfer all the materialized files
   to an output site. The location on the output site is determined by
   looking up the site catalog file, the path to which is picked up from
   the **pegasus.catalog.site.file** property value.

**executables**
   The planner looks up a Transformation Catalog to discover locations
   of the executables referred to in the executable workflow. Users can
   specify INSTALLED or STAGEABLE executables in the catalog. Stageable
   executables can be used by Pegasus to stage executables to resources
   where they are not pre-installed.

   The default location from where the replica catalog file is picked up
   is *transformations.yml* in the current working directory. To specify a
   transformation catalog residing in a different location you can specify
   **pegasus.catalog.transformation.file** property value.

**resources**
   The layout of the sites, where Pegasus can schedule jobs of a
   workflow are described in the Site Catalog. The planner looks up the
   site catalog to determine for a site what directories a job can be
   executed in, what servers to use for staging in and out data and what
   jobmanagers (if applicable) can be used for submitting jobs.

   The default location from where the replica catalog file is picked up
   is *sites.yml* in the current working directory. To specify a
   site catalog residing in a different location you can specify
   **pegasus.catalog.site.file** property value.

Options
=======

Any option will be displayed with its long options synonym(s).

**-D**\ *property=value*
   The **-D** option allows an experienced user to override certain
   properties which influence the program execution, among them the
   default location of the user’s properties file and the PEGASUS home
   location. One may set several CLI properties by giving this option
   multiple times. The **-D** option(s) must be the first option on the
   command line. A CLI property take precedence over the properties file
   property of the same key.

**-b** *prefix*; \ **--basename** *prefix*
   The basename prefix to be used while constructing per workflow files
   like the dagman file (.dag file) and other workflow specific files
   that are created by Condor. Usually this prefix, is taken from the
   name attribute specified in the root element of the dax files.

**-c** *file*\ [,*file*,…]; \ **--cache** *file*\ [,*file*,…]
   A comma separated list of paths to replica cache files that override
   the results from the replica catalog for a particular LFN.

   Each entry in the cache file describes a LFN , the corresponding PFN
   and the associated attributes. The site attribute should be specified
   for each entry.

   ::

      LFN_1 PFN_1 site=[site handle 1]
      LFN_2 PFN_2 site=[site handle 2]
       ...
      LFN_N PFN_N [site handle N]

   To treat the cache files as supplemental replica catalogs set the
   property **pegasus.catalog.replica.cache.asrc** to true. This results
   in the mapping in the cache files to be merged with the mappings in
   the replica catalog. Thus, for a particular LFN both the entries in
   the cache file and replica catalog are available for replica
   selection.

**-C** *style*\ [,*style*,…]; \ **--cluster** *style*\ [,*style*,…]
   Comma-separated list of clustering styles to apply to the workflow.
   This mode of operation results in clustering of n compute jobs into a
   larger jobs to reduce remote scheduling overhead. You can specify a
   list of clustering techniques to recursively apply them to the
   workflow. For example, this allows you to cluster some jobs in the
   workflow using horizontal clustering and then use label based
   clustering on the intermediate workflow to do vertical clustering.

   The clustered jobs can be run at the remote site, either sequentially
   or by using MPI. This can be specified by setting the property
   **pegasus.job.aggregator**. The property can be overridden by
   associating the PEGASUS profile key *collapser* either with the
   transformation in the transformation catalog or the execution site in
   the site catalog. The value specified (to the property or the
   profile), is the logical name of the transformation that is to be
   used for clustering jobs. Note that clustering will only happen if
   the corresponding transformations are catalogued in the
   transformation catalog.

   PEGASUS ships with a clustering executable *pegasus-cluster* that can
   be found in the *$PEGASUS_HOME/bin* directory. It runs the jobs in
   the clustered job sequentially on the same node at the remote site.

   In addition, an MPI based clustering tool called
   pegasus-mpi-cluster', is also distributed and can be found in the bin
   directory. pegasus-mpi-cluster can also be used in the sharedfs setup
   and needs to be compiled against the remote site MPI install.
   directory. The wrapper is run on every MPI node, with the first one
   being the master and the rest of the ones as workers.

   By default, *pegasus-cluster* is used for clustering jobs unless
   overridden in the properties or by the pegasus profile key
   *collapser*.

   The following type of clustering styles are currently supported:

   -  **horizontal** is the style of clustering in which jobs on the
      same level are aggregated into larger jobs. A level of the
      workflow is defined as the greatest distance of a node, from the
      root of the workflow. Clustering occurs only on jobs of the same
      type i.e they refer to the same logical transformation in the
      transformation catalog.

      Horizontal Clustering can operate in one of two modes. a. Job
      count based.

      The granularity of clustering can be specified by associating
      either the PEGASUS profile key *clusters.size* or the PEGASUS
      profile key *clusters.num* with the transformation.

      The *clusters.size* key indicates how many jobs need to be
      clustered into the larger clustered job. The clusters.num key
      indicates how many clustered jobs are to be created for a
      particular level at a particular execution site. If both keys are
      specified for a particular transformation, then the clusters.num
      key value is used to determine the clustering granularity.

      a. Runtime based.

         To cluster jobs according to runtimes user needs to set one
         property and two profile keys. The property
         pegasus.clusterer.preference must be set to the value
         *runtime*. In addition user needs to specify two Pegasus
         profiles. a. clusters.maxruntime which specifies the maximum
         duration for which the clustered job should run for. b.
         job.runtime which specifies the duration for which the job with
         which the profile key is associated, runs for. Ideally,
         clusters.maxruntime should be set in transformation catalog and
         job.runtime should be set for each job individually.

   -  **label** is the style of clustering in which you can label the
      jobs in your workflow. The jobs with the same level are put in the
      same clustered job. This allows you to aggregate jobs across
      levels, or in a manner that is best suited to your application.

      To label the workflow, you need to associate PEGASUS profiles with
      the jobs in the DAX. The profile key to use for labeling the
      workflow can be set by the property *pegasus.clusterer.label.key*.
      It defaults to label, meaning if you have a PEGASUS profile key
      label with jobs, the jobs with the same value for the pegasus
      profile key label will go into the same clustered job.

**--cleanup** *cleanup strategy*
   The cleanup strategy to be used for workflows. Pegasus can add
   cleanup jobs to the executable workflow that can remove files and
   directories during the workflow execution. The default strategy is
   inplace .

   The following type of cleanup strategies are currently supported:

   -  **none** disables cleanup altogether. The planner does not add any
      cleanup jobs in the executable workflow whatsoever.

   -  **leaf** the planner adds a leaf cleanup node per staging site
      that removes the directory created by the create dir job in the
      workflow.

   -  **inplace** the planner adds in addition to leaf cleanup nodes,
      cleanup nodes per level of the workflow that remove files no
      longer required during execution. For example, an added cleanup
      node will remove input files for a particular compute job after
      the job has finished successfully.

   -  **constraint** the planner adds in addition to leaf cleanup nodes,
      cleanup nodes to constraint the amount of storage space used by a
      workflow. The added cleanup node guarantees limits on disk usage.

      By default, for hierarchal workflows the inplace cleanup is always
      turned off. This is because the cleanup algorithm ( InPlace ) does
      not work across the sub workflows. For example, if you have two
      DAX jobs in your top level workflow and the child DAX job refers
      to a file generated during the execution of the parent DAX job,
      the InPlace cleanup algorithm when applied to the parent dax job
      will result in the file being deleted, when the sub workflow
      corresponding to parent DAX job is executed. This would result in
      failure of sub workflow corresponding to the child DAX job, as the
      file deleted is required to present during it’s execution.

      In case there are no data dependencies across the dax jobs, then
      yes you can enable the InPlace algorithm for the sub dax’es . To
      do this you can set the property

      pegasus.file.cleanup.scope deferred

      This will result in cleanup option to be picked up from the
      arguments for the DAX job in the top level DAX.

**--conf** *propfile*
   The path to properties file that contains the properties planner
   needs to use while planning the workflow. Defaults to
   pegasus.properties file in the current working directory, if no conf
   option is specified.

**--dir** *dir*
   The base directory where you want the output of the Pegasus Workflow
   Planner usually condor submit files, to be generated. Pegasus creates
   a directory structure in this base directory on the basis of
   username, VO Group and the label of the workflow in the DAX.

   By default the base directory is the directory from which one runs
   the **pegasus-plan** command.

**-f**; \ **--force**
   This bypasses the reduction phase in which the abstract DAG is
   reduced, on the basis of the locations of the output files returned
   by the replica catalog. This is analogous to a **make** style
   generation of the executable workflow.

**--force-replan**
   By default, for hierarichal workflows if a DAX job fails, then on job
   retry the rescue DAG of the associated workflow is submitted. This
   option causes Pegasus to replan the DAX job in case of failure
   instead.

**-g**; \ **--group**
   The VO Group to which the user belongs to.

**-h**; \ **--help**
   Displays all the options to the **pegasus-plan** command.

**--inherited-rc-files** *file*\ [,*file*,…]
   A comma separated list of paths to replica files. Locations mentioned
   in these have a lower priority than the locations in the DAX file.
   This option is usually used internally for hierarchical workflows,
   where the file locations mentioned in the parent (encompassing)
   workflow DAX, passed to the sub workflows (corresponding) to the DAX
   jobs.

**-I**; \ **--input-dir**
   A comma separated list of input directories on the submit host where
   the input files reside. This internally loads a Directory based
   Replica Catalog backend, that constructs does a directory listing to
   create the LFN→PFN mappings for the files in the input directory. You
   can specify additional properties either on the command line or the
   properties file to control the site attribute and url prefix
   associated with the mappings.

   pegasus.catalog.replica.directory.site specifies the site attribute
   to associate with the mappings. Defaults to local

   pegasus.catalog.replica.directory.url.prefix specifies the URL prefix
   to use while constructing the PFN. Defaults to file://

**-j** *prefix*; \ **--job-prefix** *prefix*
   The job prefix to be applied for constructing the filenames for the
   job submit files.

**-n**; \ **--nocleanup**
   This option is deprecated. Use --cleanup none instead.

**-o** *site*\[,*site*,…]; \ **--output-sites** *site*\[,*site*,…]
   A comma separated list of output sites where the outputs generated by
   the workflow are transferred to.

   By default the **materialized data** remains in the working directory
   on the **staging** site where it was created, unless cleanup options
   are enabled.

   Only those output files are transferred to an output site for
   which transfer attribute is set to true in the abstract workflow.

**-O** *output directory*; \ **--output-dir** *output directory*
   The output directory to which the output files of the DAX are
   transferred to.

   If -o is specified and refers to only one site, then the storage
   directory of the site specified as the output site is updated to
   be the directory passed. If no output site is specified, then this
   option internally sets the output site to local with the storage
   directory updated to the directory passed.

**-q**; \ **--quiet**
   Decreases the logging level.

**-r**\ [*dirname*]; \ **--randomdir**\ [=*dirname*]
   Pegasus Workflow Planner adds create directory jobs to the executable
   workflow that create a directory on the staging sites associated with
   the execution sites on which the workflow executes. The directory
   created is in the working directory for the staging site (specified
   in the site catalog with each site). By default, Pegasus duplicates
   the relative directory structure on the submit host on the remote site.

   This option creates random directories based on workflow label and
   the workflow uuid (listed in the braindump file in the sumit directory)
   on the remote staging sites where data transfer jobs for the workflow
   are executed. If the basename option is set, then instead of the
   workflow label, the basename is used for generating the random
   directory name along with the workflow uuid. The user can also
   specify the optional argument to this option to specify the
   the relative directory that is to be created.

   The create dir jobs refer to the **dirmanager** executable that is
   shipped as part of the PEGASUS worker package. The transformation
   catalog is searched for the transformation named
   **pegasus::dirmanager** for all the remote sites where the workflow
   has been scheduled. Pegasus can create a default path for the
   dirmanager executable, if **PEGASUS_HOME** environment variable is
   associated with the sites in the site catalog as an environment
   profile.

**--relative-dir** *dir*
   The directory relative to the base directory where the executable
   workflow is to be generated and executed. This overrides the default
   directory structure that Pegasus creates based on username, VO Group
   and the DAX label.

**--relative-submit-dir** *dir*
   The directory relative to the base directory where the executable
   workflow is to be generated. This overrides the default directory
   structure that Pegasus creates based on username, VO Group and the
   DAX label. By specifying **--relative-dir** and
   **--relative-submit-dir** you can have a different relative execution
   directory on the remote site and a different relative submit
   directory on the submit host.

**-s** *site*\ [,*site*,…]; \ **--sites** *site*\[,*site*,…]
   A comma separated list of execution sites on which the workflow is to
   be executed. Each of the sites should have an entry in the site
   catalog, that is being used.

   In case this option is not specified, all the sites in the site
   catalog other than site **local** are picked up as candidates for
   running the workflow.

**--staging-site** *s1=ss1*\[,s2=ss2[..]]
   A comma separated list of key=value pairs , where the key is the
   execution site and value is the staging site for that execution site.

   In case of running on a shared filesystem, the staging site is
   automatically associated by the planner to be the execution site. If
   only a value is specified, then that is taken to be the staging site
   for all the execution sites. e.g **--staging-site** local means that
   the planner will use the local site as the staging site for all jobs
   in the workflow.

**-s**; \ **--submit**
   Submits the generated **executable workflow** using **pegasus-run**
   script in $PEGASUS_HOME/bin directory. By default, the Pegasus
   Workflow Planner only generates the Condor submit files and does not
   submit them.

**-v**; \ **--verbose**
   Increases the verbosity of messages about what is going on. By
   default, all FATAL, ERROR, CONSOLE and WARN messages are logged. The
   logging hierarchy is as follows:

   1. FATAL

   2. ERROR

   3. CONSOLE

   4. WARN

   5. INFO

   6. CONFIG

   7. DEBUG

   8. TRACE

   For example, to see the INFO, CONFIG and DEBUG messages additionally,
   set **-vvv**.

**-V**; \ **--version**
   Displays the current version number of the Pegasus Workflow
   Management System.

*abstract-workflow*
   The YAML input file that describes an abstract workflow. If not specified
   the planner defaults to file *workflow.yml* in the current working directory.


Return Value
============

If the Pegasus Workflow Planner is able to generate an executable
workflow successfully, the exitcode will be 0.

* All runtime errors result in an exitcode of 1. This is usually in the case
  when you have misconfigured your catalogs etc.
* In the case of an error occurring while loading a specific module implementation
  at run time, the exitcode will be 2. This is usually due to factory methods
  failing while loading a module.
* In case of any other error occurring during the running of the
  command, the exitcode will be 1.

In most cases, the error message logged
should give a clear indication as to where things went wrong.



Controlling pegasus-plan Memory Consumption
===========================================

pegasus-plan will try to determine memory limits automatically using
factors such as total system memory and potential memory limits
(ulimits). The automatic limits can be overridden by setting the
JAVA_HEAPMIN and JAVA_HEAPMAX environment variables before invoking
pegasus-plan. The values are in megabytes. As a rule of thumb,
JAVA_HEAPMIN can be set to half of the value of JAVA_HEAPMAX.



Pegasus Properties
==================

This is not an exhaustive list of properties used. For the complete
description and list of properties refer to
**$PEGASUS_HOME/doc/advanced-properties.pdf**

**pegasus.selector.site**
   Identifies what type of site selector you want to use. If not
   specified the default value of **Random** is used. Other supported
   modes are **RoundRobin** and **NonJavaCallout** that calls out to a
   external site selector.

**pegasus.catalog.replica**
   Specifies the type of replica catalog to be used.

   If not specified, then the value defaults to **YAML**.

**pegasus.catalog.replica.file**
   The location of file to use as replica catalog. In case of YAML
   formatted file replica catalog, it is path to a file that defaults
   to *$PWD/replicas.yml* if not specified. In case of Text formatted
   file replica catalog, it is path to a file that defaults to
   *$PWD/rc.txt* if not specified.

**pegasus.dir.exec**
   A suffix to the workdir in the site catalog to determine the current
   working directory. If relative, the value will be appended to the
   working directory from the site.config file. If absolute it
   constitutes the working directory.

**pegasus.catalog.transformation**
   Specifies the type of transformation catalog to be used. One can use
   only a file based transformation catalog, with the value as **Text**.

**pegasus.catalog.transformation.file**
   The location of file to use as transformation catalog. In case of YAML
   formatted file catalog, it is path to a file that defaults
   to *$PWD/transformations.yml* if not specified. In case of Text formatted
   file catalog, it is path to a file that defaults to *$PWD/tc.txt* if
   not specified.

**pegasus.catalog.site**
   Specifies the type of site catalog to be used. One can use either a
   yaml formatted  or a xml formatted site catalog. At present the default is
   **YAML**.

**pegasus.catalog.site.file**
   The location of file to use as a site catalog. If not specified, then
   default value of $PWD/sites.xml is used in case of the xml based site
   catalog.

**pegasus.data.configuration**
   This property sets up Pegasus to run in different environments. This
   can be set to

   **sharedfs** If this is set, Pegasus will be setup to execute jobs on
   the shared filesystem on the execution site. This assumes, that the
   head node of a cluster and the worker nodes share a filesystem. The
   staging site in this case is the same as the execution site.

   **nonsharedfs** If this is set, Pegasus will be setup to execute jobs
   on an execution site without relying on a shared filesystem between
   the head node and the worker nodes.

   **condorio** If this is set, Pegasus will be setup to run jobs in a
   pure condor pool, with the nodes not sharing a filesystem. Data is
   staged to the compute nodes from the submit host using Condor File
   IO.

**pegasus.code.generator**
   The code generator to use. By default, Condor submit files are
   generated for the executable workflow. Setting to **Shell** results
   in Pegasus generating a shell script that can be executed on the
   submit host.



Files
=====

**$PEGASUS_HOME/share/pegasus/schema/yaml/wf-5.0.yml**
   is the suggested location of the latest YAML schema used to validate the
   abstract workflow.

**$PEGASUS_HOME/share/pegasus/schema/yaml/sc-5.0.yml**
   is the suggested location of the latest YAML schema used to validate the
   site catalog.

**$PEGASUS_HOME/share/pegasus/schema/yaml/tc-5.0.yml**
   is the suggested location of the latest YAML schema used to validate the
   transformation catalog.

**$PEGASUS_HOME/share/pegasus/schema/yaml/rc-5.0.yml**
   is the suggested location of the latest YAML schema used to validate the
   replica catalog.

**$PEGASUS_HOME/etc/sc-4.0.xsd**
   is the suggested location of the latest Site Catalog schema that is
   used to create the XML version of the site catalog

**$PEGASUS_HOME/etc/sample-5.0-data/**
   is where you can find the latest sample catalog and workflow files in
   the YAML format.

**$PEGASUS_HOME/share/pegasus/java/pegasus.jar**
   contains all compiled Java bytecode to run the Pegasus Workflow
   Planner.



See Also
========

pegasus-run(1), pegasus-status(1), pegasus-remove(1),
pegasus-rc-client(1), pegasus-analyzer(1)



Authors
=======

Karan Vahi ``<vahi at isi dot edu>``

Pegasus Team http://pegasus.isi.edu
