.. _execution-environments:

======================
Execution Environments
======================

Pegasus supports a number of execution environments. An execution
environment is a setup where jobs from a workflow are running.

Localhost
=========

In this configuration, Pegasus schedules the jobs to run locally on the
submit host. Running locally is a good approach for smaller workflows,
testing workflows, and for demonstations such as the `Pegasus
tutorial <#tutorial>`__. Pegasus supports two methods of local
execution: local HTCondor pool, and shell planner. The former is
preferred as the latter does not support all Pegasus' features (such as
notifications).

Running on a local HTCondor pool is achieved by executing the workflow
on site local (**--sites local** option to pegasus-plan). The site
"local" is a reserved site in Pegasus and results in the jobs to run on
the submit host in HTCondor universe local. The site catalog can be left
very simple in this case:

::

   <?xml version="1.0" encoding="UTF-8"?>
   <sitecatalog xmlns="http://pegasus.isi.edu/schema/sitecatalog"
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                xsi:schemaLocation="http://pegasus.isi.edu/schema/sitecatalog http://pegasus.isi.edu/schema/sc-4.0.xsd"
                version="4.0">

       <site  handle="local" arch="x86_64" os="LINUX">
           <directory type="shared-scratch" path="/tmp/wf/work">
               <file-server operation="all" url="file:///tmp/wf/work"/>
           </directory>
           <directory type="local-storage" path="/tmp/wf/storage">
               <file-server operation="all" url="file:///tmp/wf/storage"/>
           </directory>
       </site>

   </sitecatalog>

The simplest execution environment does not involve HTCondor. Pegasus is
capable of planning small workflows for local execution using a shell
planner. Please refer to the ``share/pegasus/examples`` directory in
your Pegasus installation, the shell planner's `documentation
section <#local_shell_examples>`__, or the tutorials, for details.

.. _condor-pool:

Condor Pool
===========

A HTCondor pool is a set of machines that use HTCondor for resource
management. A HTCondor pool can be a cluster of dedicated machines or a
set of distributively owned machines. Pegasus can generate concrete
workflows that can be executed on a HTCondor pool.

.. figure:: images/condor_layout.png
   :alt: The distributed resources appear to be part of a HTCondor pool.
   :width: 100.0%

   The distributed resources appear to be part of a HTCondor pool.

The workflow is submitted using DAGMan from one of the job submission
machines in the HTCondor pool. It is the responsibility of the Central
Manager of the pool to match the task in the workflow submitted by
DAGMan to the execution machines in the pool. This matching process can
be guided by including HTCondor specific attributes in the submit files
of the tasks. If the user wants to execute the workflow on the execution
machines (worker nodes) in a HTCondor pool, there should be a resource
defined in the site catalog which represents these execution machines.
The universe attribute of the resource should be vanilla. There can be
multiple resources associated with a single HTCondor pool, where each
resource identifies a subset of machine (worker nodes) in the pool.

When running on a HTCondor pool, the user has to decide how Pegasus
should transfer data. Please see the `Data Staging
Configuration <#data_staging_configuration>`__ for the options. The
easiest is to use **condorio** as that mode does not require any extra
setup - HTCondor will do the transfers using the existing HTCondor
daemons. For an example of this mode see the example workflow in
``share/pegasus/examples/condor-blackdiamond-condorio/`` . In HTCondorio
mode, the site catalog for the execution site is very simple as storage
is provided by HTCondor:

::

   <?xml version="1.0" encoding="UTF-8"?>
   <sitecatalog xmlns="http://pegasus.isi.edu/schema/sitecatalog"
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                xsi:schemaLocation="http://pegasus.isi.edu/schema/sitecatalog http://pegasus.isi.edu/schema/sc-4.0.xsd"
                version="4.0">

       <site  handle="local" arch="x86_64" os="LINUX">
           <directory type="shared-scratch" path="/tmp/wf/work">
               <file-server operation="all" url="file:///tmp/wf/work"/>
           </directory>
           <directory type="local-storage" path="/tmp/wf/storage">
               <file-server operation="all" url="file:///tmp/wf/storage"/>
           </directory>
       </site>

       <site  handle="condorpool" arch="x86_64" os="LINUX">
           <profile namespace="pegasus" key="style" >condor</profile>
           <profile namespace="condor" key="universe" >vanilla</profile>
       </site>

   </sitecatalog>

There is a set of HTCondor profiles which are used commonly when running
Pegasus workflows. You may have to set some or all of these depending on
the setup of the HTCondor pool:

::

     <!-- Change the style to HTCondor for jobs to be executed in the HTCondor Pool.
          By default, Pegasus creates jobs suitable for grid execution. -->
     <profile namespace="pegasus" key="style">condor</profile>

     <!-- Change the universe to vanilla to make the jobs go to remote compute
          nodes. The default is local which will only run jobs on the submit host -->
     <profile namespace="condor" key="universe" >vanilla</profhile>

     <!-- The requirements expression allows you to limit where your jobs go -->
     <profile namespace="condor" key="requirements">(Target.FileSystemDomain != &quot;yggdrasil.isi.edu&quot;)</profile>

     <!-- The following two profiles forces HTCondor to always transfer files. This
          has to be used if the pool does not have a shared filesystem -->
     <profile namespace="condor" key="should_transfer_files">True</profile>
     <profile namespace="condor" key="when_to_transfer_output">ON_EXIT</profile>

Glideins
--------

In this section we describe how machines from different administrative
domains and supercomputing centers can be dynamically added to a
HTCondor pool for certain timeframe. These machines join the HTCondor
pool temporarily and can be used to execute jobs in a non preemptive
manner. This functionality is achieved using a HTCondor feature called
**glideins** (see http://cs.wisc.edu/condor/glidein) . The startd daemon
is the HTCondor daemon which provides the compute slots and runs the
jobs. In the glidein case, the submit machine is usually a static
machine and the glideins are told configued to report to that submit
machine. The glideins can be submitted to any type of resource: a GRAM
enabled cluster, a campus cluster, a cloud environment such as Amazon
AWS, or even another HTCondor cluster.

   **Tip**

   As glideins are usually coming from different compute resource,
   and/or the glideins are running in an administrative domain different
   from the submit node, there is usually no shared filesystem
   available. Thus the most common `data staging
   modes <#data_staging_configuration>`__ are **condorio** and
   **nonsharedfs** .

There are many useful tools which submits and manages glideins for you:

-  `GlideinWMS <http://www.uscms.org/SoftwareComputing/Grid/WMS/glideinWMS/>`__
   is a tool and host environment used mostly on the `Open Science
   Grid <http://www.opensciencegrid.org/>`__.

-  `CorralWMS <http://pegasus.isi.edu/projects/corralwms>`__ is a
   personal frontend for GlideinWMS. CorralWMS was developed by the
   Pegasus team and works very well for high throughput workflows.

-  `condor_glidein <http://research.cs.wisc.edu/condor/manual/v7.6/condor_glidein.html>`__
   is a simple glidein tool for Globus GRAM clusters. condor_glidein is
   shipped with HTCondor.

-  Glideins can also be created by hand or scripts. This is a useful
   solution for example for cluster which have no external job submit
   mechanisms or do not allow outside networking.

CondorC
-------

Using HTCondorC users can submit workflows to remote HTCondor pools.
HTCondorC is a HTCondor specific solution for remote submission that
does not involve the setting up a GRAM on the headnode. To enable
HTCondorC submission to a site, user needs to associate pegasus profile
key named style with value as HTCondorc. In case, the remote HTCondor
pool does not have a shared filesytem between the nodes making up the
pool, users should use pegasus in the HTCondorio data configuration. In
this mode, all the data is staged to the remote node in the HTCondor
pool using HTCondor File transfers and is executed using PegasusLite.

A sample site catalog for submission to a HTCondorC enabled site is
listed below

::

   <sitecatalog xmlns="http://pegasus.isi.edu/schema/sitecatalog"
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                xsi:schemaLocation="http://pegasus.isi.edu/schema/sitecatalog http://pegasus.isi.edu/schema/sc-4.0.xsd"
                version="4.0">

       <site  handle="local" arch="x86_64" os="LINUX">
           <directory type="shared-scratch" path="/tmp/wf/work">
               <file-server operation="all" url="file:///tmp/wf/work"/>
           </directory>
           <directory type="local-storage" path="/tmp/wf/storage">
               <file-server operation="all" url="file:///tmp/wf/storage"/>
           </directory>
       </site>

       <site  handle="condorcpool" arch="x86_86" os="LINUX">
            <!-- the grid gateway entries are used to designate
                 the remote schedd for the HTCondorC pool -->
            <grid type="condor" contact="ccg-condorctest.isi.edu" scheduler="Condor" jobtype="compute" />
            <grid type="condor" contact="ccg-condorctest.isi.edu" scheduler="Condor" jobtype="auxillary" />

           <!-- enable submission using HTCondorc -->
           <profile namespace="pegasus" key="style">condorc</profile>

           <!-- specify which HTCondor collector to use.
                If not specified defaults to remote schedd specified in grid gateway -->
           <profile namespace="condor" key="condor_collector">condorc-collector.isi.edu</profile>

           <profile namespace="condor" key="should_transfer_files">Yes</profile>
           <profile namespace="condor" key="when_to_transfer_output">ON_EXIT</profile>
           <profile namespace="env" key="PEGASUS_HOME" >/usr</profile>
           <profile namespace="condor" key="universe">vanilla</profile>

       </site>

   </sitecatalog>

To enable PegasusLite in HTCondorIO mode, users should set the following
in their properties

::

   # pegasus properties
   pegasus.data.configuration    condorio

.. _cloud:

Cloud (Amazon EC2/S3, Google Cloud, ...)
========================================

This figure shows a sample environment for executing Pegasus across
multiple clouds. At this point, it is up to the user to provision the
remote resources with a proper VM image that includes a HTCondor worker
that is configured to report back to a HTCondor master, which can be
located inside one of the clouds, or outside the cloud.

The submit host is the point where a user submits Pegasus workflows for
execution. This site typically runs a HTCondor collector to gather
resource announcements, or is part of a larger HTCondor pool that
collects these announcements. HTCondor makes the remote resources
available to the submit host's HTCondor installation.

The `figure above <#concepts-fig-cloud-layout>`__ shows the way Pegasus
WMS is deployed in cloud computing resources, ignoring how these
resources were provisioned. The provisioning request shows multiple
resources per provisioning request.

The initial stage-in and final stage-out of application data into and
out of the node set is part of any Pegasus-planned workflow. Several
configuration options exist in Pegasus to deal with the dynamics of push
and pull of data, and when to stage data. In many use-cases, some form
of external access to or from the shared file system that is visible to
the application workflow is required to facilitate successful data
staging. However, Pegasus is prepared to deal with a set of boundary
cases.

The data server in the figure is shown at the submit host. This is not a
strict requirement. The data server for consumed data and data products
may both be different and external to the submit host, or one of the
object storage solution offered by the cloud providers

Once resources begin appearing in the pool managed by the submit
machine's HTCondor collector, the application workflow can be submitted
to HTCondor. A HTCondor DAGMan will manage the application workflow
execution. Pegasus run-time tools obtain timing-, performance and
provenance information as the application workflow is executed. At this
point, it is the user's responsibility to de-provision the allocated
resources.

In the figure, the cloud resources on the right side are assumed to have
uninhibited outside connectivity. This enables the HTCondor I/O to
communicate with the resources. The right side includes a setup where
the worker nodes use all private IP, but have out-going connectivity and
a NAT router to talk to the internet. The *Condor connection broker*
(CCB) facilitates this setup almost effortlessly.

The left side shows a more difficult setup where the connectivity is
fully firewalled without any connectivity except to in-site nodes. In
this case, a proxy server process, the *generic connection broker*
(GCB), needs to be set up in the DMZ of the cloud site to facilitate
HTCondor I/O between the submit host and worker nodes.

If the cloud supports data storage servers, Pegasus is starting to
support workflows that require staging in two steps: Consumed data is
first staged to a data server in the remote site's DMZ, and then a
second staging task moves the data from the data server to the worker
node where the job runs. For staging out, data needs to be first staged
from the job's worker node to the site's data server, and possibly from
there to another data server external to the site. Pegasus is capable to
plan both steps: Normal staging to the site's data server, and the
worker-node staging from and to the site's data server as part of the
job.

.. _amazon-aws:

Amazon EC2
----------

There are many different ways to set up an execution environment in
Amazon EC2. The easiest way is to use a submit machine outside the
cloud, and to provision several worker nodes and a file server node in
the cloud as shown here:

The submit machine runs Pegasus and a HTCondor master (collector,
schedd, negotiator). The workers run a HTCondor startd. And the file
server node exports an NFS file system. The startd on the workers is
configured to connect to the master running outside the cloud, and the
workers also mount the NFS file system. More information on setting up
HTCondor for this environment can be found at
`http://www.isi.edu/~gideon/condor-ec2 <http://www.isi.edu/~gideon/condor-ec2/>`__.

The site catalog entry for this configuration is similar to what you
would create for running on a local `Condor pool <#condor_pool>`__ with
a shared file system.

.. _google-cloud:

Google Cloud Platform
---------------------

Using the Google Cloud Platform is just like any other cloud platform.
You can choose to host the central manager / submit host inside the
cloud or outside. The compute VMs will have HTCondor installed and
configured to join the pool managed by the central manager.

Google Storage is supported using gsutil. First, create a .boto file by
running:

::

   gsutil config

Then, use a site catalog which specifies which .boto file to use. You
can then use gs:// URLs in your workflow. Example:

::

   <?xml version="1.0" encoding="UTF-8"?>
   <sitecatalog xmlns="http://pegasus.isi.edu/schema/sitecatalog"
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                xsi:schemaLocation="http://pegasus.isi.edu/schema/sitecatalog
                    http://pegasus.isi.edu/schema/sc-4.0.xsd" version="4.0">

       <site  handle="local" arch="x86_64" os="LINUX">
           <directory type="shared-scratch" path="/tmp">
               <file-server operation="all" url="file:///tmp"/>
           </directory>
           <profile namespace="env" key="PATH">/opt/gsutil:/usr/bin:/bin</profile>
       </site>
       <!-- compute site -->
       <site  handle="condorpool" arch="x86_86" os="LINUX">
           <profile namespace="pegasus" key="style" >condor</profile>
           <profile namespace="condor" key="universe" >vanilla</profile>
       </site>

       <!-- storage sites have to be in the site catalog, just liek a compute site -->
       <site  handle="google_storage" arch="x86_64" os="LINUX">
           <directory type="shared-scratch" path="/my-bucket/scratch">
               <file-server operation="all" url="gs://my-bucket/scratch"/>
           </directory>
           <directory type="local-storage" path="/my-bucket/outputs">
               <file-server operation="all" url="gs://my-bucket/outputs"/>
           </directory>
           <profile namespace="pegasus" key="BOTO_CONFIG">/home/myuser/.boto</profile>
       </site>

   </sitecatalog>

.. _aws-batch:

Amazon AWS Batch
================

Unlike the execution environments described in the previous section on
Cloud where the user has to start condor workers on the cloud nodes,
Amazon provides a managed service called AWS Batch. It automates the
notion of provisioning nodes in the cloud, and setting up of a compute
environment and a job queue that can submit jobs to those nodes.

Starting 4.9 release, Pegasus has support for executing horizontally
clustered jobs on Amazon AWS Batch Service using the command line tool
`pegasus-aws-batch <#cli-pegasus-aws-batch>`__. In other words, you can
get Pegasus to cluster each level of your workflow into a bag of tasks
and run those clustered jobs on Amazon Cloud using AWS Batch Service. In
upcoming releases, we plan to add support to pegasus-aws-batch to do
dependency management that will allow us to execute the whole workflow
in a single AWS Batch job.

.. _aws-batch-setup:

Setup
-----

To use AWS Batch as user you need to do some one time setup to get
started at running. Please follow the instructions carefully in this
section.

Credentials
~~~~~~~~~~~

To use AWS Batch for your workflows, we need two credential files

1. **AWS Credentials File:** This is the file that you create and use
   whenever accessing Amazon EC2 and is located at ~/.aws/credentials.
   For our purposes we need the following information in that file.

   ::

      $ cat ~/.aws/credentials
      [default]
      aws_access_key_id = XXXXXXXXXXXX
      aws_secret_access_key = XXXXXXXXXXX

2. **S3 Config File:** Pegasus workflows use pegasus-s3 command line
   tool to stage-in input data required by the tasks to S3 and push data
   output data generated to S3 when user application code runs. These
   credentials are specified in .s3cfg file usually put in the user home
   directory. This format of the file is described in the `pegaus-s3
   command line client's man page <#cli-pegasus-s3>`__. A minimalistic
   file is illustrated below

   ::

      $ cat ~/.s3cfg
      [amazon]
      # end point has to be consistent with the EC2 region you are using. Here we are referring to us-west-2 region.
      endpoint = http://s3-us-west-2.amazonaws.com


      # Amazon now allows 5TB uploads
      max_object_size = 5120
      multipart_uploads = True
      ranged_downloads = True


      [user@amazon]
      access_key = XXXXXXXXXXXX
      secret_key = XXXXXXXXXXXX

Setting up Container Image which your jobs run on
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

All jobs in AWS Batch are run in a container via the Amazon EC2
container service. The Amazon EC2 container service does not give
control over the docker run command for a container. Hence, Pegasus runs
jobs on container that is based on the `Amazon Fetch and Run
Example <https://aws.amazon.com/blogs/compute/creating-a-simple-fetch-and-run-aws-batch-job/>`__
. This container image allows us to fetch user executables automatically
from S3. All container images referred used for Pegasus workflows must
be based on the above example.

Additionally, the Docker file for your container image should include
these additional Docker run commands to install the yum packages that
Pegasus requires.

::

   RUN yum -y install perl findutils

After you have pushed the Docker image to the Amazon ECR Repository, the
image URL for that image you will use later to refer in the job
definition to use for your jobs.

One time AWS Batch Setup
~~~~~~~~~~~~~~~~~~~~~~~~

If you are using AWS Batch for the very first time, then you need to use
the Amazon Web console to create a role with your user that will give
the AWS Batch services privileges to execute to access other AWS
services such as EC2 Container Service , CloudWatchLogs etc. The
following roles need to be created

1. **AWS Batch Service IAM Role:** For convenience and ease of use make
   sure you name the role **AWSBatchServiceRole** , so that you don't
   have to make other changes. Complete the procedures listed at `AWS
   Batch Service IAM
   Role <https://docs.aws.amazon.com/batch/latest/userguide/service_IAM_role.html>`__.

2. **Amazon ECS Instance Role:** AWS Batch compute environments are
   populated with Amazon ECS container instances, and they run the
   Amazon ECS container agent locally. The Amazon ECS container agent
   makes calls to various AWS APIs on your behalf, so container
   instances that run the agent require an IAM policy and role for these
   services to know that the agent belongs to you. Complete the
   procedures listed at `Amazon ECS Instance
   Role <https://docs.aws.amazon.com/batch/latest/userguide/instance_IAM_role.html>`__.

3. **IAM Role:** Whenever a Pegasus job runs via AWS Batch it needs to
   fetch data from S3 and push data back to S3. To create this job role
   follow the instructions at section *Create an IAM role* in `Amazon
   Fetch and Run
   Example <https://aws.amazon.com/blogs/compute/creating-a-simple-fetch-and-run-aws-batch-job/>`__
   to create a IAM role named batchJobRole.

      **Note**

      batchJobRole should have full write access to S3 i.e have the
      policy **AmazonS3FullAccess** attached to it.

..

   **Note**

   It is important that you name the roles as listed above. Else, you
   will need to update the same job definition, compute environment, and
   job queue json files that you use to create the various Batch
   entities.

Creation of AWS Batch Entities for your Workflow
------------------------------------------------

AWS Batch has a notion of

1. **Job Definition** - job definition is something that allows you to
   use your container image in Amazon EC2 Repository to run one or many
   AWS Batch jobs.

2. **Compute Environment**- what sort of compute nodes you want your
   jobs to run on.

3. **Job Queue** - the queue that feeds the jobs to a compute
   environment.

Currently, with Pegasus you can only use one of each for a workflow i.e
the same job definition, compute environment and job queue need to be
used for all jobs in the workflow.

To create the above entities we recommend you to use
**pegasus-aws-batch**\ client . You can start with the sample json files
present in share/pegasus/examples/awsbatch-black-nonsharedfs directory.

-  **sample-job-definition.json :** Edit the attribute named image and
   replace it with the ARN of the container image you built for your
   account

-  **sample-compute-env.json** : Edit the attributes subnets and
   securityGroupIds

Before running the pegasus-aws-batch client make sure your properties
file has the following properties

::

   pegasus.aws.region=  [amazon ec2 region]
   pegasus.aws.account=[your aws account id - digits]

You can then use pegasus-aws-batch client to generate the job
definition, the compute environment and job queue to use.

::

   $ pegasus-aws-batch --conf ./conf/pegasusrc --prefix pegasus-awsbatch-example --create --compute-environment ./conf/sample-compute-env.json --job-definition ./conf/sample-job-definition.json --job-queue ./conf/sample-job-queue.json


   ..

   2018-01-18 15:16:00.771 INFO  [Synch] Created Job Definition
   arn:aws:batch:us-west-2:405596411149:job-definition/pegasus-awsbatch-example-job-definition:1
   2018-01-18 15:16:07.034 INFO  [Synch] Created Compute Environment
   arn:aws:batch:us-west-2:XXXXXXXXXX:compute-environment/pegasus-awsbatch-example-compute-env
   2018-01-18 15:16:11.291 INFO  [Synch] Created Job Queue
   arn:aws:batch:us-west-2:XXXXXXXXXX:job-queue/pegasus-awsbatch-example-job-queue

   2018-01-18 15:16:11.292 INFO  [PegasusAWSBatch] Time taken to execute
   is 12.194 seconds

You need to add the ARN's of created job definition, compute environment
and job queue listed in pegasus-aws-batch output to your pegasusrc file

::

   # Properties required to run on AWS Batch

   # the amazon region in which you are running workflows
   pegasus.aws.region=us-west-2

   # your AWS account id ( in digits)
   # pegasus.aws.account=XXXXXXXXXX

   # ARN of the job definition that you create using pegasus-aws-batch
   # pegasus.aws.batch.job_definition=arn:aws:batch:us-west-2:XXXXXXXXXX:job-definition/fetch_and_run

   # ARN of the job definition that you create using pegasus-aws-batch
   # pegasus.aws.batch.compute_environment=arn:aws:batch:us-west-2:XXXXXXXXXX:compute-environment/pegasus-awsbatch-example-compute-env

   # ARN of the job queue that you create using pegasus-aws-batch
   # pegasus.aws.batch.job_queue=arn:aws:batch:us-west-2:XXXXXXXXXX:job-queue/pegasus-awsbatch-example-job-queue

Site Catalog Entry for AWS Batch
--------------------------------

To run jobs on AWS Batch, you need to have an execution site in your
site catalog. Here is a sample site catalog to use for running workflows
on AWS Batch

::

   <?xml version="1.0" encoding="UTF-8"?>

   <sitecatalog xmlns="http://pegasus.isi.edu/schema/sitecatalog" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                                                 xsi:schemaLocation="http://pegasus.isi.edu/schema/sitecatalog
                                                    http://pegasus.isi.edu/schema/sc-4.0.xsd" version="4.0">

     <site  handle="local" arch="x86_64" os="LINUX" osrelease="" osversion="" glibc="">
           <directory  path="/LOCAL/shared-scratch" type="shared-scratch" free-size="" total-size="">
                   <file-server  operation="all" url="file:///LOCAL/shared-scratch">
                   </file-server>
           </directory>
           <directory  path="/LOCAL/shared-storage" type="shared-storage" free-size="" total-size="">
                   <file-server  operation="all" url="/LOCAL/shared-storage">
                   </file-server>
           </directory>
           <profile namespace="env" key="PEGASUS_HOME">/usr/bin/..</profile>
     </site>

       <site handle="aws-batch" arch="x86_64" os="LINUX">
           <directory  path="pegasus-batch-bamboo"  type="shared-scratch" free-size="" total-size="">
                   <file-server  operation="all"  url="s3://user@amazon/pegasus-batch-bamboo">
                   </file-server>
           </directory>

          <profile namespace="pegasus" key="clusters.num">1</profile>

          <profile namespace="pegasus" key="style">condor</profile>


      </site>

   </sitecatalog>

Properties
----------

Once the whole setup is complete, before running a workflow make sure
you have the following properties in your configuration file

::

   # get clustered jobs running  using AWSBatch
   pegasus.clusterer.job.aggregator AWSBatch

   #cluster even single jobs on a level
   pegasus.clusterer.allow.single True


   # Properties required to run on AWS Batch

   # the amazon region in which you are running workflows
   pegasus.aws.region=us-west-2

   # your AWS account id ( in digits)
   # pegasus.aws.account=XXXXXXXXXX

   # ARN of the job definition that you create using pegasus-aws-batch
   pegasus.aws.batch.job_definition=pegasus-awsbatch-example-job-definition

   # ARN of the job definition that you create using pegasus-aws-batch
   pegasus.aws.batch.compute_environment=pegasus-awsbatch-example-compute-env

   # ARN of the job queue that you create using pegasus-aws-batch
   pegasus.aws.batch.job_queue=pegasus-awsbatch-example-job-queue

.. _pyglidein:

Remote Cluster using PyGlidein
==============================

Glideins (HTCondor pilot jobs) provide an efficient solution for
high-throughput workflows. The glideins are submitted to the remote
cluster scheduler, and once started up, makes it appear like your
HTCondor pool extends into the remote cluster. HTCondor can then
schedule the jobs to the remote compute node in the same way it would
schedule jobs to local compute nodes.

Some infrastructures, such as `Open Science
Grid <#open_science_grid>`__, provide infrastructure level glidein
solutions, such as GlideinWMS. Another solution is `BOSCO <#bosco>`__.
For some more custom setups,
`pyglidein <https://github.com/WIPACrepo/pyglidein>`__ from the
`IceCube <http://icecube.wisc.edu/>`__ project provides a nice
framework. The architecture consists on a server on the submit host,
which job it is to determining the demand. On the remote resource, the
client can be invoked for example via cron, and submits directly to
HTCondor, SLURM and PBS schedulers. This makes pyglidein very flexible
and works well for example if the resource requires two-factor
authentication.

To get started with pyglidein, check out a copy of the Git repository on
both your submit host as well as the cluster you want to glidein to.
Starting with the submit host, first make sure you have HTCondor
configured for
`PASSWORD <http://research.cs.wisc.edu/htcondor/manual/current/3_8Security.html#SECTION00483400000000000000>`__
authentication. Make a copy of the HTCondor pool password file. You will
need it later in the configuration, and it is a binary file, so make
sure you cp instead of a copy-and-paste of the file contents.

Follow the installation instructions provided in the PyGlidein
`repo <https://github.com/WIPACrepo/pyglidein>`__. Note that you can use
virtualenv if you do not want to do a system-wide install:

::

   $ module load python2   (might not be needed on your system)
   $ virtualenv pyglidein
   New python executable in /home/user/pyglidein/bin/python
   Installing setuptools, pip, wheel...done.
   $ . pyglidein/bin/activate
   $ pip install pyglidein
   ...


Then, to get the server started:

::

   pyglidein_server --port 22001


By default, the pyglidein server will use all jobs in the system to
determine if glideins are needed. If you want user jobs to explicitly
let us know they want glideins, you can pass a constraint for the server
to use. For example, jobs could have the *+WantPSCBridges = True*
attribute, and then we could start the server with:

::

   pyglidein_server --port 22001 --constraint "'WantPSCBridges == True'"


One the server is running, you can check status by pointing a web
browser to it.

The client (running on the cluster you want glideins on), requires a few
configuration files and a *glidein.tar.gz* file containing the HTCondor
binaries, our pool password file, and a modified job wrapper script.
This *glidein.tar.gz* file can be created using the provided
*create_glidein_tarball.py* script, but an easier way is using the
already prepared tarball from and injecting your pool password file. For
example:

::

   $ wget https://download.pegasus.isi.edu/pyglidein/glidein.tar.gz
   $ mkdir glidein
   $ cd glidein
   $ tar xzf ../glidein.tar.gz
   $ cp /some/path/to/poolpasswd passwdfile
   $ tar czf ../glidein.tar.gz .
   $ cd ..
   $ rm -rf glidein


You can serve this file over for example http, but as it now contains
your pool password, we recommend you copy the *glidein.tar.gz* to the
remote cluster via scp.

Create a configuration file for your glidein. Here is an example for PSC
Bridges (other config file examples available under configs/ in the
PyGlidein GitHub repo):

::

   [Mode]
   debug = True

   [Glidein]
   address = http://workflow.isi.edu:22001/jsonrpc
   site = PSC-Bridges
   tarball = /home/rynge/pyglidein-config/glidein.tar.gz

   [Cluster]
   user = rynge
   os = RHEL7
   scheduler = slurm
   max_idle_jobs = 1
   limit_per_submit = 2
   walltime_hrs = 48
   partitions = RM

   [RM]
   gpu_only = False
   whole_node = True
   whole_node_memory = 120000
   whole_node_cpus = 28
   whole_node_disk = 8000000
   whole_node_gpus = 0
   partition = RM
   group_jobs = False
   submit_command = sbatch
   running_cmd = squeue -u $USER -t RUNNING -h -p RM | wc -l
   idle_cmd = squeue -u $USER -t PENDING -h -p RM | wc -l

   [SubmitFile]
   filename = submit.slurm
   local_dir = $LOCAL
   sbatch = #SBATCH
   custom_header = #SBATCH -C EGRESS
       #SBATCH --account=ABC123
   cvmfs_job_wrapper = False

   [StartdLogging]
   send_startd_logs = False
   url = s3.amazonaws.com
   bucket = pyglidein-logging-bridges

   [StardChecks]
   enable_startd_checks = True

   [CustomEnv]
   CLUSTER = workflow.isi.edu


This configuration will obviously look different for different clusters.
A few things to note:

-  **address** is the location of the server we started earlier

-  **tarball** is the full path to our custom glidein.tar.gz file we
   created above.

-  **CLUSTER** is the location of your HTCondor central manager. In many
   cases this is the same host you started the server on. Please note
   that if you do not set this variable, the glideins will try to
   register into the IceCube infrastructure.

-  **#SBATCH -C EGRESS** is PSC Bridges specific and enables outbound
   network connectivity from the compute nodes.

-  **#SBATCH --account=ABC123** specifies which allocation to charge the
   job to. This is a required setting on many, but not all, HPC systems.
   On PSC Bridges, you can get a list of your allocation by running the
   *projects* command, and looking for the *Charge ID* field.

We also need *secrets* file. We are not using any remote logging in this
example, but the file still has to exist with the following content:

::

   [StartdLogging]
   access_key =
   secret_key =


At this point we can try our first glidein:

::

   pyglidein_client --config=bridges.config --secrets=secrets


Once we have a seen a successful glidein, we can add the client to the
crontab:

::

   # m  h  dom mon dow   command
   */10 *   *   *   *    (cd ~/pyglidein/ && pyglidein_client --config=bridges.config --secrets=secrets) >~/cron-pyglidein.log 2>&1


With this setup, glideins will now appear automatically based on the
demand in the local HTCondor queue.

.. _globus-gram:

Remote Cluster using Globus GRAM
================================

A generic grid environment shown in the figure
`above <#concepts-fig-site-layout>`__. We will work from the left to the
right top, then the right bottom.

On the left side, you have a submit machine where Pegasus runs, HTCondor
schedules jobs, and workflows are executed. We call it the *submit host*
(SH), though its functionality can be assumed by a virtual machine
image. In order to properly communicate over secured channels, it is
important that the submit machine has a proper notion of time, i.e. runs
an NTP daemon to keep accurate time. To be able to connect to remote
clusters and receive connections from the remote clusters, the submit
host has a public IP address to facilitate this communication.

In order to send a job request to the remote cluster, HTCondor wraps the
job into Globus calls via HTCondor-G. Globus uses GRAM to manage jobs on
remote sites. In terms of a software stack, Pegasus wraps the job into
HTCondor. HTCondor wraps the job into Globus. Globus transports the job
to the remote site, and unwraps the Globus component, sending it to the
remote site's *resource manager* (RM).

To be able to communicate using the Globus security infrastructure
(GSI), the submit machine needs to have the certificate authority (CA)
certificates configured, requires a host certificate in certain
circumstances, and the user a user certificate that is enabled on the
remote site. On the remote end, the remote gatekeeper node requires a
host certificate, all signing CA certificate chains and policy files,
and a goot time source.

In a grid environment, there are one or more clusters accessible via
grid middleware like the `Globus Toolkit <http://www.globus.org/>`__. In
case of Globus, there is the Globus gatekeeper listening on TCP port
2119 of the remote cluster. The port is opened to a single machine
called *head node* (HN).The head-node is typically located in a
de-militarized zone (DMZ) of the firewall setup, as it requires limited
outside connectivity and a public IP address so that it can be
contacted. Additionally, once the gatekeeper accepted a job, it passes
it on to a jobmanager. Often, these jobmanagers require a limited port
range, in the example TCP ports 40000-41000, to call back to the submit
machine.

For the user to be able to run jobs on the remote site, the user must
have some form of an account on the remtoe site. The user's grid
identity is passed from the submit host. An entity called *grid mapfile*
on the gatekeeper maps the user's grid identity into a remote account.
While most sites do not permit account sharing, it is possible to map
multiple user certificates to the same account.

The gatekeeper is the interface through which jobs are submitted to the
remote cluster's resource manager. A resource manager is a scheduling
system like PBS, Maui, LSF, FBSNG or HTCondor that queues tasks and
allocates worker nodes. The *worker nodes* (WN) in the remote cluster
might not have outside connectivity and often use all private IP
addresses. The Globus toolkit requires a shared filesystem to properly
stage files between the head node and worker nodes.

   **Note**

   The shared filesystem requirement is imposed by Globus. Pegasus is
   capable of supporting advanced site layouts that do not require a
   shared filesystem. Please contact us for details, should you require
   such a setup.

To stage data between external sites for the job, it is recommended to
enable a GridFTP server. If a shared networked filesystem is involved,
the GridFTP server should be located as close to the file-server as
possible. The GridFTP server requires TCP port 2811 for the control
channel, and a limited port range for data channels, here as an example
the TPC ports from 40000 to 41000. The GridFTP server requires a host
certificate, the signing CA chain and policy files, a stable time
source, and a gridmap file that maps between a user's grid identify and
the user's account on the remote site.

The GridFTP server is often installed on the head node, the same as the
gatekeeper, so that they can share the grid mapfile, CA certificate
chains and other setups. However, for performance purposes it is
recommended that the GridFTP server has its own machine.

An example site catalog entry for a GRAM enabled site looks as follow in
the site catalog

::

   <sitecatalog xmlns="http://pegasus.isi.edu/schema/sitecatalog"
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                xsi:schemaLocation="http://pegasus.isi.edu/schema/sitecatalog http://pegasus.isi.edu/schema/sc-4.0.xsd"
                version="4.0">

        <site handle="Trestles" arch="x86_64" os="LINUX">
           <grid type="gt5" contact="trestles.sdsc.edu/jobmanager-fork" scheduler="Fork" jobtype="auxillary"/>
           <grid type="gt5" contact="trestles.sdsc.edu/jobmanager-pbs" scheduler="unknown" jobtype="compute"/>

           <directory type="shared-scratch" path="/oasis/projects/nsf/USERNAME">
               <file-server operation="all" url="gsiftp://trestles-dm1.sdsc.edu/oasis/projects/nsf/USERNAME"/>
           </directory>

           <!-- specify the path to a PEGASUS WORKER INSTALL on the site -->
           <profile namespace="env" key="PEGASUS_HOME" >/path/to/PEGASUS/INSTALL</profile>
       </site>


    </sitecatalog>

.. _creamce-submission:

Remote Cluster using CREAMCE
============================

`CREAM <https://wiki.italiangrid.it/twiki/bin/view/CREAM/FunctionalDescription>`__
is a webservices based job submission front end for remote compute
clusters. It can be viewed as a replaced for Globus GRAM and is mainly
popular in Europe. It widely used in the Italian Grid.

In order to submit a workflow to compute site using the CREAMCE front
end, the user needs to specify the following for the site in their site
catalog

1. **pegasus** profile **style** with value set to **cream**

2. **grid gateway**\ defined for the site with **contact** attribute set
   to CREAMCE frontend and **scheduler** attribute to remote scheduler.

3. a remote queue can be optionally specified using **globus** profile
   **queue** with value set to **queue-name**

An example site catalog entry for a creamce site looks as follow in the
site catalog

::

   <sitecatalog xmlns="http://pegasus.isi.edu/schema/sitecatalog"
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                xsi:schemaLocation="http://pegasus.isi.edu/schema/sitecatalog http://pegasus.isi.edu/schema/sc-4.0.xsd"
                version="4.0">

       <site  handle="creamce" arch="x86" os="LINUX">
           <grid type="cream" contact="https://ce01-lcg.cr.cnaf.infn.it:8443/ce-cream/services/CREAM2" scheduler="LSF" jobtype="compute" />
           <grid type="cream" contact="https://ce01-lcg.cr.cnaf.infn.it:8443/ce-cream/services/CREAM2" scheduler="LSF" jobtype="auxillary" />

            <!-- Scratch directory on the cluster -->
           <directory type="shared-scratch" path="/home/virgo034">
               <file-server operation="all" url="gsiftp://ce01-lcg.cr.cnaf.infn.it/home/virgo034"/>
           </directory>

           <!-- cream is the style to use for CREAMCE submits -->
           <profile namespace="pegasus" key="style">cream</profile>

           <!-- the remote queue is picked up from globus profile -->
           <profile namespace="globus" key="queue">virgo</profile>

           <!-- Staring HTCondor 8.0 additional cream attributes
                can be passed by setting cream_attributes -->
           <profile namespace="condor" key="cream_attributes">key1=value1;key2=value2</profile>
       </site>

    </sitecatalog>

The pegasus distribution comes with creamce examples in the examples
directory. They can be used as a starting point to configure your setup.

   **Tip**

   Usually , the CREAMCE frontends accept VOMS generated user proxies
   using the command voms-proxy-init . Steps on generating a VOMS proxy
   are listed in the CREAM User Guide
   `here <https://wiki.italiangrid.it/twiki/bin/view/CREAM/UserGuide#1_1_Before_starting_get_your_use>`__
   .

.. _glite:

Local Campus Cluster Using Glite
================================

This section describes the configuration required for Pegasus to
generate an executable workflow that uses glite to submit to a Slurm,
PBS, or SGE batch system on a local cluster. This environment is
referred to as the local campus cluster, as the workflow submit node
(Pegasus + HTCondor) need to be installed on a login node (or a node
where the local batch scheduler commands can be executed) of the
cluster.

   **Note**

   Glite is the old name for BLAH (or BLAHP). BLAH binaries are
   distributed with HTCondor as the "batch_gahp". For historical
   reasons, we often use the term "glite", and you will see "glite" and
   "batch_gahp" references in HTCondor, but all of them refer to the
   same thing, which has been renamed BLAH.

This guide covers Slurm, PBS, Moab, and SGE, but glite also works with
other PBS-like batch systems, including LSF, Cobalt and others. If you
need help configuring Pegasus and HTCondor to work with one of these
systems, please contact pegasus-support@isi.edu. For the sake of
brevity, the text below will say "PBS", but you should read that as "PBS
or PBS-like system such as SGE, Moab, LSF, and others".

This is because the glite layer communicates with the batch system
running on the cluster using ``squeue``/``qsub``/... or equivalent
commands. If you can submit jobs to the local scheduler from the
workflow submit host, then the local HTCondor can be used to submit jobs
via glite (with some modifications described below). If you need to SSH
to a different cluster head node in order to submit jobs to the
scheduler, then you can use BOSCO, which is `documented in another
section <#bosco>`__.

   **Tip**

   There is also a way to do remote job submission via glite even if you
   cannot SSH to the head node. This might be the case, for example, if
   the head node requires 2-factor authentication (e.g. RSA tokens).
   This approach is called the "Reverse GAHP" and you can find out more
   information on the `GitHub page <https://github.com/juve/rvgahp>`__.
   All it requires is SSH from the cluster head node back to the
   workflow submit host.

In either case, you need to modifiy the HTCondor glite installation that
will be used to submit jobs to the local scheduler. To do this, run the
``pegasus-configure-glite`` command. This command will install all the
required scripts to map Pegasus profiles to batch-system specific job
attributes, and add support for Moab. You may need to run it as root
depending on how you installed HTCondor.

   **Tip**

   HTCondor has an issue for the Slurm configuration when running on
   Ubuntu systems. Since in Ubuntu, ``/bin/sh`` does not link to
   ``bash``, the Slurm script will fail when trying to run the
   ``source`` command. A quick fix to this issue is to force the script
   to use ``bash``. In the ``bls_set_up_local_and_extra_args`` function
   of the ``blah_common_submit_functions.sh`` script, which is located
   in the same folder as the installation above, only add ``bash``
   before ``$bls_opt_tmp_req_file >>
           $bls_tmp_file 2> /dev/null`` command line.

In order to configure a workflow to use glite you need to create an
entry in your site catalog for the cluster and set the following
profiles:

1. **pegasus** profile **style** with value set to **glite**.

2. **condor** profile **grid_resource** with value set to **batch
   slurm**, **batch pbs**, **batch sge** or **batch moab**.

An example site catalog entry for a local glite PBS site looks like
this:

::

   <sitecatalog xmlns="http://pegasus.isi.edu/schema/sitecatalog"
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                xsi:schemaLocation="http://pegasus.isi.edu/schema/sitecatalog http://pegasus.isi.edu/schema/sc-4.0.xsd"
                version="4.0">

       <site  handle="local" arch="x86" os="LINUX">
           <directory type="shared-scratch" path="/lfs/shared-scratch/glite-sharedfs-example/work">
               <file-server operation="all" url="file:///lfs/local-scratch/glite-sharedfs-example/work"/>
           </directory>
           <directory type="local-storage" path="/shared-scratch//glite-sharedfs-example/outputs">
               <file-server operation="all" url="file:///lfs/local-scratch/glite-sharedfs-example/outputs"/>
           </directory>
       </site>

       <site  handle="local-slurm" arch="x86" os="LINUX">

           <!-- the following is a shared directory shared amongst all the nodes in the cluster -->
           <directory type="shared-scratch" path="/lfs/glite-sharedfs-example/local-slurm/shared-scratch">
               <file-server operation="all" url="file:///lfs/glite-sharedfs-example/local-slurm/shared-scratch"/>
           </directory>

           <profile namespace="env" key="PEGASUS_HOME">/lfs/software/pegasus</profile>

           <profile namespace="pegasus" key="style" >glite</profile>

           <profile namespace="condor" key="grid_resource">batch slurm</profile>
           <profile namespace="pegasus" key="queue">normal</profile>
           <profile namespace="pegasus" key="runtime">30000</profile>
       </site>

   </sitecatalog>


..

   **Tip**

   Starting 4.2.1, in the examples directory you can find a glite shared
   filesystem example that you can use to test out this configuration.

You probably don't need to know this, but Pegasus generates a
``+remote_cerequirements`` expression for an HTCondor glite job based on
the Pegasus profiles associated with the job. This expression is passed
to glite and used by the ``*_local_submit_attributes.sh`` scripts
installed by ``pegasus-configure-glite`` to generate the correct batch
submit script. An example ``+remote_cerequirements`` classad expression
in the HTCondor submit file looks like this:

::

   +remote_cerequirements = JOBNAME=="preprocessj1" && PASSENV==1 && WALLTIME=="01:00:00" && \
    EXTRA_ARGUMENTS=="-N testjob -l walltime=01:23:45 -l nodes=2" && \
    MYENV=="CONDOR_JOBID=$(cluster).$(process),PEGASUS_DAG_JOB_ID=preprocess_j1,PEGASUS_HOME=/usr,PEGASUS_WF_UUID=aae14bc4-b2d1-4189-89ca-ccd99e30464f"

The job name and environment variables are automatically passed through
to the remote job.

The following sections document the mapping of Pegasus profiles to batch
system job requirements as implemented by Pegasus, HTCondor, and glite.

.. _glite-mappings:

Setting job requirements
------------------------

The job requirements are constructed based on the following profiles:

.. table:: Mapping of Pegasus Profiles to Job Requirements

   ======================= ============================= ====================== ================== ================ =================== ================= =====================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================
   Profile Key             Key in +remote_cerequirements SLURM parameter        PBS Parameter      SGE Parameter    Moab Parameter      Cobalt Parameter  Description
   ======================= ============================= ====================== ================== ================ =================== ================= =====================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================
   pegasus.cores           CORES                         --ntasks cores         n/a                -pe ompi         n/a                 --proccount cores Pegasus uses cores to calculate either nodes or ppn. If cores and ppn are specified, then nodes is computed. If cores and nodes is specified, then ppn is computed. If both nodes and ppn are specified, then cores is ignored. The resulting values for nodes and ppn are used to set the job requirements for PBS and Moab. If neither nodes nor ppn is specified, then no requirements are set in the PBS or Moab submit script. For SGE, how the processes are distributed over nodes depends on how the parallel environment has been configured; it is set to 'ompi' by default.
   pegasus.nodes           NODES                         --nodes nodes          -l nodes           n/a              -l nodes            -n nodes          This specifies the number of nodes that the job should use. This is not used for SGE.
   pegasus.ppn             PROCS                         n/a                    -l ppn             n/a              -l ppn              --mode c[ppn]     This specifies the number of processors per node that the job should use. This is not used for SGE.
   pegasus.runtime         WALLTIME                      --time walltime        -l walltime        -l h_rt          -l walltime         -t walltime       This specifies the maximum runtime for the job in seconds. It should be an integer value. Pegasus converts it to the "hh:mm:ss" format required by the batch system. The value is rounded up to the next whole minute.
   pegasus.memory          PER_PROCESS_MEMORY            --mem memory           -l pmem            -l h_vmem        --mem-per-cpu pmem  n/a               This specifies the maximum amount of physical memory used by any process in the job. For example, if the job runs four processes and each requires up to 2 GB (gigabytes) of memory, then this value should be set to "2gb" for PBS and Moab, and "2G" for SGE. The corresponding PBS directive would be "#PBS -l pmem=2gb".
   pegasus.project         PROJECT                       --account project_name -A project_name    n/a              -A project_name     -A project_name   Causes the job time to be charged to or associated with a particular project/account. This is not used for SGE.
   pegasus.queue           QUEUE                         --partition            -q                 -q               -q                                    This specifies the queue for the job. This profile does not have a corresponding value in ``+remote_cerequirements``. Instead, Pegasus sets the ``batch_queue`` key in the Condor submit file, which gLite/blahp translates into the appropriate batch system requirement.
   globus.totalmemory      TOTAL_MEMORY                  --mem memory           -l mem             n/a              -l mem              n/a               The total memory that your job requires. It is usually better to just specify the pegasus.memory profile. This is not mapped for SGE.
   pegasus.glite.arguments EXTRA_ARGUMENTS               prefixed by "#SBATCH"  prefixed by "#PBS" prefixed by "#?" prefixed by "#MSUB" n/a               This specifies the extra arguments that must appear in the generated submit script for a job. The value of this profile is added to the submit script prefixed by the batch system-specific value. These requirements override any requirements specified using other profiles. This is useful when you want to pass through special options to the underlying batch system. For example, on the USC cluster we use resource properties to specify the network type. If you want to use the Myrinet network, you must specify something like "-l nodes=8:ppn=2:myri". For infiniband, you would use something like "-l nodes=8:ppn=2:IB". In that case, both the nodes and ppn profiles would be effectively ignored.
   ======================= ============================= ====================== ================== ================ =================== ================= =====================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================

Specifying a remote directory for the job
-----------------------------------------

gLite/blahp does not follow the ``remote_initialdir`` or ``initialdir``
classad directives. Therefore, all the jobs that have the ``glite``
style applied don't have a remote directory specified in the submit
script. Instead, Pegasus uses Kickstart to change to the working
directory when the job is launched on the remote system.

.. _bosco-sdsc:

SDSC Comet with BOSCO glideins
==============================

BOSCO documentation:
https://twiki.opensciencegrid.org/bin/view/CampusGrids/BoSCO

BOSCO is part of the HTCondor system which allows you to set up a
personal pool of resources brought in from a remote cluster. In this
section, we describe how to use BOSCO to run glideins (pilot jobs)
dynamically on the SDSC Comet cluster. The glideins are submitted based
on the demand of the user jobs in the pool.

As your regular user, on the host you want to use as a workflow submit
host, download the latest version of HTCondor from the `HTCondor
Download page <https://research.cs.wisc.edu/htcondor/downloads/>>`__. At
this point the latest version was 8.5.2 and we downloaded
condor-8.5.2-x86_64_RedHat6-stripped.tar.gz. Untar, and run the
installer:

::

   $ tar xzf condor-8.5.2-x86_64_RedHat6-stripped.tar.gz
   $ cd condor-8.5.2-x86_64_RedHat6-stripped
   $ ./bosco_install
   ...
   Created a script you can source to setup your Condor environment
   variables. This command must be run each time you log in or may
   be placed in your login scripts:
      source /home/$USER/bosco/bosco_setenv


Source the setup file as instructed, run *bosco_start*, and then test
that *condor_q* and *condor_status* works.

::

   $ source /home/$USER/bosco/bosco_setenv
   $ condor_q

   -- Schedd: workflow.iu.xsede.org : 127.0.0.1:11000?...
    ID      OWNER            SUBMITTED     RUN_TIME ST PRI SIZE CMD

   0 jobs; 0 completed, 0 removed, 0 idle, 0 running, 0 held, 0 suspended
   $ condor_status


Let's tell BOSCO about our SDSC Comet account:

::

   $ bosco_cluster -a YOUR_SDSC_USERNAME@comet-ln2.sdsc.edu pbs


BOSCO needs a little bit more information to be able to submit the
glideins to Comet. Log in to your Comet account via ssh (important -
this step has to take place on Comet) and create the
*~/bosco/glite/bin/pbs_local_submit_attributes.sh* file with the
following content. You can find your allocation by running
*show_accounts* and looking at the project column.

::

   echo "#PBS -q compute"
   echo "#PBS -l nodes=1:ppn=24"
   echo "#PBS -l walltime=24:00:00"
   echo "#PBS -A [YOUR_COMET_ALLOCATION]"


Also chmod the file:

::

   $ chmod 755 ~/bosco/glite/bin/pbs_local_submit_attributes.sh


Log out of Comet, and get back into the host and user BOSCO was
installed into. We also need to edit a few files on that host.
*~/bosco/libexec/campus_factory/share/glidein_jobs/glidein_wrapper.sh*
has a bug in some versions of HTCondor. Open up the file and make sure
the eval line in the beginning is below the unset/export HOME section.
If that is not the case, edit the file to look like:

::

   #!/bin/sh

   starting_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

   # BLAHP does weird things with home directory
   unset HOME
   export HOME

   eval campus_factory_dir=$_campusfactory_CAMPUSFACTORY_LOCATION


If the order of the HOME and eval statements are reversed in your file,
change them to look like the above. At the end of
*~/bosco/libexec/campus_factory/share/glidein_jobs/glidein_condor_config*
add:

::

   # dynamic slots
   SLOT_TYPE_1 = cpus=100%,disk=100%,swap=100%
   SLOT_TYPE_1_PARTITIONABLE = TRUE
   NUM_SLOTS = 1
   NUM_SLOTS_TYPE_1 = 1


In the file
*~/bosco/libexec/campus_factory/share/glidein_jobs/job.submit.template*
find the line reading:

::

            _condor_NUM_CPUS=1; \


You should now have a functioning BOSCO setup. Submit a Pegasus
workflow.

.. _bosco:

Remote PBS Cluster using BOSCO and SSH
======================================

`BOSCO <http://bosco.opensciencegrid.org/about/>`__ enables HTCondor to
submit jobs to remote PBS clusters using SSH. This section describes how
to specify a site catalog entry for a site that has been configured for
BOSCO job submissions.

First, the site needs to be setup for BOSCO according to the `BOSCO
documentation <https://twiki.opensciencegrid.org/bin/view/CampusGrids/BoSCO>`__.
BOSCO uses glite to submit jobs to the PBS scheduler on the remote
cluster. You will also need to configure the glite installed for BOSCO
on the remote system according to the documentation in the `glite
section <#glite>`__ in order for the mapping of Pegasus profiles to PBS
job requirements to work. In particular, you will need to install the
``pbs_local_submit_attributes.sh`` and
``sge_local_submit_attributes.sh`` scripts in the correct place in the
glite ``bin`` directory on the remote cluster, usually in the directory
*~/bosco/glite/bin/* .

Second, to tag a site for SSH submission, the following profiles need to
be specified for the site in the site catalog:

1. **pegasus** profile **style** with value set to **ssh**

2. Specify the service information as grid gateways. This should match
   what BOSCO provided when the cluster was set up.

An example site catalog entry for a BOSCO site looks like this:

::

   <sitecatalog xmlns="http://pegasus.isi.edu/schema/sitecatalog"
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                xsi:schemaLocation="http://pegasus.isi.edu/schema/sitecatalog http://pegasus.isi.edu/schema/sc-4.0.xsd"
                version="4.0">

       <site  handle="USC_HPCC_Bosco" arch="x86_64" os="LINUX">

           <!-- Specify the service information. This should match what Bosco provided when the cluster
                was set up. -->
           <grid type="batch" contact="vahi@hpc-pegasus.usc.edu" scheduler="PBS" jobtype="compute"/>
           <grid type="batch" contact="vahi@hpc-pegasus.usc.edu" scheduler="PBS" jobtype="auxillary"/>

           <!-- Scratch directory on the cluster -->
           <directory type="shared-scratch" path="/home/rcf-40/vahi/tmp">
               <file-server operation="all" url="scp://vahi@hpc-pegasus.usc.edu/home/rcf-40/vahi/tmp"/>
           </directory>

           <!-- SSH is the style to use for Bosco SSH submits -->
           <profile namespace="pegasus" key="style">ssh</profile>

           <!--  works around bug in the HTCondor GAHP, that does not
                 set the remote directory -->
           <profile namespace="pegasus" key="change.dir">true</profile>

           <!-- Job requirements should be specified using Pegasus profiles -->
           <profile namespace="pegasus" key="queue">default</profile>
           <profile namespace="pegasus" key="runtime">30</profile>

       </site>


   </sitecatalog>

..

   **Note**

   It is recommended to have a submit node configured either as a BOSCO
   submit node or a vanilla HTCondor node. You cannot have HTCondor
   configured both as a BOSCO install and a traditional HTCondor submit
   node at the same time as BOSCO will override the traditional HTCondor
   pool in the user environment.

There is a bosco-shared-fs example in the examples directory of the
distribution.

Job Requirements for the jobs can be set using the same profiles as
listed `here <#glite_mappings>`__ .

.. _campus-cluster:

Campus Cluster
==============

There are almost as many different configurations of campus clusters as
there are campus clusters, and because of that it can be hard to
determine what the best way to run Pegasus workflows. Below is a ordered
checklist with some ideas we have collected from working with users in
the past:

1. If the cluster scheduler is HTCondor, please see the HTCondor Pool
   section.

2. If the cluster is Globus GRAM enabled, see the Globus GRAM section.
   If you have have a lot of short jobs, also read the Glidein section.

3. For clusters without GRAM, you might be able to do glideins. If
   outbound network connectivity is allowed, your submit host can be
   anywhere. If the cluster is setup to not allow any network
   connections to the outside, you will probably have to run the submit
   host inside the cluster as well.

If the cluster you are trying to use is not fitting any of the above
scenarios, please post to the `Pegasus users mailing
list <http://pegasus.isi.edu/support>`__ and we will help you find a
solution.

XSEDE
=====

The `Extreme Science and Engineering Discovery Environment
(XSEDE) <https://www.xsede.org/>`__ provides a set of High Performance
Computing (HPC) and High Throughput Computing (HTC) resources.

For the HPC resources, it is recommended to run using `Globus
GRAM <#globus_gram>`__ or `glideins <#glideins>`__. Most of these
resources have fast parallel file systesm, so running with `sharedfs
data staging <#data_staging_configuration>`__ is recommended. Below is
example site catalog and pegasusrc to run on `SDSC
Trestles <http://www.sdsc.edu/us/resources/trestles/>`__:

::

   <sitecatalog xmlns="http://pegasus.isi.edu/schema/sitecatalog"
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                xsi:schemaLocation="http://pegasus.isi.edu/schema/sitecatalog http://pegasus.isi.edu/schema/sc-4.0.xsd"
                version="4.0">

       <site  handle="local" arch="x86_64" os="LINUX">
           <directory type="shared-scratch" path="/tmp/wf/work">
               <file-server operation="all" url="file:///tmp/wf/work"/>
           </directory>
           <directory type="local-storage" path="/tmp/wf/storage">
               <file-server operation="all" url="file:///tmp/wf/storage"/>
           </directory>
       </site>

       <site handle="Trestles" arch="x86_64" os="LINUX">
          <grid type="gt5" contact="trestles.sdsc.edu:2119/jobmanager-fork" scheduler="PBS" jobtype="auxillary"/>
          <grid type="gt5" contact="trestles.sdsc.edu:2119/jobmanager-pbs" scheduler="PBS" jobtype="compute"/>
          <directory type="shared-scratch" path="/phase1/USERNAME">
              <file-server operation="all" url="gsiftp://trestles-dm1.sdsc.edu/phase1/USERNAME"/>
          </directory>
       </site>

   </sitecatalog>

pegasusrc:

::

   pegasus.catalog.replica=SimpleFile
   pegasus.catalog.replica.file=rc

   pegasus.catalog.site.file=sites.xml

   pegasus.catalog.transformation=Text
   pegasus.catalog.transformation.file=tc

   pegasus.data.configuration = sharedfs

   # Pegasus might not be installed, or be of a different version
   # so stage the worker package
   pegasus.transfer.worker.package = true

The HTC resources available on XSEDE are all HTCondor based, so standard
`HTCondor Pool <#condor_pool>`__ setup will work fine.

If you need to run high throughput workloads on the HPC machines (for
example, post processing after a large parallel job),
`glideins <#glideins>`__ can be useful as it is a more efficient method
for small jobs on these systems.

.. _titan:

Titan Using Glite
=================

`Titan <https://www.olcf.ornl.gov/olcf-resources/compute-systems/titan/>`__
is part of Oak Ridge Leadership Computing Facilities (OLCF) and offers
hybrid computing resources (CPUs and GPUs) to scientists since 2012.

In order to submit to Titan, a *Titan login node* or a system that has
access to the *Lustre* filesystem and the *batch scheduler* (eg. `OLCF's
Kubernetes
Deployment <https://www.olcf.ornl.gov/wp-content/uploads/2017/11/2018UM-Day3-Kincl.pdf>`__),
must be used as the submit node. Submission style must be `Pegasus
Glite <https://pegasus.isi.edu/documentation/glite.php>`__ and an
example site calatog entry looks like this:

::

   <sitecatalog xmlns="http://pegasus.isi.edu/schema/sitecatalog"
               xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xsi:schemaLocation="http://pegasus.isi.edu/schema/sitecatalog http://pegasus.isi.edu/schema/sc-4.1.xsd"
               version="4.1">

       <site handle="local" arch="x86_64" os="LINUX">
           <directory type="shared-scratch" path="/lustre/atlas/scratch/user/workflow-dir/scratch"/>
               <file-server operation="all" url="file:///lustre/atlas/scratch/user/workflow-dir/scratch"/>
           </directory>
           <directory type="shared-storage" path="/lustre/atlas/scratch/user/workflow-dir/output/">
               <file-server operation="all" url="file:///lustre/atlas/scratch/user/workflow-dir/output"/>
           </directory>
       </site>

       <site handle="titan" arch="x86_64" os="LINUX">
           <directory type="shared-scratch" path="/lustre/atlas/scratch/user/titan/scratch">
               <file-server operation="all" url="file:///lustre/atlas/scratch/user/titan/scratch"/>
           </directory>

           <profile namespace="pegasus" key="style">glite</profile>
           <profile namespace="condor" key="grid_resource">batch pbs</profile>

           <profile namespace="pegasus" key="queue">titan</profile>
           <profile namespace="pegasus" key="auxillary.local">true</profile>

           <profile namespace="env" key="PEGASUS_HOME">/lustre/atlas/world-shared/csc320/SOFTWARE/install/pegasus/default</profile>
           <profile namespace="pegasus" key="runtime">1800</profile>
           <profile namespace="pegasus" key="nodes">1</profile>
           <profile namespace="pegasus" key="project">CSC320</profile>
       </site>
   </sitecatalog>


1. *pegasus* profile style with value set to *glite*

2. *condor* profile *grid_resource* with value set to *batch pbs*

3. *pegasus* profile *queue* is mandatory and should be set to *titan*

4. *pegasus* profile *runtime* is mandatory and should be set in sites
   or transformation catalog

5. *pegasus* profile *nodes* is mandatory and should be set in sites or
   transformation catalog

6. *pegasus* profile *project* must be set to the project name your jobs
   run under

..

   **Note**

   *pegasus* profile *cores* is incompatible with Titan's PBS
   submissions.

.. _open-science-grid:

Open Science Grid Using glideinWMS
==================================

`glideinWMS <http://www.uscms.org/SoftwareComputing/Grid/WMS/glideinWMS/>`__
is a glidein system widely used on Open Science Grid. Running on top of
glideinWMS is like running on a `Condor Pool <#condor_pool>`__ without a
shared filesystem.
