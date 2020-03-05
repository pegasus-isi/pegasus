.. _containers:

==========
Containers
==========

.. _containers-overview:

Overview
========

Application containers provides a solution to package software with
complex dependencies to be used during workflow execution. Starting with
Pegasus 4.8.0, Pegasus has support for application containers in the
non-shared filesystem or condorio data configurations using PegasusLite.
Users can specify with their transformations in the Transformation
Catalog the container in which the the transformation should be
executed. Pegasus currently has support for the following container
technologies:

1. Docker

2. Singularity

The worker package is not required to be pre-installed in images. If a
matching worker package is not installed, Pegasus will try to determine
which package is required and download it.

.. _containers-configuration:

Configuring Workflows To Use Containers
=======================================

Containers currently can only be specified in the Transformation
Catalog. Users have the option of either using a different container for
each executable or same container for all executables. In the case,
where you wants to use a container that does not have your executable
pre-installed, you can mark the executable as STAGEABLE and Pegasus will
stage the executable into the container, as part of executable staging.

The DAX API extensions don't support references for containers.

.. _containers-osg:

Containers on OSG
=================

OSG has it's own way of handling container deployments for jobs that is
hidden from the user and hence Pegasus. They don't allow a user to run
an image directly by invoking docker run or singluarity exec. Instead
the condor job wrappers deployed on OSG do it for you based on the
classads associated with the job. As a result, for a workflow to run on
OSG, one cannot specify or describe the container in the transformation
catalog. Instead you catalog the executables without a container
reference, and the path to the executable is the path in the container
you want to use. To specify the container, that needs to be setup you
instead specify the following Condor profiles

.. table:: Condor Profiles For Specifying Singularity Container for Jobs

=================
Key
=================
requirements
+SingularityImage
=================

For example you can specify the following in the site catalog for OSG
site

::

   <!-- this is our execution site -->
       <site  handle="OSG" arch="x86_64" os="LINUX">
           <profile namespace="pegasus" key="style" >condor</profile>
           <profile namespace="condor" key="universe" >vanilla</profile>
           <profile namespace="condor" key="requirements" >HAS_SINGULARITY == True</profile>
           <profile namespace="condor" key="+SingularityImage" >"/cvmfs/singularity.opensciencegrid.org/pegasus/osg-el7:latest"</profile>
           <profile namespace="condor" key="request_cpus" >1</profile>
           <profile namespace="condor" key="request_memory" >1 GB</profile>
           <profile namespace="condor" key="request_disk" >1 GB</profile>
       </site>

.. _containers-exec-model:

Container Execution Model
=========================

User's containerized applications are launched as part of PegasusLite
jobs. PegasusLite job when starting on a remote worker node.

1. Sets up a directory to run a user job in.

2. Pulls the container image to that directory

3. Optionally, loads the container from the container image file and
   sets up the user to run as in the container (only applicable for
   Docker containers)

4. Mounts the job directory into the container as /scratch for Docker
   containers, while as /srv for Singularity containers.

5. Container will run a job specific script that figures created by
   PegasusLite that does the following:

   a. Figures the appropriate Pegasus worker to use in the container if
      not already installed

   b. Sets up the job environment to use including transfer and setup of
      any credentials transferred as part of PegasusLite

   c. Pulls in all the relevant input data, executables required by the
      job

   d. Launches the user application using *pegasus-kickstart.*

6. Optionally, shuts down the container (only applicable for Docker
   containers)

7. Ships out the output data to the staging site

8. Cleans up the directory on the worker node.

..

   **Note**

   Starting Pegasus 4.9.1 the container data transfer model has been
   changed. Instead of data transfers for the job occurring outside the
   container in the PegasusLite wrapper, they now happen when the user
   job starts in the container.

In versions of Pegasus >= 4.9.1 the transfers are handled from within
the container, and thus container recipes require some extra attention.
A Dockerfile example that prepares a container for GridFTP transfers is
provided below.

In this example there are three sections.

-  Essential Packages

-  Install Globus Toolkit

-  Install CA Certs

From the "Essential Packages", **python** and either **curl** or
**wget** have to be present. "Install Globus Toolkit", sets up the
enviroment for GridFTP transfers. And "Install CA Certs" copies the grid
certificates in the container.

   **Note**

   Globus Toolkit introduced some breaking changes in August 2018 to its
   authentication module, and some sites haven't upgraded their
   installations (eg. NERSC). GridFTP in order to authenticate
   successfully, requires the libglobus-gssapi-gsi4 package to be pinned
   to the version 13.8-1. The code snipet below contains installation
   directives to handle this but they are commented out.

::

   ##########################################
   #### This Container Supports GridFTP  ####
   ##########################################

   FROM ubuntu:18.04

   #### Essential Packages ####
   RUN apt-get update &&\
   apt-get install -y software-properties-common curl wget python unzip &&\
   rm -rf /var/lib/apt/lists/*

   #### Install Globus Toolkit ####
   RUN wget -nv http://www.globus.org/ftppub/gt6/installers/repo/globus-toolkit-repo_latest_all.deb &&\
   dpkg -i globus-toolkit-repo_latest_all.deb &&\
   apt-get update &&\
   # apt-get install -y libglobus-gssapi-gsi4=13.8-1+gt6.bionic &&\
   # apt-mark hold libglobus-gssapi-gsi4 &&\
   apt-get install -y globus-data-management-client &&\
   rm -f globus-toolkit-repo_latest_all.deb &&\
   rm -rf /var/lib/apt/lists/*

   #### Install CA Certs ####
   RUN mkdir -p /etc/grid-security &&\
   cd /etc/grid-security &&\
   wget -nv https://download.pegasus.isi.edu/containers/certificates.tar.gz &&\
   tar xzf certificates.tar.gz &&\
   rm -f certificates.tar.gz

   ##########################################
   #### Your Container Specific Commands ####
   ##########################################


.. _containers-transfers:

Staging of Application Containers
=================================

Pegasus treats containers as other files in terms of data management.
Container to be used for a job is tracked as an input dependency that
needs to be staged if it is not already there. Similar to executables,
you specify the location for your container image in the Transformation
Catalog. You can specify the source URL's for containers as the
following.

1. URL to a container hosted on a central hub repository

   Example of a docker hub URL is docker:///rynge/montage:latest, while
   for singularity shub://pegasus-isi/fedora-montage

2. URL to a container image file on a file server.

   -  **Docker -**\ Docker supports loading of containers from a tar
      file, Hence, containers images can only be specified as tar files
      and the extension for the filename is not important.

   -  **Singularity -** Singularity supports container images in various
      forms and relies on the extension in the filename to determine
      what format the file is in. Pegasus supports the following
      extensions for singularity container images

      -  .img

      -  .tar

      -  .tar.gz

      -  .tar.bz2

      -  .cpio

      -  .cpio.gz

      -  .sif

      Singularity will fail to run the container if you don't specify
      the right extension , when specify the source URL for the image.

In both the cases, Pegasus will place the container image on the staging
site used for the workflow, as part of the data stage-in nodes, using
pegasus-transfer. When pulling in an image from a container hub
repository, pegasus-transfer will export the container as a tar file in
case of Docker, and as .img file in case of Singularity

.. _shifter_containers_staging:

Shifter Containers
------------------

Shifter containers are different from docker and singularity with
respect to the fact that the containers cannot be exported to a
container image file that can reside on a filesystem. Additionally, the
container are expected to be available locally on the compute sites in
the local Shifter registry. Because of this, Pegasus does not do any
transfer of Shifter containers. You can specify a shifter container
using the shifter url scheme. For example, below is a transformation
catalog for a namd transformation that is executed in a shifter
container.

::

   cont namd_image{
        # can be either docker or singularity
        type "shifter"

        # image loaded in the local shifter repository at cori
        image "shifter:///papajim/namd_image:latest"

        # optional site attribute to tell pegasus which site tar file
        # exists. useful for handling file URL's correctly
        image_site "cori"
   }

   tr namd2 {
       site cori {
           pfn "/opt/NAMD_2.12_Linux-x86_64-multicore/namd2"
           arch "x86_64"
           os "LINUX"
           type "INSTALLED"
           container "namd_image"
           profile globus "maxTime" "20"
           profile pegasus "exitcode.successmsg" "End of program"
       }
   }

.. _containers-symlinking:

Symlinking and File Copy From Host OS
-------------------------------------

Since, Pegasus by default only mounts the job directory determined by
PegasusLite into the application container, symlinking of input data
sets works only if in the container definition in the transformation
catalog user defines the directories containing the input data to be
mounted in the container using the **mount** key word. We recommend to
keep the source and destination directories to be the same i.e. the host
path is mounted in the same location in the container.

The above is also true for the case, where you input datasets are on the
shared filesystem on the compute site and you want a file copy to
happen, when PegasusLite job starts the container.

For example in the example below, we have input datasets accessible on
/lizard on the compute nodes, and mounting them as read-only into the
container at /lizard

::

   cont centos-base{
        type "singularity"

        # URL to image in a docker hub or a url to an existing
        # singularity image file
        image "gsiftp://bamboo.isi.edu/lfs1/bamboo-tests/data/centos7.img"

        # optional site attribute to tell pegasus which site tar file
        # exists. useful for handling file URL's correctly
        image_site "local"

        # mount point in the container
        mount "/lizard:/lizard:ro"

        # specify env profile via env option do docker run
        profile env "JAVA_HOME" "/opt/java/1.6"
   }

To enable symlinking for containers set the following properties

::

   # Tells Pegasus to try and create symlinks for input files
   pegasus.transfer.links true

   # Tells Pegasus to by the staging site ( creation of stage-in jobs) as
   # data is available directly on compute nodes
   pegasus.transfer.bypass.input.staging true

f you don't set pegasus.transfer.bypass.input.staging then you still can
have symlinking if

1. your staging site is same as your compute site

2. the scratch directory specified in the site catalog is visible to the
   worker nodes

3. you mount the scratch directory in the container definition, NOT the
   original source directory.

Enabling symlinking of containers is useful, when running large
workflows on a single cluster. Pegasus can pull the image from the
container repository once, and place it on the shared filesystem where
it can then be symlinked from, when the PegasusLite jobs start on the
worker nodes of that cluster. In order to do this, you need to be
running the nonsharedfs data configuration mode with the staging site
set to be the same as the compute site.

.. _containers-example:

Container Example - Montage Workflow
====================================
