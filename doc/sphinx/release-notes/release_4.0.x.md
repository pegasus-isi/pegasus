## Pegasus 4.0.x Series

### Pegasus 4.0.1

This is a minor release, that fixes some bugs and has minor enhancements. 


#### NEW FEATURES

1) pegasus lite local wrapper is now used for local universe jobs in
   shared fs mode also, if condor io is detected. 

   Also  remote_initialdir is not implemented consistently across
   universes in Condor.  For vanilla universe condor file io does not
   transfer the file to the remote_initialdir.

2) task summary queries were reimplemented

   The task summary queries ( that list the number of successful and
   failed tasks )  in the Stampede Statisitcs API was
   reimplemented for better performance.


3) pegasus-monitord sets PEGASUS_BIN_DIR while calling out notfication
scripts . 
	
   https://jira.isi.edu/browse/PM-598

4) the default notification script can send out emails to multiple
recipients. 

5) Support for new condor keys 

   Pegasus allows users to specify the following condor keys as
   profiles in the Condor namespace. The new keys have been introduced
   in Condor 7.8.0

   request_cpus 
   request_memory 
   request_disk 
   
   https://jira.isi.edu/browse/PM-600


#### BUGS FIXED

1) pegasus-kickstart does not collect procs and tasks statistics on kernels >= 3.0
   
   When kickstart is executed on a Linux kernel >= 3.0, logic in the
   machine extensions prevented the proc statistics gathering, because
   it was a reasonable assumption that the API might have changed (it
   did between 2.4 and 2.6). This is now fixed, as it is supported for
   kernels 3.0 through 3.2

   https://jira.isi.edu/browse/PM-571

2) scp transfer mode did not create remote directories
   
   When transferring to a scp endpoint, pegasus-transfer failed unless
   the remote directory already existed. This broke deep LFNs and
   staging to output sites.  This is now fixed.

   https://jira.isi.edu/browse/PM-579

3) Incorrect resolution of PEGASUS_HOME path in the site catalog for
remote sites  in some cases
       
   If a user specified a path to PEGASUS_HOME for remote sites in the
   site catalog and the directory also existed on the submit machine,
   the path was resolved locally. Hence if the local directory was a
   symlink, the symlink was resolved and that path was used for the
   remote site's PEGASUS_HOME.

   https://jira.isi.edu/browse/PM-577

4) pegasus-analyzer did not work correctly against the MySQL Stampede
DB
	
   pegasus-analyzer had problems querying MySQL stampede database
   because of a query aliasing error in the API underneath. This is
   now fixed.

   https://jira.isi.edu/browse/PM-580

5) Wrong timezone offsets for ISO timestamps 

   Pegasus python library was generating the wrong time zone offset
   for ISO 8601 time stamps. This was because of an underlying bug in
   python where %z does not work correctly across all platforms.

   https://jira.isi.edu/browse/PM-576

6) pegasus-analyzer warns about "exitcode not an integer!"
 
   pegasus-analyzer throwed a warning if a long value for an exitcode
   was detected.
 
    https://jira.isi.edu/browse/PM-584

7) Perl DAX generator uses 'out' instead of 'output' for stderr and
stdout linkage 

  The perl DAX generator API generated the wrong link attribute for
  stdout files. Instead of having link = output it generated link =
  out.

  https://jira.isi.edu/browse/PM-585

8) Updated Stampede Queries to handle both GRID_SUBMIT and
GLOBUS_SUBMIT events.

  Two of the queries ( get_job_statistics and get_job_state ) were
  broken for CondorG workflows when operating against a MySQL database
  backend. In that case,  both GRID_SUBMIT and GLOBUS_SUBMIT can be
  logged for the jobs. In that case, some of the subqueries were
  breaking against MySQL has MySQL has stricter checks on queries
  returning a single value.


9) Support for DAGMAN_COPY_TO_SPOOL Condor configuration parameter

   Condor has a setting DAGMAN_COPY_TO_SPOOL that if set to true,
   results in Condor copying the DAGMan binary to the spool directory
   before launching the workflow. In case of Pegasus, condor dagman is
   launched by a wrapper called pegasus-dagman. Because of this ,
   pegasus dagman was copied to the condor spool directory before
   being launched in lieu of condor dagman binary.

   This is now fixed whereby pegasus-dagman will copy condor_dagman
   binary to the submit directory for the workflow before launching
   the workflow.

   More details at 
   https://jira.isi.edu/browse/PM-595

### Pegasus 4.0.0

This is a major release of Pegasus that introduces new advanced data
handling capabilities, and contains improved support for running
workflows in non-shared filesystem scenarios such as clouds and Condor
pools. Pegasus now optionally separates the data staging site from the
workflow execution site for more flexible data management. A new
feature is PegasusLite - an autonomous lightweight execution
environment to manage jobs on the compute nodes and handles data
movement to/from such jobs against the workflow staging site. The RPM
and Debian packages conform to the Filesystem Hierarchy Standard
(FHS).


#### NEW FEATURES

1) PegasusLite

   Pegasus 4.0 has improved support for running workflows in a non
   shared fileysystem setup . This is useful for running in cloud
   environments and allows for more dynamic placement of jobs. 
   Pegasus 4.0 introduces the concept of a staging site for the
   worklfows, that can be different from an execution site. The
   planner places the data on the staging site for the workflow. When
   the jobs start on the remote compute nodes, they are launched by a
   lightweight component called PegasusLite, that stages in the data
   from the staging site to a local directory on the worker node
   filesystem . The output data generated by the compute job is
   similarly pushed back to the staging site, when a job completes.

   Users can now setup Pegasus for different environments by setting
   the property

   pegasus.data.configuration 

   More details can be found here

   http://pegasus.isi.edu/wms/docs/trunk/running_workflows.php#data_staging_configuration 

   

2) Move to FHS layout

   Pegasus 4.0 is the first release of Pegasus which is Filesystem
   Hierarchy Standard (FHS) compliant. The native packages no longer
   installs under /opt. Instead, pegasus-* binaries are in /usr/bin/
   and example workflows can be found under
   /usr/share/pegasus/examples/.

   To find Pegasus system components, a pegasus-config tool is
   provided. pegasus-config supports setting up the environment for

     - Python
     - Perl
     - Java
     - Shell

3) Improved Credential Handling
   
   Pegasus 4.0 has improved credential handling. The planner while
   planning the worklfow automatically associates the jobs with the
   credentials it may require. This is done by inspecting the URL's
   for the files a job requires. 

   More details on how the credentials are set can be found here
   http://pegasus.isi.edu/wms/docs/4.0/reference.php#cred_staging


4) New clients for directory creation and file cleanup

   Starting 4.0, Pegasus has changed the way how the scratch
   directories are created on the staging site. The planner now
   prefers to schedule the directory creation and cleanup jobs
   locally. The jobs refer to python based tools, that call out to
   protocol specific clients to determine what client is picked
   up. For protocols, where specific remote cleanup and directory
   creation clients don't exist ( for example gridftp ), the python
   tools rely on the corresponding transfer tool to create a directory
   by initiating a transfer of an empty file. The python clients used
   to create directories and remove files are called
      - pegasus-create-dir
      - pegasus-cleanup

    More details about the clients can be found in the transfers
    chapter
    
    http://pegasus.isi.edu/wms/docs/trunk/reference.php#transfer

5) Runtime based clustering

   Users can now do horizontal clustering based on job runtimes. If
   the jobs in the DAX are annotated with job runtimes ( use of
   pegasus profile key job.runtime ) , then Pegasus can horizontally
   cluster the jobs in such a way that the clustered job will not run
   more than a maxruntime ( specified by use of profile
   clusters.maxruntime). 

   More details can be found in the clustering chapter of the online
   guide. 
   http://pegasus.isi.edu/wms/docs/4.0/reference.php#job_clustering

6) pegasus-analyzer works with stampede database

   Starting with 4.0 release, by default pegasus analyzer queries the
   database generated in the workflow submit directory ( unless using
   mysql) to debug the workflow. If you want it to use files in the
   submit directory , use the --files option.

7) CSV formatted output files for pegasus statistcs
   
   pegasus-statistics now generates it's output in a csv formatted
   file also, in addition to the txt files it creates. This is useful,
   for importing statistics in tools like Mircrosoft Excel.

8) Tracking of job exitcode in the stampede schema
   
   The stampede database schema was updated to associate a job
   exitcode field with the job_instance table. This makes it easier
   for the user and the mining tools to determine whether a job
   succeeded for failed at the Condor level.

   Earlier that was handled at query time by looking up the last state
   in the jobstate table.

   The updated stampede schema can be found here

   http://pegasus.isi.edu/wms/docs/4.0/monitoring_debugging_stats.php#monitoring_pegasus-monitord
   
9) Change to how exitcode is stored in the stampede database
   
   Kickstart records capture raw status in addition to the exitcode
   . The exitcode is derived from the raw status. Starting with
   Pegasus 4.0 release, all exitcode columns ( i.e invocation and job
   instance table columns ) are stored with the raw status by
   pegasus-monitord. If an exitcode is encountered while parsing the
   dagman log files , the value is converted to the corresponding raw
   status before it is stored. All user tools, pegasus-analyzer and
   pegasus-statistics then convert the raw status to exitcode when
   retrieving from the database.

10) Accounting for MPI jobs in the stampede database

   Starting with the 4.0 release, there is a multiplier factor
   associated with the jobs in the job_instance table. It defaults to
   one, unless the user associates a Pegasus profile key named cores
   with the job in the DAX. The factor can be used for getting more
   accurate accounting statistics for jobs that run on multiple
   processors/cores or mpi jobs.

   Full details in the pegasus-monitord chapter.

11) Stampede database upgrade tool

   Starting with the 4.0 release, users can use the stampede database
   upgrade tool to upgrade a 3.1.x database to the latest version.

   All statistics and debugging tools will complain if they determine
   at runtime that the database is from an old version.

   More details can be found in the migration guide
   http://pegasus.isi.edu/wms/docs/4.0/useful_tips.php#id727831

#### BUGS FIXED

1) pegasus-plots invocation breakdown

   In certain cases, it was found that the invocation breakdown piechart
   generated by pegasus-plots was broken. This is now fixed.

   More details at
   https://jira.isi.edu/browse/PM-566

2) UUID devices confuse pegasus-keg

   It is possible with Linux to describe devices in /etc/fstab with
   their UUID instead of the device name, i.e.

   UUID=xxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx / ext3 defaults 1 1 
   instead of 
   /dev/sda1 / ext3 default 1 1 
   
   However, some logic in keg relies on the device starting with a
   slash to recognize a true-file device. 
   This is now fixed.
   