Purpose
	- This example highlight a simple MPI hello world program that is run through Pegasus.
	  It is setup to submit the MPI job directly to the MIRA BlueGene Cluster at ALCF
	  Utilizes support for cobalt in the Glite interface in Pegasus

	
Tested with
       - Pegasus 4.6.2
       - Condor 8.5.6 . 


Before you RUN
       - Compile the pegasus-mpi-hw.c executable. A Makefile is provided with the example.

       - Make sure you are running HTCondor on the MIRA login nodes miralac1-3

       - Condor needs special configuration to submit to cobalt. Please refer to HTCondor documentation.
         Sample condor configuration files that worked for us on MIRA had the following entries.
	 Update accordingly
	 
	 [miralac3]$ more /home/vahi/condor/LOCAL/condor_config.local

	 CONDOR_HOST = miralac3.pub.alcf.anl.gov
	 NETWORK_INTERFACE=140.221.69.10

	 SHARED_PORT_PORT = 9619
	 COLLECTOR_HOST = $(CONDOR_HOST):$(SHARED_PORT_PORT)

	 #RELEASE_DIR = /home/jfrey/for_karan/release_dir
	 RELEASE_DIR = /home/vahi/condor/release_dir

	 DAEMON_LIST = MASTER, SCHEDD, COLLECTOR

	 CONDOR_ADMIN = vahi@isi.edu

	 ALLOW_ADMINISTRATOR = $(FULL_HOSTNAME) $(IP_ADDRESS) 127.0.0.1
	 ALLOW_DAEMON = $(FULL_HOSTNAME) $(IP_ADDRESS) 127.0.0.1

	 GRIDMANAGER_DEBUG = D_FULLDEBUG

	 GRIDMANAGER_JOB_PROBE_INTERVAL = 15

	 ALLOW_WRITE = 140.221.69.*
       


HOW TO RUN
         
    # the example directory should be in your home directory
    # or any directory that is mounted both on the  cluster
    # and miralac login node

    ./submit will generate the workflow , plan it with pegasus and
    submit it for execution

    the sites.xml file will have a alcf site that designates the ALCF cluster.
    
    # ALCF directory will be where the workflow scratch directory is
    
    # the outputs of the worklfow will appear in the outputs directory      



