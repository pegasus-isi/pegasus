Purpose
	- This example highlight a simple MPI hello world program that is run through Pegasus.
	  It is setup to submit the MPI job directly to the underlying PBS cluster using the
	  Glite support in Pegasus

	
Tested with
       - Pegasus 4.4.0
       - Condor 7.9.3 . Should work with stable condor release also.


Before you RUN
       - Compile the pegasus-mpi-hw.c executable. A Makefile is provided with the example.

       - Put the pbs_local_submit_attributes.sh file distributed with
       Pegasus in share/pegasus/htcondor/glite directory in the GLITE bin
       directory of the  HTCondor installation.

       - GLITE directory can be determined by running
       condor_config_val GLITE_LOCATION
       


HOW TO RUN
         
    # the example directory should be in your home directory
    # or any directory that is mounted both on the HPCC cluster
    # and hpc-pegasus

    ./submit will generate the workflow , plan it with pegasus and
    submit it for execution

    the sites.xml file will have a hpcc site that designates the HPCC cluster.
    
    # HPCC directory will be where the workflow scratch directory is
    
    # the outputs of the worklfow will appear in the outputs directory      



