Purpose
	- This example illustrates a sample usage of pegasus-mpi-keg that is run 
	  through Pegasus to simulate an MPI job. It is setup to submit the MPI 
	  job to a PBS-managed cluster through the Globus GRAM support in Pegasus. 
	  This example uses the Hopper cluster at NERSC, which is a Cray XE6.
	
Tested with
       - Pegasus 4.4.0
       - Condor 7.9.3 . Should work with stable condor release also.

Before you RUN
	- You will need to compile and install pegasus-mpi-keg on the target 
	  system. It is in the Pegasus source distribution under 
	  src/tools/pegasus-keg. You may need to make changes to the Makefile to 
	  specify the correct MPI compiler wrapper for your target system (the 
	  MPICC variable in Makefile. On Hopper, for example, the MPI compiler is 
	  CC. Also, before compiling pegasus-mpi-keg, you need to swap you 
	  programming environment to GNU:
	  $ module swap PrgEnv-pgi PrgEnv-gnu

	- You need to create a grid proxy in order to use GridFTP and submit jobs 
	  via GRAM, e.g.:
	  $ myproxy-logon -s nerscca.nersc.gov:7512 -t 720 -T -l YOUR_NERSC_USERNAME

	- Specify locations in the sites.xml file for shared scratch and storage 
	  directories to include your username.

HOW TO RUN
	- ./submit will generate the workflow , plan it with pegasus and
    	  submit it for execution

    	# the outputs of the worklfow will appear in the outputs directory      
