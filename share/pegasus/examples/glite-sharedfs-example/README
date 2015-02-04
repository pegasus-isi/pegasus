Purpose
	- An example to submit a blackdiamond workflow directly to
	local PBS using the condor glite functionality

	
Tested with
       - Pegasus 4.2.0
       - Condor 7.9.3 . Should work with stable condor release also.


Setting PBS Parameters for jobs
       - Put the pbs_local_submit_attributes.sh file distributed with
       Pegasus in share/pegasus/htcondor/glite directory in the GLITE bin
       directory of the HTCondor installation.

       - GLITE directory can be determined by running
       condor_config_val GLITE_LOCATION

       - The profiles to set the PBS parameters for jobs are explained
         here
	 https://pegasus.isi.edu/wms/docs/latest/execution_environments.php#glite

	

Before you RUN

       Before you run the example, make sure that the glite bindings
       are  installed in your condor install. Do the following

        -  condor_config_val GLITE_LOCATION
    	this will tell you the glite location

	 -  do an ls in that directory to make sure the binaries
            exists
	    
       Sample Usage
       [vahi@ccg-testing2 ~]$  condor_config_val GLITE_LOCATION
       /usr/libexec/condor/glite

       [vahi@ccg-testing2 ~]$ ls -lh /usr/libexec/condor/glite/
       total 12K
       drwxr-xr-x 2 root root 4.0K Feb  7 10:20 bin
       drwxr-xr-x 2 root root 4.0K Feb  7 10:20 etc	
       drwxr-xr-x 3 root root 4.0K Feb  7 10:20 share


HOW TO RUN
    ./submit will generate the workflow , plan it with pegasus and
    submit it for execution

    the sites.xml file will have a local-pbs site that designates your
    cluster. 
       
