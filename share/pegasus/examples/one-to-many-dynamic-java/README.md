
This is a sample workflow that takes in 1 input file, splits it into N outputs.
The second job determines the number of outputs produced and generates a sub dax that has a single job for each split output file

The image of this workflow is shown in the included workflow.png diagram

In the root level there are 3 jobs, the first one takes an input file Input with N input lines 
and divides it into Files with 1 line each resulting in N output files. 
It also produces a specially formated list file to list the resultant output files.

The second job takes this list file and produces a subdax for the next job which is a sub dax job. 
This sub dax contains the parallel jobs (N jobs) one each to process each input file.

The third job is a DAX job that takes the input dax file produced in the previous job and plans it 
just in time and submits it to the same remote resource as the split job.

The binary for the parallel nodes is a simple "mock application" name "keg"
which reads input files designated by arguments, writes them back onto output files,
Keg ships with Pegasus in the bin directory.

The top level workflow generator is the RootWorkflow.java code
The inner dax generator that gets called at runtime is called SubWorkflow.java

This example assumes you have access to a cluster with Globus installed.

A pre-ws gatekeeper and gridftp server is required.
You also need Globus and Pegasus installed, both on the machine you are
submitting from, and the cluster. Condor needs to be installed and
running on your submit machine. See the Pegasus manual.


This example ships with a "submit" script which will build the site
catalog. When you  create your own workflows, such a submit script is not
needed if you want to maintain the catalogs manually.

To test the example, edit the "submit" script and change the cluster
config to the setup and install locations for your cluster. 
You will have to copy the supplied split executable to the CLUSTER_SOFTWARE 
location defined in the submit file
Then run:
./submit

The workflow should now be submitted and in the output you should see
a submit dir location for the instance. With that directory you can
monitor the workflow with:

pegasus-status [submitdir]

Once the workflow is done, you can make sure it was sucessful with:

pegasus-analyzer -d [submitdir]




