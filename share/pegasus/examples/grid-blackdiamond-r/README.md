
This is a sample workflow with 4 nodes, layed out in a diamond shape:


        a
       / \
      b   c
       \ /
        d


The binary for the nodes is a simple "mock application" name "keg"
("canonical example for the grid") which reads input files designated by
arguments, writes them back onto output files, and produces on STDOUT a
summary of where and when it was run. Keg ships with Pegasus in the bin
directory.

This example assumes you have access to a cluster with Globus installed.
A pre-ws gatekeeper and gridftp server is required.

You also need Globus and Pegasus installed, both on the machine you are
submitting from, and the cluster. Condor needs to be installed and
running on your submit machine. See the Pegasus manual.

This example ships with a "submit" script which will build the site
catalog. When you  create your own workflows, such a submit script is not
needed if you want to maintain the catalogs manually.

To test the example, edit the "submit" script and change the cluster
config to the setup and install locations for your cluster. Then run:

./submit

The workflow should now be submitted and in the output you should see
a submit dir location for the instance. With that directory you can
monitor the workflow with:

pegasus-status [submitdir]

Once the workflow is done, you can make sure it was sucessful with:

pegasus-analyzer -d [submitdir]




