TEST DESCRIPTION
- This test runs a montage workflow in condorio mode with bypassing of input files turned on.
- The test pulls down the input files first to an input directory and only renames the fits files to match the LFN name.
  The tbl and hdr files are not changed. This is to ensure that the planner does not do the bypass and lets the stagein jobs created do the rename for us. 
- Clustering, cleanup and staging of executables both is turned on .

PURPOSE
- To make sure that Condor IO works well with bypass of input file staging turned on.

Associated JIRA Item
- https://jira.isi.edu/browse/PM-698
