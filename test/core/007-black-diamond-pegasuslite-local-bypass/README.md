TEST DESCRIPTION
- This test runs a blackdiamond workflow in the Pegasus Lite mode on
local site. 
- Clustering and staging of executables both is turned on .

PURPOSE
- The purpose is to make sure the pegasus-lite-local.sh wrapper works
correctly for local universe jobs with condor io set to true .
- check for bypass staging and ensure it works for local/condor universe

Associated JIRA Item
- https://jira.isi.edu/browse/PM-542
https://github.com/pegasus-isi/pegasus/issues/2065