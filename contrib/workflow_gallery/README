Introduction
------------

Pegasus provides a two command line utilities pegasus-create-workflow-page and 
pegasus-create-workflow-type-page for generating workflow gallery.

Prerequisite
------------

Utility has a dependency on the pegasus-config file in the pegasus.home/bin 
directory. So pegasus-create-workflow-page and pegasus-create-workflow-type-page  
should be copied to bin directory for running the the utility.The tool also copies  
the protovis javascript file from pegasus.home/lib/javascript , php files from
pegasus.home/lib/javascript and image and cssfiles from /share/pegasus/plots/ 
directory. 


Functionality
-------------

pegasus-create-workflow-page - Parses the given directory for workflow tar files
and generates a run directory corresponding to each workflow run which 
contains workflow page which contains statistics information and charts to
visualize the run. It also generates a workflow_info.txt file which is 
used by the pegasus-create-workflow-type-page. If output option is given 
the files are generated to a gallery folder inside the given directory where
tar files are copied. 

pegasus-create-workflow-type-page - Parses the run directory created by the 
pegasus-create-workflow-page and generates an index file that is used to
link all the workflow runs of a given type.

pegasus-create-workflow-type-page looks for space separated property file
'workflow_type.txt'set the name of the workflow type, type description and 
workflow type image. A sample format is given below. 
name Broadband
image broadband.jpg
desc info.txt

Gallery Directory Structure
---------------------------

pegasus-create-workflow-type-page generates an index.php file ,images and css 
directories as output. These pages should be placed as parent directory while
the gallery is setup. Similarly , it has links to index.php and help.php
which are the workflow gallery home directory and gallery information links
that need to placed two levels higher than the directory where the output files
are copied.


The workflow gallery structure should be like this

workflow_gallery [root directory]
	index.php
	help.php
	css/
	gallery/
		broadband/
			broadband.jpg
			gallery_header.php
			gallery_footer.php
			index.php
			info.txt
			workflow_type.txt
			css/
			images/
			run_1/
			run_2/
		cybershake/
			cybershake.jpg
			gallery_header.php
			gallery_footer.php
			index.php
			info.txt
			workflow_type.txt
			css/
			images/
			run_1/
			run_2/
	images/
	

