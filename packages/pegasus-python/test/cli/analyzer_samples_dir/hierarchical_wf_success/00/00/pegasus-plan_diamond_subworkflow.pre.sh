#!/bin/bash
set -e
pegasus_lite_version_major="5"
pegasus_lite_version_minor="0"
pegasus_lite_version_patch="5"
pegasus_lite_enforce_strict_wp_check="false"
pegasus_lite_version_allow_wp_auto_download="false"
pegasus_lite_log_file="pegasus-plan_diamond_subworkflow.pre.err"

 # set for pegasus-plan invocation 
export PEGASUS_HOME="/home/mzalam/pegasus-5.0.5"

. pegasus-lite-common.sh

pegasus_lite_init

# cleanup in case of failures
trap pegasus_lite_signal_int INT
trap pegasus_lite_signal_term TERM
trap pegasus_lite_unexpected_exit EXIT

printf "\n########################[Pegasus Lite] Setting up workdir ########################\n"  1>&2
# work dir
pegasus_lite_setup_work_dir

pegasus_lite_section_start stage_in
printf "\n###################### Staging in input data and executables ######################\n"  1>&2
# stage in data and executables
pegasus-transfer --threads 1  1>&2 << 'eof'
[
 { "type": "transfer",
   "linkage": "input",
   "lfn": "sites.yml",
   "id": 1,
   "src_urls": [
     { "site_label": "local", "url": "file:///home/mzalam/hierar/hierarichal-sample-wf/work/local-site/scratch/mzalam/pegasus/hierarchical-workflow/run0001/./sites.yml", "checkpoint": "false" }
   ],
   "dest_urls": [
     { "site_label": "local", "url": "file://$PWD/sites.yml" }
   ] }
 ,
 { "type": "transfer",
   "linkage": "input",
   "lfn": "inner_diamond_workflow_tc.yml",
   "id": 2,
   "src_urls": [
     { "site_label": "local", "url": "file:///home/mzalam/hierar/hierarichal-sample-wf/work/local-site/scratch/mzalam/pegasus/hierarchical-workflow/run0001/./inner_diamond_workflow_tc.yml", "checkpoint": "false" }
   ],
   "dest_urls": [
     { "site_label": "local", "url": "file://$PWD/inner_diamond_workflow_tc.yml" }
   ] }
 ,
 { "type": "transfer",
   "linkage": "input",
   "lfn": "inner_diamond_workflow.pegasus.properties",
   "id": 3,
   "src_urls": [
     { "site_label": "local", "url": "file:///home/mzalam/hierar/hierarichal-sample-wf/work/local-site/scratch/mzalam/pegasus/hierarchical-workflow/run0001/./inner_diamond_workflow.pegasus.properties", "checkpoint": "false" }
   ],
   "dest_urls": [
     { "site_label": "local", "url": "file://$PWD/inner_diamond_workflow.pegasus.properties" }
   ] }
]
eof

pegasus_lite_section_end stage_in
set +e
job_ec=0
pegasus_lite_section_start task_execute
printf "\n######################[Pegasus Lite] Executing the user task ######################\n"  1>&2
pegasus-kickstart  -n pegasus-plan_diamond_subworkflow.pre.sh -N diamond_subworkflow -R local  -s f.d=f.d -L hierarchical-workflow -T 2023-03-04T01:00:54-08:00 /home/mzalam/pegasus-5.0.5/bin/pegasus-plan $@
job_ec=$?
pegasus_lite_section_end task_execute
set -e
pegasus_lite_section_start stage_out
pegasus_lite_section_end stage_out

set -e


# clear the trap, and exit cleanly
trap - EXIT
pegasus_lite_final_exit

