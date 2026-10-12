#!/bin/bash
set -e
pegasus_lite_version_major="5"
pegasus_lite_version_minor="1"
pegasus_lite_version_patch="3-dev.0"
pegasus_lite_enforce_strict_wp_check="true"
pegasus_lite_version_allow_wp_auto_download="true"
pegasus_metrics="true"


. pegasus-lite-common.sh

pegasus_lite_init

# cleanup in case of failures
trap pegasus_lite_signal_int INT
trap pegasus_lite_signal_term TERM
trap pegasus_lite_unexpected_exit EXIT

printf "\n########################[Pegasus Lite] Setting up workdir ########################\n"  1>&2
# work dir
pegasus_lite_setup_work_dir

printf "\n##############[Pegasus Lite] Figuring out the worker package to use ##############\n"  1>&2
# figure out the worker package to use
pegasus_lite_worker_package

set -e
pegasus_lite_section_start stage_in
printf "\n###################### Staging in input data and executables ######################\n"  1>&2
# stage in data and executables
pegasus-transfer --threads 1  1>&2 << 'eof'
[
 { "type": "transfer",
   "linkage": "input",
   "lfn": "adonis.tsv",
   "id": 1,
   "src_urls": [
     { "site_label": "compute", "url": "file:///pscratch/sd/h/hsafri/wf-scratch/hsafri/pegasus/subductcr-end-to-end/run0023/./adonis.tsv", "checkpoint": "false" }
   ],
   "dest_urls": [
     { "site_label": "compute", "url": "file://$PWD/adonis.tsv" }
   ] }
 ,
 { "type": "transfer",
   "linkage": "input",
   "lfn": "carbon_flux.tsv",
   "id": 2,
   "src_urls": [
     { "site_label": "compute", "url": "file:///pscratch/sd/h/hsafri/wf-scratch/hsafri/pegasus/subductcr-end-to-end/run0023/./carbon_flux.tsv", "checkpoint": "false" }
   ],
   "dest_urls": [
     { "site_label": "compute", "url": "file://$PWD/carbon_flux.tsv" }
   ] }
 ,
 { "type": "transfer",
   "linkage": "input",
   "lfn": "make_report",
   "id": 3,
   "src_urls": [
     { "site_label": "compute", "url": "file:///pscratch/sd/h/hsafri/wf-scratch/hsafri/pegasus/subductcr-end-to-end/run0023/./make_report", "checkpoint": "false" }
   ],
   "dest_urls": [
     { "site_label": "compute", "url": "file://$PWD/make_report" }
   ] }
 ,
 { "type": "transfer",
   "linkage": "input",
   "lfn": "asv_clique_env.tsv",
   "id": 4,
   "src_urls": [
     { "site_label": "compute", "url": "file:///pscratch/sd/h/hsafri/wf-scratch/hsafri/pegasus/subductcr-end-to-end/run0023/./asv_clique_env.tsv", "checkpoint": "false" }
   ],
   "dest_urls": [
     { "site_label": "compute", "url": "file://$PWD/asv_clique_env.tsv" }
   ] }
 ,
 { "type": "transfer",
   "linkage": "input",
   "lfn": "nmds.tsv",
   "id": 5,
   "src_urls": [
     { "site_label": "compute", "url": "file:///pscratch/sd/h/hsafri/wf-scratch/hsafri/pegasus/subductcr-end-to-end/run0023/./nmds.tsv", "checkpoint": "false" }
   ],
   "dest_urls": [
     { "site_label": "compute", "url": "file://$PWD/nmds.tsv" }
   ] }
 ,
 { "type": "transfer",
   "linkage": "input",
   "lfn": "gene_clique_env.tsv",
   "id": 6,
   "src_urls": [
     { "site_label": "compute", "url": "file:///pscratch/sd/h/hsafri/wf-scratch/hsafri/pegasus/subductcr-end-to-end/run0023/./gene_clique_env.tsv", "checkpoint": "false" }
   ],
   "dest_urls": [
     { "site_label": "compute", "url": "file://$PWD/gene_clique_env.tsv" }
   ] }
]
eof

printf "\n##################### Setting the xbit for executables staged #####################\n"  1>&2
# set the xbit for any executables staged
/bin/chmod +x make_report

printf "\n##################### Checking file integrity for input files #####################\n"  1>&2
# do file integrity checks
pegasus-integrity --print-timings --verify=stdin 1>&2 << 'eof'
adonis.tsv;;;carbon_flux.tsv;;;make_report;;;asv_clique_env.tsv;;;nmds.tsv;;;gene_clique_env.tsv
eof

pegasus_lite_section_end stage_in

printf "\n########[Pegasus Lite] Writing out script to launch user task in container ########\n"  1>&2

cat <<EOF > make_report_make_report-cont.sh
#!/bin/bash
set -e

# setting environment variables for job
EOF
container_env /scratch >> make_report_make_report-cont.sh
cat <<EOF2 >> make_report_make_report-cont.sh
pegasus_lite_version_major=$pegasus_lite_version_major
pegasus_lite_version_minor=$pegasus_lite_version_minor
pegasus_lite_version_patch=$pegasus_lite_version_patch
pegasus_lite_enforce_strict_wp_check=$pegasus_lite_enforce_strict_wp_check
pegasus_lite_version_allow_wp_auto_download=$pegasus_lite_version_allow_wp_auto_download
pegasus_lite_inside_container=true
export pegasus_lite_work_dir=/scratch

cd /scratch
. ./pegasus-lite-common.sh
pegasus_lite_init

printf "\n##############[Container] Figuring out Pegasus worker package to use ##############\n"  1>&2
# figure out the worker package to use
pegasus_lite_worker_package
printf "PATH in container is set to is set to \$PATH\n"  1>&2

printf "\n#########################[Container] Launching user task #########################\n"  1>&2

pegasus-kickstart  -n make_report -N make_report -R compute  -S @$_CONDOR_SCRATCH_DIR/make_report_make_report.in.lof  -s @$_CONDOR_SCRATCH_DIR/make_report_make_report.out.lof  -L subductcr-end-to-end -T 2026-09-11T05:13:20+00:00 -k 42900 -K 150 ./make_report asv_clique_env.tsv gene_clique_env.tsv nmds.tsv adonis.tsv carbon_flux.tsv report.html
EOF2


chmod +x make_report_make_report-cont.sh
if ! [ $pegasus_lite_start_dir -ef . ]; then
	cp $pegasus_lite_start_dir/pegasus-lite-common.sh . 
fi

set +e
job_ec=0
shifter_init swarmourr/subductcr-r:latest
job_ec=$(($job_ec + $?))

shifter --image swarmourr/subductcr-r:latest --volume $PWD:/scratch --workdir=/scratch ./make_report_make_report-cont.sh 
job_ec=$(($job_ec + $?))

pegasus_lite_section_start stage_out
printf "\n############################ Staging out output files ############################\n"  1>&2
# stage out
pegasus-transfer --threads 1  1>&2 << 'eof'
[
 { "type": "transfer",
   "linkage": "output",
   "lfn": "report.html",
   "id": 1,
   "src_urls": [
     { "site_label": "compute", "url": "file://$PWD/report.html", "checkpoint": "false" }
   ],
   "dest_urls": [
     { "site_label": "compute", "url": "file:///pscratch/sd/h/hsafri/wf-scratch/hsafri/pegasus/subductcr-end-to-end/run0023/./report.html" }
   ] }
]
eof

pegasus_lite_section_end stage_out

set -e


# clear the trap, and exit cleanly
trap - EXIT
pegasus_lite_final_exit

