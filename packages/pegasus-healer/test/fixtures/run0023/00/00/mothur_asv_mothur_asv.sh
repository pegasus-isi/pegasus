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
   "lfn": "mothur_asv",
   "id": 1,
   "src_urls": [
     { "site_label": "compute", "url": "file:///pscratch/sd/h/hsafri/wf-scratch/hsafri/pegasus/subductcr-end-to-end/run0023/./mothur_asv", "checkpoint": "false" }
   ],
   "dest_urls": [
     { "site_label": "compute", "url": "file://$PWD/mothur_asv" }
   ] }
 ,
 { "type": "transfer",
   "linkage": "input",
   "lfn": "silva_v132.db",
   "id": 2,
   "src_urls": [
     { "site_label": "compute", "url": "file:///pscratch/sd/h/hsafri/wf-scratch/hsafri/pegasus/subductcr-end-to-end/run0023/./silva_v132.db", "checkpoint": "false" }
   ],
   "dest_urls": [
     { "site_label": "compute", "url": "file://$PWD/silva_v132.db" }
   ] }
 ,
 { "type": "transfer",
   "linkage": "input",
   "lfn": "silva_v132.tax",
   "id": 3,
   "src_urls": [
     { "site_label": "compute", "url": "file:///pscratch/sd/h/hsafri/wf-scratch/hsafri/pegasus/subductcr-end-to-end/run0023/./silva_v132.tax", "checkpoint": "false" }
   ],
   "dest_urls": [
     { "site_label": "compute", "url": "file://$PWD/silva_v132.tax" }
   ] }
 ,
 { "type": "transfer",
   "linkage": "input",
   "lfn": "kebj01_16s.fasta",
   "id": 4,
   "src_urls": [
     { "site_label": "compute", "url": "file:///pscratch/sd/h/hsafri/wf-scratch/hsafri/pegasus/subductcr-end-to-end/run0023/./kebj01_16s.fasta", "checkpoint": "false" }
   ],
   "dest_urls": [
     { "site_label": "compute", "url": "file://$PWD/kebj01_16s.fasta" }
   ] }
]
eof

printf "\n##################### Setting the xbit for executables staged #####################\n"  1>&2
# set the xbit for any executables staged
/bin/chmod +x mothur_asv

printf "\n##################### Checking file integrity for input files #####################\n"  1>&2
# do file integrity checks
pegasus-integrity --print-timings --verify=stdin 1>&2 << 'eof'
mothur_asv;;;silva_v132.db;;;silva_v132.tax;;;kebj01_16s.fasta
eof

pegasus_lite_section_end stage_in

printf "\n########[Pegasus Lite] Writing out script to launch user task in container ########\n"  1>&2

cat <<EOF > mothur_asv_mothur_asv-cont.sh
#!/bin/bash
set -e

# setting environment variables for job
EOF
container_env /scratch >> mothur_asv_mothur_asv-cont.sh
cat <<EOF2 >> mothur_asv_mothur_asv-cont.sh
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

pegasus-kickstart  -n mothur_asv -N mothur_asv -R compute  -S @$_CONDOR_SCRATCH_DIR/mothur_asv_mothur_asv.in.lof  -s @$_CONDOR_SCRATCH_DIR/mothur_asv_mothur_asv.out.lof  -L subductcr-end-to-end -T 2026-09-11T05:13:20+00:00 -k 42900 -K 150 ./mothur_asv kebj01_16s.fasta silva_v132.db silva_v132.tax asv_table.tsv taxonomy.tsv
EOF2


chmod +x mothur_asv_mothur_asv-cont.sh
if ! [ $pegasus_lite_start_dir -ef . ]; then
	cp $pegasus_lite_start_dir/pegasus-lite-common.sh . 
fi

set +e
job_ec=0
shifter_init swarmourr/subductcr-mothur:latest
job_ec=$(($job_ec + $?))

shifter --image swarmourr/subductcr-mothur:latest --volume $PWD:/scratch --workdir=/scratch ./mothur_asv_mothur_asv-cont.sh 
job_ec=$(($job_ec + $?))

pegasus_lite_section_start stage_out
printf "\n############################ Staging out output files ############################\n"  1>&2
# stage out
pegasus-transfer --threads 1  1>&2 << 'eof'
[
 { "type": "transfer",
   "linkage": "output",
   "lfn": "asv_table.tsv",
   "id": 1,
   "src_urls": [
     { "site_label": "compute", "url": "file://$PWD/asv_table.tsv", "checkpoint": "false" }
   ],
   "dest_urls": [
     { "site_label": "compute", "url": "file:///pscratch/sd/h/hsafri/wf-scratch/hsafri/pegasus/subductcr-end-to-end/run0023/./asv_table.tsv" }
   ] }
 ,
 { "type": "transfer",
   "linkage": "output",
   "lfn": "taxonomy.tsv",
   "id": 2,
   "src_urls": [
     { "site_label": "compute", "url": "file://$PWD/taxonomy.tsv", "checkpoint": "false" }
   ],
   "dest_urls": [
     { "site_label": "compute", "url": "file:///pscratch/sd/h/hsafri/wf-scratch/hsafri/pegasus/subductcr-end-to-end/run0023/./taxonomy.tsv" }
   ] }
]
eof

pegasus_lite_section_end stage_out

set -e


# clear the trap, and exit cleanly
trap - EXIT
pegasus_lite_final_exit

