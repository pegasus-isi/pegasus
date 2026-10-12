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
   "lfn": "QH2F_R1.fastq.gz",
   "id": 1,
   "src_urls": [
     { "site_label": "compute", "url": "file:///pscratch/sd/h/hsafri/wf-scratch/hsafri/pegasus/subductcr-end-to-end/run0023/./QH2F_R1.fastq.gz", "checkpoint": "false" }
   ],
   "dest_urls": [
     { "site_label": "compute", "url": "file://$PWD/QH2F_R1.fastq.gz" }
   ] }
 ,
 { "type": "transfer",
   "linkage": "input",
   "lfn": "QH2F_R2.fastq.gz",
   "id": 2,
   "src_urls": [
     { "site_label": "compute", "url": "file:///pscratch/sd/h/hsafri/wf-scratch/hsafri/pegasus/subductcr-end-to-end/run0023/./QH2F_R2.fastq.gz", "checkpoint": "false" }
   ],
   "dest_urls": [
     { "site_label": "compute", "url": "file://$PWD/QH2F_R2.fastq.gz" }
   ] }
 ,
 { "type": "transfer",
   "linkage": "input",
   "lfn": "trim_reads",
   "id": 3,
   "src_urls": [
     { "site_label": "compute", "url": "file:///pscratch/sd/h/hsafri/wf-scratch/hsafri/pegasus/subductcr-end-to-end/run0023/./trim_reads", "checkpoint": "false" }
   ],
   "dest_urls": [
     { "site_label": "compute", "url": "file://$PWD/trim_reads" }
   ] }
]
eof

printf "\n##################### Setting the xbit for executables staged #####################\n"  1>&2
# set the xbit for any executables staged
/bin/chmod +x trim_reads

printf "\n##################### Checking file integrity for input files #####################\n"  1>&2
# do file integrity checks
pegasus-integrity --print-timings --verify=stdin 1>&2 << 'eof'
QH2F_R1.fastq.gz;;;QH2F_R2.fastq.gz;;;trim_reads
eof

pegasus_lite_section_end stage_in

printf "\n########[Pegasus Lite] Writing out script to launch user task in container ########\n"  1>&2

cat <<EOF > trim_reads_trim_QH2F-cont.sh
#!/bin/bash
set -e

# setting environment variables for job
EOF
container_env /scratch >> trim_reads_trim_QH2F-cont.sh
cat <<EOF2 >> trim_reads_trim_QH2F-cont.sh
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

pegasus-kickstart  -n trim_reads -N trim_QH2F -R compute  -S @$_CONDOR_SCRATCH_DIR/trim_reads_trim_QH2F.in.lof  -s @$_CONDOR_SCRATCH_DIR/trim_reads_trim_QH2F.out.lof  -L subductcr-end-to-end -T 2026-09-11T05:13:20+00:00 -k 42900 -K 150 ./trim_reads QH2F_R1.fastq.gz QH2F_R2.fastq.gz QH2F_trim_R1.fastq.gz QH2F_trim_R2.fastq.gz
EOF2


chmod +x trim_reads_trim_QH2F-cont.sh
if ! [ $pegasus_lite_start_dir -ef . ]; then
	cp $pegasus_lite_start_dir/pegasus-lite-common.sh . 
fi

set +e
job_ec=0
shifter_init swarmourr/subductcr-mothur:latest
job_ec=$(($job_ec + $?))

shifter --image swarmourr/subductcr-mothur:latest --volume $PWD:/scratch --workdir=/scratch ./trim_reads_trim_QH2F-cont.sh 
job_ec=$(($job_ec + $?))

pegasus_lite_section_start stage_out
printf "\n############################ Staging out output files ############################\n"  1>&2
# stage out
pegasus-transfer --threads 1  1>&2 << 'eof'
[
 { "type": "transfer",
   "linkage": "output",
   "lfn": "QH2F_trim_R2.fastq.gz",
   "id": 1,
   "src_urls": [
     { "site_label": "compute", "url": "file://$PWD/QH2F_trim_R2.fastq.gz", "checkpoint": "false" }
   ],
   "dest_urls": [
     { "site_label": "compute", "url": "file:///pscratch/sd/h/hsafri/wf-scratch/hsafri/pegasus/subductcr-end-to-end/run0023/./QH2F_trim_R2.fastq.gz" }
   ] }
 ,
 { "type": "transfer",
   "linkage": "output",
   "lfn": "QH2F_trim_R1.fastq.gz",
   "id": 2,
   "src_urls": [
     { "site_label": "compute", "url": "file://$PWD/QH2F_trim_R1.fastq.gz", "checkpoint": "false" }
   ],
   "dest_urls": [
     { "site_label": "compute", "url": "file:///pscratch/sd/h/hsafri/wf-scratch/hsafri/pegasus/subductcr-end-to-end/run0023/./QH2F_trim_R1.fastq.gz" }
   ] }
]
eof

pegasus_lite_section_end stage_out

set -e


# clear the trap, and exit cleanly
trap - EXIT
pegasus_lite_final_exit

