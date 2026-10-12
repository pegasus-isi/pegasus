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
   "lfn": "PGS_enzymes.tsv",
   "id": 1,
   "src_urls": [
     { "site_label": "compute", "url": "file:///pscratch/sd/h/hsafri/wf-scratch/hsafri/pegasus/subductcr-end-to-end/run0023/./PGS_enzymes.tsv", "checkpoint": "false" }
   ],
   "dest_urls": [
     { "site_label": "compute", "url": "file://$PWD/PGS_enzymes.tsv" }
   ] }
 ,
 { "type": "transfer",
   "linkage": "input",
   "lfn": "FAS_enzymes.tsv",
   "id": 2,
   "src_urls": [
     { "site_label": "compute", "url": "file:///pscratch/sd/h/hsafri/wf-scratch/hsafri/pegasus/subductcr-end-to-end/run0023/./FAS_enzymes.tsv", "checkpoint": "false" }
   ],
   "dest_urls": [
     { "site_label": "compute", "url": "file://$PWD/FAS_enzymes.tsv" }
   ] }
 ,
 { "type": "transfer",
   "linkage": "input",
   "lfn": "BRS2_enzymes.tsv",
   "id": 3,
   "src_urls": [
     { "site_label": "compute", "url": "file:///pscratch/sd/h/hsafri/wf-scratch/hsafri/pegasus/subductcr-end-to-end/run0023/./BRS2_enzymes.tsv", "checkpoint": "false" }
   ],
   "dest_urls": [
     { "site_label": "compute", "url": "file://$PWD/BRS2_enzymes.tsv" }
   ] }
 ,
 { "type": "transfer",
   "linkage": "input",
   "lfn": "BQS_enzymes.tsv",
   "id": 4,
   "src_urls": [
     { "site_label": "compute", "url": "file:///pscratch/sd/h/hsafri/wf-scratch/hsafri/pegasus/subductcr-end-to-end/run0023/./BQS_enzymes.tsv", "checkpoint": "false" }
   ],
   "dest_urls": [
     { "site_label": "compute", "url": "file://$PWD/BQS_enzymes.tsv" }
   ] }
 ,
 { "type": "transfer",
   "linkage": "input",
   "lfn": "PLS_enzymes.tsv",
   "id": 5,
   "src_urls": [
     { "site_label": "compute", "url": "file:///pscratch/sd/h/hsafri/wf-scratch/hsafri/pegasus/subductcr-end-to-end/run0023/./PLS_enzymes.tsv", "checkpoint": "false" }
   ],
   "dest_urls": [
     { "site_label": "compute", "url": "file://$PWD/PLS_enzymes.tsv" }
   ] }
 ,
 { "type": "transfer",
   "linkage": "input",
   "lfn": "CYF_enzymes.tsv",
   "id": 6,
   "src_urls": [
     { "site_label": "compute", "url": "file:///pscratch/sd/h/hsafri/wf-scratch/hsafri/pegasus/subductcr-end-to-end/run0023/./CYF_enzymes.tsv", "checkpoint": "false" }
   ],
   "dest_urls": [
     { "site_label": "compute", "url": "file://$PWD/CYF_enzymes.tsv" }
   ] }
 ,
 { "type": "transfer",
   "linkage": "input",
   "lfn": "QNF_enzymes.tsv",
   "id": 7,
   "src_urls": [
     { "site_label": "compute", "url": "file:///pscratch/sd/h/hsafri/wf-scratch/hsafri/pegasus/subductcr-end-to-end/run0023/./QNF_enzymes.tsv", "checkpoint": "false" }
   ],
   "dest_urls": [
     { "site_label": "compute", "url": "file://$PWD/QNF_enzymes.tsv" }
   ] }
 ,
 { "type": "transfer",
   "linkage": "input",
   "lfn": "QH2F_enzymes.tsv",
   "id": 8,
   "src_urls": [
     { "site_label": "compute", "url": "file:///pscratch/sd/h/hsafri/wf-scratch/hsafri/pegasus/subductcr-end-to-end/run0023/./QH2F_enzymes.tsv", "checkpoint": "false" }
   ],
   "dest_urls": [
     { "site_label": "compute", "url": "file://$PWD/QH2F_enzymes.tsv" }
   ] }
 ,
 { "type": "transfer",
   "linkage": "input",
   "lfn": "PFS_enzymes.tsv",
   "id": 9,
   "src_urls": [
     { "site_label": "compute", "url": "file:///pscratch/sd/h/hsafri/wf-scratch/hsafri/pegasus/subductcr-end-to-end/run0023/./PFS_enzymes.tsv", "checkpoint": "false" }
   ],
   "dest_urls": [
     { "site_label": "compute", "url": "file://$PWD/PFS_enzymes.tsv" }
   ] }
 ,
 { "type": "transfer",
   "linkage": "input",
   "lfn": "BRS1_enzymes.tsv",
   "id": 10,
   "src_urls": [
     { "site_label": "compute", "url": "file:///pscratch/sd/h/hsafri/wf-scratch/hsafri/pegasus/subductcr-end-to-end/run0023/./BRS1_enzymes.tsv", "checkpoint": "false" }
   ],
   "dest_urls": [
     { "site_label": "compute", "url": "file://$PWD/BRS1_enzymes.tsv" }
   ] }
 ,
 { "type": "transfer",
   "linkage": "input",
   "lfn": "ec_carbon.csv",
   "id": 11,
   "src_urls": [
     { "site_label": "compute", "url": "file:///pscratch/sd/h/hsafri/wf-scratch/hsafri/pegasus/subductcr-end-to-end/run0023/./ec_carbon.csv", "checkpoint": "false" }
   ],
   "dest_urls": [
     { "site_label": "compute", "url": "file://$PWD/ec_carbon.csv" }
   ] }
 ,
 { "type": "transfer",
   "linkage": "input",
   "lfn": "EPF_enzymes.tsv",
   "id": 12,
   "src_urls": [
     { "site_label": "compute", "url": "file:///pscratch/sd/h/hsafri/wf-scratch/hsafri/pegasus/subductcr-end-to-end/run0023/./EPF_enzymes.tsv", "checkpoint": "false" }
   ],
   "dest_urls": [
     { "site_label": "compute", "url": "file://$PWD/EPF_enzymes.tsv" }
   ] }
 ,
 { "type": "transfer",
   "linkage": "input",
   "lfn": "SIF_enzymes.tsv",
   "id": 13,
   "src_urls": [
     { "site_label": "compute", "url": "file:///pscratch/sd/h/hsafri/wf-scratch/hsafri/pegasus/subductcr-end-to-end/run0023/./SIF_enzymes.tsv", "checkpoint": "false" }
   ],
   "dest_urls": [
     { "site_label": "compute", "url": "file://$PWD/SIF_enzymes.tsv" }
   ] }
 ,
 { "type": "transfer",
   "linkage": "input",
   "lfn": "ETS_enzymes.tsv",
   "id": 14,
   "src_urls": [
     { "site_label": "compute", "url": "file:///pscratch/sd/h/hsafri/wf-scratch/hsafri/pegasus/subductcr-end-to-end/run0023/./ETS_enzymes.tsv", "checkpoint": "false" }
   ],
   "dest_urls": [
     { "site_label": "compute", "url": "file://$PWD/ETS_enzymes.tsv" }
   ] }
 ,
 { "type": "transfer",
   "linkage": "input",
   "lfn": "RSF_enzymes.tsv",
   "id": 15,
   "src_urls": [
     { "site_label": "compute", "url": "file:///pscratch/sd/h/hsafri/wf-scratch/hsafri/pegasus/subductcr-end-to-end/run0023/./RSF_enzymes.tsv", "checkpoint": "false" }
   ],
   "dest_urls": [
     { "site_label": "compute", "url": "file://$PWD/RSF_enzymes.tsv" }
   ] }
 ,
 { "type": "transfer",
   "linkage": "input",
   "lfn": "QHS1_enzymes.tsv",
   "id": 16,
   "src_urls": [
     { "site_label": "compute", "url": "file:///pscratch/sd/h/hsafri/wf-scratch/hsafri/pegasus/subductcr-end-to-end/run0023/./QHS1_enzymes.tsv", "checkpoint": "false" }
   ],
   "dest_urls": [
     { "site_label": "compute", "url": "file://$PWD/QHS1_enzymes.tsv" }
   ] }
 ,
 { "type": "transfer",
   "linkage": "input",
   "lfn": "BR1F_enzymes.tsv",
   "id": 17,
   "src_urls": [
     { "site_label": "compute", "url": "file:///pscratch/sd/h/hsafri/wf-scratch/hsafri/pegasus/subductcr-end-to-end/run0023/./BR1F_enzymes.tsv", "checkpoint": "false" }
   ],
   "dest_urls": [
     { "site_label": "compute", "url": "file://$PWD/BR1F_enzymes.tsv" }
   ] }
 ,
 { "type": "transfer",
   "linkage": "input",
   "lfn": "ARS_enzymes.tsv",
   "id": 18,
   "src_urls": [
     { "site_label": "compute", "url": "file:///pscratch/sd/h/hsafri/wf-scratch/hsafri/pegasus/subductcr-end-to-end/run0023/./ARS_enzymes.tsv", "checkpoint": "false" }
   ],
   "dest_urls": [
     { "site_label": "compute", "url": "file://$PWD/ARS_enzymes.tsv" }
   ] }
 ,
 { "type": "transfer",
   "linkage": "input",
   "lfn": "QHS2_enzymes.tsv",
   "id": 19,
   "src_urls": [
     { "site_label": "compute", "url": "file:///pscratch/sd/h/hsafri/wf-scratch/hsafri/pegasus/subductcr-end-to-end/run0023/./QHS2_enzymes.tsv", "checkpoint": "false" }
   ],
   "dest_urls": [
     { "site_label": "compute", "url": "file://$PWD/QHS2_enzymes.tsv" }
   ] }
 ,
 { "type": "transfer",
   "linkage": "input",
   "lfn": "EPS_enzymes.tsv",
   "id": 20,
   "src_urls": [
     { "site_label": "compute", "url": "file:///pscratch/sd/h/hsafri/wf-scratch/hsafri/pegasus/subductcr-end-to-end/run0023/./EPS_enzymes.tsv", "checkpoint": "false" }
   ],
   "dest_urls": [
     { "site_label": "compute", "url": "file://$PWD/EPS_enzymes.tsv" }
   ] }
 ,
 { "type": "transfer",
   "linkage": "input",
   "lfn": "BQF_enzymes.tsv",
   "id": 21,
   "src_urls": [
     { "site_label": "compute", "url": "file:///pscratch/sd/h/hsafri/wf-scratch/hsafri/pegasus/subductcr-end-to-end/run0023/./BQF_enzymes.tsv", "checkpoint": "false" }
   ],
   "dest_urls": [
     { "site_label": "compute", "url": "file://$PWD/BQF_enzymes.tsv" }
   ] }
 ,
 { "type": "transfer",
   "linkage": "input",
   "lfn": "ESF9_enzymes.tsv",
   "id": 22,
   "src_urls": [
     { "site_label": "compute", "url": "file:///pscratch/sd/h/hsafri/wf-scratch/hsafri/pegasus/subductcr-end-to-end/run0023/./ESF9_enzymes.tsv", "checkpoint": "false" }
   ],
   "dest_urls": [
     { "site_label": "compute", "url": "file://$PWD/ESF9_enzymes.tsv" }
   ] }
 ,
 { "type": "transfer",
   "linkage": "input",
   "lfn": "SLF_enzymes.tsv",
   "id": 23,
   "src_urls": [
     { "site_label": "compute", "url": "file:///pscratch/sd/h/hsafri/wf-scratch/hsafri/pegasus/subductcr-end-to-end/run0023/./SLF_enzymes.tsv", "checkpoint": "false" }
   ],
   "dest_urls": [
     { "site_label": "compute", "url": "file://$PWD/SLF_enzymes.tsv" }
   ] }
 ,
 { "type": "transfer",
   "linkage": "input",
   "lfn": "TCS_enzymes.tsv",
   "id": 24,
   "src_urls": [
     { "site_label": "compute", "url": "file:///pscratch/sd/h/hsafri/wf-scratch/hsafri/pegasus/subductcr-end-to-end/run0023/./TCS_enzymes.tsv", "checkpoint": "false" }
   ],
   "dest_urls": [
     { "site_label": "compute", "url": "file://$PWD/TCS_enzymes.tsv" }
   ] }
 ,
 { "type": "transfer",
   "linkage": "input",
   "lfn": "SIS_enzymes.tsv",
   "id": 25,
   "src_urls": [
     { "site_label": "compute", "url": "file:///pscratch/sd/h/hsafri/wf-scratch/hsafri/pegasus/subductcr-end-to-end/run0023/./SIS_enzymes.tsv", "checkpoint": "false" }
   ],
   "dest_urls": [
     { "site_label": "compute", "url": "file://$PWD/SIS_enzymes.tsv" }
   ] }
 ,
 { "type": "transfer",
   "linkage": "input",
   "lfn": "QNS_enzymes.tsv",
   "id": 26,
   "src_urls": [
     { "site_label": "compute", "url": "file:///pscratch/sd/h/hsafri/wf-scratch/hsafri/pegasus/subductcr-end-to-end/run0023/./QNS_enzymes.tsv", "checkpoint": "false" }
   ],
   "dest_urls": [
     { "site_label": "compute", "url": "file://$PWD/QNS_enzymes.tsv" }
   ] }
 ,
 { "type": "transfer",
   "linkage": "input",
   "lfn": "CYS_enzymes.tsv",
   "id": 27,
   "src_urls": [
     { "site_label": "compute", "url": "file:///pscratch/sd/h/hsafri/wf-scratch/hsafri/pegasus/subductcr-end-to-end/run0023/./CYS_enzymes.tsv", "checkpoint": "false" }
   ],
   "dest_urls": [
     { "site_label": "compute", "url": "file://$PWD/CYS_enzymes.tsv" }
   ] }
 ,
 { "type": "transfer",
   "linkage": "input",
   "lfn": "SLS_enzymes.tsv",
   "id": 28,
   "src_urls": [
     { "site_label": "compute", "url": "file:///pscratch/sd/h/hsafri/wf-scratch/hsafri/pegasus/subductcr-end-to-end/run0023/./SLS_enzymes.tsv", "checkpoint": "false" }
   ],
   "dest_urls": [
     { "site_label": "compute", "url": "file://$PWD/SLS_enzymes.tsv" }
   ] }
 ,
 { "type": "transfer",
   "linkage": "input",
   "lfn": "RVF_enzymes.tsv",
   "id": 29,
   "src_urls": [
     { "site_label": "compute", "url": "file:///pscratch/sd/h/hsafri/wf-scratch/hsafri/pegasus/subductcr-end-to-end/run0023/./RVF_enzymes.tsv", "checkpoint": "false" }
   ],
   "dest_urls": [
     { "site_label": "compute", "url": "file://$PWD/RVF_enzymes.tsv" }
   ] }
 ,
 { "type": "transfer",
   "linkage": "input",
   "lfn": "BRF2_enzymes.tsv",
   "id": 30,
   "src_urls": [
     { "site_label": "compute", "url": "file:///pscratch/sd/h/hsafri/wf-scratch/hsafri/pegasus/subductcr-end-to-end/run0023/./BRF2_enzymes.tsv", "checkpoint": "false" }
   ],
   "dest_urls": [
     { "site_label": "compute", "url": "file://$PWD/BRF2_enzymes.tsv" }
   ] }
 ,
 { "type": "transfer",
   "linkage": "input",
   "lfn": "RSS_enzymes.tsv",
   "id": 31,
   "src_urls": [
     { "site_label": "compute", "url": "file:///pscratch/sd/h/hsafri/wf-scratch/hsafri/pegasus/subductcr-end-to-end/run0023/./RSS_enzymes.tsv", "checkpoint": "false" }
   ],
   "dest_urls": [
     { "site_label": "compute", "url": "file://$PWD/RSS_enzymes.tsv" }
   ] }
 ,
 { "type": "transfer",
   "linkage": "input",
   "lfn": "PGF_enzymes.tsv",
   "id": 32,
   "src_urls": [
     { "site_label": "compute", "url": "file:///pscratch/sd/h/hsafri/wf-scratch/hsafri/pegasus/subductcr-end-to-end/run0023/./PGF_enzymes.tsv", "checkpoint": "false" }
   ],
   "dest_urls": [
     { "site_label": "compute", "url": "file://$PWD/PGF_enzymes.tsv" }
   ] }
 ,
 { "type": "transfer",
   "linkage": "input",
   "lfn": "PBS_enzymes.tsv",
   "id": 33,
   "src_urls": [
     { "site_label": "compute", "url": "file:///pscratch/sd/h/hsafri/wf-scratch/hsafri/pegasus/subductcr-end-to-end/run0023/./PBS_enzymes.tsv", "checkpoint": "false" }
   ],
   "dest_urls": [
     { "site_label": "compute", "url": "file://$PWD/PBS_enzymes.tsv" }
   ] }
 ,
 { "type": "transfer",
   "linkage": "input",
   "lfn": "MTF_enzymes.tsv",
   "id": 34,
   "src_urls": [
     { "site_label": "compute", "url": "file:///pscratch/sd/h/hsafri/wf-scratch/hsafri/pegasus/subductcr-end-to-end/run0023/./MTF_enzymes.tsv", "checkpoint": "false" }
   ],
   "dest_urls": [
     { "site_label": "compute", "url": "file://$PWD/MTF_enzymes.tsv" }
   ] }
 ,
 { "type": "transfer",
   "linkage": "input",
   "lfn": "PFF_enzymes.tsv",
   "id": 35,
   "src_urls": [
     { "site_label": "compute", "url": "file:///pscratch/sd/h/hsafri/wf-scratch/hsafri/pegasus/subductcr-end-to-end/run0023/./PFF_enzymes.tsv", "checkpoint": "false" }
   ],
   "dest_urls": [
     { "site_label": "compute", "url": "file://$PWD/PFF_enzymes.tsv" }
   ] }
 ,
 { "type": "transfer",
   "linkage": "input",
   "lfn": "TCF_enzymes.tsv",
   "id": 36,
   "src_urls": [
     { "site_label": "compute", "url": "file:///pscratch/sd/h/hsafri/wf-scratch/hsafri/pegasus/subductcr-end-to-end/run0023/./TCF_enzymes.tsv", "checkpoint": "false" }
   ],
   "dest_urls": [
     { "site_label": "compute", "url": "file://$PWD/TCF_enzymes.tsv" }
   ] }
 ,
 { "type": "transfer",
   "linkage": "input",
   "lfn": "gene_merge",
   "id": 37,
   "src_urls": [
     { "site_label": "compute", "url": "file:///pscratch/sd/h/hsafri/wf-scratch/hsafri/pegasus/subductcr-end-to-end/run0023/./gene_merge", "checkpoint": "false" }
   ],
   "dest_urls": [
     { "site_label": "compute", "url": "file://$PWD/gene_merge" }
   ] }
]
eof

printf "\n##################### Setting the xbit for executables staged #####################\n"  1>&2
# set the xbit for any executables staged
/bin/chmod +x gene_merge

printf "\n##################### Checking file integrity for input files #####################\n"  1>&2
# do file integrity checks
pegasus-integrity --print-timings --verify=stdin 1>&2 << 'eof'
PGS_enzymes.tsv;;;FAS_enzymes.tsv;;;BRS2_enzymes.tsv;;;BQS_enzymes.tsv;;;PLS_enzymes.tsv;;;CYF_enzymes.tsv;;;QNF_enzymes.tsv;;;QH2F_enzymes.tsv;;;PFS_enzymes.tsv;;;BRS1_enzymes.tsv;;;ec_carbon.csv;;;EPF_enzymes.tsv;;;SIF_enzymes.tsv;;;ETS_enzymes.tsv;;;RSF_enzymes.tsv;;;QHS1_enzymes.tsv;;;BR1F_enzymes.tsv;;;ARS_enzymes.tsv;;;QHS2_enzymes.tsv;;;EPS_enzymes.tsv;;;BQF_enzymes.tsv;;;ESF9_enzymes.tsv;;;SLF_enzymes.tsv;;;TCS_enzymes.tsv;;;SIS_enzymes.tsv;;;QNS_enzymes.tsv;;;CYS_enzymes.tsv;;;SLS_enzymes.tsv;;;RVF_enzymes.tsv;;;BRF2_enzymes.tsv;;;RSS_enzymes.tsv;;;PGF_enzymes.tsv;;;PBS_enzymes.tsv;;;MTF_enzymes.tsv;;;PFF_enzymes.tsv;;;TCF_enzymes.tsv;;;gene_merge
eof

pegasus_lite_section_end stage_in

printf "\n########[Pegasus Lite] Writing out script to launch user task in container ########\n"  1>&2

cat <<EOF > gene_merge_gene_merge-cont.sh
#!/bin/bash
set -e

# setting environment variables for job
EOF
container_env /scratch >> gene_merge_gene_merge-cont.sh
cat <<EOF2 >> gene_merge_gene_merge-cont.sh
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

pegasus-kickstart  -n gene_merge -N gene_merge -R compute  -S @$_CONDOR_SCRATCH_DIR/gene_merge_gene_merge.in.lof  -s @$_CONDOR_SCRATCH_DIR/gene_merge_gene_merge.out.lof  -L subductcr-end-to-end -T 2026-09-11T05:13:20+00:00 -k 42900 -K 150 ./gene_merge ec_carbon.csv gene_table.tsv ARS_enzymes.tsv BQF_enzymes.tsv BQS_enzymes.tsv BR1F_enzymes.tsv BRF2_enzymes.tsv BRS1_enzymes.tsv BRS2_enzymes.tsv CYF_enzymes.tsv CYS_enzymes.tsv EPF_enzymes.tsv EPS_enzymes.tsv ESF9_enzymes.tsv ETS_enzymes.tsv FAS_enzymes.tsv MTF_enzymes.tsv PBS_enzymes.tsv PFF_enzymes.tsv PFS_enzymes.tsv PGF_enzymes.tsv PGS_enzymes.tsv PLS_enzymes.tsv QH2F_enzymes.tsv QHS1_enzymes.tsv QHS2_enzymes.tsv QNF_enzymes.tsv QNS_enzymes.tsv RSF_enzymes.tsv RSS_enzymes.tsv RVF_enzymes.tsv SIF_enzymes.tsv SIS_enzymes.tsv SLF_enzymes.tsv SLS_enzymes.tsv TCF_enzymes.tsv TCS_enzymes.tsv
EOF2


chmod +x gene_merge_gene_merge-cont.sh
if ! [ $pegasus_lite_start_dir -ef . ]; then
	cp $pegasus_lite_start_dir/pegasus-lite-common.sh . 
fi

set +e
job_ec=0
shifter_init swarmourr/subductcr-r:latest
job_ec=$(($job_ec + $?))

shifter --image swarmourr/subductcr-r:latest --volume $PWD:/scratch --workdir=/scratch ./gene_merge_gene_merge-cont.sh 
job_ec=$(($job_ec + $?))

pegasus_lite_section_start stage_out
printf "\n############################ Staging out output files ############################\n"  1>&2
# stage out
pegasus-transfer --threads 1  1>&2 << 'eof'
[
 { "type": "transfer",
   "linkage": "output",
   "lfn": "gene_table.tsv",
   "id": 1,
   "src_urls": [
     { "site_label": "compute", "url": "file://$PWD/gene_table.tsv", "checkpoint": "false" }
   ],
   "dest_urls": [
     { "site_label": "compute", "url": "file:///pscratch/sd/h/hsafri/wf-scratch/hsafri/pegasus/subductcr-end-to-end/run0023/./gene_table.tsv" }
   ] }
]
eof

pegasus_lite_section_end stage_out

set -e


# clear the trap, and exit cleanly
trap - EXIT
pegasus_lite_final_exit

