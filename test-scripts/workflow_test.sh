#!/bin/bash
#-----------------------------------------------------------------------------
# Scripts that updates the settings on the policy server to match a particular workflow test,
# runs the workflow, and writes the statistics to a CSV file.
#-----------------------------------------------------------------------------
# Time-stamp: <2012-04-23 08:46:40 ward>
#-----------------------------------------------------------------------------
datestamp=`date +%Y%m%d%M%S`
# set some defaults
start=2
degrees=1
workflow="montage-nocluster"
refiner_type="Basic"
clusters=0
run_id=
policy_type="nopolicy"
statistics_file="statistics.csv"
default_streams=4

# This is only for output reporting
# The file size needs to be configured on the cloud
file_size="0M"
total_transfer_time=
total_cleanup_time=
workflow_wall_time=
workflow_cummulative_job_wall_time=
cummulative_job_walltime_from_submit=
transfers_failed=
policy_server_url="http://lonestar.isi.edu:8080/policy"
max_parallel_streams=

# Change these to match your cloud, staging hosts
# and email address
transfer_hosts="vm-103.alamo.futuregrid.org=>obelix.isi.edu"
cluster_hosts="obelix.isi.edu=>obelix.isi.edu"
email_address="smithd@isi.edu"

while [ $# -gt 1 ]; do
  if [ "$1" = "--default-streams" ]; then
    default_streams=$2
  elif [ "$1" = "--max-streams" ]; then
    max_parallel_streams=$2
    policy_type="${2}greedy"
  elif [ "$1" = "--file-size" ]; then
    file_size="$2"
  else
    echo "Invalid argument $1"
    echo ""
    echo "Usage: $0 [args]"
    echo "  where:"
    echo "--default-streams <default streams>: default streams in policy"
    echo "--max-streams <max streams>: default max_streams in policy"
    echo "--file-size <file size>: file size attached"
    exit 1
  fi
  shift 2
done
function status_check {
# Called with the exit status of a command and the text of an error
# message to display if the status indicates an error.    
    if [ $1 -ne 0 ] ; then
        echo -n "Error status $1: "
        echo $2
        exit $1
    fi
}

startdate=`date`
if [ "x$policy_server_url" != "x" ]; then
  # Redirect curl output to keep our log less cluttered.
  curl -i -H "Content-Type: application/json; charset=utf-8" -X PUT -d "$default_streams" \
$policy_server_url/global/third_party_transfer_parallel_default/ &> /dev/null
  status_check $? "Update of Policy Service streams failed! Aborting experiments!"
  echo "Set third_party_transfer_parallel_default=$default_streams"
  curl -i -H "Content-Type: application/json; charset=utf-8" -X PUT -d "$max_parallel_streams" \
  $policy_server_url/global/maxParallelStreams/key/$transfer_hosts/ &> /dev/null
  status_check $? "Update of Policy Service max parallel streams failed! Aborting experiments!"
  echo "Set maxParallelStreams=$max_parallel_streams for $transfer_hosts"
  curl -i -H "Content-Type: application/json; charset=utf-8" -X PUT -d "$max_parallel_streams" \
  $policy_server_url/global/maxParallelStreams/key/$cluster_hosts/ &> /dev/null
  status_check $? "Update of Policy Service max parallel streams failed! Aborting experiments!"
  echo "Set maxParallelStreams=$max_parallel_streams for $cluster_hosts"
else
  echo "No policy server specified - not updating streams"
fi
echo "======================================="
RUN_ID=`/bin/date +'%F_%H%M%S'`
#RUN_DIR=`pwd`/work/$RUN_ID
RUN_DIR=/nfs/ccg4/scratch-6-months-purge/smithd/work/$RUN_ID

echo "Submitting workflow control script $workflow with argument $RUN_DIR"
./$workflow $RUN_DIR
status_check $? "Script $workflow returned non-zero! Aborting experiments!"
echo "======================================="

WATCH_DIR=`grep pegasus-remove $RUN_DIR/pegasus-plan.out | awk '{print $5}'`
status_check $? "Failed to determine WATCH_DIR value! Aborting experiments!"
./check-status $WATCH_DIR
status_check $? "Script check-status returned non-zero! Aborting experiments!"
echo "======================================="

sleep 1m

# Script takes a second argument. Not set so NULL in script.
echo "Running statistics gathering script with argument $WATCH_DIR."
pegasus-statistics -s all $WATCH_DIR
status_check $? "Workflow statistics script returned non-zero! Aborting experiments"

total_transfer_time=`sed -n "/# $WORKFLOW_ID/,/# All/p" $WATCH_DIR/statistics/breakdown.txt | grep "pegasus::transfer" | awk '{print $8}'`
total_cleanup_time=`sed -n "/# $WORKFLOW_ID/,/# All/p" $WATCH_DIR/statistics/breakdown.txt | grep "pegasus::cleanup" | awk '{print $8}'`

workflow_wall_time=`cat $WATCH_DIR/statistics/summary.txt | grep "^Workflow wall time" | awk '{print $10}'`
workflow_cumulative_job_wall_time=`cat $WATCH_DIR/statistics/summary.txt | grep "^Workflow cumulative job wall time" | awk '{print $12}'`
cumulative_job_walltime_from_submit=`cat $WATCH_DIR/statistics/summary.txt | grep "^Cumulative job walltime as seen from submit side" | awk '{print $15}'`

transfers_failed=`sed -n "/# $WORKFLOW_ID/,/# All/p" $WATCH_DIR/statistics/breakdown.txt | grep "pegasus::transfer" | awk '{print $4}'`

echo "total_transfer_time=$total_transfer_time"
echo "total_cleanup_time=$total_cleanup_time"
echo "workflow_wall_time=$workflow_wall_time"
echo "workflow_cumulative_job_wall_time=$workflow_cumulative_job_wall_time"
echo "cumulative_job_walltime_from_submit=$cumulative_job_walltime_from_submit"
echo "transfers_failed=$transfers_failed"

echo "Writing statistics to $statistics_file"
echo "$datestamp,$workflow:$degrees degrees,$refiner_type,$clusters,$RUN_ID,$policy_type,$max_parallel_streams,$default_streams,$file_size,$total_transfer_time,$total_cleanup_time,$workflow_wall_time,$workflow_cumulative_job_wall_time,$cumulative_job_walltime_from_submit,$transfers_failed" >> $statistics_file
echo "======================================="
echo "Saving statistics to jacoby.isi.edu:~/workflow/${datestamp}_${file_size}_${policy_type}_${default_streams}ps..."
scp -r $WATCH_DIR/statistics jacoby.isi.edu:~/workflow/${datestamp}_${file_size}_${policy_type}_${default_streams}ps

if [ $? -ne 0 ]; then
  echo "Workflow statistics failed to copy to jacoby!"
fi

echo "Email confirmation to ${email_address}..."
email_subject="Test complete: workflow=$worklow policy_type=$policy_type max_parallel_streams=$max_parallel_streams default_streams=$default_streams file_size=$file_size"
mutt -s "${email_subject}" -- ${email_address} < /dev/null

echo "Cleaning up workflow $WATCH_DIR..."
rm -rf $WATCH_DIR
if [ $? -ne 0 ]; then
echo "Failed to cleanup workflow dir $WATCH_DIR"
fi

echo "======================================="
echo "Stop time is" `date`
echo "End of Performance Test Sequence." 
echo "======================================="
