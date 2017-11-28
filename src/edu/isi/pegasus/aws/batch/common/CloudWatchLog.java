/**
 *  Copyright 2007-2017 University Of Southern California
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package edu.isi.pegasus.aws.batch.common;

import edu.isi.pegasus.aws.batch.classes.Tuple;
import edu.isi.pegasus.aws.batch.impl.Synch;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.Priority;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.batch.BatchClient;
import software.amazon.awssdk.services.batch.model.AttemptDetail;
import software.amazon.awssdk.services.batch.model.DescribeJobsRequest;
import software.amazon.awssdk.services.batch.model.DescribeJobsResponse;
import software.amazon.awssdk.services.batch.model.JobDetail;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;

import software.amazon.awssdk.services.cloudwatchlogs.model.GetLogEventsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.GetLogEventsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.OutputLogEvent;
import software.amazon.awssdk.services.cloudwatchlogs.model.DeleteLogStreamRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DeleteLogStreamResponse;

/**
 * A class to retrieve the cloud watch logs
 *
 * @author Karan Vahi
 */
public class CloudWatchLog {

    private Logger mLogger;

    private BatchClient mBatchClient;

    private CloudWatchLogsClient mCWL;

    private String mLogGroup;

    /**
     * The default constructor
     */
    public CloudWatchLog() {

    }

    /**
     * Initialize the log.
     *
     * @param awsRegion the aws region
     * @param logLevel the logging level
     * @param logGroup the cloud watch log group
     */
    public void initialze(Region awsRegion, Level logLevel, String logGroup) {
        //"405596411149";
        mLogger = Logger.getLogger(Synch.class.getName());
        mLogger.setLevel(logLevel);
        mLogGroup = logGroup;
        mBatchClient = BatchClient.builder().region(awsRegion).build();
        mCWL = CloudWatchLogsClient.builder().region(awsRegion).build();
    }

    /**
     * Retrieves a cloud watch log for an AWS Job
     *
     * @param awsJobID the AWS job ID for the job
     *
     * @return the file to which it is retrieved
     */
    public File retrieve(String awsJobID) {
        DescribeJobsRequest jobsRequest = DescribeJobsRequest.builder().
                jobs(awsJobID).
                build();
        DescribeJobsResponse jobsResponse = mBatchClient.describeJobs(jobsRequest);
        for (JobDetail jobDetail : jobsResponse.jobs()) {
            try {
                Tuple<String, String> log = determineLog(jobDetail);
                return this.retrieve(jobDetail.jobName(), log.getKey(), log.getValue());
            } catch (Exception e) {
                mLogger.error("Error while retrieving cloud watch log for job " + awsJobID, e);
            }
        }
        return null;
    }

    /**
     * Retrieves a cloud watch log for an AWS Job
     *
     * @param jobName the job name
     * @param logGroup the cloud watch log group
     * @param streamName the stream name
     *
     * @return the file to which it is retrieved
     */
    public File retrieve(String jobName, String logGroup, String streamName) {
        mLogger.info("Retrieving log for " + jobName + " for log group " + logGroup + " with stream name " + streamName);

        GetLogEventsRequest gle = GetLogEventsRequest.builder().
                logGroupName(logGroup).
                logStreamName(streamName).
                startFromHead​(true).
                build();
        boolean done = false;
        String previousToken = null;
        PrintWriter pw = null;
        File f = null;
        try {
            f = new File(jobName + ".out");
            pw = new PrintWriter(new BufferedWriter(new FileWriter(f)));
            mLogger.debug("Will write out log to " + f.getAbsolutePath());
            while (!done) {
                GetLogEventsResponse response = mCWL.getLogEvents(gle);
                for (OutputLogEvent event : response.events()) {
                    mLogger.debug("Retrieved event " + event.message());
                    pw.println(event.message());
                }
                String nextToken = response.nextForwardToken();

                if (nextToken == null || nextToken.equals(previousToken)) {
                    //not clear if that is the right way to exit with token matching
                    done = true;
                } else {
                    gle = GetLogEventsRequest.builder().
                            logGroupName(logGroup).
                            logStreamName(streamName).
                            startFromHead​(true).
                            nextToken(nextToken).
                            build();
                }
                previousToken = nextToken;
            }
            pw.flush();
        } catch (IOException ex) {
            mLogger.log(Priority.ERROR, ex);
        } finally {
            if (pw != null) {
                pw.close();
            }
        }
        return f;
    }

    /**
     * Returns a tuple and the log group name
     *
     * @param jobDetail
     * @return
     */
    private Tuple<String, String> determineLog(JobDetail jobDetail) {
        //go through the attemps and get last attempt
        AttemptDetail detail = null;
        StringBuilder logStreamName = new StringBuilder();//karan-batch-synch-test-job-definition/default/e6b3eb37-46d3-4aa5-9208-e80eec481550
        mLogger.debug("determining cloud watch log ");
        for (Iterator<AttemptDetail> it = jobDetail.attempts().iterator(); it.hasNext();) {
            detail = it.next();
        }
        if (detail != null) {
            String taskARN = detail.container().taskArn();
            String jobDefinition = jobDetail.jobDefinition();

            mLogger.debug("log group: " + mLogGroup + " job defn: " + jobDefinition + " task arn: " + taskARN);

            String taskARNID = taskARN.substring(taskARN.lastIndexOf("/") + 1);
            String jdBase = jobDefinition.substring(
                    jobDefinition.indexOf(":job-definition/") + ":job-definition/".length(),
                    jobDefinition.lastIndexOf(":"));
            logStreamName.append(jdBase).append("/default/").append(taskARNID);
        }
        mLogger.info("Log Stream name is " + logStreamName);
        return new Tuple(mLogGroup, logStreamName.toString());

    }

}
