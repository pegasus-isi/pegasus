/**
 * Copyright 2007-2008 University Of Southern California
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.isi.pegasus.planner.code.generator;

import edu.isi.pegasus.common.logging.LogFormatter;
import edu.isi.pegasus.common.logging.LogFormatterFactory;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LoggingKeys;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.AggregatedJob;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.partitioner.graph.GraphNode;
import java.io.IOException;
import java.io.Writer;
import java.util.Iterator;

/**
 * This class can write out the job mappings that link jobs with jobs in the DAX to a Writer stream
 * in the netlogger format.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class NetloggerJobMapper {

    public static final String NETLOGGER_LOG_FORMATTER_IMPLEMENTOR = "Netlogger";

    /** The handle to the netlogger log formatter. */
    private LogFormatter mLogFormatter;

    /** The handle to pegasus logger used for run. */
    private LogManager mLogger;

    /**
     * The default constructor.
     *
     * @param logger the logger instance to use for logging
     */
    public NetloggerJobMapper(LogManager logger) {
        mLogFormatter = LogFormatterFactory.loadInstance(NETLOGGER_LOG_FORMATTER_IMPLEMENTOR);
        mLogger = logger;
    }

    /**
     * Writes out the job mappings for a workflow.
     *
     * @param writer the writer stream to which to write out the mappings
     * @param dag the dag for which to write out the mappings
     * @throws IOException
     */
    public void writeOutMappings(Writer writer, ADag dag) throws IOException {

        for (Iterator<GraphNode> it = dag.jobIterator(); it.hasNext(); ) {
            GraphNode node = it.next();
            Job job = (Job) node.getContent();
            int type = job.getJobType();
            mLogFormatter.addEvent("pegasus.job", LoggingKeys.JOB_ID, job.getID());
            mLogFormatter.add("job.class", Integer.toString(type));
            mLogFormatter.add("job.xform", job.getCompleteTCName());
            // determine count of jobs
            int taskCount = getTaskCount(job);

            mLogFormatter.add("task.count", Integer.toString(taskCount));
            writer.write(mLogFormatter.createLogMessage());
            writer.write("\n");
            mLogFormatter.popEvent();

            // add mapping events only if task count > 0
            if (taskCount > 0) {
                if (job instanceof AggregatedJob) {
                    AggregatedJob j = (AggregatedJob) job;
                    for (Iterator<Job> jit = j.constituentJobsIterator(); jit.hasNext(); ) {
                        Job cJob = jit.next();

                        mLogFormatter.addEvent("pegasus.job.map", LoggingKeys.JOB_ID, job.getID());
                        writer.write(generateLogEvent(cJob, "task."));
                        writer.write("\n");
                        mLogFormatter.popEvent();
                    }
                } else {
                    mLogFormatter.addEvent("pegasus.job.map", LoggingKeys.JOB_ID, job.getID());
                    writer.write(generateLogEvent(job, "task."));
                    writer.write("\n");
                    mLogFormatter.popEvent();
                }
            }
        }
    }

    /**
     * Generates a log event message in the netlogger format for a job
     *
     * @param job the job
     * @param prefix prefix if any to add to the keys
     * @return netlogger formatted message
     */
    private String generateLogEvent(Job job, String prefix) {
        String result = null;
        /*String taskID = (( job.getJobType() == Job.COMPUTE_JOB ||
                             job.getJobType() == Job.STAGED_COMPUTE_JOB ) &&
                               !(job instanceof AggregatedJob) )?
                       job.getLogicalID():
                       "";
        */
        mLogFormatter.add("task.id", job.getLogicalID());
        mLogFormatter.add(getKey(prefix, "class"), Integer.toString(job.getJobType()));
        mLogFormatter.add(getKey(prefix, "xform"), job.getCompleteTCName());
        result = mLogFormatter.createLogMessage();
        return result;
    }

    /**
     * Adds a prefix to the key and returns it.
     *
     * @param prefix the prefix to be added
     * @param key the key
     * @return the key with prefix added.
     */
    private String getKey(String prefix, String key) {
        if (prefix == null || prefix.length() == 0) {
            return key;
        }
        StringBuffer result = new StringBuffer();
        result.append(prefix).append(key);
        return result.toString();
    }

    /**
     * Returns the task count for a job. The task count is the number of jobs associated with the
     * job in the DAX
     *
     * @param job
     * @return task count
     */
    private int getTaskCount(Job job) {
        int count = 0;
        int type = job.getJobType();
        // explicitly exclude cleanup jobs that are instance
        // of aggregated jobs. This is because while creating
        // the cleanup job we use the clone method. To be fixed.
        // Karan April 17 2009
        if (job instanceof AggregatedJob && type != Job.CLEANUP_JOB) {
            // a clustered job the number of constituent is count
            count = ((AggregatedJob) job).numberOfConsitutentJobs();
        } else if (type == Job.COMPUTE_JOB /*|| type == Job.STAGED_COMPUTE_JOB*/) {
            // non clustered job check whether compute or not
            // and make sure there is dax job associated with it
            if (job.getLogicalID().length() == 0) {
                // takes care of the untar job that is tagged as compute
                mLogger.log(
                        "Not creating event pegasus.job.map for job " + job.getID(),
                        LogManager.DEBUG_MESSAGE_LEVEL);
                count = 0;
            } else {
                count = 1;
            }
        }
        return count;
    }
}
