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

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.AggregatedJob;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.Notifications;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.code.CodeGenerator;
import edu.isi.pegasus.planner.code.CodeGeneratorException;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.dax.Invoke;
import edu.isi.pegasus.planner.dax.Invoke.WHEN;
import edu.isi.pegasus.planner.partitioner.graph.GraphNode;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * A MonitordNotify Input File Generator that generates the input file required for
 * pegasus-monitord.
 *
 * @author Rajiv Mayani
 * @version $Revision$
 */
public class MonitordNotify implements CodeGenerator {

    /** The suffix to use while constructing the name of the metrics file */
    public static final String NOTIFICATIONS_FILE_SUFFIX = ".notify";

    /** The constant string to write for work flow notifications. */
    public static final String WORKFLOW = "WORKFLOW";

    /** The constant string to write for job notifications. */
    public static final String JOB = "JOB";

    /** The constant string to write for invocation notifications. */
    public static final String INVOCATION = "INVOCATION";

    /** The constant string to write for dag job notifications. */
    public static final String DAG_JOB = "DAGJOB";

    /** The constant string to write for dax job notifications. */
    public static final String DAX_JOB = "DAXJOB";

    /** The delimiter with which to separate different fields in the notifications file. */
    public static final String DELIMITER = " ";

    /** The bag of initialization objects. */
    protected PegasusBag mBag;

    /** The directory where all the submit files are to be generated. */
    protected String mSubmitFileDir;

    /** The object holding all the properties pertaining to Pegasus. */
    protected PegasusProperties mProps;

    /** The object containing the command line options specified to the planner at runtime. */
    protected PlannerOptions mPOptions;

    /** The handle to the logging object. */
    protected LogManager mLogger;

    /** The handle to the PrintWriter that writes out the notifications file */
    private PrintWriter mNotificationsWriter;

    /**
     * Initializes the Code Generator implementation.
     *
     * @param bag the bag of initialization objects.
     * @throws CodeGeneratorException in case of any error occurring code generation.
     */
    public void initialize(PegasusBag bag) throws CodeGeneratorException {
        mNotificationsWriter = null;
        mBag = bag;
        mProps = bag.getPegasusProperties();
        mPOptions = bag.getPlannerOptions();
        mSubmitFileDir = mPOptions.getSubmitDirectory();
        mLogger = bag.getLogger();
    }

    /**
     * Generates the notifications input file. The method initially generates work-flow level
     * notification records, followed by job-level notification records.
     *
     * @param dag the concrete work-flow.
     * @return the Collection of <code>File</code> objects for the files written out.
     * @throws CodeGeneratorException in case of any error occurring code generation.
     */
    public Collection<File> generateCode(ADag dag) throws CodeGeneratorException {

        File f =
                new File(
                        mSubmitFileDir,
                        Abstract.getDAGFilename(
                                this.mPOptions,
                                dag.getLabel(),
                                dag.getIndex(),
                                MonitordNotify.NOTIFICATIONS_FILE_SUFFIX));

        try {
            mNotificationsWriter = new PrintWriter(new BufferedWriter(new FileWriter(f, true)));
        } catch (IOException ioe) {
            mLogger.log(
                    "Unable to intialize writer for notifications file ",
                    ioe,
                    LogManager.ERROR_MESSAGE_LEVEL);
            throw new CodeGeneratorException(
                    "Unable to intialize writer for notifications file ", ioe);
        }

        // lets first generate code for the workflow level
        // notifications
        String uuid = dag.getWorkflowUUID();
        Notifications notfications = dag.getNotifications();
        for (WHEN when : WHEN.values()) {
            for (Invoke invoke : notfications.getNotifications(when)) {
                mNotificationsWriter.println(
                        MonitordNotify.WORKFLOW
                                + DELIMITER
                                + uuid
                                + DELIMITER
                                + when.toString()
                                + DELIMITER
                                + invoke.getWhat());
            }
        }

        // walk through the workflow and generate code for
        // job notifications if specified
        for (Iterator<GraphNode> it = dag.jobIterator(); it.hasNext(); ) {
            GraphNode node = it.next();
            Job job = (Job) node.getContent();
            this.generateCode(dag, job);
        }

        mNotificationsWriter.close();

        Collection<File> result = new LinkedList<File>();
        result.add(f);
        return result;
    }

    /**
     * Not implemented
     *
     * @param dag the work-flow
     * @param job the job for which the code is to be generated.
     * @throws edu.isi.pegasus.planner.code.CodeGeneratorException
     */
    public void generateCode(ADag dag, Job job) throws CodeGeneratorException {
        String sType = null;
        String sJobId = job.getID();

        switch (job.getJobType()) {
            case Job.DAG_JOB:
                sType = MonitordNotify.DAG_JOB;
                break;

            case Job.DAX_JOB:
                sType = MonitordNotify.DAX_JOB;
                break;

            default:
                sType = MonitordNotify.JOB;
                break;
        }

        // a new line only if there are some notification
        // to print out.
        if (!job.getNotifications().isEmpty()) {
            mNotificationsWriter.println();
        }

        for (WHEN when : WHEN.values()) {
            for (Invoke invoke : job.getNotifications(when)) {
                mNotificationsWriter.println(
                        sType
                                + DELIMITER
                                + sJobId
                                + DELIMITER
                                + when.toString()
                                + DELIMITER
                                + invoke.getWhat());
            }
        }

        // for clustered jobs we need to list notifications
        // per invocation of clustered job.
        if (job instanceof AggregatedJob) {
            AggregatedJob aggJob = (AggregatedJob) job;
            int invID = 1;
            for (Iterator it = aggJob.constituentJobsIterator(); it.hasNext(); invID++) {
                Job j = (Job) it.next();

                // a new line only if there are some notification
                // to print out.
                if (!j.getNotifications().isEmpty()) {
                    mNotificationsWriter.println();
                }

                for (WHEN when : WHEN.values()) {
                    for (Invoke invoke : j.getNotifications(when)) {
                        StringBuffer sb = new StringBuffer();
                        sb.append(MonitordNotify.INVOCATION)
                                .append(DELIMITER)
                                .append(job.getID())
                                .append(DELIMITER)
                                .append(invID)
                                .append(DELIMITER)
                                .append(when.toString())
                                .append(DELIMITER)
                                .append(invoke.getWhat());

                        mNotificationsWriter.println(sb.toString());
                    }
                }
            }
        }
    }

    /** Not implemented */
    public boolean startMonitoring() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /** Not implemented */
    public void reset() throws CodeGeneratorException {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
