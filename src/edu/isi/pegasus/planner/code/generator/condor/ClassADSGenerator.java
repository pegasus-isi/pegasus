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
package edu.isi.pegasus.planner.code.generator.condor;

import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.AggregatedJob;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.namespace.Pegasus;
import java.io.PrintWriter;

/**
 * A helper class, that generates Pegasus specific classads for the jobs.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class ClassADSGenerator {

    /** The name of the generator. */
    public static final String GENERATOR = "Pegasus";

    /** The complete classad designating Pegasus as the generator. */
    public static final String GENERATOR_AD_KEY = "pegasus_generator";

    /**
     * The class ad key for the version id.
     *
     * @see org.griphyn.cPlanner.classes.DagInfo#releaseVersion
     */
    public static final String VERSION_AD_KEY = "pegasus_version";

    /** The classad key for the pegasus build. */
    public static final String BUILD_AD_KEY = "pegasus_build";

    /** The classad for the root workflow uuid */
    public static final String ROOT_WF_UUID_KEY = "pegasus_root_wf_uuid";

    /** The classad for the workflow uuid */
    public static final String WF_UUID_KEY = "pegasus_wf_uuid";

    /**
     * The classad for the flow id.
     *
     * @see org.griphyn.cPlanner.classes.DagInfo#flowIDName
     */
    public static final String WF_NAME_AD_KEY = "pegasus_wf_name";

    /** The classad for generating the workflow app key */
    public static final String WF_APP_KEY = "pegasus_wf_app";

    /**
     * The classad for the timestamp.
     *
     * @see org.griphyn.cPlanner.classes.DagInfo#mFlowTimestamp
     */
    public static final String WF_TIME_AD_KEY = "pegasus_wf_time";

    /** The classad for the complete transformation name. */
    public static final String XFORMATION_AD_KEY = "pegasus_wf_xformation";

    /** The classad for generating the DAX ID */
    public static final String DAX_JOB_ID_KEY = "pegasus_wf_dax_job_id";

    /**
     * The class ad for job Class.
     *
     * @see org.griphyn.cPlanner.classes.Job#jobClass
     */
    public static final String JOB_CLASS_AD_KEY = "pegasus_job_class";

    /**
     * The class ad for the jobId.
     *
     * @see org.griphyn.cPlanner.classes.Job#jobID
     */
    public static final String DAG_JOB_ID_KEY = "pegasus_wf_dag_job_id";

    /** The class ad for the expected job value */
    public static final String JOB_RUNTIME_AD_KEY = "pegasus_job_runtime";

    /** The key for the number of cores for the multiplier factor in stampede. */
    public static final String CORES_KEY = "pegasus_cores";

    /**
     * The class ad to store the execution pool at which the job is run. The globusscheduler
     * specified in the submit file refers to the jobmanager on this execution pool.
     */
    public static final String RESOURCE_AD_KEY = "pegasus_site";

    public static final String PLUS_RESOURCE_AD_KEY = "+" + RESOURCE_AD_KEY;

    /** The class ad to designate the size of the clustered jobs. */
    public static final String JOB_CLUSTER_SIZE_AD_KEY = "pegasus_cluster_size";

    /**
     * Writes out the classads for a workflow to corresponding writer stream.
     *
     * @param writer is an open stream for the Condor submit file.
     * @param dag the workflow object containing metadata about the workflow like the workflow id
     *     and the release version.
     * @param name the name of the app
     */
    public static void generate(PrintWriter writer, ADag dag, String appName) {
        // get hold of the object holding the metadata
        // information about the workflow
        // pegasus is the generator
        writer.println(generateClassAdAttribute(GENERATOR_AD_KEY, GENERATOR));

        // the root workflow and workflow uuid
        writer.println(generateClassAdAttribute(ROOT_WF_UUID_KEY, dag.getRootWorkflowUUID()));
        writer.println(generateClassAdAttribute(WF_UUID_KEY, dag.getWorkflowUUID()));

        // the vds version
        writer.println(generateClassAdAttribute(VERSION_AD_KEY, dag.getReleaseVersion()));

        // the workflow name
        writer.println(generateClassAdAttribute(WF_NAME_AD_KEY, dag.getFlowName()));

        // PM-1277 associate app from properties
        if (appName != null) {
            writer.println(generateClassAdAttribute(WF_APP_KEY, appName));
        }

        // the workflow time
        if (dag.getMTime() != null) {
            writer.println(generateClassAdAttribute(WF_TIME_AD_KEY, dag.getFlowTimestamp()));
        }
    }

    /**
     * Writes out the classads for a job to corresponding writer stream. The writer stream points to
     * a Condor Submit file for the job.
     *
     * @param writer is an open stream for the Condor submit file.
     * @param dag the workflow object containing metadata about the workflow like the workflow id
     *     and the release version.
     * @param job the <code>Job</code> object for which the writer stream is passed.
     * @paran appName the app name
     */
    public static void generate(PrintWriter writer, ADag dag, Job job, String appName) {

        // get all the workflow classads
        generate(writer, dag, appName);

        // get the job classads

        // the tranformation name
        writer.println(
                generateClassAdAttribute(
                        ClassADSGenerator.XFORMATION_AD_KEY, job.getCompleteTCName()));

        // put in the DAX
        writer.println(generateClassAdAttribute(DAX_JOB_ID_KEY, job.getDAXID()));

        // the supernode id
        writer.println(generateClassAdAttribute(ClassADSGenerator.DAG_JOB_ID_KEY, job.getID()));

        // the class of the job
        writer.println(
                generateClassAdAttribute(ClassADSGenerator.JOB_CLASS_AD_KEY, job.getJobType()));

        // the resource on which the job is scheduled
        // PM-796 only generate the resource ad key
        // if job is not previously associated with it
        String plusResourceKey = ClassADSGenerator.PLUS_RESOURCE_AD_KEY;
        if (job.condorVariables.containsKey(plusResourceKey)) {
            // pick the one pre populated
            writer.println(
                    generateClassAdAttribute(
                            ClassADSGenerator.RESOURCE_AD_KEY,
                            (String) job.condorVariables.removeKey(plusResourceKey)));
        } else {
            // generate the default one
            writer.println(
                    generateClassAdAttribute(
                            ClassADSGenerator.RESOURCE_AD_KEY, job.getSiteHandle()));
        }

        // add the pegasus value if defined.
        String value = (String) job.vdsNS.getStringValue(Pegasus.RUNTIME_KEY);
        // else see if globus maxwalltime defined
        value = (value == null) ? (String) job.globusRSL.get("maxwalltime") : value;
        int runtime = 0;
        try {
            runtime = (value == null) ? 0 : Integer.parseInt(value);
        } catch (Exception e) {
            // ignore
        }
        writer.println(generateClassAdAttribute(ClassADSGenerator.JOB_RUNTIME_AD_KEY, runtime));

        // write out the cores if specified for job
        String coresvalue = job.vdsNS.getStringValue(Pegasus.CORES_KEY);
        int cores = 1;
        try {
            cores = (coresvalue == null) ? 1 : Integer.parseInt(coresvalue);
        } catch (Exception e) {
            // ignore
        }
        writer.println(generateClassAdAttribute(ClassADSGenerator.CORES_KEY, cores));

        // determine the cluster size
        int csize =
                (job instanceof AggregatedJob)
                        ? ((AggregatedJob) job).numberOfConsitutentJobs()
                        : 1;
        writer.println(generateClassAdAttribute(ClassADSGenerator.JOB_CLUSTER_SIZE_AD_KEY, csize));
    }

    /**
     * Generates a classad attribute given the name and the value.
     *
     * @param name the attribute name.
     * @param value the value/expression making the classad attribute.
     * @return the classad attriubute.
     */
    private static String generateClassAdAttribute(String name, String value) {
        return generateClassAdAttribute(name, value, false);
    }

    /**
     * Generates a classad attribute given the name and the value. It by default adds a new line
     * character at start of each attribute.
     *
     * @param name the attribute name.
     * @param value the value/expression making the classad attribute.
     * @return the classad attriubute.
     */
    private static String generateClassAdAttribute(String name, int value) {
        StringBuffer sb = new StringBuffer(10);

        sb.append("+");
        sb.append(name).append(" = ");
        sb.append(value);

        return sb.toString();
    }

    /**
     * Generates a classad attribute given the name and the value.
     *
     * @param name the attribute name.
     * @param value the value/expression making the classad attribute.
     * @param newLine boolean denoting whether to add a new line character at start or not.
     * @return the classad attriubute.
     */
    private static String generateClassAdAttribute(String name, String value, boolean newLine) {

        StringBuffer sb = new StringBuffer(10);
        if (newLine) sb.append("\n");

        sb.append("+");
        sb.append(name).append(" = ");
        sb.append("\"");
        sb.append(value);
        sb.append("\"");

        return sb.toString();
    }
}
