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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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
     * @see edu.isi.pegasus.planner.classes.DagInfo#releaseVersion
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
     * @see edu.isi.pegasus.planner.classes.DagInfo#flowIDName
     */
    public static final String WF_NAME_AD_KEY = "pegasus_wf_name";

    /** The classad for generating the workflow app key */
    public static final String WF_APP_KEY = "pegasus_wf_app";

    /**
     * The classad for the timestamp.
     *
     * @see edu.isi.pegasus.planner.classes.DagInfo#mFlowTimestamp
     */
    public static final String WF_TIME_AD_KEY = "pegasus_wf_time";

    /** The classad for the complete transformation name. */
    public static final String XFORMATION_AD_KEY = "pegasus_wf_xformation";

    /** The classad for generating the DAX ID */
    public static final String DAX_JOB_ID_KEY = "pegasus_wf_dax_job_id";

    /**
     * The class ad for job Class.
     *
     * @see edu.isi.pegasus.planner.classes.Job#jobClass
     */
    public static final String JOB_CLASS_AD_KEY = "pegasus_job_class";

    /**
     * The class ad for the jobId.
     *
     * @see edu.isi.pegasus.planner.classes.Job#jobID
     */
    public static final String DAG_JOB_ID_KEY = "pegasus_wf_dag_job_id";

    private static HashMap<String, String> mPegasusClassAdsToPegasusProfiles;

    /**
     * Maps ClassAD keys to corresponding Pegasus Profile Keys
     *
     * @return
     */
    public static Map<String, String> pegasusClassAdKeysToPegasusProfiles() {
        if (mPegasusClassAdsToPegasusProfiles == null) {
            mPegasusClassAdsToPegasusProfiles = new HashMap();
            mPegasusClassAdsToPegasusProfiles.put(ClassADSGenerator.MEMORY_KEY, Pegasus.MEMORY_KEY);
            mPegasusClassAdsToPegasusProfiles.put(ClassADSGenerator.CORES_KEY, Pegasus.CORES_KEY);
            mPegasusClassAdsToPegasusProfiles.put(ClassADSGenerator.GPUS_KEY, Pegasus.GPUS_KEY);
            mPegasusClassAdsToPegasusProfiles.put(
                    ClassADSGenerator.DISKSPACE_KEY, Pegasus.DISKSPACE_KEY);
            mPegasusClassAdsToPegasusProfiles.put(
                    ClassADSGenerator.JOB_RUNTIME_AD_KEY, Pegasus.RUNTIME_KEY);
            mPegasusProfilesToPegasusClassAdKeys.put(
                    ClassADSGenerator.PROJECT_KEY, Pegasus.PROJECT_KEY);
            mPegasusProfilesToPegasusClassAdKeys.put(
                    ClassADSGenerator.QUEUE_KEY, Pegasus.QUEUE_KEY);
            mPegasusProfilesToPegasusClassAdKeys.put(
                    ClassADSGenerator.GLITE_ARGUMENTS_KEY, Pegasus.GLITE_ARGUMENTS_KEY);
        }
        return mPegasusClassAdsToPegasusProfiles;
    }

    private static HashMap<String, String> mPegasusProfilesToPegasusClassAdKeys;

    /**
     * Maps ClassAD keys to corresponding Pegasus Profile Keys
     *
     * @return
     */
    public static Map<String, String> pegasusProfilesToPegasusClassAdKeys() {
        if (mPegasusProfilesToPegasusClassAdKeys == null) {
            mPegasusProfilesToPegasusClassAdKeys = new HashMap();
            mPegasusProfilesToPegasusClassAdKeys.put(
                    Pegasus.MEMORY_KEY, ClassADSGenerator.MEMORY_KEY);
            mPegasusProfilesToPegasusClassAdKeys.put(
                    Pegasus.CORES_KEY, ClassADSGenerator.CORES_KEY);
            mPegasusProfilesToPegasusClassAdKeys.put(Pegasus.GPUS_KEY, ClassADSGenerator.GPUS_KEY);
            mPegasusProfilesToPegasusClassAdKeys.put(
                    Pegasus.DISKSPACE_KEY, ClassADSGenerator.DISKSPACE_KEY);
            mPegasusProfilesToPegasusClassAdKeys.put(
                    Pegasus.RUNTIME_KEY, ClassADSGenerator.JOB_RUNTIME_AD_KEY);
            mPegasusProfilesToPegasusClassAdKeys.put(
                    Pegasus.PROJECT_KEY, ClassADSGenerator.PROJECT_KEY);
            mPegasusProfilesToPegasusClassAdKeys.put(
                    Pegasus.QUEUE_KEY, ClassADSGenerator.QUEUE_KEY);
            mPegasusProfilesToPegasusClassAdKeys.put(
                    Pegasus.GLITE_ARGUMENTS_KEY, ClassADSGenerator.GLITE_ARGUMENTS_KEY);
        }
        return mPegasusProfilesToPegasusClassAdKeys;
    }

    /**
     * Maps a Pegasus resource profile key to a corressponding Pegasus Classad variable
     *
     * @param profileKey
     * @return
     */
    public static final String mapPegasusResourceProfileToPegasusClassAdVariable(
            String profileKey) {
        StringBuffer variable = new StringBuffer();
        String varKey = ClassADSGenerator.pegasusProfilesToPegasusClassAdKeys().get(profileKey);
        if (varKey != null) {
            // $(MY.pegasus_memory_mb)
            variable.append("$(MY.").append(varKey).append(")");
        } else {
            throw new RuntimeException(
                    "Unable to map pegasus profile key to pegasus classad - " + profileKey);
        }

        return variable.toString();
    }

    /** The class ad for the expected job value */
    public static final String JOB_RUNTIME_AD_KEY = "pegasus_job_runtime";

    /** The key for the number of cores for the multiplier factor in stampede. */
    public static final String CORES_KEY = "pegasus_cores";

    /** The key for the number of gpus associated with the job */
    public static final String GPUS_KEY = "pegasus_gpus";

    /** The key for the queue if specified */
    public static final String QUEUE_KEY = "pegasus_queue";

    /** The key for the project if specified */
    public static final String PROJECT_KEY = "pegasus_project";

    /** The key to specify the extra arguments to be passed to the HPC scheduler */
    public static final String GLITE_ARGUMENTS_KEY = "pegasus_glite_arguments";

    /** The key for memory request for the job in MB */
    public static final String MEMORY_KEY = "pegasus_memory_mb";

    /** The key for diskspace request for the job in MB */
    public static final String DISKSPACE_KEY = "pegasus_diskspace_mb";

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
     * @param appName
     * @paran appName the app name
     */
    public static void generate(PrintWriter writer, ADag dag, Job job, String appName) {

        // GH-2183 lets generate the expr profiles as variables first
        Pegasus pegasusProfiles = job.vdsNS;
        for (Iterator it = pegasusProfiles.getProfileKeyIterator(); it.hasNext(); ) {
            String key = (String) it.next();
            if (key.endsWith(Pegasus.EXPRESSION_PROFILE_KEYS_SUFFIX)) {
                // replace . in the keys with _ and pegasus_ prefix
                // so cores_expr becomes pegasus_cores_expr
                StringBuilder newKey = new StringBuilder();
                newKey.append("pegasus_").append(key.replace(".", "_"));
                writer.println(
                        generateSubmitFileVariable(
                                newKey.toString(), pegasusProfiles.getStringValue(key)));
            }
        }

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

        // PM-1621 write out the gpus if specified for job
        String gpusValue = job.vdsNS.getStringValue(Pegasus.GPUS_KEY);
        int gpus = -1;
        try {
            gpus = (gpusValue == null) ? 0 : Integer.parseInt(gpusValue);
        } catch (Exception e) {
            // ignore
        }
        if (gpus >= 0) {
            writer.println(generateClassAdAttribute(ClassADSGenerator.GPUS_KEY, gpus));
        }

        // GH-2170 generate diskspace and memory
        String memoryValue = job.vdsNS.getStringValue(Pegasus.MEMORY_KEY);
        int memory = -1;
        try {
            memory = (memoryValue == null) ? -1 : Integer.parseInt(memoryValue);
        } catch (Exception e) {
            // GH-2174 they can also be expressions in case a user
            // explicitly specified condor profile key request_memory
            writer.println(generateClassAdAttribute(ClassADSGenerator.MEMORY_KEY, memoryValue));
        }
        if (memory >= 0) {
            writer.println(generateClassAdAttribute(ClassADSGenerator.MEMORY_KEY, memory));
        }

        String diskValue = job.vdsNS.getStringValue(Pegasus.DISKSPACE_KEY);
        int disk = -1;
        try {
            disk = (diskValue == null) ? -1 : Integer.parseInt(diskValue);
        } catch (Exception e) {
            writer.println(generateClassAdAttribute(ClassADSGenerator.DISKSPACE_KEY, disk));
        }
        if (disk >= 0) {
            // GH-2174 they can also be expressions in case a user
            // explicitly specified condor profile key request_disk
            writer.println(generateClassAdAttribute(ClassADSGenerator.DISKSPACE_KEY, disk));
        }

        // GH-2175 specify project and queue if present in pegasus profiles
        if (job.vdsNS.containsKey(Pegasus.QUEUE_KEY)) {
            // pick the one pre populated
            writer.println(
                    generateClassAdAttribute(
                            ClassADSGenerator.QUEUE_KEY,
                            job.vdsNS.getStringValue(Pegasus.QUEUE_KEY)));
        }
        if (job.vdsNS.containsKey(Pegasus.PROJECT_KEY)) {
            // pick the one pre populated
            writer.println(
                    generateClassAdAttribute(
                            ClassADSGenerator.PROJECT_KEY,
                            job.vdsNS.getStringValue(Pegasus.PROJECT_KEY)));
        }
        if (job.vdsNS.containsKey(Pegasus.GLITE_ARGUMENTS_KEY)) {
            // pick the one pre populated
            writer.println(
                    generateClassAdAttribute(
                            ClassADSGenerator.GLITE_ARGUMENTS_KEY,
                            job.vdsNS.getStringValue(Pegasus.GLITE_ARGUMENTS_KEY)));
        }

        // determine the cluster size
        int csize =
                (job instanceof AggregatedJob)
                        ? ((AggregatedJob) job).numberOfConsitutentJobs()
                        : 1;
        writer.println(generateClassAdAttribute(ClassADSGenerator.JOB_CLUSTER_SIZE_AD_KEY, csize));
    }

    /**
     * Generates a submit file variable given the name and the value. The value gets quoted in
     * single quotes.
     *
     * @param name the attribute name.
     * @param value the value/expression making the classad variable.
     * @return the classad attriubute.
     */
    private static String generateSubmitFileVariable(String name, String value) {
        StringBuilder sb = new StringBuilder();

        sb.append(name).append(" = ");
        sb.append("'").append(value).append("'");

        return sb.toString();
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
        StringBuilder sb = new StringBuilder(10);

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
        sb.append("'");
        sb.append(value);
        sb.append("'");

        return sb.toString();
    }
}
