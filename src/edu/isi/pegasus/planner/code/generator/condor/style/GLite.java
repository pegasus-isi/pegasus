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
package edu.isi.pegasus.planner.code.generator.condor.style;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.classes.Profiles;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.TransferJob;
import edu.isi.pegasus.planner.code.generator.condor.CondorEnvironmentEscape;
import edu.isi.pegasus.planner.code.generator.condor.CondorQuoteParser;
import edu.isi.pegasus.planner.code.generator.condor.CondorQuoteParserException;
import edu.isi.pegasus.planner.code.generator.condor.CondorStyleException;
import edu.isi.pegasus.planner.namespace.Condor;
import edu.isi.pegasus.planner.namespace.Globus;
import edu.isi.pegasus.planner.namespace.Namespace;
import edu.isi.pegasus.planner.namespace.Pegasus;
import java.util.Map;

/**
 * This implementation enables a job to be submitted via gLite to a grid sites. This is the style
 * applied when job has a pegasus profile style key with value GLite associated with it.
 *
 * <p>This style should only be used when the condor on the submit host can directly talk to
 * scheduler running on the cluster. In Pegasus there should be a separate compute site that has
 * this style associated with it. This style should not be specified for the local site.
 *
 * <p>As part of applying the style to the job, this style adds the following classads expressions
 * to the job description
 *
 * <pre>
 *      batch_queue  - value picked up from a ( Globus or Pegasus profile queue)  OR can be
 *                     set directly as a Condor profile named batch_queue
 *      +remote_cerequirements - See below
 * </pre>
 *
 * <p>The remote CE requirements are constructed from the following profiles associated with the
 * job.The profiles for a job are derived from various sources - user properties - transformation
 * catalog - site catalog - DAX
 *
 * <p>Note it is upto the user to specify these or a subset of them.
 *
 * <p>The following globus profiles if associated with the job are picked up and translated to
 * +remote_cerequirements key in the job submit files.
 *
 * <pre>
 *
 * hostcount    -> NODES
 * xcount       -> PROCS
 * maxwalltime  -> WALLTIME
 * totalmemory  -> TOTAL_MEMORY
 * maxmemory    -> PER_PROCESS_MEMORY
 * </pre>
 *
 * The following condor profiles if associated with the job are picked up
 *
 * <pre>
 * priority  -> PRIORITY
 * </pre>
 *
 * All the env profiles are translated to MYENV
 *
 * <p>For e.g. the expression in the submit file may look as
 *
 * <pre>
 * +remote_cerequirements = "PROCS==18 && NODES==1 && PRIORITY==10 && WALLTIME==3600
 *   && PASSENV==1 && JOBNAME==\"TEST JOB\" && MYENV ==\"MONTAGE_HOME=/usr/montage,JAVA_HOME=/usr\""
 * </pre>
 *
 * The pbs_local_attributes.sh file in share/pegasus/htcondor/glite picks up these values and
 * translated to appropriate PBS parameters
 *
 * <pre>
 * NODES                 -> nodes
 * PROCS                 -> ppn
 * WALLTIME              -> walltime
 * TOTAL_MEMORY          -> mem
 * PER_PROCESS_MEMORY    -> pmem
 * </pre>
 *
 * All the jobs that have this style applied dont have a remote directory specified in the submit
 * directory. They rely on kickstart to change to the working directory when the job is launched on
 * the remote node.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class GLite extends Abstract {
    /** The name of the style being implemented. */
    public static final String STYLE_NAME = "GLite";

    /** The Condor remote directory classad key to be used with Glite */
    public static final String CONDOR_REMOTE_DIRECTORY_KEY = "+remote_iwd";

    /** The condor key to set the remote environment via BLAHP */
    public static final String CONDOR_REMOTE_ENVIRONMENT_KEY = "+remote_environment";

    public static final String SGE_GRID_RESOURCE = "sge";

    public static final String PBS_GRID_RESOURCE = "pbs";

    /** the panda grid resource */
    public static String PANDA_GRID_RESOURCE_PREFIX = "panda";

    /** the panda grid resource */
    public static String BATCH_GRID_RESOURCE_PREFIX = "batch";

    /**
     * The internal mapping of globus keys to BLAHP directives.
     *
     * <pre>
     * Pegasus Profile Key     Batch Key in Condor submit file    Comment from Jaime
     * pegasus.cores           +NodeNumber	 		This is the number of cpus you want
     * pegasus.nodes           +HostNumber			This is the number of hosts you want
     * pegasus.ppn             +SMPGranularity		    	This is the number of processes per host you want
     * pegasus.project         +BatchProject			The project to use
     * </pre>
     */
    public static final String GLOBUS_BLAHP_DIRECTIVES_MAPPING[][] = {
        {Globus.COUNT_KEY, "+NodeNumber"}, // pegasus.cores
        {Globus.HOST_COUNT_KEY, "+HostNumber"}, // pegasus.nodes
        {Globus.XCOUNT_KEY, "+SMPGranularity"}, // pegasus.ppn
        {Globus.PROJECT_KEY, "+BatchProject"}, // pegasus.project
        {Globus.MAX_WALLTIME_KEY, "+BatchWallclock"} // pegasus.runtime specify in seconds
    };

    /**
     * Convenience method to retrieve the batch system associated with a grid_resource entry.
     *
     * @param job
     * @return the batch system. If not found returns null
     */
    public static final String getBatchSystem(Job job) {
        return GLite.getBatchSystem(job, GLite.getGridResource(job));
    }

    /**
     * Convenience method to retrieve the batch system associated with a grid_resource entry.
     *
     * @param job
     * @return the batch system. If not found returns null
     */
    public static final String getBatchSystem(Job job, String gridResource) {
        if (gridResource == null) {
            return null;
        }

        // Sample grid resource constructed for bosco/ssh
        // batch slurm user@bridges.psc.edu
        String batchSystem = gridResource.replaceAll("^(batch|panda) ", "");
        return batchSystem.split(" ")[0];
    }

    /**
     * Return the grid resource associated with the job
     *
     * @param job
     * @return the associated grid resource, else null
     */
    public static final String getGridResource(Job job) {
        /* figure out the remote scheduler. should be specified with the job*/
        String gridResource = (String) job.condorVariables.get(Condor.GRID_RESOURCE_KEY);
        if (gridResource == null) {
            return null;
        }
        // PM-1087 make it lower case first
        gridResource = gridResource.toLowerCase();
        return gridResource;
    }

    /** Handle to escaping class for environment variables */
    private CondorEnvironmentEscape mEnvEscape;

    private CondorG mCondorG;

    /** The default Constructor. */
    public GLite() {
        super();
        mEnvEscape = new CondorEnvironmentEscape();
        mCondorG = new CondorG();
    }

    /**
     * Apply a style to a SiteCatalogEntry. This allows the style classes to add or modify the
     * existing profiles for the site so far.
     *
     * @param site the site catalog entry object
     * @throws CondorStyleException in case of any error occuring code generation.
     */
    public void apply(SiteCatalogEntry site) throws CondorStyleException {
        Namespace pegasusProfiles = site.getProfiles().get(Profiles.NAMESPACES.pegasus);
        String styleKey = Pegasus.STYLE_KEY;
        if (pegasusProfiles.containsKey(styleKey)) {
            String style = (String) pegasusProfiles.get(styleKey);
            if (style.equals(Pegasus.GLITE_STYLE)) {
                // add change.dir key for it always
                String key = Pegasus.CHANGE_DIR_KEY;
                this.setProfileIfNotPresent(pegasusProfiles, key, "true");
                mLogger.log(
                        "Set pegasus profile "
                                + key
                                + " to "
                                + pegasusProfiles.get(key)
                                + " for site "
                                + site.getSiteHandle(),
                        LogManager.DEBUG_MESSAGE_LEVEL);

                key = Pegasus.CONDOR_QUOTE_ARGUMENTS_KEY;
                this.setProfileIfNotPresent(pegasusProfiles, key, "false");
                mLogger.log(
                        "Set pegasus profile "
                                + key
                                + " to "
                                + pegasusProfiles.get(key)
                                + " for site "
                                + site.getSiteHandle(),
                        LogManager.DEBUG_MESSAGE_LEVEL);
            }
        }
    }

    /**
     * Convenience method to set a profile if there is no matching key already
     *
     * @param profiles
     * @param key
     * @param value
     */
    protected void setProfileIfNotPresent(Namespace profiles, String key, String value) {
        if (!profiles.containsKey(key)) {
            profiles.checkKeyInNS(key, value);
        }
    }

    /**
     * Applies the gLite style to the job.
     *
     * @param job the job on which the style needs to be applied.
     * @throws CondorStyleException in case of any error occuring code generation.
     */
    public void apply(Job job) throws CondorStyleException {

        String workdir = job.getDirectory();

        /* universe is always set to grid*/
        job.condorVariables.construct(Condor.UNIVERSE_KEY, Condor.GRID_UNIVERSE);

        String gridResource = GLite.getGridResource(job);
        if (gridResource == null) {
            throw new CondorStyleException(missingKeyError(job, Condor.GRID_RESOURCE_KEY));
        }

        String batchSystem = GLite.getBatchSystem(job, gridResource);
        if (!supportedBatchSystem(batchSystem)) {
            // if it is not one of the support types, log a warning but use PBS.
            mLogger.log(
                    "Glite mode supports only pbs, sge, slurm, moab or cobalt submission. Will use PBS style attributes for job "
                            + job.getID()
                            + " with grid resource "
                            + gridResource,
                    LogManager.WARNING_MESSAGE_LEVEL);
            batchSystem = "pbs";
        }

        if (gridResource.startsWith(GLite.PANDA_GRID_RESOURCE_PREFIX)) {
            // PM-1539 at the moment condor does not support grid resource of type
            // panda . so we revert the grid resoure back to batch
            gridResource =
                    gridResource.replaceAll(
                            "^" + GLite.PANDA_GRID_RESOURCE_PREFIX,
                            GLite.BATCH_GRID_RESOURCE_PREFIX);
            job.condorVariables.construct(Condor.GRID_RESOURCE_KEY, gridResource);
        }

        job.condorVariables.construct(
                GLite.CONDOR_REMOTE_DIRECTORY_KEY, workdir == null ? null : quote(workdir));

        // also set it as an environment variable, since for MPI jobs
        // glite and BLAHP dont honor +remote_iwd and we cannot use kickstart
        // the only way to get it to work is for the wrapper around the mpi
        // executable to a cd to the directory pointed to by this variable.
        if (workdir != null) {
            job.envVariables.construct("_PEGASUS_SCRATCH_DIR", workdir);
            // PM-961 also associate the value as an environment variable
            job.envVariables.construct(
                    edu.isi.pegasus.planner.namespace.ENV.PEGASUS_SCRATCH_DIR_KEY, workdir);
        }

        /* transfer_executable does not work with gLite
         * Explicitly set to false */
        // PM-950 looks like it works now. for pegasus lite modes we need
        // the planner to be able to set it to true
        // job.condorVariables.construct( Condor.TRANSFER_EXECUTABLE_KEY, "false" );

        /* convert some condor keys and globus keys to remote ce requirements
        +remote_cerequirements = blah */
        job.condorVariables.construct(
                "+remote_cerequirements", getCERequirementsForJob(job, batchSystem));
        generateBLAHPDirectives(job, batchSystem);

        /* retrieve some keys from globus rsl and convert to gLite format */
        if (job.globusRSL.containsKey("queue")) {
            job.condorVariables.construct("batch_queue", (String) job.globusRSL.get("queue"));
        }

        // PM-1116 set the task requirements as environment variables
        Globus rsl = job.globusRSL;

        for (Map.Entry<String, String> entry : Globus.rslToEnvProfiles().entrySet()) {
            String rslKey = entry.getKey();
            String envKey = entry.getValue();

            if (rsl.containsKey(rslKey)) {
                String value = (String) rsl.get(rslKey);
                if (rslKey.equals(Globus.MAX_WALLTIME_KEY)) {
                    // handle runtime key as a special case as, as a globus profile it is in minutes
                    // and we want in seconds
                    long runtime = Long.parseLong(value) * 60;
                    value = Long.toString(runtime);
                }

                job.envVariables.construct(envKey, value);
            }
        }

        /* do special handling for jobs scheduled to local site
         * as condor file transfer mechanism does not work
         * Special handling for the JPL cluster */
        if (job.getSiteHandle().equals("local") && job instanceof TransferJob) {
            /* remove the change dir requirments for the
             * third party transfer on local host */
            job.condorVariables.removeKey(GLite.CONDOR_REMOTE_DIRECTORY_KEY);
        }

        /* similar handling for registration jobs */
        if (job.getSiteHandle().equals("local") && job.getJobType() == Job.REPLICA_REG_JOB) {
            /* remove the change dir requirments for the
             * third party transfer on local host */
            job.condorVariables.removeKey(GLite.CONDOR_REMOTE_DIRECTORY_KEY);
        }

        if (job.getSiteHandle().equals("local")) {
            applyCredentialsForLocalExec(job);
        } else {
            applyCredentialsForRemoteExec(job);
        }

        /*
         PM-934 construct environment accordingly
         PM-1084 +remote_environment should be created only after credentials
                 have been figured out , as paths to remote credentials are set
                 as environment variables.
        */
        job.condorVariables.construct(
                GLite.CONDOR_REMOTE_ENVIRONMENT_KEY, mEnvEscape.escape(job.envVariables));
        job.envVariables.reset();
    }

    /**
     * Constructs the value for remote CE requirements expression for the job .
     *
     * <p>For e.g. the expression in the submit file may look as
     *
     * <pre>
     * +remote_cerequirements = "PROCS==18 && NODES==1 && PRIORITY==10 && WALLTIME==3600
     *   && PASSENV==1 && JOBNAME==\"TEST JOB\" && MYENV ==\"GAURANG=MEHTA,KARAN=VAHI\""
     *
     * </pre>
     *
     * The requirements are generated on the basis of certain profiles associated with the jobs. The
     * following globus profiles if associated with the job are picked up
     *
     * <pre>
     * hostcount    -> NODES
     * xcount       -> PROCS
     * maxwalltime  -> WALLTIME
     * totalmemory  -> TOTAL_MEMORY
     * maxmemory    -> PER_PROCESS_MEMORY
     * </pre>
     *
     * The following condor profiles if associated with the job are picked up
     *
     * <pre>
     * priority  -> PRIORITY
     * </pre>
     *
     * All the env profiles are translated to MYENV
     *
     * @param job
     * @param batchSystem
     * @return the value to the expression and it is condor quoted
     * @throws CondorStyleException in case of condor quoting error
     */
    protected String getCERequirementsForJob(Job job, String batchSystem)
            throws CondorStyleException {
        StringBuffer value = new StringBuffer();

        /* append the job name */
        /* job name cannot have - or _ */
        String id = job.getID().replace("-", "");
        id = id.replace("_", "");
        // the jobname in case of pbs can only be 15 characters long
        id = (id.length() > 15) ? id.substring(0, 15) : id;

        // add the jobname so that it appears when we do qstat
        addSubExpression(value, "JOBNAME", id);
        value.append(" && ");

        /* always have PASSENV to true */
        // value.append( " && ");
        addSubExpression(value, "PASSENV", 1);

        /* specifically pass the queue in the requirement since
        some versions dont handle +remote_queue correctly */
        if (job.globusRSL.containsKey("queue")) {
            value.append(" && ");
            addSubExpression(value, "QUEUE", (String) job.globusRSL.get("queue"));
        }

        this.handleResourceRequirements(job, batchSystem);

        /* the globus key hostCount is NODES */
        if (job.globusRSL.containsKey("hostcount")) {
            value.append(" && ");
            addSubExpression(value, "NODES", (String) job.globusRSL.get("hostcount"));
        }

        /* the globus key count is CORES */
        if (job.globusRSL.containsKey("count")) {
            value.append(" && ");
            addSubExpression(value, "CORES", (String) job.globusRSL.get("count"));
        }

        /* the globus key xcount is PROCS */
        if (job.globusRSL.containsKey("xcount")) {
            value.append(" && ");
            addSubExpression(value, "PROCS", (String) job.globusRSL.get("xcount"));
        }

        /* the globus key maxwalltime is WALLTIME */
        if (job.globusRSL.containsKey("maxwalltime")) {
            value.append(" && ");
            if (batchSystem.equals("lsf")) {
                addSubExpression(
                        value,
                        "WALLTIME",
                        lsfFormattedTimestamp((String) job.globusRSL.get("maxwalltime")));
            } else {
                addSubExpression(
                        value,
                        "WALLTIME",
                        pbsFormattedTimestamp((String) job.globusRSL.get("maxwalltime")));
            }
        }

        /* the globus key maxmemory is PER_PROCESS_MEMORY */
        if (job.globusRSL.containsKey("maxmemory")) {
            value.append(" && ");
            addSubExpression(value, "PER_PROCESS_MEMORY", (String) job.globusRSL.get("maxmemory"));
        }

        /* the globus key totalmemory is TOTAL_MEMORY */
        if (job.globusRSL.containsKey("totalmemory")) {
            value.append(" && ");
            addSubExpression(value, "TOTAL_MEMORY", (String) job.globusRSL.get("totalmemory"));
        }

        /* the globus key project is PROJECT */
        if (job.globusRSL.containsKey(Globus.PROJECT_KEY)) {
            value.append(" && ");
            addSubExpression(value, "PROJECT", (String) job.globusRSL.get(Globus.PROJECT_KEY));
        }

        /* the condor key priority is PRIORITY */
        if (job.condorVariables.containsKey("priority")) {
            value.append(" && ");
            addSubExpression(
                    value,
                    "PRIORITY",
                    Integer.parseInt((String) job.condorVariables.get("priority")));
        }

        /* the pegasus key glite.arguments is EXTRA_ARGUMENTS */
        if (job.vdsNS.containsKey(Pegasus.GLITE_ARGUMENTS_KEY)) {
            value.append(" && ");
            addSubExpression(
                    value, "EXTRA_ARGUMENTS", (String) job.vdsNS.get(Pegasus.GLITE_ARGUMENTS_KEY));
        }

        return value.toString();
    }

    /**
     * Adds a sub expression to a string buffer
     *
     * @param sb the StringBuffer
     * @param key the key
     * @param value the value
     */
    protected void addSubExpression(StringBuffer sb, String key, String value) {
        // PM-802
        sb.append(key).append("==").append("\"").append(value).append("\"");
    }

    /**
     * Adds a sub expression to a string buffer
     *
     * @param sb the StringBuffer
     * @param key the key
     * @param value the value
     */
    protected void addSubExpression(StringBuffer sb, String key, Integer value) {
        sb.append(key).append("==").append(value);
    }

    /**
     * Constructs an error message in case of invalid combination of cores, nodes and ppn
     *
     * @param job the job object.
     * @param cores
     * @param nodes
     * @param ppn
     * @param reason
     * @return
     */
    protected String invalidCombinationError(
            Job job, Integer cores, Integer nodes, Integer ppn, String reason) {
        StringBuffer sb = new StringBuffer();
        StringBuilder comb = new StringBuilder();
        // Only two of (nodes, cores, ppn) should be specified for job
        sb.append("Invalid combination of ");
        comb.append("(");
        if (cores != null) {
            sb.append(" cores ");
            comb.append(cores).append(",");
        }
        if (nodes != null) {
            sb.append(" nodes ");
            comb.append(nodes).append(",");
        }
        if (ppn != null) {
            sb.append(" ppn ");
            comb.append(ppn).append(",");
        }
        comb.append(")");
        sb.append(" for job ").append(job.getID());
        sb.append(" ").append(comb);
        if (reason != null) {
            sb.append(" ").append(reason);
        }

        return sb.toString();
    }

    /**
     * Constructs an error message in case of style mismatch.
     *
     * @param job the job object.
     * @param key the missing key
     */
    protected String missingKeyError(Job job, String key) {
        StringBuffer sb = new StringBuffer();
        sb.append("( ")
                .append("Missing key ")
                .append(key)
                .append(" for job ")
                .append(job.getName())
                .append("with style ")
                .append(STYLE_NAME);

        return sb.toString();
    }

    /**
     * Condor Quotes a string
     *
     * @param string the string to be quoted.
     * @return quoted string.
     * @throws CondorStyleException in case of condor quoting error
     */
    private String quote(String string) throws CondorStyleException {
        String result;
        try {
            mLogger.log("Unquoted string is  " + string, LogManager.TRACE_MESSAGE_LEVEL);
            result = CondorQuoteParser.quote(string, true);
            mLogger.log("Quoted string is  " + result, LogManager.TRACE_MESSAGE_LEVEL);
        } catch (CondorQuoteParserException e) {
            throw new CondorStyleException("CondorQuoting Problem " + e.getMessage());
        }
        return result;
    }

    /**
     * Converts minutes into hh:dd:ss for PBS formatting purposes
     *
     * @param minutes
     * @return
     */
    public String pbsFormattedTimestamp(String minutes) {
        int minutesValue = Integer.parseInt(minutes);

        if (minutesValue < 0) {
            throw new IllegalArgumentException(
                    "Invalid value for minutes provided for conversion " + minutes);
        }

        int hours = minutesValue / 60;
        int mins = minutesValue % 60;

        StringBuffer result = new StringBuffer();
        if (hours < 10) {
            result.append("0").append(hours);
        } else {
            result.append(hours);
        }
        result.append(":");
        if (mins < 10) {
            result.append("0").append(mins);
        } else {
            result.append(mins);
        }
        result.append(":00"); // we don't have second precision

        return result.toString();
    }

    /**
     * Converts minutes into hh:dd for LSF formatting purposes
     *
     * @param minutes
     * @return
     */
    public String lsfFormattedTimestamp(String minutes) {
        int minutesValue = Integer.parseInt(minutes);

        if (minutesValue < 0) {
            throw new IllegalArgumentException(
                    "Invalid value for minutes provided for conversion " + minutes);
        }

        int hours = minutesValue / 60;
        int mins = minutesValue % 60;

        StringBuffer result = new StringBuffer();
        if (hours > 0 && hours < 10) {
            result.append("0").append(hours);
            result.append(":");
        } else if (hours >= 10) {
            result.append(hours);
            result.append(":");
        }

        if (mins < 10) {
            result.append("0").append(mins);
        } else {
            result.append(mins);
        }

        return result.toString();
    }

    /**
     * This translates the Pegasus resource profiles to corresponding globus profiles that are used
     * to set the shell parameters for local attributes shell script for the LRMS.
     *
     * @param job
     * @param batchSystem the grid resource associated with the job. can be pbs | sge.
     */
    protected void handleResourceRequirements(Job job, String batchSystem)
            throws CondorStyleException {
        // PM-962 we update the globus RSL keys on basis
        // of Pegasus profile keys before doing any translation

        mCondorG.handleResourceRequirements(job);

        if (batchSystem.equals("pbs")
                || batchSystem.equals("cobalt")
                || batchSystem.equals("moab")) {
            // check for cores / count if set
            boolean coresSet = job.globusRSL.containsKey(Globus.COUNT_KEY);
            boolean nodesSet = job.globusRSL.containsKey(Globus.HOST_COUNT_KEY);
            boolean ppnSet = job.globusRSL.containsKey(Globus.XCOUNT_KEY);

            if (coresSet) {
                // we need to arrive at PPN which is cores/nodes
                int cores = Integer.parseInt((String) job.globusRSL.get(Globus.COUNT_KEY));
                // sanity check
                if (!(nodesSet || ppnSet)) {
                    // neither nodes or ppn are set
                    // cannot do any translation
                    throw new CondorStyleException(
                            "Cannot translate to "
                                    + batchSystem
                                    + " attributes. Only cores "
                                    + cores
                                    + " specified for job "
                                    + job.getID());
                }

                // need to do some arithmetic to arrive at nodes and ppn
                if (nodesSet) {

                    int nodes = Integer.parseInt((String) job.globusRSL.get(Globus.HOST_COUNT_KEY));
                    int ppn = cores / nodes;
                    // sanity check
                    if (cores % nodes != 0) {
                        throw new CondorStyleException(
                                invalidCombinationError(
                                        job,
                                        cores,
                                        nodes,
                                        null,
                                        "because cores not perfectly divisible by nodes."));
                    }
                    if (ppnSet) {
                        // all three were set . check if derived value is same as
                        // existing
                        int existing =
                                Integer.parseInt((String) job.globusRSL.get(Globus.XCOUNT_KEY));
                        if (existing != ppn) {
                            throw new CondorStyleException(
                                    invalidCombinationError(
                                            job,
                                            cores,
                                            nodes,
                                            existing,
                                            "do not satisfy cores = nodes * ppn. Please specify only two of (nodes, cores, ppn)."));
                        }
                    } else {
                        job.globusRSL.construct(Globus.XCOUNT_KEY, Integer.toString(ppn));
                    }
                } else {
                    // we need to arrive at nodes which is cores/ppn
                    int ppn = Integer.parseInt((String) job.globusRSL.get(Globus.XCOUNT_KEY));
                    int nodes = cores / ppn;
                    // sanity check
                    if (cores % ppn != 0) {
                        // PM-1051 if they are not perfectly divisible just take the ceiling value.
                        // accounts for case where ppn is specified in site catalog and cores
                        // associated
                        // with job
                        nodes = nodes + 1;
                        StringBuilder message = new StringBuilder();
                        message.append("For job ")
                                .append(job.getID())
                                .append(" with (cores,ppn) as ")
                                .append("(")
                                .append(cores)
                                .append(" , ")
                                .append(ppn)
                                .append("). ")
                                .append("Set the nodes to be a ceiling of cores/ppn - ")
                                .append(nodes);
                        mLogger.log(message.toString(), LogManager.DEBUG_MESSAGE_LEVEL);
                    }
                    job.globusRSL.construct(Globus.HOST_COUNT_KEY, Integer.toString(nodes));
                }
            } else {
                // usually for PBS users specify nodes and ppn
                // globus rsl keys are already set appropriately for translation
                // FIXME:  should we complain if nothing is associated or
                // set some default value?
            }

        } else if (batchSystem.equals("sge")) {
            // for SGE case
            boolean coresSet = job.globusRSL.containsKey(Globus.COUNT_KEY);
            boolean nodesSet = job.globusRSL.containsKey(Globus.HOST_COUNT_KEY);
            boolean ppnSet = job.globusRSL.containsKey(Globus.XCOUNT_KEY);

            if (coresSet) {
                // then that is what SGE really needs.
                // ignore other values.
            } else {
                // we need to attempt to arrive at a value or specify a default value
                if (nodesSet && ppnSet) {
                    // set cores to multiple
                    int nodes = Integer.parseInt((String) job.globusRSL.get(Globus.HOST_COUNT_KEY));
                    int ppn = Integer.parseInt((String) job.globusRSL.get(Globus.XCOUNT_KEY));
                    job.globusRSL.construct(Globus.COUNT_KEY, Integer.toString(nodes * ppn));
                } else if (nodesSet || ppnSet) {
                    throw new CondorStyleException(
                            "Either cores or ( nodes and ppn) need to be set for SGE submission for job "
                                    + job.getID());
                }
                // default case nothing specified
            }

        } else if (batchSystem.equals("slurm")) {
            // for SLURM case
            boolean coresSet = job.globusRSL.containsKey(Globus.COUNT_KEY);
            boolean nodesSet = job.globusRSL.containsKey(Globus.HOST_COUNT_KEY);
            boolean ppnSet = job.globusRSL.containsKey(Globus.XCOUNT_KEY);

            if (coresSet) {
                // then that is what SLURM really needs.
                // ignore other values.
            } else {
                // we need to attempt to arrive at a value or specify a default value
                if (nodesSet && ppnSet) {
                    // set cores to multiple
                    int nodes = Integer.parseInt((String) job.globusRSL.get(Globus.HOST_COUNT_KEY));
                    int ppn = Integer.parseInt((String) job.globusRSL.get(Globus.XCOUNT_KEY));
                    job.globusRSL.construct(Globus.COUNT_KEY, Integer.toString(nodes * ppn));
                } else if (nodesSet || ppnSet) {
                    throw new CondorStyleException(
                            "Either cores or ( nodes and ppn) need to be set for SLURM submission for job "
                                    + job.getID());
                }
                // default case nothing specified
            }

        } else if (batchSystem.equals("lsf")) {
            // For LSF case on Summit.
            // IMPORTANT: Not tested anywhere else
            boolean coresSet = job.globusRSL.containsKey(Globus.COUNT_KEY);
            boolean nodesSet = job.globusRSL.containsKey(Globus.HOST_COUNT_KEY);
            boolean ppnSet = job.globusRSL.containsKey(Globus.XCOUNT_KEY);

            if (nodesSet) {
                // then that is what LSF really needs on Summit
                // ignore other values.
            } else {
                // we need to attempt to arrive at a value or specify a default value
                if (coresSet && ppnSet) {
                    // set nodes to div, we don't handle the case where cores/ppn is not divisable
                    int cores = Integer.parseInt((String) job.globusRSL.get(Globus.COUNT_KEY));
                    int ppn = Integer.parseInt((String) job.globusRSL.get(Globus.XCOUNT_KEY));
                    job.globusRSL.construct(Globus.HOST_COUNT_KEY, Integer.toString(cores / ppn));
                } else if (coresSet || ppnSet) {
                    throw new CondorStyleException(
                            "Either cores or ( nodes and ppn) need to be set for LSF submission for job "
                                    + job.getID());
                }
                // default case nothing specified
            }
        } else {
            // unreachable code
            throw new CondorStyleException(
                    "Invalid grid resource associated for job " + job.getID() + " " + batchSystem);
        }
    }

    /**
     * Returns whether the batch system is supported or not.
     *
     * @param batchSystem
     * @return
     */
    protected boolean supportedBatchSystem(String batchSystem) {
        return (batchSystem == null)
                ? false
                : batchSystem.equals("pbs")
                        || batchSystem.equals("sge")
                        || batchSystem.equals("slurm")
                        || batchSystem.equals("moab")
                        || batchSystem.equals("lsf")
                        || batchSystem.equals("cobalt");
    }

    /**
     * Generates BLAHP directives for the job. The following mapping is followed from the pegasus
     * task requirement profiles
     *
     * <pre>
     * Pegasus Profile Key     Batch Key in Condor submit file    Comment from Jaime
     * pegasus.cores           +NodeNumber	 		This is the number of cpus you want
     * pegasus.nodes           +HostNumber			This is the number of hosts you want
     * pegasus.ppn             +SMPGranularity		    	This is the number of processes per host you want
     * pegasus.project         +BatchProject			The project to use
     * </pre>
     *
     * @param job
     * @param batchSystem
     */
    private void generateBLAHPDirectives(Job job, String batchSystem) {
        // sanity check
        /*generate blahp directives only for cobalt
         * till we verify it works for blahp backends in condor
         */
        if (!batchSystem.equals("cobalt")) {
            return;
        }

        for (int i = 0; i < GLite.GLOBUS_BLAHP_DIRECTIVES_MAPPING.length; i++) {
            String globusKey = GLOBUS_BLAHP_DIRECTIVES_MAPPING[i][0];
            String blahpDirective = GLOBUS_BLAHP_DIRECTIVES_MAPPING[i][1];

            if (job.globusRSL.containsKey(globusKey)) {
                String value = (String) job.globusRSL.get(globusKey);
                if (globusKey.equals(Globus.PROJECT_KEY)) {
                    // value has to be quoted
                    value = "\"" + value + "\"";
                }

                job.condorVariables.construct(blahpDirective, value);
            }
        }
        return;
    }

    public static void main(String[] args) {
        GLite gl = new GLite();

        System.out.println("0 mins is " + gl.pbsFormattedTimestamp("0"));
        System.out.println("11 mins is " + gl.pbsFormattedTimestamp("11"));
        System.out.println("60 mins is " + gl.pbsFormattedTimestamp("60"));
        System.out.println("69 mins is " + gl.pbsFormattedTimestamp("69"));
        System.out.println("169 mins is " + gl.pbsFormattedTimestamp("169"));
        System.out.println("1690 mins is " + gl.pbsFormattedTimestamp("1690"));
    }
}
