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
package edu.isi.pegasus.planner.namespace;

import edu.isi.pegasus.common.credential.impl.BotoConfig;
import edu.isi.pegasus.common.credential.impl.GoogleP12;
import edu.isi.pegasus.common.credential.impl.Irods;
import edu.isi.pegasus.common.credential.impl.Proxy;
import edu.isi.pegasus.common.credential.impl.S3CFG;
import edu.isi.pegasus.common.credential.impl.Ssh;
import edu.isi.pegasus.planner.catalog.classes.Profiles;
import edu.isi.pegasus.planner.classes.Profile;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.namespace.aggregator.Aggregator;
import edu.isi.pegasus.planner.namespace.aggregator.MAX;
import edu.isi.pegasus.planner.namespace.aggregator.Sum;
import edu.isi.pegasus.planner.namespace.aggregator.UniqueMerge;
import edu.isi.pegasus.planner.namespace.aggregator.Update;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * A Planner specific namespace. It defines profiles that are used to fine tune Pegasus behaviour on
 * a per job basis if required.
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 * @version $Revision$
 */
public class Pegasus extends Namespace {

    /** The name of the namespace that this class implements. */
    public static final String NAMESPACE_NAME = Profile.VDS;

    /**
     * Key to indicate that the site filesystem is accessible on the local site, and hence auxillary
     * jobs for the site can be run on local site
     */
    public static final String LOCAL_VISIBLE_KEY = "auxillary.local";

    /** The name of the key that sets a remote initial dir for a condor globus job. */
    public static final String REMOTE_INITIALDIR_KEY = "workdir";

    /**
     * The name of the key that if set, determines the number of super jobs that are made
     * corresponding to a logical transformation and an execution pool. It overrides the collapse
     * key if set.
     *
     * @see #COLLAPSE_KEY
     */
    public static final String BUNDLE_KEY = "clusters.num";

    /**
     * The name of the key that if set in the Pegasus namespace determines the number of jobs that
     * are collapsed into the super job.
     */
    public static final String COLLAPSE_KEY = "clusters.size";

    /**
     * The name of the key that if set in the Pegasus namespace specifies the maximum amount of time
     * for which a cluster should run. This key is used while clustering jobs horizontally. Only
     * those jobs are grouped together whose combined runtime is less than or equal to the max
     * runtime.
     */
    public static final String MAX_RUN_TIME = "clusters.maxruntime";

    /**
     * The name of the key that determines the clusterig executable to be used to run the
     * merged/collapsed job.
     */
    public static final String JOB_AGGREGATOR_KEY = "job.aggregator";

    /**
     * The name of the key that determines the collapser executable to be used to run the
     * merged/collapsed job.
     *
     * @deprecated
     */
    public static final String COLLAPSER_KEY = "collapser";

    /** The name of the profile key in vds namespace that does the grouping. */
    public static final String GROUP_KEY = "group";

    /** The name of the profile key in vds namespace that does the labelling by default. */
    public static final String LABEL_KEY = "label";

    /**
     * The name of the profile key that determines the launching executable to be used to launch a
     * job on the grid.
     */
    public static final String GRIDSTART_KEY = "gridstart";

    /**
     * The name of the profile key, that determines the arguments with which the GridStart that is
     * used to launch a job on the remote site is invoked with. The arguments are appended to the
     * ones constructed by default by the GridStart implementation.
     */
    public static final String GRIDSTART_ARGUMENTS_KEY = "gridstart.arguments";

    /** The name of the profile key that designates the path to a gridstart. */
    public static final String GRIDSTART_PATH_KEY = "gridstart.path";

    /** An optional executable or script to launch kickstart. */
    public static final String GRIDSTART_LAUNCHER_KEY = "gridstart.launcher";

    /** Any arguments to be passed to the kickstart launcher */
    public static final String GRIDSTART_LAUNCHER_ARGUMENTS_KEY = "gridstart.launcher.arguments";

    /** The message to look for while detecting failures condor job's stdout/stderr */
    public static final String EXITCODE_FAILURE_MESSAGE = "exitcode.failuremsg";

    /** the message to look for while detecting success in condor job's stdout/stderr */
    public static final String EXITCODE_SUCCESS_MESSAGE = "exitcode.successmsg";

    /**
     * The deprecated change dir key.
     *
     * @see #CHANGE_DIR_KEY
     */
    public static final String DEPRECATED_CHANGE_DIR_KEY = "change_dir";

    /**
     * The name of the profile key that triggers the kickstart to change directory before launching
     * an executable instead of launching the executable from the directory where kickstart is being
     * run.
     */
    public static final String CHANGE_DIR_KEY = "change.dir";

    /**
     * The name of the profile key that triggers the kickstart to create and then the working
     * directory to it before launching an executable.
     */
    public static final String CREATE_AND_CHANGE_DIR_KEY = "create.dir";

    /**
     * The deprecated bundle stagein key.
     *
     * @see #CHANGE_DIR_KEY
     */
    public static final String DEPRECATED_BUNDLE_STAGE_IN_KEY = "bundle.stagein";

    /** The name of the key that determines the bundling parameter for the stagein transfer node. */
    public static final String BUNDLE_STAGE_IN_KEY = "stagein.clusters";

    /**
     * The name of the key that determines the bundling parameter for the remote stagein transfer
     * node.
     */
    public static final String BUNDLE_REMOTE_STAGE_IN_KEY = "stagein.remote.clusters";

    /**
     * The name of the key that determines the bundling parameter for the local stagein transfer
     * node.
     */
    public static final String BUNDLE_LOCAL_STAGE_IN_KEY = "stagein.local.clusters";

    /**
     * The name of the key that determines the bundling parameter for the remote stagein transfer
     * node.
     */
    public static final String BUNDLE_REMOTE_STAGE_OUT_KEY = "stageout.remote.clusters";

    /**
     * The name of the key that determines the bundling parameter for the local stagein transfer
     * node.
     */
    public static final String BUNDLE_LOCAL_STAGE_OUT_KEY = "stageout.local.clusters";

    /**
     * The name of the key that determines the bundling parameter for the stageout transfer node.
     */
    public static final String BUNDLE_STAGE_OUT_KEY = "stageout.clusters";

    /**
     * The name of the key that determines the clustering parameter for the stagein transfer node.
     */
    public static final String CLUSTER_STAGE_IN_KEY = "stagein.clusters";

    /**
     * The name of the key that determines the clustering parameter for the stagein transfer node.
     */
    public static final String CLUSTER_REMOTE_STAGE_IN_KEY = "stagein.remote.clusters";

    /**
     * The name of the key that determines the cluster parameter for the local stagein transfer
     * node.
     */
    public static final String CLUSTER_LOCAL_STAGE_IN_KEY = "stagein.local.clusters";

    /**
     * The name of the key that determines the clustering parameter for the stageout transfer node.
     */
    public static final String CLUSTER_STAGE_OUT_KEY = "stageout.clusters";

    /**
     * The name of the key that determines the bundling parameter for the remote stagein transfer
     * node.
     */
    public static final String CLUSTER_REMOTE_STAGE_OUT_KEY = "stageout.remote.clusters";

    /**
     * The name of the key that determines the cluster parameter for the local stagein transfer
     * node.
     */
    public static final String CLUSTER_LOCAL_STAGE_OUT_KEY = "stageout.local.clusters";

    /**
     * The name of the key that determines the number of chains of stagein nodes that are to be
     * created per site.
     */
    public static final String CHAIN_STAGE_IN_KEY = "chain.stagein";

    /**
     * The name of the profile key if associated with a job, results in an explicit transfer of the
     * proxy from the submit host to the remote site, instead of banking upon CondorG to transfer
     * the proxy.
     */
    public static final String TRANSFER_PROXY_KEY = "transfer.proxy";

    /**
     * The name of the profile key, that when associated with transfer jobs determines the arguments
     * with which the transfer executable is invoked.
     */
    public static final String TRANSFER_ARGUMENTS_KEY = "transfer.arguments";

    /**
     * The name of the profile key, that when associated with transfer jobs determines the number of
     * threads that pegasus-transfer uses to do the transfer
     */
    public static final String TRANSFER_THREADS_KEY = "transfer.threads";

    /**
     * The name of the profile key, that when associated with transfer jobs determines the arguments
     * with which the transfer executable is invoked.
     */
    public static final String TRANSFER_SLS_ARGUMENTS_KEY = "transfer.lite.arguments";

    /**
     * The name of the profile key, that when associated with transfer jobs determines the number of
     * threads that pegasus-transfer uses to do the transfer
     */
    public static final String TRANSFER_SLS_THREADS_KEY = "transfer.lite.threads";

    /** The directory in which job needs to execute on worker node tmp. */
    public static final String WORKER_NODE_DIRECTORY_KEY = "wntmp";

    /** Arguments that need to be passed to the clustering executable. */
    public static final String CLUSTER_ARGUMENTS = "cluster.arguments";

    /**
     * The name of the key, that denotes the style of the dag that is constructed. Possible styles
     * can be -condor(glidein,flocking,submitting directly to condor pool) -globus(condorg)
     */
    public static final String STYLE_KEY = "style";

    /**
     * The name of the key that denotes the type of the job. Whether it is recursive or not. Still
     * protypical.
     */
    public static final String TYPE_KEY = "type";

    /**
     * The style indicating that the submit files are to be generated for a vanilla condor
     * execution.
     */
    public static final String CONDOR_STYLE = "condor";

    /**
     * The style indicating that the submit files are to be generated for a CondorC submission to
     * remote schedds.
     */
    public static final String CONDORC_STYLE = "condorc";

    /** The style indicating that the submit files are to be generated for a creamce submission */
    public static final String CREAMCE_STYLE = "cream";

    /** The style indicating that the submit files are to be generated for a CondorG execution. */
    public static final String GLOBUS_STYLE = "globus";

    /** The style indicating that the submit files are to be generated for a glidein execution. */
    public static final String GLIDEIN_STYLE = "glidein";

    /**
     * The style indicating that the submit files are to be generated for a glideinwms execution.
     */
    public static final String GLIDEINWMS_STYLE = "glideinwms";

    /** The style indicating that the submit files are to be generated for a glite execution. */
    public static final String GLITE_STYLE = "glite";

    /**
     * The style indicating that the submit files are to be generated for a direct ssh submission
     */
    public static final String SSH_STYLE = "ssh";

    /** A key to designate the memory required by a job in MB by pegasus-mpi-cluster. */
    public static final String PMC_REQUEST_MEMORY_KEY = "pmc_request_memory";

    /** A key to designate the number of CPU's requested by pegasus-mpi-cluster. */
    public static final String PMC_REQUEST_CPUS_KEY = "pmc_request_cpus";

    /** A key to designate a priority to the jobs run by pegasus-mpi-cluster. */
    public static final String PMC_PRIORITY_KEY = "pmc_priority";

    /** Arguments that need to be passed to the PMC clustering executable. */
    public static final String PMC_TASK_ARGUMENTS = "pmc_task_arguments";

    /** Key indicating whether to consider job for data reuse in the partial mode. */
    public static final String ENABLE_FOR_DATA_REUSE_KEY = "enable_for_data_reuse";

    /**
     * Key indicating indicating time in minutes after which kickstart sends out a TERM signal to a
     * job indicating that is should create a checkpoint file.
     */
    public static final String CHECKPOINT_TIME_KEY = "checkpoint.time";

    /**
     * Key indicating indicating time in seconds after which kickstart sends out a TERM signal to a
     * job indicating that is should create a checkpoint file.
     */
    public static final String DEPRECATED_CHECKPOINT_TIME_KEY = "checkpoint_time";

    /** Key indicating max walltime for a job in minutes */
    public static final String MAX_WALLTIME = "maxwalltime";

    // PM-962 resource requirements
    /**
     * The name of the profile key when associated with a transformation in the transformation
     * catalog gives expected runtime in seconds.
     */
    public static final String RUNTIME_KEY = "runtime";

    /**
     * The name of the key that if set in the Pegasus namespace specifies the approximate runtime of
     * the job. This key is used in while clustering jobs according to run times.
     */
    public static final String DEPRECATED_RUNTIME_KEY = "job.runtime";

    /**
     * The number of cores that are associated with the job. To be used for multiplying the job
     * runtimes accordingly.
     */
    public static final String CORES_KEY = "cores";

    /** Key indicating max diskspace used by a job a in MB */
    public static final String DISKSPACE_KEY = "diskspace";

    /** Key indicating the number of nodes a job requires */
    public static final String NODES_KEY = "nodes";

    /** Key indicating the number of processors per node to be used */
    public static final String PPN_KEY = "ppn";

    /** Key indicating max memory used by a job a in MB */
    public static final String MEMORY_KEY = "memory";

    /** The queue to use. */
    public static final String QUEUE_KEY = "queue";

    /** The project for the job to be associated with. */
    public static final String PROJECT_KEY = "project";

    /** Key indicating data configuration property. */
    public static final String DATA_CONFIGURATION_KEY = "data.configuration";

    /**
     * Users specified arguments for the local LRMS that we pass through glite and add to the
     * eventual qsub file.
     */
    public static final String GLITE_ARGUMENTS_KEY = "glite.arguments";

    /** Profile key to determine condor quoting for a job. */
    public static final String CONDOR_QUOTE_ARGUMENTS_KEY = "condor.arguments.quote";

    // credential related constant keys
    private static final String S3CFG_FILE_VARIABLE = S3CFG.S3CFG_FILE_VARIABLE.toLowerCase();
    private static final String SSH_PRIVATE_KEY_VARIABLE =
            Ssh.SSH_PRIVATE_KEY_VARIABLE.toLowerCase();
    private static final String IRODSENVFILE = Irods.IRODSENVFILE.toLowerCase();
    private static final String X509_USER_PROXY_KEY = Proxy.X509_USER_PROXY_KEY.toLowerCase();
    private static final String BOTOCONFIG = BotoConfig.BOTO_CONFIG_FILE_VARIABLE.toLowerCase();
    private static final String GOOGLEP12 = GoogleP12.GOOGLEP12_FILE_VARIABLE.toLowerCase();

    /** Static Handle to the delimiter aggregator. */
    private static Aggregator ERROR_MESSAGE_AGGREGATOR = new UniqueMerge();

    /** Static Handle to the delimiter aggregator. */
    private static Aggregator SUCCESS_MESSAGE_AGGREGATOR = new UniqueMerge();

    /**
     * The name of the implementing namespace. It should be one of the valid namespaces always.
     *
     * @see Namespace#isNamespaceValid(String)
     */
    protected String mNamespace;

    /** The table containing the mapping of the deprecated keys to the newer keys. */
    protected static Map mDeprecatedTable = null;

    private static Map<String, Aggregator> mAggregatorTable;

    /**
     * The default aggregator to be used for profile aggregation, if none specified in the
     * aggregator table;
     */
    public static Aggregator mDefaultAggregator = new Update();

    /** Initializer block that populates the Aggregator table just once. */
    static {
        mAggregatorTable = new HashMap(5);
        Aggregator max = new MAX();
        Aggregator sum = new Sum();

        mAggregatorTable.put(Pegasus.EXITCODE_FAILURE_MESSAGE, ERROR_MESSAGE_AGGREGATOR);
        mAggregatorTable.put(Pegasus.EXITCODE_SUCCESS_MESSAGE, SUCCESS_MESSAGE_AGGREGATOR);

        // all the times need to be added
        mAggregatorTable.put(Pegasus.RUNTIME_KEY, sum);
        mAggregatorTable.put(Pegasus.MAX_WALLTIME, sum);

        // for the memory rsl params we take max
        mAggregatorTable.put(Pegasus.MEMORY_KEY, max);
    }

    /**
     * The default constructor. Note that the map is not allocated memory at this stage. It is done
     * so in the overloaded construct function.
     */
    public Pegasus() {
        mProfileMap = null;
        mNamespace = NAMESPACE_NAME;
    }

    /**
     * The overloaded constructor.
     *
     * @param mp the initial map.
     */
    public Pegasus(Map mp) {
        mProfileMap = new TreeMap(mp);
        mNamespace = NAMESPACE_NAME;
    }

    /**
     * Returns the name of the namespace associated with the profile implementations.
     *
     * @return the namespace name.
     * @see #NAMESPACE_NAME
     */
    public String namespaceName() {
        return mNamespace;
    }

    /**
     * Constructs a new element of the format (key=value). It first checks if the map has been
     * initialised or not. If not then allocates memory first. It converts the key to lower case
     * before storing.
     *
     * @param key is the left-hand-side
     * @param value is the right hand side
     */
    public void construct(String key, String value) {
        if (mProfileMap == null) mProfileMap = new TreeMap();
        mProfileMap.put(key.toLowerCase(), value);
    }

    /**
     * This checks whether the key passed by the user is valid in the current namespace or not.
     *
     * @param key (left hand side)
     * @param value (right hand side)
     * @return Namespace.VALID_KEY Namespace.UNKNOWN_KEY Namespace.EMPTY_KEY Namespace.MERGE_KEY
     */
    public int checkKey(String key, String value) {
        int res = 0;

        if (key == null || key.length() < 2) {
            res = MALFORMED_KEY;
            return res;
        }

        if (value == null || value.length() < 1) {
            res = EMPTY_KEY;
            return res;
        }

        // convert key to lower case
        key = key.toLowerCase();

        switch (key.charAt(0)) {
            case 'a':
                if (key.compareTo(LOCAL_VISIBLE_KEY) == 0) {
                    res = VALID_KEY;
                } else {
                    res = UNKNOWN_KEY;
                }
                break;

            case 'b':
                if ((key.compareTo(BUNDLE_STAGE_IN_KEY) == 0)
                        || (key.compareTo(BUNDLE_STAGE_OUT_KEY) == 0)
                        || (key.compareTo(BUNDLE_REMOTE_STAGE_IN_KEY) == 0)
                        || (key.compareTo(BOTOCONFIG) == 0)) {
                    res = VALID_KEY;
                } else if (key.compareTo(DEPRECATED_BUNDLE_STAGE_IN_KEY) == 0) {
                    res = DEPRECATED_KEY;
                } else {
                    res = UNKNOWN_KEY;
                }
                break;

            case 'c':
                if ((key.compareTo(BUNDLE_KEY) == 0)
                        || (key.compareTo(COLLAPSE_KEY) == 0)
                        || (key.compareTo(CHANGE_DIR_KEY) == 0)
                        || (key.compareTo(CHAIN_STAGE_IN_KEY) == 0)
                        || (key.compareTo(MAX_RUN_TIME) == 0)
                        || (key.compareTo(CREATE_AND_CHANGE_DIR_KEY) == 0)
                        || (key.compareTo(CLUSTER_ARGUMENTS) == 0)
                        || (key.compareTo(CORES_KEY) == 0)
                        || (key.compareTo(Pegasus.CHECKPOINT_TIME_KEY) == 0)
                        || (key.compareTo(Pegasus.DEPRECATED_CHECKPOINT_TIME_KEY) == 0)
                        || (key.compareTo(Pegasus.CONDOR_QUOTE_ARGUMENTS_KEY) == 0)) {
                    res = VALID_KEY;
                } else if (key.compareTo(DEPRECATED_CHANGE_DIR_KEY) == 0
                        || (key.compareTo(COLLAPSER_KEY) == 0)) {
                    res = DEPRECATED_KEY;
                } else {
                    res = UNKNOWN_KEY;
                }
                break;

            case 'd':
                if ((key.compareTo(DATA_CONFIGURATION_KEY) == 0)
                        || (key.compareTo(DISKSPACE_KEY) == 0)) {
                    res = VALID_KEY;
                } else {
                    res = UNKNOWN_KEY;
                }
                break;

            case 'e':
                if ((key.compareTo(Pegasus.EXITCODE_FAILURE_MESSAGE) == 0)
                        || (key.compareTo(Pegasus.EXITCODE_SUCCESS_MESSAGE) == 0)) {
                    res = MERGE_KEY;
                } else if ((key.compareTo(Pegasus.ENABLE_FOR_DATA_REUSE_KEY) == 0)) {
                    res = VALID_KEY;
                } else {
                    res = UNKNOWN_KEY;
                }
                break;

            case 'g':
                if (key.compareTo(GROUP_KEY) == 0
                        || key.compareTo(GRIDSTART_KEY) == 0
                        || key.compareTo(GRIDSTART_PATH_KEY) == 0
                        || key.compareTo(GRIDSTART_ARGUMENTS_KEY) == 0
                        || key.compareTo(GRIDSTART_LAUNCHER_KEY) == 0
                        || key.compareTo(GRIDSTART_LAUNCHER_ARGUMENTS_KEY) == 0
                        || key.compareTo(GOOGLEP12) == 0
                        || key.compareTo(GLITE_ARGUMENTS_KEY) == 0) {
                    res = VALID_KEY;
                } else {
                    res = UNKNOWN_KEY;
                }
                break;

            case 'i':
                if (key.compareTo(IRODSENVFILE) == 0) {
                    res = VALID_KEY;
                } else {
                    res = UNKNOWN_KEY;
                }
                break;

            case 'j':
                if (key.compareTo(DEPRECATED_RUNTIME_KEY) == 0) {
                    res = DEPRECATED_KEY;
                } else if (key.compareTo(JOB_AGGREGATOR_KEY) == 0) {
                    res = VALID_KEY;
                } else {
                    res = UNKNOWN_KEY;
                }
                break;

            case 'l':
                if (key.compareTo(LABEL_KEY) == 0) {
                    res = VALID_KEY;
                } else {
                    res = UNKNOWN_KEY;
                }
                break;

            case 'm':
                if (key.compareTo(MAX_WALLTIME) == 0 || key.compareTo(MEMORY_KEY) == 0) {
                    res = VALID_KEY;
                } else {
                    res = UNKNOWN_KEY;
                }
                break;

            case 'n':
                if (key.compareTo(NODES_KEY) == 0) {
                    res = VALID_KEY;
                } else {
                    res = UNKNOWN_KEY;
                }
                break;

            case 'p':
                if (key.compareTo(PMC_REQUEST_MEMORY_KEY) == 0
                        || key.compareTo(PMC_REQUEST_CPUS_KEY) == 0
                        || key.compareTo(PMC_PRIORITY_KEY) == 0
                        || key.compareTo(PMC_TASK_ARGUMENTS) == 0
                        || key.compareTo(PPN_KEY) == 0
                        || key.compareTo(PROJECT_KEY) == 0) {
                    res = VALID_KEY;
                } else {
                    res = UNKNOWN_KEY;
                }
                break;

            case 'q':
                if (key.compareTo(QUEUE_KEY) == 0) {
                    res = VALID_KEY;
                } else {
                    res = UNKNOWN_KEY;
                }
                break;

            case 'r':
                if (key.compareTo(RUNTIME_KEY) == 0) {
                    res = VALID_KEY;
                } else {
                    res = UNKNOWN_KEY;
                }
                break;

            case 's':
                if (key.compareTo(STYLE_KEY) == 0
                        || key.compareTo(S3CFG_FILE_VARIABLE) == 0
                        || key.compareTo(SSH_PRIVATE_KEY_VARIABLE) == 0
                        || key.compareTo(CLUSTER_STAGE_IN_KEY) == 0
                        || key.compareTo(CLUSTER_REMOTE_STAGE_IN_KEY) == 0
                        || key.compareTo(CLUSTER_LOCAL_STAGE_IN_KEY) == 0
                        || key.compareTo(CLUSTER_STAGE_OUT_KEY) == 0
                        || key.compareTo(CLUSTER_REMOTE_STAGE_OUT_KEY) == 0
                        || key.compareTo(CLUSTER_LOCAL_STAGE_OUT_KEY) == 0) {
                    res = VALID_KEY;
                } else {
                    res = UNKNOWN_KEY;
                }

                break;

            case 't':
                if ((key.compareTo(TRANSFER_PROXY_KEY) == 0)
                        || (key.compareTo(TRANSFER_ARGUMENTS_KEY) == 0)
                        || (key.compareTo(TRANSFER_THREADS_KEY) == 0)
                        || (key.compareTo(TRANSFER_SLS_ARGUMENTS_KEY) == 0)
                        || (key.compareTo(TRANSFER_SLS_THREADS_KEY) == 0)
                        || (key.compareTo(TYPE_KEY) == 0)) {
                    res = VALID_KEY;
                } else {
                    res = UNKNOWN_KEY;
                }
                break;

            case 'w':
                if ((key.compareTo(REMOTE_INITIALDIR_KEY) == 0)
                        || (key.compareTo(WORKER_NODE_DIRECTORY_KEY) == 0)) {
                    res = VALID_KEY;
                } else {
                    res = UNKNOWN_KEY;
                }
                break;

            case 'x':
                if (key.compareTo(X509_USER_PROXY_KEY) == 0) {
                    res = VALID_KEY;
                } else {
                    res = UNKNOWN_KEY;
                }
                break;

            default:
                res = UNKNOWN_KEY;
        }

        return res;
    }

    /**
     * It puts in the namespace specific information specified in the properties file into the
     * namespace. The name of the pool is also passed, as many of the properties specified in the
     * properties file are on a per pool basis. This is used to load the appropriate collapser for
     * the job. Any preexisting profile is preferred over the one in the property file.
     *
     * @param properties the <code>PegasusProperties</code> object containing all the properties
     *     that the user specified at various places (like .chimerarc, properties file, command
     *     line).
     * @param pool the pool name where the job is scheduled to run.
     * @see #COLLAPSER_KEY
     * @see #TRANSFER_PROXY_KEY
     */
    public void checkKeyInNS(PegasusProperties properties, String pool) {

        this.assimilate(properties, Profiles.NAMESPACES.pegasus);
    }

    /**
     * Merges key value to an existing value if it exists
     *
     * @param key
     * @param value
     */
    public void mergeKey(String key, String value) {
        /*String existing = this.getStringValue(key);
        if( key.equals( Pegasus.EXITCODE_FAILURE_MESSAGE) ||  key.equals( Pegasus.EXITCODE_SUCCESS_MESSAGE) ){
             existing = ( existing == null )?
                          value:
                          existing  + UniqueMerge.DEFAULT_DELIMITER + value;
             this.construct(key, existing);
        }*/

        if (key.equals(Pegasus.EXITCODE_FAILURE_MESSAGE)) {
            this.construct(key, ERROR_MESSAGE_AGGREGATOR.compute((String) get(key), value, null));
        } else if (key.equals(Pegasus.EXITCODE_SUCCESS_MESSAGE)) {
            this.construct(key, SUCCESS_MESSAGE_AGGREGATOR.compute((String) get(key), value, null));
        } else {
            throw new RuntimeException(
                    "Merge operation not supported for pegasus profile key " + key);
        }
    }

    /**
     * UniqueMerge the profiles in the namespace in a controlled manner. In case of intersection,
     * the new profile value (except for key runtime where the values are summed ) overrides, the
     * existing profile value.
     *
     * @param profiles the <code>Namespace</code> object containing the profiles.
     */
    public void merge(Namespace profiles) {
        // check if we are merging profiles of same type
        if (!(profiles instanceof Pegasus)) {
            // throw an error
            throw new IllegalArgumentException("Profiles mismatch while merging");
        }
        String key;
        for (Iterator it = profiles.getProfileKeyIterator(); it.hasNext(); ) {
            // construct directly. bypassing the checks!
            key = (String) it.next();

            Aggregator agg = this.aggregator(key);
            // load the appropriate aggregator to merge the profiles
            this.construct(key, agg.compute((String) get(key), (String) profiles.get(key), "0"));
        }
    }

    /**
     * Returns the aggregator to be used for the profile key while merging. If no aggregator is
     * found, the then default Aggregator (Update) is used.
     *
     * @param key the key for which the aggregator is found.
     * @return the aggregator for the profile key.
     */
    protected Aggregator aggregator(String key) {
        Object aggregator = this.mAggregatorTable.get(key);
        return (aggregator == null) ? mDefaultAggregator : (Aggregator) aggregator;
    }

    /**
     * Singleton access to the deprecated table that holds the deprecated keys, and the keys that
     * replace them.
     *
     * @return Map
     */
    public java.util.Map deprecatedTable() {
        if (mDeprecatedTable == null) {
            // only initialize once and only once, as needed.
            mDeprecatedTable = new java.util.TreeMap();
            mDeprecatedTable.put(DEPRECATED_BUNDLE_STAGE_IN_KEY, BUNDLE_STAGE_IN_KEY);
            mDeprecatedTable.put(DEPRECATED_CHANGE_DIR_KEY, CHANGE_DIR_KEY);
            mDeprecatedTable.put(DEPRECATED_RUNTIME_KEY, RUNTIME_KEY);
        }

        return mDeprecatedTable;
    }

    /**
     * Converts the contents of the map into the string that can be put in the Condor file for
     * printing.
     *
     * @return the textual description.
     */
    public String toCondor() {
        StringBuffer st = new StringBuffer();
        String key = null;
        String value = null;
        if (mProfileMap == null) return "";

        Iterator it = mProfileMap.keySet().iterator();
        while (it.hasNext()) {
            key = (String) it.next();
            value = (String) mProfileMap.get(key);
            st.append(key).append(" = ").append(value).append("\n");
        }

        return st.toString();
    }

    /**
     * Warns about an unknown profile key and constructs it anyway. Constructs a new RSL element of
     * the format (key=value).
     *
     * @param key is the left-hand-side
     * @param value is the right hand side
     */
    public void unknownKey(String key, String value) {
        // mLogger.log("unknown profile " + mNamespace + "." + key +
        //            ",  using anyway", LogManager.DEBUG_MESSAGE_LEVEL);

        // Starting 4.4 unknown keys are not added to jobs. this will help
        // in reducing the number of profiles associated with the jobs, as
        // not all pegasus properties are handled as profiles.
        // construct(key, value);

    }

    /**
     * Returns true if the namespace contains a mapping for the specified key. More formally,
     * returns true if and only if this map contains at a mapping for a key k such that (key==null ?
     * k==null : key.equals(k)). (There can be at most one such mapping.) It also returns false if
     * the map does not exist.
     *
     * @param key The key that you want to search for in the namespace.
     * @return boolean
     */
    public boolean containsKey(Object key) {
        return (mProfileMap == null) ? false : mProfileMap.containsKey(key);
    }

    /**
     * Returns the value to which this namespace maps the specified key. Returns null if the map
     * contains no mapping for this key. A return value of null does not necessarily indicate that
     * the map contains no mapping for the key; it's also possible that the map explicitly maps the
     * key to null. The containsKey operation may be used to distinguish these two cases.
     *
     * @param key The key whose value you want.
     * @return the object
     */
    public Object get(Object key) {
        return (mProfileMap == null) ? null : mProfileMap.get(key);
    }

    /**
     * Returns a int value, that a particular key is mapped to in this namespace. If the key is
     * mapped to a non integer, then the default value is returned
     *
     * @param key The key whose boolean value you desire.
     * @return boolean
     */
    public boolean getBooleanValue(Object key) {
        boolean value = false;
        if (mProfileMap != null && mProfileMap.containsKey(key)) {
            value = Boolean.valueOf((String) mProfileMap.get(key)).booleanValue();
        }
        return value;
    }

    /**
     * Returns a String value, that a particular key is mapped to in this namespace. If is not
     * populated in the namespace null is returned.
     *
     * @param key The key whose boolean value you desire.
     * @return String if key is in the namespace null otherwise.
     */
    public String getStringValue(Object key) {

        return containsKey(key) ? get(key).toString() : null;
    }

    /**
     * Returns a copy of the current namespace object
     *
     * @return the Cloned object
     */
    public Object clone() {
        return (mProfileMap == null) ? new Pegasus() : new Pegasus(this.mProfileMap);
    }
}
