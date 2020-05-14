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

import static edu.isi.pegasus.planner.code.generator.condor.style.Condor.WHEN_TO_TRANSFER_OUTPUT_KEY;

import edu.isi.pegasus.planner.catalog.classes.Profiles;
import edu.isi.pegasus.planner.classes.Profile;
import edu.isi.pegasus.planner.common.PegasusProperties;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * This helper class helps in handling the arguments specified in the Condor namespace by the user
 * either through dax or through profiles in pool.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class Condor extends Namespace {

    /** The name of the namespace that this class implements. */
    public static final String NAMESPACE_NAME = Profile.CONDOR;

    private static Map<String, String> mClassAdToPegasus;

    /**
     * Maps Globus RSL keys to corresponding Pegasus Profile Keys
     *
     * @return
     */
    public static Map<String, String> classAdKeysToPegasusProfiles() {
        if (mClassAdToPegasus == null) {
            mClassAdToPegasus = new HashMap();
            mClassAdToPegasus.put(Condor.REQUEST_MEMORY_KEY, Pegasus.MEMORY_KEY);
            mClassAdToPegasus.put(Condor.REQUEST_CPUS_KEY, Pegasus.CORES_KEY);
            mClassAdToPegasus.put(Condor.REQUEST_DISK_KEY, Pegasus.DISKSPACE_KEY);
        }
        return mClassAdToPegasus;
    }

    /** The name of the key that denotes the arguments of the job. */
    public static final String ARGUMENTS_KEY = "arguments";

    /** The queue to be used when using batch gahp. */
    public static final String BATCH_QUEUE_KEY = "batch_queue";

    /** The name of the key that denotes the executable of the job. */
    public static final String EXECUTABLE_KEY = "executable";

    /** The name of the key that denotes the requirements of the job. */
    public static final String REQUIREMENTS_KEY = "requirements";

    /** The name of the key that denotes the condor universe key. */
    public static final String UNIVERSE_KEY = "universe";

    /** The name of the key that denotes the remote condor universe key. */
    public static final String REMOTE_UNIVERSE_KEY = "remote_universe";

    /**
     * The name of the key that denotes the File System Domain. Is actually propogated to the
     * expression for the Requirements Key.
     *
     * @see #REQUIREMENTS_KEY
     */
    public static final String FILE_SYSTEM_DOMAIN_KEY = "filesystemdomain";

    /** The name of the key that specifies the grid job type. */
    public static final String GRID_JOB_TYPE_KEY = "grid_type";

    /** The name of the key that specifies the jobmanager type. */
    public static final String JOBMANAGER_TYPE_KEY = "jobmanager_type";

    /**
     * The name of the key that designates that files should be transferred via Condor File Transfer
     * mechanism.
     */
    public static final String SHOULD_TRANSFER_FILES_KEY = "should_transfer_files";

    /**
     * The corresponding remote kye name that designates that files should be transferred via Condor
     * File Transfer mechanism.
     */
    public static final String REMOTE_SHOULD_TRANSFER_FILES_KEY = "+remote_ShouldTransferFiles";

    /** The name of key that designates when to transfer output. */
    public static final String WHEN_TO_TRANSFER_OUTPUT_KEY = "when_to_transfer_output";

    /** The corresponding name of the remote key that designated when to transfer output. */
    public static final String REMOTE_WHEN_TO_TRANSFER_OUTPUT_KEY = "+remote_WhenToTransferOutput";

    /** The name of the key that specifies whether to stream stderr or not */
    public static final String STREAM_STDERR_KEY = "stream_error";

    /** The name of the key that specifies whether to stream stderr or not */
    public static final String STREAM_STDOUT_KEY = "stream_output";

    /** The name of the key that specifies transfer of input files. */
    public static final String TRANSFER_IP_FILES_KEY = "transfer_input_files";

    /** The name of the key that specifies transfer of input files. */
    public static final String TRANSFER_OP_FILES_KEY = "transfer_output_files";

    /** The name of the key that specifies transfer of executable */
    public static final String TRANSFER_EXECUTABLE_KEY = "transfer_executable";

    /** The name of the key that specifies the priority for the job. */
    public static final String PRIORITY_KEY = "priority";

    /** The name of the key that specifies the periodic release */
    public static final String PERIODIC_RELEASE_KEY = "periodic_release";

    /** The name of the key that specifies the periodic remove */
    public static final String PERIODIC_REMOVE_KEY = "periodic_remove";

    /** The name of the key that specifies the periodic remove */
    public static final String PERIODIC_HOLD = "periodic_hold";

    /** The condor key for designation the grid_resource. */
    public static final String GRID_RESOURCE_KEY = "grid_resource";

    /** The key that designates the collector associated with the job. */
    public static final String COLLECTOR_KEY = "condor_collector";

    /** The key that overrides the default x509 proxy location. */
    public static final String X509USERPROXY_KEY = "x509userproxy";

    // new condor keys starting 7.8.0
    /** The Condor Key designating the numnber of cpu's to request. */
    public static final String REQUEST_CPUS_KEY = "request_cpus";

    /** The Condor Key designating the numnber of cpu's to request. */
    public static final String REQUEST_GPUS_KEY = "request_gpus";

    /** The Condor Key designating the amount of memory to request. */
    public static final String REQUEST_MEMORY_KEY = "request_memory";

    /** The Condor Key designating the amount of disk to request. */
    public static final String REQUEST_DISK_KEY = "request_disk";

    /** The condor universe key value for vanilla universe. */
    public static final String VANILLA_UNIVERSE = "vanilla";

    /** The condor universe key value for grid universe. */
    public static final String GRID_UNIVERSE = "grid";

    /** The condor key for using the local environment */
    public static final String GET_ENV_KEY = "getenv";

    /** The condor universe key value for vanilla universe. */
    public static final String GLOBUS_UNIVERSE = "globus";

    /** The condor universe key value for scheduler universe. */
    public static final String SCHEDULER_UNIVERSE = "scheduler";

    /** The condor universe key value for standard universe. */
    public static final String STANDARD_UNIVERSE = "standard";

    /** The condor universe key value for local universe. */
    public static final String LOCAL_UNIVERSE = "local";

    /** The condor universe key value for parallel universe. */
    public static final String PARALLEL_UNIVERSE = "parallel";

    /** concurrency limits key */
    public static String CONCURRENCY_LIMITS_KEY = "concurrency_limits";

    public String USE_USER_X509_USER_PROXY = "use_x509userproxy";

    /**
     * The name of the implementing namespace. It should be one of the valid namespaces always.
     *
     * @see Namespace#isNamespaceValid(String)
     */
    protected String mNamespace;

    /** The default constructor. */
    public Condor() {
        mProfileMap = new TreeMap();
        mNamespace = NAMESPACE_NAME;
    }

    /**
     * The overloaded constructor
     *
     * @param mp map containing the profile keys.
     */
    public Condor(Map mp) {
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
     * Returns a comma separated list of files that are designated for transfer via condor file
     * transfer mechanism for the job.
     *
     * @return a csv file else null
     */
    public String getIPFilesForTransfer() {
        return (this.containsKey(Condor.TRANSFER_IP_FILES_KEY))
                ? (String) this.get(Condor.TRANSFER_IP_FILES_KEY)
                : null;
    }

    /**
     * Returns a comma separated list of files that are designated for transfer via condor file
     * transfer mechanism for the job.
     *
     * @return a csv file else null
     */
    public String getOutputFilesForTransfer() {
        return (this.containsKey(Condor.TRANSFER_OP_FILES_KEY))
                ? (String) this.get(Condor.TRANSFER_OP_FILES_KEY)
                : null;
    }

    /**
     * Remove the input files that were designated for transfer using Condor File Transfer
     * Mechanism.
     */
    public void removeIPFilesForTransfer() {
        Object obj = this.removeKey(Condor.TRANSFER_IP_FILES_KEY);
        if (obj != null) {
            // delete stf and wto only if no output files tx
            // and transfer_executable is not set
            if (!this.containsKey(Condor.TRANSFER_OP_FILES_KEY)
                    && !this.containsKey(Condor.TRANSFER_EXECUTABLE_KEY)) {
                this.removeKey("should_transfer_files");
                this.removeKey("when_to_transfer_output");
            }
        }
    }

    /**
     * Remove the output files that were designated for transfer using Condor File Transfer
     * Mechanism.
     */
    public void removeOutputFilesForTransfer() {
        Object obj = this.removeKey(Condor.TRANSFER_OP_FILES_KEY);
        if (obj != null) {
            // delete stf and wto only if no output files tx
            // and transfer_executable is not set
            if (!this.containsKey(Condor.TRANSFER_IP_FILES_KEY)
                    && !this.containsKey(Condor.TRANSFER_EXECUTABLE_KEY)) {
                this.removeKey("should_transfer_files");
                this.removeKey("when_to_transfer_output");
            }
        }
    }

    /** Adds the executable for transfer via the condor file transfer mechanism. */
    public void setExecutableForTransfer() {
        this.construct(Condor.TRANSFER_EXECUTABLE_KEY, "true");
        this.construct("should_transfer_files", "YES");
        this.constructWhenToTransferOutput();
    }

    /**
     * Adds multiple files that are to be transferred from the submit host via the Condor File
     * Transfer Mechanism. It also sets the associated condor keys like when_to_transfer and
     * should_transfer_files.
     *
     * @param file the path to the file on the submit host.
     */
    public void addIPFileForTransfer(Collection<String> files) {

        this.addFilesForTransfer(files, Condor.TRANSFER_IP_FILES_KEY);
    }

    /**
     * Adds an input file that is to be transferred from the submit host via the Condor File
     * Transfer Mechanism. It also sets the associated condor keys like when_to_transfer and
     * should_transfer_files.
     *
     * @param file the path to the file on the submit host.
     */
    public void addIPFileForTransfer(String file) {
        this.addFilesForTransfer(file, Condor.TRANSFER_IP_FILES_KEY);
    }

    /**
     * Adds multiple output files that are to be transferred from the submit host via the Condor
     * File Transfer Mechanism. It also sets the associated condor keys like when_to_transfer and
     * should_transfer_files.
     *
     * @param file the path to the file on the submit host.
     */
    public void addOPFileForTransfer(Collection<String> files) {

        this.addFilesForTransfer(files, Condor.TRANSFER_OP_FILES_KEY);
    }

    /**
     * Adds an output file that is to be transferred from the submit host via the Condor File
     * Transfer Mechanism. It also sets the associated condor keys like when_to_transfer and
     * should_transfer_files.
     *
     * @param file the path to the file on the submit host.
     */
    public void addOPFileForTransfer(String file) {
        this.addFilesForTransfer(file, Condor.TRANSFER_OP_FILES_KEY);
    }

    /**
     * Adds multiple files that are to be transferred from the submit host via the Condor File
     * Transfer Mechanism. It also sets the associated condor keys like when_to_transfer and
     * should_transfer_files.
     *
     * @param file the path to the file on the submit host.
     * @param key the name of the Condor key to be added
     */
    public void addFilesForTransfer(Collection<String> files, String key) {
        // sanity check
        if (files == null || files.isEmpty()) {
            return;
        }

        StringBuffer sb = new StringBuffer();
        for (String f : files) {
            sb.append(f).append(",");
        }
        String existing;
        // check if the key is already set.
        if (this.containsKey(key)) {
            // update the existing list.
            existing = (String) this.get(key);
            sb.append(existing);
        } else {
            // set the additional keys only once
            this.construct("should_transfer_files", "YES");
            this.constructWhenToTransferOutput();
        }
        // remove any trailing ,
        int lastIndex = sb.length() - 1;
        String addOn =
                (sb.lastIndexOf(",") == lastIndex) ? sb.substring(0, lastIndex) : sb.toString();

        this.construct(key, addOn);
    }

    /**
     * Adds an input file that is to be transferred from the submit host via the Condor File
     * Transfer Mechanism. It also sets the associated condor keys like when_to_transfer and
     * should_transfer_files.
     *
     * @param file the path to the file on the submit host.
     * @param key the name of the Condor key
     */
    public void addFilesForTransfer(String file, String key) {
        // sanity check
        if (file == null || file.length() == 0) {
            return;
        }
        String files;
        // check if the key is already set.
        if (this.containsKey(key)) {
            // update the existing list.
            files = (String) this.get(key);

            if (files.charAt(files.length() - 1) == ',') {
                files = files + file;
            } else {
                files = files + "," + file;
            }
        } else {
            files = file;
            // set the additional keys only once
            this.construct("should_transfer_files", "YES");
            this.constructWhenToTransferOutput();
        }
        this.construct(key, files);
    }

    /** Construct when to transfer output key */
    private void constructWhenToTransferOutput() {
        String wtto = (String) this.get(Condor.WHEN_TO_TRANSFER_OUTPUT_KEY);
        if (wtto == null) {
            // default value
            this.construct(Condor.WHEN_TO_TRANSFER_OUTPUT_KEY, "ON_EXIT");
        } else {
            // PM-1350 prefer the value specified by the user
            this.construct(WHEN_TO_TRANSFER_OUTPUT_KEY, wtto);
        }
    }

    /**
     * Additional method to handle the Condor namespace with convenience mappings. Currently the
     * following keys are not supported keys as they clash with Pegasus internals
     *
     * <pre>
     * accounting_group - OK
     * arguments	- not supported, got from the arguments tag in DAX
     * batch_queue      - the batch queue to be used
     * copy_to_spool    - supported, limited to LCG sites at present where one needs
     *                    to stage in the kickstart. Pegasus sets it to false by default
     *                    for arch start stuff on the local pool, unless the user
     *                    overrides it.
     * environment	- not supported, use env namespace fpr this
     * executable       - not supported, this is got from the transformation catalog
     * FileSystemDomain - supported, but is propogated to the classad expression
     *                    for requirements.
     * globusscheduler  - not supported, Pegasus determines this on the basis of
     *                    it's planning strategy
     * globusrsl        - not supported, rsl to populated through Globus namespace.
     * grid_type        - OK (like gt2, gt4, condor)
     * grid_resource    - supported . used for glite
     * getevn           - OK
     * log              - not supported, as it has to be same for the whole dag
     * notification     - OK
     * noop_job         - OK (used for synchronizing jobs in graph)
     * noop_job_exit_signal - OK
     * noop_job_exit_code - OK
     * periodic_release - OK
     * periodic_remove  - OK
     * periodic_hold  - OK
     * priority         - OK
     * queue		- required thing. always added
     * remote_initialdir- not allowed, the working directory is picked up from
     *                    pool file and properties file
     * request_cpus     - number of cpu's required. New in Condor 7.8.0
     * request_memory   - amount of memory required . New in Condor 7.8.0
     * request_disk     - amount of disk required. New in Condor 7.8.0.
     * stream_error     -  supported,however it is applicable only for globus jobs.
     *
     * stream_output    -  supported, however it is applicable only for globus jobs.
     *
     * transfer_executable  - supported, limited to LCG sites at present where one needs
     *                        to stage in the kickstart.
     * transfer_input_files - supported, especially used to transfer proxies in
     *                        case of glide in pools.
     * universe         - supported, especially used to incorporate glide in pools.
     * x509userpoxy     - supported, overrides x509 default proxy and proxy transfers in
     *                    for glideins and vanilla jobs
     * +xxxx            - supported, this is used to add extra classads with the jobs.
     * </pre>
     *
     * @param key is the key within the globus namespace, must be lowercase!
     * @param value is the value for the given key.
     * @return MALFORMED_KEY VALID_KEY UNKNOWN_KEY NOT_PERMITTED_KEY DEPRECATED_KEY EMPTY_KEY
     */
    public int checkKey(String key, String value) {
        // sanity checks first
        int res = 0;

        if (key == null || key.length() < 2) {
            res = MALFORMED_KEY;
            return res;
        }

        if (value == null || value.length() < 1) {
            res = EMPTY_KEY;
            return res;
        }

        // before checking convert the key to lower case
        key = key.toLowerCase();

        switch (key.charAt(0)) {
            case 'a':
                if (key.compareTo("accounting_group") == 0) {
                    res = VALID_KEY;
                } else if (key.compareTo("arguments") == 0) {
                    res = NOT_PERMITTED_KEY;
                } else {
                    res = UNKNOWN_KEY;
                }
                break;

            case 'b':
                if (key.compareTo(BATCH_QUEUE_KEY) == 0) {
                    res = VALID_KEY;
                } else {
                    res = UNKNOWN_KEY;
                }
                break;

            case 'c':
                if (key.compareTo("copy_to_spool") == 0) {
                    res = VALID_KEY;
                } else if (key.compareTo("concurrency_limits") == 0) {
                    res = VALID_KEY;
                } else if (key.compareTo("cream_attributes") == 0) {
                    res = VALID_KEY;
                } else if (key.compareTo(COLLECTOR_KEY) == 0) {
                    res = VALID_KEY;
                } else {
                    res = UNKNOWN_KEY;
                }
                break;

            case 'e':
                if (key.compareTo("environment") == 0 || key.compareTo("executable") == 0) {
                    res = NOT_PERMITTED_KEY;
                } else {
                    res = UNKNOWN_KEY;
                }
                break;

            case 'f':
                // want to preserve case
                if (key.compareTo(FILE_SYSTEM_DOMAIN_KEY) == 0) {
                    res = VALID_KEY;
                } else {
                    res = UNKNOWN_KEY;
                }
                break;

            case 'g':
                if (key.compareTo(GRID_JOB_TYPE_KEY) == 0) {
                    res = VALID_KEY;
                } else if (key.compareTo(GET_ENV_KEY) == 0) {
                    res = VALID_KEY;
                } else if (key.compareTo(GRID_RESOURCE_KEY) == 0) {
                    res = VALID_KEY;
                } else if (key.compareTo("globusscheduler") == 0
                        || key.compareTo("globusrsl") == 0) {
                    res = NOT_PERMITTED_KEY;
                } else {
                    res = UNKNOWN_KEY;
                }
                break;

            case 'j':
                if (key.compareTo(JOBMANAGER_TYPE_KEY) == 0) {
                    res = VALID_KEY;
                } else {
                    res = UNKNOWN_KEY;
                }
                break;

            case 'l':
                if (key.compareTo("log") == 0) {
                    res = NOT_PERMITTED_KEY;
                } else {
                    res = UNKNOWN_KEY;
                }
                break;

            case 'n':
                if (key.compareTo("notification") == 0
                        || key.compareTo("noop_job") == 0
                        || key.compareTo("noop_job_exit_code") == 0
                        || key.compareTo("noop_job_exit_signal") == 0) {

                    res = VALID_KEY;
                } else {
                    res = UNKNOWN_KEY;
                }
                break;

            case 'p':
                if (key.compareTo(Condor.PRIORITY_KEY) == 0
                        || key.compareTo(Condor.PERIODIC_RELEASE_KEY) == 0
                        || key.compareTo(Condor.PERIODIC_REMOVE_KEY) == 0
                        || key.compareTo(Condor.PERIODIC_HOLD) == 0) {
                    res = VALID_KEY;
                } else {
                    res = UNKNOWN_KEY;
                }
                break;

            case 'q':
                if (key.compareTo("queue") == 0) {
                    res = NOT_PERMITTED_KEY;
                } else {
                    res = UNKNOWN_KEY;
                }
                break;

            case 'r':
                if (key.compareTo("remote_initialdir") == 0) {
                    res = NOT_PERMITTED_KEY;
                } else if (key.compareTo("requirements") == 0) {
                    res = VALID_KEY;
                } else if (key.compareTo("rank") == 0) {
                    res = VALID_KEY;
                } else if (key.compareTo(Condor.REQUEST_CPUS_KEY) == 0
                        || key.compareTo(Condor.REQUEST_GPUS_KEY) == 0
                        || key.compareTo(Condor.REQUEST_MEMORY_KEY) == 0
                        || key.compareTo(Condor.REQUEST_DISK_KEY) == 0) {
                    res = VALID_KEY;
                } else if (key.compareTo(Condor.REMOTE_UNIVERSE_KEY) == 0) {
                    res = VALID_KEY;
                } else {
                    res = UNKNOWN_KEY;
                }
                break;

            case 's':
                if (key.compareTo("stream_error") == 0 || key.compareTo("stream_output") == 0) {
                    res = VALID_KEY;
                } else if (key.compareTo("should_transfer_files") == 0) {
                    res = VALID_KEY;
                } else {
                    res = UNKNOWN_KEY;
                }
                break;

            case 't':
                if (key.compareTo(TRANSFER_EXECUTABLE_KEY) == 0
                        || key.compareTo(TRANSFER_IP_FILES_KEY) == 0) {
                    res = VALID_KEY;
                } else if (key.compareTo("transfer_output") == 0) {
                    res = VALID_KEY;
                } else if (key.compareTo("transfer_error") == 0) {
                    res = VALID_KEY;
                } else {
                    res = UNKNOWN_KEY;
                }
                break;

            case 'u':
                if (key.compareTo(UNIVERSE_KEY) == 0) {
                    res = VALID_KEY;
                } else if (key.compareTo(USE_USER_X509_USER_PROXY) == 0) {
                    res = VALID_KEY;
                } else {
                    res = UNKNOWN_KEY;
                }
                break;

            case 'w':
                if (key.compareTo(Condor.WHEN_TO_TRANSFER_OUTPUT_KEY) == 0) {
                    res = VALID_KEY;
                } else {
                    res = UNKNOWN_KEY;
                }
                break;

            case 'x':
                if (key.compareTo(Condor.X509USERPROXY_KEY) == 0) {
                    res = VALID_KEY;
                } else {
                    res = UNKNOWN_KEY;
                }
                break;

            case '+':
                res = VALID_KEY;
                break;

            default:
                res = UNKNOWN_KEY;
        }

        return res;
    }

    /**
     * It puts in the namespace specific information specified in the properties file into the
     * namespace. The name of the pool is also passed, as many of the properties specified in the
     * properties file are on a per pool basis. It handles the periodic_remove and periodic_release
     * characteristics for condor jobs.
     *
     * @param properties the <code>PegasusProperties</code> object containing all the properties
     *     that the user specified at various places (like .chimerarc, properties file, command
     *     line).
     * @param pool the pool name where the job is scheduled to run.
     */
    public void checkKeyInNS(PegasusProperties properties, String pool) {
        // retrieve the relevant profiles from properties
        // and merge them into the existing.
        this.assimilate(properties, Profiles.NAMESPACES.condor);
    }

    /**
     * Returns a boolean value indicating if the string passed is an integer or not
     *
     * @param value the value
     * @return boolean
     */
    public boolean isInteger(String value) {
        boolean result = true;

        try {
            Integer.parseInt(value);
        } catch (Exception e) {
            result = false;
        }
        return result;
    }

    /**
     * This checks the whether a key value pair specified is valid in the current namespace or not
     * by calling the checkKey function and then on the basis of the values returned puts them into
     * the associated map in the class. In addition it transfers the FILE_SYSTEM_DOMAIN_KEY to the
     * REQUIREMENTS_KEY.
     *
     * @param key key that needs to be checked in the namespace for validity.
     * @param value value of the key
     */
    public void checkKeyInNS(String key, String value) {
        int rslVal = checkKey(key, value);

        switch (rslVal) {
            case Namespace.MALFORMED_KEY:
                // key is malformed ignore
                malformedKey(key, value);
                break;

            case Namespace.NOT_PERMITTED_KEY:
                notPermitted(key);
                break;

            case Namespace.UNKNOWN_KEY:
                unknownKey(key, value);
                break;

            case Namespace.VALID_KEY:
                if (key.equalsIgnoreCase(FILE_SYSTEM_DOMAIN_KEY)) {
                    // set it to the REQUIREMENTS_KEY
                    key = REQUIREMENTS_KEY;
                    // construct the classad expression
                    value = FILE_SYSTEM_DOMAIN_KEY + " == " + "\"" + value + "\"";
                }
                construct(key, value);
                break;

            case Namespace.EMPTY_KEY:
                emptyKey(key);
                break;
        }
    }

    /**
     * Merge the profiles in the namespace in a controlled manner. In case of intersection, the new
     * profile value overrides, the existing profile value.
     *
     * @param profiles the <code>Namespace</code> object containing the profiles.
     */
    public void merge(Namespace profiles) {
        // check if we are merging profiles of same type
        if (!(profiles instanceof Condor)) {
            // throw an error
            throw new IllegalArgumentException("Profiles mismatch while merging");
        }
        String key, value;
        for (Iterator it = profiles.getProfileKeyIterator(); it.hasNext(); ) {
            // construct directly. bypassing the checks!
            key = (String) it.next();
            value = (String) profiles.get(key);

            // override only if key is not transfer_ip_files
            if (key.equals(this.TRANSFER_IP_FILES_KEY)) {
                // add to existing
                this.addIPFileForTransfer(value);
            }

            // overriding the arguments makes no sense.
            if (key.equals(this.ARGUMENTS_KEY)) {
                continue;
            } else {
                this.construct(key, value);
            }
        }
    }

    /**
     * Constructs a new element of the format (key=value). All the keys are converted to lower case
     * before storing.
     *
     * @param key is the left-hand-side
     * @param value is the right hand side
     */
    public void construct(String key, String value) {
        if (key.startsWith("+")) {
            // we preserve the case
            // PM-615
        } else {
            key = key.toLowerCase();
        }

        mProfileMap.put(key, value);
    }

    /**
     * Returns a boolean value, that a particular key is mapped to in this namespace. If the key is
     * mapped to a non boolean value or the key is not populated in the namespace false is returned.
     *
     * @param key The key whose boolean value you desire.
     * @return boolean
     */
    public boolean getBooleanValue(Object key) {
        boolean value;
        if (mProfileMap.containsKey(key)) {
            value = Boolean.valueOf((String) mProfileMap.get(key)).booleanValue();
        } else {
            // the key is not in the namespace
            // return false
            return false;
        }
        return value;
    }

    /**
     * Converts the contents of the map into the string that can be put in the Condor file for
     * printing.
     *
     * @return the textual description
     */
    public String toCondor() {
        StringBuffer st = new StringBuffer();
        String key = null;
        String value = null;

        Iterator it = mProfileMap.keySet().iterator();
        while (it.hasNext()) {
            key = (String) it.next();
            value = (String) mProfileMap.get(key);
            if (value == null || value.equals("")) {
                continue;
            }
            st.append(key).append(" = ").append(value).append("\n");
        }

        return st.toString();
    }

    /**
     * Returns a copy of the current namespace object.
     *
     * @return the Cloned object
     */
    public Object clone() {
        return new Condor(this.mProfileMap);
    }
}
