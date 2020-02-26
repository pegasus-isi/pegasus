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

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.classes.Profiles;
import edu.isi.pegasus.planner.classes.Profile;
import edu.isi.pegasus.planner.common.PegasusProperties;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * This profile namespace is the placeholder for the keys that go into the .dag file . Keys like
 * RETRY that trigger retries in dagman in the event of a job failing would go in here. All the keys
 * stored in it are in UPPERCASE irrespective of the case specified by the user in the various
 * catalogs. To specify a post script or a pre script use POST and PRE keys.
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 * @version $Revision$
 */
public class Dagman extends Namespace {

    /** The name of the namespace that this class implements. */
    public static final String NAMESPACE_NAME = Profile.DAGMAN;

    /**
     * The name of the key that determines what post script is to be invoked when the job completes.
     */
    public static final String POST_SCRIPT_KEY = "POST";

    /**
     * The name of the key that determines the arguments that need to be passed to the postscript.
     */
    public static final String POST_SCRIPT_ARGUMENTS_KEY = "POST.ARGUMENTS";

    /** The key prefix that determines the path to a postscript */
    public static final String POST_SCRIPT_PATH_PREFIX = "POST.PATH";

    /** The key prefix that determines the path to a postscript */
    public static final String POST_SCRIPT_SCOPE_KEY = "POST.SCOPE";

    /** The default value for the arguments passed to postscript */
    public static final String DEFAULT_POST_SCRIPT_ARGUMENTS_KEY_VALUE = "";

    /** The name of the key that determines what pre script is to be invoked when the job is run. */
    public static final String PRE_SCRIPT_KEY = "PRE";

    /**
     * The name of the key that determines the arguments that need to be passed to the postscript.
     */
    public static final String PRE_SCRIPT_ARGUMENTS_KEY = "PRE.ARGUMENTS";

    /**
     * The name of the key that determines the file on the submit host on which postscript is to be
     * invoked.
     */
    public static final String OUTPUT_KEY = "OUTPUT";

    /** The default value for the arguments passed to prescript */
    public static final String DEFAULT_PRE_SCRIPT_ARGUMENTS_KEY_VALUE = "";

    /** The name of the key that determines how many times DAGMAN should be retrying the job. */
    public static final String RETRY_KEY = "RETRY";

    /** The default value for the JOB Retries */
    public static final String DEFAULT_RETRY_VALUE = "1";

    /** The name of the key that determines the category to which the job belongs to. */
    public static final String CATEGORY_KEY = "CATEGORY";

    /** The name of the key that determines the priority a job is assigned. */
    public static final String PRIORITY_KEY = "PRIORITY";

    /** The name of the key that indicates the path to the corresponding submit file for the job. */
    public static final String JOB_KEY = "JOB";

    /** The name of the key that indicates the path to the external subdag */
    public static final String SUBDAG_EXTERNAL_KEY = "SUBDAG EXTERNAL";

    /** The name of the key that indicates the directory in which the DAG has to be execute */
    public static final String DIRECTORY_EXTERNAL_KEY = "DIR";

    /** The name of the key that indicates the NOOP key */
    public static final String NOOP_KEY = "NOOP";

    /** The key name for the post script that is put in the .dag file. */
    private static final String POST_SCRIPT_REPLACEMENT_KEY = "SCRIPT POST";

    /** The key name for the pre script that is put in the .dag file. */
    private static final String PRE_SCRIPT_REPLACEMENT_KEY = "SCRIPT PRE";

    /** The prefix for the max keys */
    public static final String MAX_KEYS_PREFIX = "MAX";

    /** The key name for max pre setting for dagman */
    public static final String MAXPRE_KEY = "MAXPRE";

    /** The key name for max post setting for dagman */
    public static final String MAXPOST_KEY = "MAXPOST";

    /** The key name for max idle setting for dagman */
    public static final String MAXIDLE_KEY = "MAXIDLE";

    /** The key name for max jobs setting for dagman */
    public static final String MAXJOBS_KEY = "MAXJOBS";

    /**
     * The key name for triggering a job return code to abort the DAG ABORT-DAG-ON JobName
     * AbortExitValue [RETURN DAGReturnValue]
     */
    public static final String ABORT_DAG_ON_KEY = "ABORT-DAG-ON";

    /** To associate any variables that you want to reference in the job submit file */
    public static final String VARS_KEY = "VARS";

    /**
     * Determines whether a key is category related or not.
     *
     * @param key the key in question
     * @return
     */
    public static boolean categoryRelatedKey(String key) {
        boolean result = true;
        int dotIndex = -1;
        if ((dotIndex = key.indexOf(".")) != -1 && dotIndex != key.length() - 1) {
            // the key has a . in it
            if (key.equals(Dagman.POST_SCRIPT_ARGUMENTS_KEY)
                    || key.equals(Dagman.POST_SCRIPT_SCOPE_KEY)
                    || key.equals(Dagman.PRE_SCRIPT_ARGUMENTS_KEY)
                    || key.startsWith(Dagman.POST_SCRIPT_PATH_PREFIX)) {

                // these are note category related keys

                return !result;
            }
        } else {

            return !result;
        }

        return result;
    }

    /**
     * The name of the job (jobname) to which the profiles for this namespace belong.
     *
     * @see org.griphyn.cPlanner.classes.SubInfo#jobName
     */
    private String mJobName;

    /**
     * The name of the implementing namespace. It should be one of the valid namespaces always.
     *
     * @see Namespace#isNamespaceValid(String)
     */
    protected String mNamespace;

    /**
     * The default constructor. We always initialize the map, as the map is guarenteed to store at
     * least the postscript value for a job.
     */
    public Dagman() {
        mProfileMap = new TreeMap();
        mNamespace = NAMESPACE_NAME;
        mJobName = null;
    }

    /**
     * The overloaded constructor.
     *
     * @param mp the initial map containing the profile keys for this namespace.
     */
    public Dagman(Map mp) {
        this();
        mProfileMap = new TreeMap(mp);
    }

    /**
     * The overloaded constructor.
     *
     * @param mp the initial map containing the profile keys for this namespace.
     * @param name name of the job with which these profile keys are associated.
     */
    public Dagman(Map mp, String name) {
        this(mp);
        mJobName = name;
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
     * It sets the name of the job that is associated with the profiles contained in this
     * placeholder.
     *
     * @param name name of the job with which these profile keys are associated.
     */
    public void setJobName(String name) {
        mJobName = name;
    }

    /**
     * Constructs a new element of the format (key=value). The underlying map is allocated memory in
     * the constructors always. All the keys are converted to UPPER CASE before storing.
     *
     * @param key is the left-hand-side
     * @param value is the right hand side
     */
    public void construct(String key, String value) {
        // convert to uppercase the key
        mProfileMap.put(key.toUpperCase(), value);
    }

    /**
     * This checks the whether a key value pair specified is valid in the current namespace or not
     * by calling the checkKey function and then on the basis of the values returned puts them into
     * the associated map in the class.
     *
     * @param key key that needs to be checked in the namespace for validity.
     * @param value value of the key
     */
    public void checkKeyInNS(String key, String value) {

        // convert key to lower case
        key = key.toUpperCase();

        // special handling for category related keys
        if (categoryRelatedKey(key)) {
            // category related key is ignored
            mLogger.log(
                    "Dagman category related key cannot be associated with job " + key,
                    LogManager.DEBUG_MESSAGE_LEVEL);
            return;
        }

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
                construct(key, value);
                break;

            case Namespace.DEPRECATED_KEY:
                deprecatedKey(key, value);
                break;

            case Namespace.EMPTY_KEY:
                emptyKey(key);
                break;
        }
    }

    /**
     * This checks whether the key passed by the user is valid in the current namespace or not. All
     * keys are assumed valid currently.
     *
     * @param key (left hand side)
     * @param value (right hand side)
     * @return Namespace.VALID_KEY
     */
    public int checkKey(String key, String value) {
        // all are valid because of certain keys
        // are defined in SCRIPT POST, that needs
        // to be corrected
        int res = 0;
        if (key == null || key.length() < 2 || value == null || value.length() < 2) {
            res = MALFORMED_KEY;
        }

        switch (key.charAt(0)) {
            case 'A':
                if (key.compareTo(Dagman.ABORT_DAG_ON_KEY) == 0) {
                    res = VALID_KEY;
                } else {
                    res = NOT_PERMITTED_KEY;
                }
                break;

            case 'C':
                if (key.compareTo(Dagman.CATEGORY_KEY) == 0) {
                    res = VALID_KEY;
                } else {
                    res = NOT_PERMITTED_KEY;
                }
                break;

            case 'D':
                if (key.compareTo(Dagman.DIRECTORY_EXTERNAL_KEY) == 0) {
                    res = VALID_KEY;
                } else {
                    res = NOT_PERMITTED_KEY;
                }
                break;

            case 'J':
                if (key.compareTo(Dagman.JOB_KEY) == 0) {
                    res = VALID_KEY;
                } else {
                    res = NOT_PERMITTED_KEY;
                }
                break;

            case 'M':
                if (key.startsWith(MAX_KEYS_PREFIX)) {
                    res = VALID_KEY;
                } else {
                    res = NOT_PERMITTED_KEY;
                }
                break;

            case 'N':
                if (key.startsWith(NOOP_KEY)) {
                    res = VALID_KEY;
                } else {
                    res = NOT_PERMITTED_KEY;
                }
                break;

            case 'O':
                if (key.compareTo(Dagman.OUTPUT_KEY) == 0) {
                    res = VALID_KEY;
                } else {
                    res = NOT_PERMITTED_KEY;
                }
                break;

            case 'P':
                if ((key.compareTo(Dagman.POST_SCRIPT_KEY) == 0)
                        || (key.compareTo(Dagman.POST_SCRIPT_ARGUMENTS_KEY) == 0)
                        || (key.compareTo(Dagman.PRE_SCRIPT_KEY) == 0)
                        || (key.compareTo(Dagman.PRE_SCRIPT_ARGUMENTS_KEY) == 0)
                        || (key.compareTo(Dagman.POST_SCRIPT_SCOPE_KEY) == 0)
                        || (key.compareTo(Dagman.PRIORITY_KEY) == 0)
                        || (key.startsWith(Dagman.POST_SCRIPT_PATH_PREFIX))) {
                    res = VALID_KEY;
                } else {
                    res = NOT_PERMITTED_KEY;
                }
                break;

            case 'R':
                if (key.compareTo(Dagman.RETRY_KEY) == 0) {
                    res = VALID_KEY;
                } else {
                    res = NOT_PERMITTED_KEY;
                }
                break;

            case 'S':
                if (key.compareTo(Dagman.SUBDAG_EXTERNAL_KEY) == 0) {
                    res = VALID_KEY;
                } else {
                    res = NOT_PERMITTED_KEY;
                }
                break;

            case 'V':
                if (key.compareTo(Dagman.VARS_KEY) == 0) {
                    res = VALID_KEY;
                } else {
                    res = NOT_PERMITTED_KEY;
                }
                break;

            default:
                res = NOT_PERMITTED_KEY;
        }

        return res;
    }

    /**
     * Returns the path to the postscript of a particular type
     *
     * @param type type of postscript
     * @return the path
     */
    public String getPOSTScriptPath(String type) {
        StringBuffer property = new StringBuffer();
        property.append(Dagman.POST_SCRIPT_PATH_PREFIX).append(".").append(type.toUpperCase());

        return (String) this.get(property.toString());
    }

    /**
     * It puts in the namespace specific information specified in the properties file into the
     * namespace. The profile information is populated only if the corresponding key does not exist
     * in the object already.
     *
     * @param properties the <code>PegasusProperties</code> object containing all the properties
     *     that the user specified at various places (like .chimerarc, properties file, command
     *     line).
     * @param pool the pool name where the job is scheduled to run.
     */
    public void checkKeyInNS(PegasusProperties properties, String pool) {
        // retrieve the relevant profiles from properties
        // and merge them into the existing.
        this.assimilate(properties, Profiles.NAMESPACES.dagman);

        // check if the arguments for the
        // post script are specified or not

        // System.out.println( this.mProfileMap );
        if (!this.containsKey(Dagman.POST_SCRIPT_ARGUMENTS_KEY)) {
            // push in the default arguments for the post script
            this.checkKeyInNS(
                    Dagman.POST_SCRIPT_ARGUMENTS_KEY,
                    Dagman.DEFAULT_POST_SCRIPT_ARGUMENTS_KEY_VALUE);
        }

        // check if the arguments for the
        // pre script are specified or not
        if (!this.containsKey(Dagman.PRE_SCRIPT_ARGUMENTS_KEY)) {
            // push in the default arguments for the post script
            this.checkKeyInNS(
                    Dagman.PRE_SCRIPT_ARGUMENTS_KEY, Dagman.DEFAULT_PRE_SCRIPT_ARGUMENTS_KEY_VALUE);
        }

        // what type of postscript needs to be invoked for the job
        /*
                if( !this.containsKey( this.POST_SCRIPT_KEY ) ){
                    //get one from the properties
                    String ps = properties.getPOSTScript();
                    if( ps != null ){ checkKeyInNS( this.POST_SCRIPT_KEY, properties.getPOSTScript() ); }
                }
        */
    }

    /**
     * Assimilate the profiles in the namespace in a controlled manner. During assimilation all
     * category related keys are ignored.
     *
     * @param profiles the <code>Namespace</code> object containing the profiles.
     * @param namespace the namespace for which the profiles need to be assimilated.
     */
    public void assimilate(PegasusProperties properties, Profiles.NAMESPACES namespace) {
        Namespace profiles = properties.getProfiles(namespace);
        for (Iterator it = profiles.getProfileKeyIterator(); it.hasNext(); ) {
            String key = (String) it.next();

            // profiles assimilated from properties have lowest priority
            if (!this.containsKey(key)) {
                this.checkKeyInNS(key, (String) profiles.get(key));
            }
        }

        // profiles in properties have lowest priority
        // we associate default retry only if user did
        // not specify in properties
        if (!this.containsKey(Dagman.RETRY_KEY)) {
            this.construct(RETRY_KEY, DEFAULT_RETRY_VALUE);
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
        if (!(profiles instanceof Dagman)) {
            // throw an error
            throw new IllegalArgumentException("Profiles mismatch while merging");
        }
        String key;
        for (Iterator it = profiles.getProfileKeyIterator(); it.hasNext(); ) {
            // construct directly. bypassing the checks!
            key = (String) it.next();
            this.construct(key, (String) profiles.get(key));
        }
    }

    /**
     * Converts the contents of the map into the string that can be put in the Condor file for
     * printing.
     *
     * @return the the textual description.
     */
    public String toCondor() {
        return toString(mJobName);
    }

    /**
     * Converts the contents of the map into the string that can be put in the Condor file for
     * printing.
     *
     * @param name the name of the condor job that contains these variables.
     * @return the textual description.
     */
    public String toString(String name) {
        StringBuffer sb = new StringBuffer();

        if (mProfileMap == null) {
            // no profile keys were stored in here
            return sb.toString();
        }
        String key = null;
        for (Iterator it = mProfileMap.keySet().iterator(); it.hasNext(); ) {
            key = (String) it.next();

            // continue to next if the key has to be ignored.
            if (ignore(key)) {
                continue;
            }

            append(sb, replacementKey(key), name, replacementValue(key));

            //            sb.append( replacementKey(key) ).append(" ").append(name).
            //               append(" ").
            //               /*append((String)mProfileMap.get(key))*/
            //               append( replacementValue(key)).
            //            append("\n");

        }

        // add the ABORT_DAG_ON ky
        if (this.containsKey(Dagman.ABORT_DAG_ON_KEY)) {
            append(sb, Dagman.ABORT_DAG_ON_KEY, name, replacementValue(Dagman.ABORT_DAG_ON_KEY));
        }

        // add the category key in the end if required
        if (this.containsKey(Dagman.CATEGORY_KEY)) {
            append(
                    sb,
                    replacementKey(Dagman.CATEGORY_KEY),
                    name,
                    replacementValue(Dagman.CATEGORY_KEY));
        }

        // PM-1049 always add VARS dagnode retry
        append(sb, Dagman.VARS_KEY, name, "+DAGNodeRetry=\"$(RETRY)\"");

        return sb.toString();
    }

    protected StringBuffer append(StringBuffer sb, String key, String name, String value) {
        return sb.append(key).append(" ").append(name).append(" ").append(value).append("\n");
    }

    /**
     * Helper method to decide whether a key has to be ignored or not.
     *
     * @param key the key
     * @return boolean
     */
    private boolean ignore(String key) {

        return key.equals(Dagman.POST_SCRIPT_ARGUMENTS_KEY)
                || key.equals(Dagman.PRE_SCRIPT_ARGUMENTS_KEY)
                || key.equals(Dagman.OUTPUT_KEY)
                || key.equals(Dagman.CATEGORY_KEY)
                || key.equals(Dagman.POST_SCRIPT_SCOPE_KEY)
                || key.startsWith(Dagman.POST_SCRIPT_PATH_PREFIX)
                || key.startsWith(Dagman.MAX_KEYS_PREFIX)
                || key.startsWith(Dagman.ABORT_DAG_ON_KEY)
                || key.equals(Dagman.NOOP_KEY);
    }

    /**
     * Returns the replacement key that needs to be printed in .dag file in lieu of the key.
     *
     * @param key the key
     * @return the replacement key.
     */
    private String replacementKey(String key) {
        String replacement = key;
        if (key.equalsIgnoreCase(Dagman.POST_SCRIPT_KEY)) {
            replacement = Dagman.POST_SCRIPT_REPLACEMENT_KEY;
        } else if (key.equalsIgnoreCase(Dagman.PRE_SCRIPT_KEY)) {
            replacement = Dagman.PRE_SCRIPT_REPLACEMENT_KEY;
        }
        return replacement;
    }

    /**
     * Returns the replacement value that needs to be printed in .dag file for a key. This helps us
     * tie the post script path to the arguments, and same for prescript.
     *
     * @param key the key
     * @return the replacement value
     */
    private String replacementValue(String key) {
        StringBuffer value = new StringBuffer();

        // append the value for the key
        value.append((String) mProfileMap.get(key));

        if (key.equals(JOB_KEY)) {
            // PM-987 add NOOP key for job if required
            if (this.containsKey(NOOP_KEY)) {
                // value does not matter
                value.append(" ").append(NOOP_KEY);
            }
        }
        // for postscript and prescript in addition put in the arguments.
        else if (key.equalsIgnoreCase(Dagman.POST_SCRIPT_KEY)) {
            // append the postscript arguments
            value.append(" ").append((String) this.get(Dagman.POST_SCRIPT_ARGUMENTS_KEY));
            // append the output file
            value.append(" ").append((String) this.get(Dagman.OUTPUT_KEY));

        } else if (key.equalsIgnoreCase(Dagman.PRE_SCRIPT_KEY)) {
            // append the prescript arguments
            value.append(" ").append((String) this.get(Dagman.PRE_SCRIPT_ARGUMENTS_KEY));
        }
        return value.toString();
    }

    /**
     * Returns a copy of the current namespace object.
     *
     * @return the Cloned object
     */
    public Object clone() {
        Dagman ns = (mProfileMap == null) ? new Dagman() : new Dagman(this.mProfileMap);
        ns.mJobName = (mJobName == null) ? null : new String(this.mJobName);
        return ns;
    }
}
