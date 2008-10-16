/**
 *  Copyright 2007-2008 University Of Southern California
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

package org.griphyn.cPlanner.namespace;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.griphyn.cPlanner.classes.Profile;

import edu.isi.pegasus.common.logging.LogManager;
import org.griphyn.cPlanner.common.PegasusProperties;


/**
 * A Planner specific namespace. It defines profiles that are used to fine
 * tune Pegasus behaviour on a per job basis if required.
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 * @version $Revision$
 */

public class VDS extends Namespace {

    /**
     * The name of the namespace that this class implements.
     */
    public static final String NAMESPACE_NAME = Profile.VDS;

    /**
     * The name of the key that sets a remote initial dir for a condor globus
     * job.
     */
    public static final String REMOTE_INITIALDIR_KEY = "workdir";

    /**
     * The name of the key that if set, determines the number of super jobs
     * that are made corresponding to a logical transformation and an execution
     * pool. It overrides the collapse key if set.
     *
     * @see #COLLAPSE_KEY
     */
    public static final String BUNDLE_KEY = "bundle";

    /**
     * The name of the key that if set in the VDS namespace determines the
     * number of jobs that are collapsed into the super job.
     */
    public static final String COLLAPSE_KEY = "collapse";

    /**
     * The name of the key that determines the collapser executable to be used
     * to run the merged/collapsed job.
     */
    public static final String COLLAPSER_KEY = "collapser";

    /**
     * The name of the profile key in vds namespace that does the grouping.
     */
    public static final String GROUP_KEY = "group";

    /**
     * The name of the profile key in vds namespace that does the labelling
     * by default.
     */
    public static final String LABEL_KEY = "label";


    /**
     * The name of the profile key that determines the launching executable
     * to be used to launch a job on the grid.
     */
    public static final String GRIDSTART_KEY = "gridstart";

    /**
     * The name of the profile key, that determines the arguments with which
     * the GridStart that is used to launch a job on the remote site is invoked
     * with. The arguments are appended to the ones constructed by default
     * by the GridStart implementation.
     */
    public static final String GRIDSTART_ARGUMENTS_KEY = "gridstart.arguments";

    /**
     * The name of the profile key that designates the path to a gridstart.
     */
    public static final String GRIDSTART_PATH_KEY = "gridstart.path";


    /**
     * The deprecated change dir key.
     * @see #CHANGE_DIR_KEY
     */
    public static final String DEPRECATED_CHANGE_DIR_KEY = "change_dir";

    /**
     * The name of the profile key that triggers the kickstart to change directory
     * before launching an executable instead of launching the executable from
     * the directory where kickstart is being run.
     */
    public static final String CHANGE_DIR_KEY = "change.dir";

    /**
     * The deprecated bundle stagein key.
     * @see #CHANGE_DIR_KEY
     */
    public static final String DEPRECATED_BUNDLE_STAGE_IN_KEY = "bundle_stagein";

    /**
     * The name of the key that determines the bundling parameter for the
     * stagein transfer node.
     */
    public static final String BUNDLE_STAGE_IN_KEY = "bundle.stagein";

    /**
     * The name of the key that determines the bundling parameter for the
     * stageout transfer node.
     */
    public static final String BUNDLE_STAGE_OUT_KEY = "bundle.stageout";
    
    /**
     * The name of the key that determines the clustering parameter for the
     * stagein transfer node.
     */
    public static final String CLUSTER_STAGE_IN_TX_KEY = "cluster.stagein";

    /**
     * The name of the key that determines the clustering parameter for the
     * stageout transfer node.
     */
    public static final String CLUSTER_STAGE_OUT_TX_KEY = "cluster.stageout";


    /**
     * The name of the key that determines the number of chains of stagein
     * nodes that are to be created per site.
     */
    public static final String CHAIN_STAGE_IN_KEY = "chain.stagein";


    /**
     * The name of the profile key if associated with a job, results in an explicit
     * transfer of the proxy from the submit host to the remote site, instead of
     * banking upon CondorG to transfer the proxy.
     */
    public static final String TRANSFER_PROXY_KEY = "transfer.proxy";

    /**
     * The name of the profile key, that when associated with transfer jobs
     * determines the arguments with which the transfer executable is invoked.
     */
    public static final String TRANSFER_ARGUMENTS_KEY = "transfer.arguments";

    /**
     * The name of the profile key when associated with a transformation in the
     * transformation catalog gives expected runtime in seconds.
     */
    public static final String RUNTIME_KEY = "runtime";

    /**
     * The name of the key, that denotes the style of the dag that is constructed.
     * Possible styles can be
     *      -condor(glidein,flocking,submitting directly to condor pool)
     *      -globus(condorg)
     */
    public static final String STYLE_KEY = "style";

    /**
     * The name of the key that denotes the type of the job. Whether it is
     * recursive or not. Still protypical.
     */
    public static final String TYPE_KEY = "type";

    /**
     * The style indicating that the submit files are to be generated for
     * a vanilla condor execution.
     */
    public static final String CONDOR_STYLE = "condor";

    /**
     * The style indicating that the submit files are to be generated for
     * a CondorG execution.
     */
    public static final String GLOBUS_STYLE = "globus";

    /**
     * The style indicating that the submit files are to be generated for a
     * glidein execution.
     */
    public static final String GLIDEIN_STYLE = "glidein";

    /**
     * The name of the implementing namespace. It should be one of the valid
     * namespaces always.
     *
     * @see Namespace#isNamespaceValid(String)
     */
    protected String mNamespace;

    /**
     * The table containing the mapping of the deprecated keys to the newer keys.
     */
    protected static Map mDeprecatedTable = null;


    /**
     * The default constructor.
     * Note that the map is not allocated memory at this stage. It is done so
     * in the overloaded construct function.
     */
    public VDS() {
        mProfileMap = null;
        mNamespace = NAMESPACE_NAME;
    }

    /**
     * The overloaded constructor.
     *
     * @param mp  the initial map.
     */
    public VDS(Map mp) {
        mProfileMap = new TreeMap(mp);
        mNamespace = NAMESPACE_NAME;
    }

    /**
     * Returns the name of the namespace associated with the profile implementations.
     *
     * @return the namespace name.
     * @see #NAMESPACE_NAME
     */
    public String namespaceName(){
        return mNamespace;
    }


    /**
     * Constructs a new element of the format (key=value).
     * It first checks if the map has been initialised or not. If not then
     * allocates memory first. It converts the key to lower case before storing.
     *
     * @param key is the left-hand-side
     * @param value is the right hand side
     */
    public void construct(String key, String value) {
        if(mProfileMap == null)
            mProfileMap = new TreeMap();
        mProfileMap.put(key.toLowerCase(), value);
    }


    /**
     * This checks whether the key passed by the user is valid in the current
     * namespace or not.
     *
     * @param  key (left hand side)
     * @param  value (right hand side)
     *
     * @return Namespace.VALID_KEY
     * @return Namespace.UNKNOWN_KEY
     *
     */
    public int checkKey(String key, String value) {
        int res = 0;

        if (key == null || key.length() < 2 ||
            value == null || value.length() < 1) {
            res = MALFORMED_KEY ;
            return res;
        }

        //convert key to lower case
        key = key.toLowerCase();

        switch (key.charAt(0)) {

            case 'b':
                if ( (key.compareTo(BUNDLE_KEY) == 0) ||
                     (key.compareTo(BUNDLE_STAGE_IN_KEY) == 0) ||
                     (key.compareTo(BUNDLE_STAGE_OUT_KEY) == 0 )) {
                    res = VALID_KEY;
                }
                else if( key.compareTo(DEPRECATED_BUNDLE_STAGE_IN_KEY) == 0){
                    res = DEPRECATED_KEY;
                }
                else {
                    res = UNKNOWN_KEY;
                }
                break;

            case 'c':
                if ((key.compareTo(COLLAPSE_KEY) == 0) ||
                    (key.compareTo(COLLAPSER_KEY) == 0) ||
                    (key.compareTo(CHANGE_DIR_KEY) == 0) ||
                    (key.compareTo(CHAIN_STAGE_IN_KEY) == 0)) {
                    res = VALID_KEY;
                }
                else if(key.compareTo(DEPRECATED_CHANGE_DIR_KEY) == 0){
                    res = DEPRECATED_KEY;
                }
                else {
                    res = UNKNOWN_KEY;
                }
                break;

            case 'g':
                if (key.compareTo( GROUP_KEY ) == 0 ||
                    key.compareTo( GRIDSTART_KEY ) == 0 ||
                    key.compareTo( GRIDSTART_PATH_KEY ) == 0 ||
                    key.compareTo( GRIDSTART_ARGUMENTS_KEY ) == 0 ) {
                    res = VALID_KEY;
                }
                else {
                    res = UNKNOWN_KEY;
                }
                break;

            case 'l':
                if( key.compareTo( LABEL_KEY ) == 0 ){
                    res = VALID_KEY;
                }
                else{
                    res = UNKNOWN_KEY;
                }
                break;

            case 'r':
                if( key.compareTo( RUNTIME_KEY ) == 0 ){
                    res = VALID_KEY;
                }
                else{
                    res = UNKNOWN_KEY;
                }
                break;

            case 's':
                if(key.compareTo(STYLE_KEY) == 0){
                    res = VALID_KEY;
                }
                else{
                    res = UNKNOWN_KEY;
                }

                break;

            case 't':
                if ((key.compareTo(TRANSFER_PROXY_KEY) == 0) ||
		     (key.compareTo(TRANSFER_ARGUMENTS_KEY) == 0)){
                    res = VALID_KEY;
                }
                else{
                    res = UNKNOWN_KEY;
                }
                break;

            case 'w':
                if (key.compareTo(REMOTE_INITIALDIR_KEY) == 0) {
                    res = VALID_KEY;
                }
                else {
                    res = UNKNOWN_KEY;
                }
                break;


            default:
                res = UNKNOWN_KEY;
        }


        return res;
    }

    /**
     * It puts in the namespace specific information specified in the properties
     * file into the namespace. The name of the pool is also passed, as many of
     * the properties specified in the properties file are on a per pool basis.
     * This is used to load the appropriate collapser for the job.
     * Any preexisting profile is preferred over the one in the property file.
     *
     * @param properties  the <code>PegasusProperties</code> object containing
     *                    all the properties that the user specified at various
     *                    places (like .chimerarc, properties file, command line).
     * @param pool        the pool name where the job is scheduled to run.
     *
     * @see #COLLAPSER_KEY
     * @see #TRANSFER_PROXY_KEY
     */
    public void checkKeyInNS(PegasusProperties properties, String pool){
        //get the value that might have been populated
        //from other profile sources
        String value = (String)get(this.COLLAPSER_KEY);
        value = (value == null)?
                //load the global from the properties file
                properties.getJobAggregator():
                //prefer the existing one
                value;

        //no strict type check required
        //populate directly
        this.construct(this.COLLAPSER_KEY,value);

        value = (String)get(this.TRANSFER_PROXY_KEY);
        value = (value == null) ?
                //load the property from the properties file
                Boolean.toString(properties.transferProxy()):
                //prefer the existing one
                value;
        //no strict type check required
        //populate directly
        this.construct(this.TRANSFER_PROXY_KEY,value);

	value = (String)get(this.TRANSFER_ARGUMENTS_KEY);
        value = (value == null) ?
	    //load the property from the properties file
	    properties.getTransferArguments():
	    //prefer the existing one
	    value;

	if(value!=null){
	    //no strict type check required
	    //populate directly
	    this.construct(this.TRANSFER_ARGUMENTS_KEY,value);
	}

    }

    /**
     * Merge the profiles in the namespace in a controlled manner.
     * In case of intersection, the new profile value overrides, the existing
     * profile value.
     *
     * @param profiles  the <code>Namespace</code> object containing the profiles.
     */
    public void merge( Namespace profiles ){
        //check if we are merging profiles of same type
        if (!(profiles instanceof VDS )){
            //throw an error
            throw new IllegalArgumentException( "Profiles mismatch while merging" );
        }
        String key;
        for ( Iterator it = profiles.getProfileKeyIterator(); it.hasNext(); ){
            //construct directly. bypassing the checks!
            key = (String)it.next();
            this.construct( key, (String)profiles.get( key ) );
        }
    }


    /**
     * Singleton access to the deprecated table that holds the deprecated keys,
     * and the keys that replace them.
     *
     * @return Map
     */
    public  java.util.Map deprecatedTable() {
        if ( mDeprecatedTable == null ) {
            // only initialize once and only once, as needed.
            mDeprecatedTable = new java.util.TreeMap();
            mDeprecatedTable.put(DEPRECATED_BUNDLE_STAGE_IN_KEY,
                                 BUNDLE_STAGE_IN_KEY);
            mDeprecatedTable.put(DEPRECATED_CHANGE_DIR_KEY,
                                 CHANGE_DIR_KEY);
        }

        return mDeprecatedTable;
    }


    /**
     * Converts the contents of the map into the string that can be put in the
     * Condor file for printing.
     *
     * @return the textual description.
     */
    public String toString() {
        StringBuffer st = new StringBuffer();
        String key = null;
        String value = null;
        if(mProfileMap == null)
            return "";

        Iterator it = mProfileMap.keySet().iterator();
        while(it.hasNext()){
            key = (String)it.next();
            value = (String)mProfileMap.get(key);
            st.append(key).append(" = ").append(value).append("\n");
        }

        return st.toString();
    }

    /**
    * Warns about an unknown profile key and constructs it anyway.
    * Constructs a new RSL element of the format (key=value).
    *
    * @param key is the left-hand-side
    * @param value is the right hand side
    */
   public void unknownKey(String key, String value) {
       mLogger.log("unknown profile " + mNamespace + "." + key +
                   ",  using anyway", LogManager.DEBUG_MESSAGE_LEVEL);
       construct(key, value);
   }


    /**
     * Returns true if the namespace contains a mapping
     * for the specified key. More formally, returns true
     * if and only if this map contains at a mapping for a
     * key k such that (key==null ? k==null : key.equals(k)).
     * (There can be at most one such mapping.)
     * It also returns false if the map does not exist.
     *
     * @param key   The key that you want to search for
     *              in the namespace.
     *
     * @return boolean
     */
    public boolean containsKey(Object key){
        return (mProfileMap == null)? false : mProfileMap.containsKey(key);
    }


    /**
     * Returns the value to which this namespace maps the specified key.
     * Returns null if the map contains no mapping for this key. A return value
     * of null does not necessarily indicate that the map contains no mapping for
     * the key; it's also possible that the map explicitly maps the key to null.
     * The containsKey operation may be used to distinguish these two cases.
     *
     * @param key The key whose value you want.
     *
     * @return the object
     */
    public Object get(Object key){
        return (mProfileMap == null) ? null : mProfileMap.get(key);
    }

    /**
     * Returns a boolean value, that a particular key is mapped to in this
     * namespace. If the key is mapped to a non boolean
     * value or the key is not populated in the namespace false is returned.
     *
     * @param key  The key whose boolean value you desire.
     *
     * @return boolean
     */
    public boolean getBooleanValue(Object key){
        boolean value = false;
        if(mProfileMap != null && mProfileMap.containsKey(key)){
            value = Boolean.valueOf((String)mProfileMap.get(key)).booleanValue();
        }
        return value;
    }


    /**
     * Returns a String value, that a particular key is mapped to in this
     * namespace. If is not populated in the namespace null is returned.
     *
     * @param key  The key whose boolean value you desire.
     *
     * @return String if key is in the namespace
     *         null otherwise.
     */
    public String getStringValue(Object key){

        return containsKey(key)?
            get(key).toString():
            null;
    }


    /**
     * Returns a copy of the current namespace object
     *
     * @return the Cloned object
     */
    public Object clone() {
        return (mProfileMap == null)?new VDS() : new VDS(this.mProfileMap);
    }

}
