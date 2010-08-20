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


import org.griphyn.cPlanner.classes.Profile;

import edu.isi.pegasus.common.logging.LogManager;

import org.griphyn.cPlanner.common.PegasusProperties;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * This helper class helps in handling the arguments specified in the
 * Condor namespace by the user either through dax or through profiles in pool.
 *
 * @author Karan Vahi
 * @version $Revision$
 */

public class Condor extends Namespace{

    /**
     * The name of the namespace that this class implements.
     */
    public static final String NAMESPACE_NAME = Profile.CONDOR;

    /**
     * The name of the key that denotes the arguments of the job.
     */
    public static final String ARGUMENTS_KEY = "arguments";


    /**
     * The name of the key that denotes the requirements of the job.
     */
    public static final String REQUIREMENTS_KEY = "requirements";

    /**
     * The name of the key that denotes the condor universe key.
     */
    public static final String UNIVERSE_KEY = "universe";
    
    /**
     * The name of the key that denotes the remote condor universe key.
     */
    public static final String REMOTE_UNIVERSE_KEY = "remote_universe";

    /**
     * The name of the key that denotes the File System Domain. Is actually
     * propogated to the expression for the Requirements Key.
     *
     * @see #REQUIREMENTS_KEY
     */
    public static final String FILE_SYSTEM_DOMAIN_KEY = "filesystemdomain";

    /**
     * The name of the key that specifies the grid job type.
     */
    public static final String GRID_JOB_TYPE_KEY = "grid_type";

    /**
     * The name of the key that specifies the jobmanager type.
     */
    public static final String JOBMANAGER_TYPE_KEY = "jobmanager_type";

    /**
     * The name of the key that designates that files should be transferred
     * via Condor File Transfer mechanism.
     */
    public static final String SHOULD_TRANSFER_FILES_KEY = "should_transfer_files";
    
    /**
     * The corresponding remote kye name that designates that files should be
     * transferred via Condor File Transfer mechanism.
     */
    public static final String REMOTE_SHOULD_TRANSFER_FILES_KEY = "+remote_ShouldTransferFiles";
    
    /**
     * The name of key that designates when to transfer output.
     */
    public static final String WHEN_TO_TRANSFER_OUTPUT_KEY =  "when_to_transfer_output";
    
    /**
     * The corresponding name of the remote key that designated when to transfer output.
     */
    public static final String REMOTE_WHEN_TO_TRANSFER_OUTPUT_KEY = "+remote_WhenToTransferOutput";
    
    /**
     * The name of the key that specifies transfer of input files.
     */
    public static final String TRANSFER_IP_FILES_KEY = "transfer_input_files";

    /**
     * The name of the key that specifies transfer of input files.
     */
    public static final String TRANSFER_OP_FILES_KEY = "transfer_output_files";

    /**
     * The name of the key that specifies transfer of executable
     */
    public static final String TRANSFER_EXECUTABLE_KEY = "transfer_executable";


    /**
     * The name of the key that specifies the priority for the job.
     */
    public static final String PRIORITY_KEY = "priority";

    /**
     * The condor key for designation the grid_resource.
     */
    public static final String GRID_RESOURCE_KEY = "grid_resource";
    
    /**
     * The key that designates the collector associated with the job.
     */
    public static final String COLLECTOR_KEY = "condor_collector";
    
    /**
     * The condor universe key value for vanilla universe.
     */
    public static final String VANILLA_UNIVERSE = "vanilla";

    /**
     * The condor universe key value for grid universe.
     */
    public static final String GRID_UNIVERSE = "grid";

    /**
     * The condor universe key value for vanilla universe.
     */
    public static final String GLOBUS_UNIVERSE  = "globus";

    /**
     * The condor universe key value for scheduler universe.
     */
    public static final String SCHEDULER_UNIVERSE = "scheduler";

    /**
     * The condor universe key value for standard universe.
     */
    public static final String STANDARD_UNIVERSE = "standard";

    /**
     * The condor universe key value for local universe.
     */
    public static final String LOCAL_UNIVERSE = "local";

    /**
     * The condor universe key value for parallel universe.
     */
    public static final String PARALLEL_UNIVERSE = "parallel";


    /**
     * The name of the implementing namespace. It should be one of the valid
     * namespaces always.
     *
     * @see Namespace#isNamespaceValid(String)
     */
    protected String mNamespace;


    /**
     * The default constructor.
     */
    public Condor(){
        mProfileMap = new TreeMap();
        mNamespace = NAMESPACE_NAME;
    }


    /**
     * The overloaded constructor
     *
     * @param mp  map containing the profile keys.
     */
    public Condor(Map mp){
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
     * Returns a comma separated list of files that are designated
     * for transfer via condor file transfer mechanism for the job.
     * 
     * @return  a csv file else null
     */
    public String getIPFilesForTransfer(){
        return ( this.containsKey( Condor.TRANSFER_IP_FILES_KEY ) )?
                (String)this.get(Condor.TRANSFER_IP_FILES_KEY):
                null;
    }
    
    /**
     * Remove the input files that were designated for transfer using
     * Condor File Transfer Mechanism.
     */
    public void removeIPFilesForTransfer() {
        Object obj = this.removeKey( Condor.TRANSFER_IP_FILES_KEY );
        if( obj != null ){
            //delete stf and wto only if no output files tx
            //and transfer_executable is not set
            if( !this.containsKey( Condor.TRANSFER_OP_FILES_KEY ) &&
                !this.containsKey( Condor.TRANSFER_EXECUTABLE_KEY )){
                this.removeKey( "should_transfer_files" );
                this.removeKey( "when_to_transfer_output" );
            }
        }
    }
    
    /**
     * Adds the executable for transfer via the condor file transfer mechanism.
     *
     * 
     */
    public void setExecutableForTransfer( ){
        this.construct( Condor.TRANSFER_EXECUTABLE_KEY, "true" );
        this.construct("should_transfer_files","YES");
        this.construct("when_to_transfer_output","ON_EXIT");
    }
    
    /**
     * Adds an input file that is to be transferred from the submit host via
     * the Condor File Transfer Mechanism. It also sets the associated condor
     * keys like when_to_transfer and should_transfer_files.
     *
     * @param file  the path to the file on the submit host.
     */
    public void addIPFileForTransfer(String file){
        //sanity check
        if(file == null || file.length() == 0){
            return ;
        }
        String files;
        //check if the key is already set.
        if(this.containsKey(Condor.TRANSFER_IP_FILES_KEY)){
            //update the existing list.
            files = (String)this.get(Condor.TRANSFER_IP_FILES_KEY);
            files =  files + "," + file;
        }
        else{
            files = file;
            //set the additional keys only once
            this.construct("should_transfer_files","YES");
            this.construct("when_to_transfer_output","ON_EXIT");
        }
        this.construct(Condor.TRANSFER_IP_FILES_KEY,files);
    }

    /**
     * Adds an output file that is to be transferred from the submit host via
     * the Condor File Transfer Mechanism. It also sets the associated condor
     * keys like when_to_transfer and should_transfer_files.
     *
     * @param file  the path to the file on the submit host.
     */
    public void addOPFileForTransfer( String file ){
        //sanity check
        if(file == null || file.length() == 0){
            return ;
        }
        String files;
        //check if the key is already set.
        if(this.containsKey( Condor.TRANSFER_OP_FILES_KEY )){
            //update the existing list.
            files = (String)this.get( Condor.TRANSFER_OP_FILES_KEY );
            files =  files + "," + file;
        }
        else{
            files = file;
            //set the additional keys only once
            this.construct("should_transfer_files","YES");
            this.construct("when_to_transfer_output","ON_EXIT");
        }
        this.construct( Condor.TRANSFER_OP_FILES_KEY, files );
    }


    /**
     * Additional method to handle the Condor namespace with
     * convenience mappings. Currently the following keys
     * are not supported keys as they clash with Pegasus
     * internals
     *
     * <pre>
     * arguments	- not supported, got from the arguments tag in DAX
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
     * log              - not supported, as it has to be same for the whole dag
     * notification     - OK
     * noop_job         - OK (used for synchronizing jobs in graph)
     * noop_job_exit_signal - OK
     * noop_job_exit_code - OK
     * periodic_release - OK
     * periodic_remove  - OK
     * priority         - OK
     * queue		- required thing. always added
     * remote_initialdir- not allowed, the working directory is picked up from
     *                    pool file and properties file
     * stream_error     - not supported, as it is applicable only for globus jobs.
     *                    However it can be set thru properties.
     * stream_output    - not supported, as it is applicable only for globus jobs.
     *                    However it can be set thru properties.
     * transfer_executable  - supported, limited to LCG sites at present where one needs
     *                        to stage in the kickstart.
     * transfer_input_files - supported, especially used to transfer proxies in
     *                        case of glide in pools.
     * universe         - supported, especially used to incorporate glide in pools.
     * +xxxx            - supported, this is used to add extra classads with the jobs.
     * </pre>
     *
     * @param key is the key within the globus namespace, must be lowercase!
     * @param value is the value for the given key.
     *
     * @return   MALFORMED_KEY
     *           VALID_KEY
     *           UNKNOWN_KEY
     *           NOT_PERMITTED_KEY
     *           DEPRECATED_KEY
     *           EMPTY_KEY
     */
    public  int checkKey(String key, String value) {
        // sanity checks first
        int res = 0;

        if (key == null || key.length() < 2 ) {
            res = MALFORMED_KEY ;
            return res;
        }
        
        if( value == null || value.length() < 1 ){
            res = EMPTY_KEY;
            return res;
        }

        //before checking convert the key to lower case
        key = key.toLowerCase();

        switch (key.charAt(0)) {
            case 'a':
                if (key.compareTo("arguments") == 0) {
                    res = NOT_PERMITTED_KEY;
                }
                else {
                    res = UNKNOWN_KEY;
                }
                break;

            case 'c':
                if (key.compareTo("copy_to_spool") == 0) {
                    res = VALID_KEY;
                }
                else {
                    res = UNKNOWN_KEY;
                }
                break;

            case 'e':
                if (key.compareTo("environment") == 0 ||
                    key.compareTo("executable") == 0) {
                    res = NOT_PERMITTED_KEY;
                }
                else {
                    res = UNKNOWN_KEY;
                }
                break;

            case 'f':
                //want to preserve case
                if (key.compareTo(FILE_SYSTEM_DOMAIN_KEY) == 0 ) {
                    res = VALID_KEY;
                }
                else {
                    res = UNKNOWN_KEY;
                }
                break;

            case 'g':
                if (key.compareTo(GRID_JOB_TYPE_KEY) == 0){
                    res = VALID_KEY;
                }
                else if (key.compareTo("globusscheduler") == 0 ||
                         key.compareTo("globusrsl") == 0) {
                    res = NOT_PERMITTED_KEY;
                }
                else {
                    res = UNKNOWN_KEY;
                }
                break;

            case 'j':
                if (key.compareTo(JOBMANAGER_TYPE_KEY) == 0){
                    res = VALID_KEY;
                }
                else{
                    res = UNKNOWN_KEY;
                }
                break;

            case 'l':
                if (key.compareTo("log") == 0) {
                    res = NOT_PERMITTED_KEY;
                }
                else {
                    res = UNKNOWN_KEY;
                }
                break;

            case 'n':
                if (key.compareTo("notification") == 0 ||
                    key.compareTo("noop_job") == 0     ||
                    key.compareTo("noop_job_exit_code") == 0 ||
                    key.compareTo("noop_job_exit_signal") == 0) {

                    res = VALID_KEY;
                }
                else {
                    res = UNKNOWN_KEY;
                }
                break;

            case 'p':
                if (key.compareTo("periodic_release") == 0 ||
                    key.compareTo("periodic_remove") == 0 ||
                    key.compareTo(PRIORITY_KEY) == 0) {
                    res = VALID_KEY;
                }
                else {
                    res = UNKNOWN_KEY;
                }
                break;


            case 'q':
                if (key.compareTo("queue") == 0) {
                    res = NOT_PERMITTED_KEY;
                }
                else {
                    res = UNKNOWN_KEY;
                }
                break;

            case 'r':
                if (key.compareTo("remote_initialdir") == 0) {
                    res = NOT_PERMITTED_KEY;
                }
                else {
                    res = UNKNOWN_KEY;
                }
                break;


            case 's':
                if (key.compareTo("stream_error") == 0 ||
                    key.compareTo("stream_output") == 0) {
                    res = NOT_PERMITTED_KEY;
                }
                else {
                    res = UNKNOWN_KEY;
                }
                break;

            case 't':
                if (key.compareTo(TRANSFER_EXECUTABLE_KEY) == 0 ||
                    key.compareTo(TRANSFER_IP_FILES_KEY) == 0){
                    res = VALID_KEY;
                }
                else {
                    res = UNKNOWN_KEY;
                }
                break;

            case 'u':
                if (key.compareTo(UNIVERSE_KEY) == 0 ){
                    res = VALID_KEY;
                }
                else {
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
     * It puts in the namespace specific information specified in the properties
     * file into the namespace. The name of the pool is also passed, as many of
     * the properties specified in the properties file are on a per pool basis.
     * It handles the periodic_remove and periodic_release characteristics for
     * condor jobs.
     *
     * @param properties  the <code>PegasusProperties</code> object containing
     *                    all the properties that the user specified at various
     *                    places (like .chimerarc, properties file, command line).
     * @param pool        the pool name where the job is scheduled to run.
     */
    public void checkKeyInNS(PegasusProperties properties, String pool){
        String value = null;

        //Karan Oct 19, 2005. The values in property file
        //should only be treated as default. Need to reverse
        //below.

        //get the periodic release values always a default
        //value is got if not specified.
        String releaseval = properties.getCondorPeriodicReleaseValue();
        String oldval = (String) this.removeKey("periodic_release");
        oldval = (oldval == null) ?
            //put in default value
            "3" :
            //keep the one from profiles or dax
            oldval;
        releaseval = (releaseval == null) ? oldval : releaseval;

        String removeval = properties.getCondorPeriodicRemoveValue();
        oldval = (String) this.removeKey("periodic_remove");
        if(oldval == null){
            //maintaining backward compatibility
            oldval = properties.getCondorPeriodicReleaseValue();
        }
        oldval = (oldval == null) ?
            //put in default value
            "3" :
            //keep the one from profiles or dax
            oldval;
        removeval = (removeval == null) ? oldval : removeval;


        boolean removeint  = this.isInteger( removeval );
        boolean releaseint = this.isInteger( releaseval );

        if( removeint && releaseint ){
            if( Integer.parseInt(removeval) > Integer.parseInt(releaseval)){
                removeval = releaseval;
                //throw a warning down
                mLogger.log(
                    " periodic_remove > periodic_release " +
                    "for job " + /*sinfo.jobName +*/
                     ". Setting periodic_remove=periodic_release",
                    LogManager.WARNING_MESSAGE_LEVEL);
            }
        }

        //construct the periodic_release and periodic_remove
        //values to whatever they are set
        if( !releaseval.isEmpty() ){
            this.construct("periodic_release", releaseval);
        }

        if( !removeval.isEmpty() ){
            this.construct("periodic_remove", removeval);
        }

        //the job priorities from property file
        //picked as default
        String priority = containsKey(Condor.PRIORITY_KEY)?
                          (String)this.get(Condor.PRIORITY_KEY):
                          null;
        priority = (priority == null)?
                    //pick default from properties
                    properties.getJobPriority():
                    //prefer existing
                    priority;
        //construct only if priority is not null
        if(priority != null){
            this.construct(Condor.PRIORITY_KEY,priority);
        }



    }
    
    /**
     * Returns a boolean value indicating if the string passed is an integer or not
     * 
     * @param value the value
     * 
     * @return boolean
     */
    public boolean isInteger( String value ){
        boolean result = true;
        
        try{
            Integer.parseInt(value);
        }
        catch( Exception e ){
            result = false;
        }
        return result;
    }




   /**
     * This checks the whether a key value pair specified is valid in the current
     * namespace or not by calling the checkKey function and then on the basis of
     * the values returned puts them into the associated map in the class.
     * In addition it transfers the FILE_SYSTEM_DOMAIN_KEY to the
     * REQUIREMENTS_KEY.
     *
     * @param key   key that needs to be checked in the namespace for validity.
     * @param value value of the key
     *
     */
    public void checkKeyInNS(String key, String value){
        int rslVal = checkKey(key,value);

        switch (rslVal){

            case Namespace.MALFORMED_KEY:
                //key is malformed ignore
                malformedKey(key,value);
                break;

            case Namespace.NOT_PERMITTED_KEY:
                notPermitted(key);
                break;

            case Namespace.UNKNOWN_KEY:
                unknownKey(key, value);
                break;

            case Namespace.VALID_KEY:
                if(key.equalsIgnoreCase(FILE_SYSTEM_DOMAIN_KEY)){
                    //set it to the REQUIREMENTS_KEY
                    key = REQUIREMENTS_KEY;
                    //construct the classad expression
                    value = FILE_SYSTEM_DOMAIN_KEY + " == " +
                            "\"" + value + "\"";
                }
                construct(key, value);
                break;
                
            case Namespace.EMPTY_KEY:
                emptyKey( key );
                break;
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
        if (!(profiles instanceof Condor )){
            //throw an error
            throw new IllegalArgumentException( "Profiles mismatch while merging" );
        }
        String key, value;
        for ( Iterator it = profiles.getProfileKeyIterator(); it.hasNext(); ){
            //construct directly. bypassing the checks!
            key = (String)it.next();
            value = (String) profiles.get(key);

            //override only if key is not transfer_ip_files
            if( key.equals( this.TRANSFER_IP_FILES_KEY )){
                //add to existing
                this.addIPFileForTransfer( value );
            }

            //overriding the arguments makes no sense.
            if( key.equals( this.ARGUMENTS_KEY )){
                continue;
            }
            else{
                this.construct(key, value );
            }
        }
    }


   /**
     * Constructs a new element of the format (key=value). All the keys
     * are converted to lower case before storing.
     *
     * @param key is the left-hand-side
     * @param value is the right hand side
     */
    public void construct(String key, String value) {
        mProfileMap.put(key.toLowerCase(), value);
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
        boolean value;
        if(mProfileMap.containsKey(key)){
            value = Boolean.valueOf((String)mProfileMap.get(key)).booleanValue();
        }
        else{
            //the key is not in the namespace
            //return false
            return false;
        }
        return value;
    }

    



    /**
     * Converts the contents of the map into the string that can be put in the
     * Condor file for printing.
     *
     * @return the textual description
     */
    public String toCondor(){
        StringBuffer st = new StringBuffer();
        String key = null;
        String value = null;

        Iterator it = mProfileMap.keySet().iterator();
        while(it.hasNext()){
            key = (String)it.next();
            value = (String)mProfileMap.get(key);
            st.append(key).append(" = ").append(value).append("\n");
        }

        return st.toString();
    }

    /**
     * Returns a copy of the current namespace object.
     *
     * @return the Cloned object
     */
    public Object clone(){
        return new Condor(this.mProfileMap);
    }

}
