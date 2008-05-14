/*
 * This file or a portion of this file is licensed under the terms of
 * the Globus Toolkit Public License, found in file GTPL, or at
 * http://www.globus.org/toolkit/download/license.html. This notice must
 * appear in redistributions of this file, with or without modification.
 *
 * Redistributions of this Software, with or without modification, must
 * reproduce the GTPL in: (1) the Software, or (2) the Documentation or
 * some other similar material which is provided with the Software (if
 * any).
 *
 * Copyright 1999-2004 University of Chicago and The University of
 * Southern California. All rights reserved.
 */

package org.griphyn.cPlanner.namespace;

import org.griphyn.cPlanner.classes.Profile;
import org.griphyn.cPlanner.common.PegasusProperties;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;


/**
 * This profile namespace is the placeholder for the keys that go into the .dag
 * file . Keys like RETRY that trigger retries in dagman in the event of a job
 * failing would go in here.
 * All the keys stored in it are in UPPERCASE irrespective of the case specified
 * by the user in the various catalogs. To specify a post script or a pre script
 * use POST and PRE keys.
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 * @version $Revision$
 */

public class Dagman extends Namespace {

    /**
     * The name of the namespace that this class implements.
     */
    public static final String NAMESPACE_NAME = Profile.DAGMAN;

    /**
     * The name of the key that determines what post script is to be invoked
     * when the job completes.
     */
    public static final String POST_SCRIPT_KEY = "POST";

    /**
     * The name of the key that determines the arguments that need to be passed
     * to the postscript.
     */
    public static final String POST_SCRIPT_ARGUMENTS_KEY = "POST_ARGS";

    /**
     * The name of the key that determines the file on the submit host on
     * which postscript is to be invoked.
     */
    public static final String OUTPUT_KEY = "OUTPUT";

    /**
     * The name of the key that determines what pre script is to be invoked
     * when the job is run.
     */
    public static final String PRE_SCRIPT_KEY = "PRE";

    /**
     * The name of the key that determines the arguments that need to be passed
     * to the postscript.
     */
    public static final String PRE_SCRIPT_ARGUMENTS_KEY = "PRE_ARGS";

    /**
     * The name of the key that determines how many times DAGMAN should be
     * retrying the job.
     */
    public static final String RETRY_KEY = "RETRY";

    /**
     * The name of the key that determines the category to which the job
     * belongs to.
     */
    public static final String CATEGORY_KEY = "CATEGORY";


    /**
     * The name of the key that indicates the path to the corresponding
     * submit file for the job.
     */
    public static final String JOB_KEY = "JOB";

    /**
     * The key name for the post script that is put in the .dag file.
     */
    private static final String POST_SCRIPT_REPLACEMENT_KEY = "SCRIPT POST";

    /**
     * The key name for the pre script that is put in the .dag file.
     */
    private static final String PRE_SCRIPT_REPLACEMENT_KEY = "SCRIPT PRE";

    /**
     * The name of the job (jobname) to which the profiles for this namespace
     * belong.
     *
     * @see org.griphyn.cPlanner.classes.SubInfo#jobName
     */
    private String mJobName;

    /**
     * The name of the implementing namespace. It should be one of the valid
     * namespaces always.
     *
     * @see Namespace#isNamespaceValid(String)
     */
    protected String mNamespace;


    /**
     * The default constructor.
     * We always initialize the map, as the map is guarenteed to store at least
     * the postscript value for a job.
     */
    public Dagman() {
        mProfileMap = new TreeMap();
        mNamespace = NAMESPACE_NAME;
        mJobName = null;
    }

    /**
     * The overloaded constructor.
     *
     * @param mp  the initial map containing the profile keys for this namespace.
     */
    public Dagman(Map mp) {
        mProfileMap = new TreeMap(mp);
        mNamespace = NAMESPACE_NAME;
        mJobName = null;
    }

    /**
     * The overloaded constructor.
     *
     * @param mp   the initial map containing the profile keys for this namespace.
     * @param name name of the job with which these profile keys are associated.
     */
    public Dagman(Map mp, String name) {
        mProfileMap = new TreeMap(mp);
        mNamespace  = NAMESPACE_NAME;
        mJobName    = name;
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
     * It sets the name of the job that is associated with the profiles contained
     * in this placeholder.
     *
     * @param name name of the job with which these profile keys are associated.
     */
    public void setJobName(String name){
        mJobName = name;
    }


    /**
     * Constructs a new element of the format (key=value).
     * The underlying map is allocated memory in the constructors always.
     * All the keys are converted to UPPER CASE before storing.
     *
     * @param key is the left-hand-side
     * @param value is the right hand side
     */
    public void construct(String key, String value) {
        //convert to uppercase the key
        mProfileMap.put(key.toUpperCase(), value);
    }


    /**
     * This checks whether the key passed by the user is valid in the current
     * namespace or not. All keys are assumed valid currently.
     *
     * @param  key (left hand side)
     * @param  value (right hand side)
     *
     * @return Namespace.VALID_KEY
     *
     */
    public int checkKey(String key, String value) {
        //all are valid because of certain keys
        //are defined in SCRIPT POST, that needs
        //to be corrected
        int res = 0;
        if (key == null || key.length() < 2 ||
            value == null || value.length() < 2) {
            res = MALFORMED_KEY ;
        }

        //convert key to lower case
        key = key.toUpperCase();

        switch (key.charAt(0)) {

            case 'C':
                if ( key.compareTo( this.CATEGORY_KEY ) == 0 ){
                    res = VALID_KEY;
                }
                else {
                    res = NOT_PERMITTED_KEY;
                }
                break;

            case 'J':
                if (key.compareTo(this.JOB_KEY) == 0) {
                    res = VALID_KEY;
                }
                else {
                    res = NOT_PERMITTED_KEY;
                }
                break;

            case 'O':
                if (key.compareTo(this.OUTPUT_KEY) == 0) {
                    res = VALID_KEY;
                }
                else {
                    res = NOT_PERMITTED_KEY;
                }
                break;

            case 'P':
                if ( (key.compareTo(this.POST_SCRIPT_KEY) == 0) ||
                     (key.compareTo(this.POST_SCRIPT_ARGUMENTS_KEY) == 0)||
                     (key.compareTo(this.PRE_SCRIPT_KEY) == 0) ||
                     (key.compareTo(this.PRE_SCRIPT_ARGUMENTS_KEY) == 0)
                     ) {
                    res = VALID_KEY;
                }
                else {
                    res = NOT_PERMITTED_KEY;
                }
                break;

            case 'R':
                if (key.compareTo(this.RETRY_KEY) == 0) {
                    res = VALID_KEY;
                }
                else {
                    res = NOT_PERMITTED_KEY;
                }
                break;


            default:
                res = NOT_PERMITTED_KEY;
        }


        return res;
    }

    /**
     * It puts in the namespace specific information specified in the properties
     * file into the namespace. The profile information is populated only if the
     * corresponding key does not exist in the object already.
     *
     * @param properties  the <code>PegasusProperties</code> object containing
     *                    all the properties that the user specified at various
     *                    places (like .chimerarc, properties file, command line).
     * @param pool        the pool name where the job is scheduled to run.
     */
    public void checkKeyInNS(PegasusProperties properties, String pool){
        //check if RETRY key already exists
        if(!this.containsKey(this.RETRY_KEY)){
            //try to get one from the condor file
            String val = properties.getCondorRetryValue();
            if (val != null && Integer.parseInt(val) > 0)
                //construct the RETRY key and put it in
                //assuming val is a proper integer
                this.checkKeyInNS(this.RETRY_KEY, val);
        }

        //check if the arguments for the
        //post script are specified or not
        if(!this.containsKey(this.POST_SCRIPT_ARGUMENTS_KEY)){
            //push in the default arguments for the post script
            this.checkKeyInNS(this.POST_SCRIPT_ARGUMENTS_KEY,
                              properties.getPOSTScriptArguments());
        }

        //check if the arguments for the
        //pre script are specified or not
        if(!this.containsKey(this.PRE_SCRIPT_ARGUMENTS_KEY)){
            //push in the default arguments for the post script
            this.checkKeyInNS(this.PRE_SCRIPT_ARGUMENTS_KEY,
                              properties.getPrescriptArguments());
        }

        //what type of postscript needs to be invoked for the job
        if( !this.containsKey( this.POST_SCRIPT_KEY ) ){
            //get one from the properties
            String ps = properties.getPOSTScript();
            if( ps != null ){ checkKeyInNS( this.POST_SCRIPT_KEY, properties.getPOSTScript() ); }
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
        if (!(profiles instanceof Dagman )){
            //throw an error
            throw new IllegalArgumentException( "Profiles mismatch while merging" );
        }
        String key;
        for ( Iterator it = profiles.getProfileKeyIterator(); it.hasNext(); ){
            //construct directly. bypassing the checks!
            key = (String)it.next();
            this.construct( key, (String)profiles.get( key ));
        }
    }

    /**
     * Converts the contents of the map into the string that can be put in the
     * Condor file for printing.
     *
     * @return the the textual description.
     */
    public String toString() {
        return toString(mJobName);
    }

    /**
     * Converts the contents of the map into the string that can be put in the
     * Condor file for printing.
     *
     * @param name  the name of the condor job that contains these variables.
     *
     * @return the textual description.
     */
    public String toString(String name) {
        StringBuffer sb = new StringBuffer();

        if(mProfileMap == null){
            //no profile keys were stored in here
            return sb.toString();
        }
        String key = null;
        for(Iterator it = mProfileMap.keySet().iterator();it.hasNext();){
            key = (String) it.next();

            //continue to next if the key has to be ignored.
            if( ignore(key) ){ continue;}

            append( sb, replacementKey( key ), name, replacementValue( key ) );

//            sb.append( replacementKey(key) ).append(" ").append(name).
//               append(" ").
//               /*append((String)mProfileMap.get(key))*/
//               append( replacementValue(key)).
//            append("\n");

        }

        //add the category key in the end if required
        if( this.containsKey( this.CATEGORY_KEY ) ){
            append( sb, replacementKey( this.CATEGORY_KEY  ), name, replacementValue( this.CATEGORY_KEY  ) );
        }

        return sb.toString();
    }


    protected StringBuffer append ( StringBuffer sb, String key, String name, String value ){
        return sb.append( key ).append(" ").append( name ).
               append(" ").append( value).
               append("\n");

    }


    /**
     * Helper method to decide whether a key has to be ignored or not.
     *
     * @param key  the key
     *
     * @return boolean
     */
    private boolean ignore(String key){
        return key.equals( this.POST_SCRIPT_ARGUMENTS_KEY ) ||
               key.equals( this.PRE_SCRIPT_ARGUMENTS_KEY) ||
               key.equals( this.OUTPUT_KEY ) ||
               key.equals( this.CATEGORY_KEY );
    }


    /**
     * Returns the replacement key that needs to be printed in .dag file in
     * lieu of the key.
     *
     * @param key  the key
     *
     * @return the replacement key.
     */
    private String replacementKey(String key){
        String replacement = key;
        if(key.equalsIgnoreCase(this.POST_SCRIPT_KEY)){
            replacement = this.POST_SCRIPT_REPLACEMENT_KEY;
        }
        else if(key.equalsIgnoreCase(this.PRE_SCRIPT_KEY)){
            replacement = this.PRE_SCRIPT_REPLACEMENT_KEY;
        }
        return replacement;
    }

    /**
     * Returns the replacement value that needs to be printed in .dag file for
     * a key. This helps us tie the post script path to the arguments, and same
     * for prescript.
     *
     * @param key   the key
     *
     * @return the replacement value
     */
    private String replacementValue(String key){
        StringBuffer value = new StringBuffer();

        //append the value for the key
        value.append( (String)mProfileMap.get(key));

        //for postscript and prescript in addition put in the arguments.
        if(key.equalsIgnoreCase(this.POST_SCRIPT_KEY)){
            //append the postscript arguments
            value.append(" ").append( (String)this.get( this.POST_SCRIPT_ARGUMENTS_KEY) );
            //append the output file
            value.append(" ").append( (String)this.get( this.OUTPUT_KEY ) );

        }
        else if(key.equalsIgnoreCase(this.PRE_SCRIPT_KEY)){
            //append the prescript arguments
            value.append(" ").
                  append( (String)this.get( this.PRE_SCRIPT_ARGUMENTS_KEY));
        }
        return value.toString();
    }


    /**
     * Returns a copy of the current namespace object.
     *
     * @return the Cloned object
     */
    public Object clone() {
        Dagman ns = (mProfileMap == null) ? new Dagman():new Dagman(this.mProfileMap);
        ns.mJobName = (mJobName == null)? null : new String(this.mJobName);
        return ns;
    }

}
