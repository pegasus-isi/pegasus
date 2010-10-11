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


package edu.isi.pegasus.planner.namespace;

import org.griphyn.cPlanner.classes.Profile;
import org.griphyn.cPlanner.common.PegasusProperties;

import java.util.Map;
import java.util.TreeMap;
import java.util.Iterator;

/**
 * An empty mechanical implementation for the
 * namespace. At present we do not
 * know what the meaning is. The meaning is
 * is determined at the point of writing the
 * submit files.
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 * @version $Revision$
 */

public class Hints extends Namespace {

    /**
     * The name of the namespace that this class implements.
     */
    public static final String NAMESPACE_NAME = Profile.HINTS;


    /**
     * The jobmanager universe key.
     */
    public static final String JOBMANAGER_UNIVERSE_KEY = "jobmanager.universe";
    
    /**
     * The execution pool key
     */
    public static final String EXECUTION_POOL_KEY = "executionPool";
    
    /**
     * The globus scheduler hints key
     */
    public static final String GLOBUS_SCHEDULER_KEY = "globusScheduler";
    
    /**
     * The pfnHint key
     */
    public static final String PFN_HINT_KEY = "pfnHint";

    /**
     * The name of the implementing namespace. It should be one of the valid
     * namespaces always.
     *
     * @see Namespace#isNamespaceValid(String)
     */
    protected String mNamespace;


    /**
     * The default constructor.
     * Note that the map is not allocated memory at this stage. It is done so
     * in the overloaded construct function.
     */
    public Hints() {
        mProfileMap = null;
        mNamespace = NAMESPACE_NAME;
    }

    /**
     * The overloaded constructor.
     *
     * @param mp  the map containing the profiles to be prepopulated with.
     */
    public Hints(Map mp) {
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
     * Provides an iterator to traverse the profiles by their keys.
     *
     * @return an iterator over the keys to walk the profile list.
     */
    public Iterator getProfileKeyIterator() {
    return ( this.mProfileMap == null ) ? new EmptyIterator() : this.mProfileMap.keySet().iterator();
}



    /**
     * Constructs a new element of the format (key=value). It first checks if
     * the map has been initialised or not. If not then allocates memory first.
     *
     * @param key is the left-hand-side.
     * @param value is the right hand side.
     */
    public void construct(String key, String value) {
        if(mProfileMap == null)
            mProfileMap = new TreeMap();
        mProfileMap.put(key, value);
    }


    /**
    * Returns true if the namespace contains a mapping for the specified key.
    * More formally, returns true if and only if this map contains at a mapping
    * for a key k such that (key==null ? k==null : key.equals(k)).
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
     * This checks whether the key passed by the user is valid in the current
     * namespace or not. At present, for this namespace only a limited number of
     * keys have been assigned semantics.
     *
     * @param  key (left hand side)
     * @param  value (right hand side)
     *
     * @return Namespace.VALID_KEY
     * @return Namespace.NOT_PERMITTED_KEY
     *
     */
    public int checkKey(String key, String value) {
        // sanity checks first
        int res = 0;

        if (key == null || key.length() < 2 ||
            value == null || value.length() < 2) {
            res = MALFORMED_KEY ;
        }

        switch (key.charAt(0)) {
            case 'e':
                if (key.compareTo( Hints.EXECUTION_POOL_KEY ) == 0) {
                    res = VALID_KEY;
                }
                else {
                    res = NOT_PERMITTED_KEY;
                }
                break;

            case 'g':
                if (key.compareTo( Hints.GLOBUS_SCHEDULER_KEY ) == 0) {
                    res = VALID_KEY;
                }
                else {
                    res = NOT_PERMITTED_KEY;
                }
                break;

            case 'j':
                if (key.compareTo( Hints.JOBMANAGER_UNIVERSE_KEY  ) == 0 ) {
                    res = VALID_KEY;
                }
                else {
                    res = NOT_PERMITTED_KEY;
                }
                break;


            case 'p':
                if (key.compareTo( Hints.PFN_HINT_KEY ) == 0 /*||
                    key.compareTo("pfnUniverse") == 0*/) {
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
     * file into the namespace. The name of the pool is also passed, as many of
     * the properties specified in the properties file are on a per pool basis.
     * An empty implementation for the timebeing.
     *
     * @param properties  the <code>PegasusProperties</code> object containing
     *                    all the properties that the user specified at various
     *                    places (like .chimerarc, properties file, command line).
     * @param pool        the pool name where the job is scheduled to run.
     */
    public void checkKeyInNS(PegasusProperties properties, String pool){

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
        if (!(profiles instanceof Hints )){
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
     * Converts the contents of the map into the string that can be put in the
     * Condor file for printing.
     *
     * @return String
     */
    public String toCondor() {
        StringBuffer st = new StringBuffer();
        String key = null;
        String value = null;
        if(mProfileMap == null)
            return "";

        for ( Iterator it = mProfileMap.keySet().iterator(); it.hasNext(); ){
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
    public Object clone() {
        return (mProfileMap == null)?new Hints():new Hints(this.mProfileMap);
    }




}
