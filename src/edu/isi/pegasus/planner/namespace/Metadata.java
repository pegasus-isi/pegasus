/**
 *  Copyright 2007-2015 University Of Southern California
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

import edu.isi.pegasus.planner.catalog.classes.Profiles;
import edu.isi.pegasus.planner.classes.Profile;
import edu.isi.pegasus.planner.common.PegasusProperties;
import java.util.HashMap;

import java.util.Map;
import java.util.Iterator;

/**
 * 
 * @author Karan Vahi
 * 
 * @version $Revision$
 */

public class Metadata extends Namespace {

    /**
     * The name of the namespace that this class implements.
     */
    public static final String NAMESPACE_NAME = Profile.METADATA;
    
    //some predefined keys that Pegasus uses for integrity checking
    public static final String CHECKSUM_TYPE_KEY  = "checksum.type";
    public static final String CHECKSUM_VALUE_KEY = "checksum.value";
    
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
    public Metadata() {
        mProfileMap = null;
        mNamespace = NAMESPACE_NAME;
    }

    /**
     * The overloaded constructor.
     *
     * @param mp  the map containing the profiles to be prepopulated with.
     */
    public Metadata(Map mp) {
        mProfileMap = new HashMap(mp);
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
            mProfileMap = new HashMap();
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

        //all keys are permitted currenlyt
        switch (key.charAt(0)) {
            default:
                res = VALID_KEY;

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
        //retrieve the relevant profiles from properties
       //and merge them into the existing.
       this.assimilate( properties, Profiles.NAMESPACES.metadata  );
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
        if (!(profiles instanceof Metadata )){
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
     * @return an empty string
     */
    public String toCondor() {
        return "";
    }

    /**
     * Returns a copy of the current namespace object.
     *
     * @return the Cloned object
     */
    public Object clone() {
        return (mProfileMap == null)?new Metadata():new Metadata(this.mProfileMap);
    }




}
