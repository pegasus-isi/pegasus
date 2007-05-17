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
                if (key.compareTo("executionPool") == 0) {
                    res = VALID_KEY;
                }
                else {
                    res = NOT_PERMITTED_KEY;
                }
                break;

            case 'g':
                if (key.compareTo("globusScheduler") == 0) {
                    res = VALID_KEY;
                }
                else {
                    res = NOT_PERMITTED_KEY;
                }
                break;

            case 'p':
                if (key.compareTo("pfnHint") == 0 ||
                    key.compareTo("pfnUniverse") == 0) {
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
     */
    public String toString() {
        return null;

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