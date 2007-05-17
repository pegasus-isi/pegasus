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

import org.griphyn.cPlanner.classes.Data;
import org.griphyn.cPlanner.classes.Profile;

import org.griphyn.cPlanner.common.LogManager;

import org.griphyn.cPlanner.common.PegasusProperties;

import org.griphyn.common.catalog.TransformationCatalogEntry;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The base namespace class that all the othernamepsace handling classes extend.
 * Some constants are defined.
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 * @version $Revision: 1.16 $
 */

public abstract class Namespace extends Data{

    /**
     * The version number associated with this API of Profile Namespaces.
     */
    public static final String VERSION = "1.2";


    //constants for whether the key
   //is valid in the namespace or not

   /**
    * Either the key or the value specified is null or malformed.
    */
   public static final int MALFORMED_KEY      = -1;

   /**
    * The key is a valid key and can be put in the profiles.
    */
   public static final int VALID_KEY          = 0;

   /**
    * The key is unknown. Upto the profile namespace implementation whether to
    *  use it or not.
    */
   public static final int UNKNOWN_KEY        = 1;

   /**
    * The key is not permitted in as it clashes with default Pegasus constructs.
    */
   public static final int NOT_PERMITTED_KEY  = 2;

   /**
    * The key is deprecated. Support is for a limited time.
    */
   public static final int DEPRECATED_KEY     = 3;

   /**
    * The Map object that contains the profiles for a particular namespace.
    * The Map is indexed by profile key. Each value, is a profile value.
    */
   protected Map mProfileMap;

   /**
    * Checks if the namespace specified is valid or not.
    *
    * @param namespace The namespace you want to check
    */
   public static boolean isNamespaceValid(String namespace){

       boolean valid = false;

       //sanity checks
       if( namespace == null || namespace.length() < 2){
           return valid;
       }

       if (namespace.equalsIgnoreCase(Profile.CONDOR) ||
           namespace.equalsIgnoreCase(Profile.GLOBUS) ||
           namespace.equalsIgnoreCase(Profile.VDS) ||
           namespace.equalsIgnoreCase(Profile.DAGMAN) ||
           namespace.equalsIgnoreCase(Profile.HINTS) ||
           namespace.equalsIgnoreCase(Profile.ENV)) {

           valid = true;
       }

       return valid;
   }

   /**
    * This checks the whether a key value pair specified is valid in the current
    * namespace or not, and whether it clashes with other key value pairs that
    * might have been set by Pegasus internally.
    *
    * @return   MALFORMED_KEY
    *           VALID_KEY
    *           UNKNOWN_KEY
    *           NOT_PERMITTED_KEY
    */
   public abstract int checkKey(String key, String value);

   /**
    * Merge the profiles in the namespace in a controlled manner.
    * The profiles should be merged only if the namespace object containing them
    * matches to the current namespace.
    *
    * @param profiles  the <code>Namespace</code> object containing the profiles.
    */
   public abstract void merge( Namespace profiles );


   /**
    * Returns the name of the namespace associated with the profile implementations.
    *
    * @return the namespace name.
    */
   public abstract String namespaceName();


   /**
   * Provides an iterator to traverse the profiles by their keys.
   *
   * @return an iterator over the keys to walk the profile list.
   */
  public Iterator getProfileKeyIterator()
  {
    return ( mProfileMap == null )? new EmptyIterator() : this.mProfileMap.keySet().iterator();
  }


   /**
    * Singleton access to the deprecated table that holds the deprecated keys,
    * and the keys that replace them. It should be overriden in the namespaces,
    * that have deprecated keys.
    *
    * @return Map
    */
   public Map deprecatedTable() {
       throw new UnsupportedOperationException("No Deprecation support in the namespace " +
                                               namespaceName());
   }


   /**
    * It puts in the namespace specific information from the Transformation
    * Catalog into the namespace.
    *
    * @param entry  the <code>TCEntry</code> object containing the result from
    *               the Transformation Catalog.
    */
   public void checkKeyInNS(TransformationCatalogEntry entry){
       //sanity check
       if(entry == null) {
           return;
       }
       //pass down the list of Profile objects to be sucked in.
       checkKeyInNS(entry.getProfiles(this.namespaceName()));

   }

   /**
     * It takes in a Profiles object and puts them into the namespace after
     * checking if the namespace in the Profile object is same as the namepsace
     * implementation.
     *
     * @param profile  the <code>Profile</code> object containing the key and
     *                 value.
     *
     * @exception IllegalArgumentException if the namespace in the profile
     *            is not the same as the profile namepsace in which the profile
     *            is being incorporated.
     *
     * @see org.griphyn.cPlanner.classes.Profile
     */
    public void checkKeyInNS(Profile profile) throws IllegalArgumentException{
        if(profile.getProfileNamespace().equals(this.namespaceName())){
            checkKeyInNS(profile.getProfileKey(),
                         profile.getProfileValue());
        }
        else{
            //throw an exception for the illegal Profile Argument
            throw new IllegalArgumentException("Illegal Profile " + profile);
        }


    }


    /**
     * It takes in a list of profiles and puts them into the namespace after
     * checking if they are valid or not. Note, there are no checks on the
     * namespace however. The user should ensure that each Profile object in
     * the list is of the same namespace type.
     *
     * @param vars  List of <code>Profile</code> objects, each referring
     *                  to a key value for the profile.
     *
     *
     * @see org.griphyn.cPlanner.classes.Profile
     */
    public void checkKeyInNS(List vars){
        if(vars == null || vars.isEmpty()){
            //no variables to insert
            return;
        }

        Profile p = null;
        for( Iterator it = vars.iterator(); it.hasNext(); ){
            p = (Profile)it.next();
            checkKeyInNS(p.getProfileKey(),p.getProfileValue());
        }

    }


    /**
     * It puts in the namespace specific information specified in the properties
     * file into the namespace. The name of the pool is also passed, as many of
     * the properties specified in the properties file are on a per pool basis.
     *
     * @param properties  the <code>PegasusProperties</code> object containing
     *                    all the properties that the user specified at various
     *                    places (like .chimerarc, properties file, command line).
     * @param pool        the pool name where the job is scheduled to run.
     */
    public abstract void checkKeyInNS(PegasusProperties properties, String pool);


    /**
     * This checks the whether a key value pair specified is valid in the current
     * namespace or not by calling the checkKey function and then on the basis of
     * the values returned puts them into the associated map in the class.
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
                construct(key, value);
                break;

            case Namespace.DEPRECATED_KEY:
                deprecatedKey(key,value);
                break;
        }

   }


   /**
    * Returns true if the namespace contains a mapping for the specified key.
    * More formally, returns true if and only if this map contains at a mapping
    * for a key k such that (key==null ? k==null : key.equals(k)).
    * (There can be at most one such mapping.)
    *
    * @param key   The key that you want to search for
    *              in the namespace.
    */
   public boolean containsKey(Object key){
       return (mProfileMap == null)?
              false:
              mProfileMap.containsKey(key);
   }

   /**
    * Constructs a new element of the format (key=value).
    *
    * @param key is the left-hand-side
    * @param value is the right hand side
    */
   public void construct(String key, String value) {
       mProfileMap.put(key, value);
   }

   /**
    * Removes the key from the namespace.
    *
    * @param key  The key you want to remove.
    *
    * @return the value object if it exists.
    *         null if the key does not exist in the namespace.
    */
   public Object removeKey(Object key){
       return mProfileMap.remove(key);
   }

   /**
    * Returns the key set associated with the namespace.
    *
    * @return key set if the mProfileMap is populated.
    *         null if the associated mProfileMap is not populated.
    */
   public Set keySet(){
       return (mProfileMap == null) ? null: mProfileMap.keySet();
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
    */
   public Object get(Object key){
      return mProfileMap.get(key);
   }

   /**
    * Warns about an unknown profile key and constructs it anyway.
    * Constructs a new RSL element of the format (key=value).
    *
    * @param key is the left-hand-side
    * @param value is the right hand side
    */
   public void unknownKey(String key, String value) {
       mLogger.log("unknown profile " + namespaceName() + "." + key +
                   ",  using anyway", LogManager.WARNING_MESSAGE_LEVEL);
       construct(key, value);
   }

   /**
    * Warns about a deprecated profile key. It constructs the corresponding
    * replacement key.
    *
    * @param key is the left-hand-side
    * @param value is the right hand side
    *
    * @see #deprecatedTable()
    */
   public void deprecatedKey(String key, String value) {
       String replacement = (String)deprecatedTable().get(key);
       if(replacement == null){
           //no replacement key for the deprecated
           //profile! Fatal Internal Error
           StringBuffer error = new StringBuffer();
           error.append( "No replacement key exists for deprecated profile ").
                 append( namespaceName() ).append( "." ).append( key );
           throw new RuntimeException( error.toString() );
       }
       mLogger.log(
            "profile " + namespaceName() + "." + key +
            " is deprecated. Replacing with " + namespaceName() + "." + replacement,
            LogManager.WARNING_MESSAGE_LEVEL);

       if(containsKey(replacement)){
           //replacement key already exists.
           //use that ! might break profile overriding ??
       }
       else{
           construct(replacement,value);
       }

   }


   /**
    * Warns about a namespace profile key that cannot be permitted.
    *
    * @param key is the key that induced the warning.
    */
   public void notPermitted(String key) {
       mLogger.log(
            "profile " + namespaceName() + "." + key +
            " is not permitted, ignoring!", LogManager.WARNING_MESSAGE_LEVEL);
   }

   /**
    * Warns about a namespace profile key-value pair that is malformed.
    *
    * @param key   is the key that induced the warning.
    * @param value is the corresponding value of the key.
    */
   public void malformedKey(String key, String value) {
       mLogger.log(
            "profile " + namespaceName() + "." + key +
            " with value " + value + " is malformed, ignoring!",
            LogManager.WARNING_MESSAGE_LEVEL);
   }

   /**
     * An empty iterator that allows me to traverse in case of null objects.
     */
    protected class EmptyIterator implements Iterator{

        /**
         * Always returns false, as an empty iterator.
         *
         * @return false
         */
        public boolean	hasNext(){
            return false;
        }

        /**
         * Returns a null as we are iterating over nothing.
         *
         * @return null
         */
        public Object next(){
            return null;
        }

        /**
         * Returns a false, as no removal
         *
         */
       public void remove(){
       }


    }

}
