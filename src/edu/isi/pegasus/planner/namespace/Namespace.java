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

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.planner.catalog.classes.Profiles;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.classes.Profile;
import edu.isi.pegasus.planner.common.PegasusProperties;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The base namespace class that all the othernamepsace handling classes extend. Some constants are
 * defined.
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 * @version $Revision$
 */
public abstract class Namespace /*extends Data*/ {

    /** The LogManager object which is used to log all the messages. */
    public LogManager mLogger = LogManagerFactory.loadSingletonInstance();

    /** The version number associated with this API of Profile Namespaces. */
    public static final String VERSION = "1.2";

    // constants for whether the key
    // is valid in the namespace or not

    /** Either the key or the value specified is null or malformed. */
    public static final int MALFORMED_KEY = -1;

    /** The key is a valid key and can be put in the profiles. */
    public static final int VALID_KEY = 0;

    /** The key is unknown. Upto the profile namespace implementation whether to use it or not. */
    public static final int UNKNOWN_KEY = 1;

    /** The key is not permitted in as it clashes with default Pegasus constructs. */
    public static final int NOT_PERMITTED_KEY = 2;

    /** The key is deprecated. Support is for a limited time. */
    public static final int DEPRECATED_KEY = 3;

    /** The key value is empty . */
    public static final int EMPTY_KEY = 4;

    /** The key value is valid but contents should be merged to existing value if it exists */
    public static final int MERGE_KEY = 5;

    /**
     * The Map object that contains the profiles for a particular namespace. The Map is indexed by
     * profile key. Each value, is a profile value.
     */
    @Expose
    @SerializedName("profiles")
    protected Map mProfileMap;

    /**
     * Checks if the namespace specified is valid or not.
     *
     * @param namespace The namespace you want to check
     */
    public static boolean isNamespaceValid(String namespace) {

        return Profile.namespaceValid(namespace);
    }

    /**
     * This checks the whether a key value pair specified is valid in the current namespace or not,
     * and whether it clashes with other key value pairs that might have been set by Pegasus
     * internally.
     *
     * @return MALFORMED_KEY VALID_KEY UNKNOWN_KEY NOT_PERMITTED_KEY
     */
    public abstract int checkKey(String key, String value);

    /**
     * Merge the profiles in the namespace in a controlled manner. The profiles should be merged
     * only if the namespace object containing them matches to the current namespace.
     *
     * @param profiles the <code>Namespace</code> object containing the profiles.
     */
    public abstract void merge(Namespace profiles);

    /**
     * Returns the name of the namespace associated with the profile implementations.
     *
     * @return the namespace name.
     */
    public abstract String namespaceName();

    /**
     * Returns the contents as String. Currently, it returns condor compatible string that can be
     * put in the condor submit file
     *
     * @return textual description
     */
    public String toString() {
        return this.toCondor();
    }

    /**
     * Returns a condor description that can be used to put the contents of the namespace into the
     * condor submit file during code generation.
     *
     * @return String
     */
    public abstract String toCondor();

    /**
     * Provides an iterator to traverse the profiles by their keys.
     *
     * @return an iterator over the keys to walk the profile list.
     */
    public Iterator getProfileKeyIterator() {
        return (mProfileMap == null) ? new EmptyIterator() : this.mProfileMap.keySet().iterator();
    }

    /**
     * Singleton access to the deprecated table that holds the deprecated keys, and the keys that
     * replace them. It should be overriden in the namespaces, that have deprecated keys.
     *
     * @return Map
     */
    public Map deprecatedTable() {
        throw new UnsupportedOperationException(
                "No Deprecation support in the namespace " + namespaceName());
    }

    /**
     * It puts in the namespaces keys from another namespace instance.
     *
     * @param nm the namespace to be assimilated
     */
    public void checkKeyInNS(Namespace nm) {
        if (!nm.namespaceName().equals(this.namespaceName())) {
            // mismatch of namespaces
            throw new RuntimeException(
                    "Mismatch of namespaces " + this.namespaceName() + " " + nm.namespaceName());
        }
        for (Iterator it = nm.getProfileKeyIterator(); it.hasNext(); ) {
            String key = (String) it.next();
            this.checkKeyInNS(key, (String) nm.get(key));
        }
    }

    /**
     * It puts in the namespace specific information from the Transformation Catalog into the
     * namespace.
     *
     * @param entry the <code>TCEntry</code> object containing the result from the Transformation
     *     Catalog.
     */
    public void checkKeyInNS(TransformationCatalogEntry entry) {
        // sanity check
        if (entry == null) {
            return;
        }
        // pass down the list of Profile objects to be sucked in.
        checkKeyInNS(entry.getProfiles(this.namespaceName()));
    }

    /**
     * It takes in a Profiles object and puts them into the namespace after checking if the
     * namespace in the Profile object is same as the namepsace implementation.
     *
     * @param profile the <code>Profile</code> object containing the key and value.
     * @exception IllegalArgumentException if the namespace in the profile is not the same as the
     *     profile namepsace in which the profile is being incorporated.
     * @see org.griphyn.cPlanner.classes.Profile
     */
    public void checkKeyInNS(Profile profile) throws IllegalArgumentException {
        if (profile.getProfileNamespace().equals(this.namespaceName())) {
            checkKeyInNS(profile.getProfileKey(), profile.getProfileValue());
        } else {
            // throw an exception for the illegal Profile Argument
            throw new IllegalArgumentException("Illegal Profile " + profile);
        }
    }

    /**
     * It takes in a list of profiles and puts them into the namespace after checking if they are
     * valid or not. Note, there are no checks on the namespace however. The user should ensure that
     * each Profile object in the list is of the same namespace type.
     *
     * @param vars List of <code>Profile</code> objects, each referring to a key value for the
     *     profile.
     * @see org.griphyn.cPlanner.classes.Profile
     */
    public void checkKeyInNS(List vars) {
        if (vars == null || vars.isEmpty()) {
            // no variables to insert
            return;
        }

        Profile p = null;
        for (Iterator it = vars.iterator(); it.hasNext(); ) {
            p = (Profile) it.next();
            checkKeyInNS(p.getProfileKey(), p.getProfileValue());
        }
    }

    /**
     * It puts in the namespace specific information specified in the properties file into the
     * namespace. The name of the pool is also passed, as many of the properties specified in the
     * properties file are on a per pool basis.
     *
     * @param properties the <code>PegasusProperties</code> object containing all the properties
     *     that the user specified at various places (like .chimerarc, properties file, command
     *     line).
     * @param pool the pool name where the job is scheduled to run.
     */
    public abstract void checkKeyInNS(PegasusProperties properties, String pool);

    /**
     * This checks the whether a key value pair specified is valid in the current namespace or not
     * by calling the checkKey function and then on the basis of the values returned puts them into
     * the associated map in the class.
     *
     * @param key key that needs to be checked in the namespace for validity.
     * @param value value of the key
     */
    public void checkKeyInNS(String key, String value) {
        int action = checkKey(key, value);

        switch (action) {
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

            case Namespace.MERGE_KEY:
                mergeKey(key, value);
                break;

            default:
                throw new RuntimeException("Invalid return type for checkKey " + key + " " + value);
        }
    }

    /**
     * Assimilate the profiles in the namespace in a controlled manner. In case of intersection, the
     * new profile value overrides, the existing profile value.
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
    }

    /**
     * Returns true if the namespace contains a mapping for the specified key. More formally,
     * returns true if and only if this map contains at a mapping for a key k such that (key==null ?
     * k==null : key.equals(k)). (There can be at most one such mapping.)
     *
     * @param key The key that you want to search for in the namespace.
     */
    public boolean containsKey(Object key) {
        return (mProfileMap == null) ? false : mProfileMap.containsKey(key);
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
     * @param key The key you want to remove.
     * @return the value object if it exists. null if the key does not exist in the namespace.
     */
    public Object removeKey(Object key) {
        return mProfileMap.remove(key);
    }

    /**
     * Returns the key set associated with the namespace.
     *
     * @return key set if the mProfileMap is populated. null if the associated mProfileMap is not
     *     populated.
     */
    public Set keySet() {
        return (mProfileMap == null) ? null : mProfileMap.keySet();
    }

    /**
     * Returns the number of profiles in the namespace
     *
     * @return the count of keys
     */
    public int size() {
        return (mProfileMap == null) ? 0 : mProfileMap.keySet().size();
    }

    /**
     * Returns a boolean indicating if the object is empty.
     *
     * <p>The object is empty if the underlying map's key set is empty.
     *
     * @return
     */
    public boolean isEmpty() {
        return (mProfileMap == null) ? true : mProfileMap.keySet().isEmpty();
    }

    /**
     * Returns the value to which this namespace maps the specified key. Returns null if the map
     * contains no mapping for this key. A return value of null does not necessarily indicate that
     * the map contains no mapping for the key; it's also possible that the map explicitly maps the
     * key to null. The containsKey operation may be used to distinguish these two cases.
     *
     * @param key The key whose value you want.
     */
    public Object get(Object key) {
        return (mProfileMap == null) ? null : mProfileMap.get(key);
    }

    /**
     * Returns a int value, that a particular key is mapped to in this namespace. If the key is
     * mapped to a non int value or the key is not populated in the namespace , then default value
     * is returned.
     *
     * @param key The key whose boolean value you desire.
     * @return boolean
     */
    public int getIntValue(Object key, int deflt) {
        int value = deflt;
        if (mProfileMap != null && mProfileMap.containsKey(key)) {
            try {
                value = Integer.parseInt((String) mProfileMap.get(key));
            } catch (Exception e) {

            }
        }
        return value;
    }

    /**
     * Returns a long value, that a particular key is mapped to in this namespace. If the key is
     * mapped to a non long value or the key is not populated in the namespace , then default value
     * is returned.
     *
     * @param key The key whose boolean value you desire.
     * @return long value
     */
    public long getLongValue(Object key, long deflt) {
        long value = deflt;
        if (mProfileMap != null && mProfileMap.containsKey(key)) {
            try {
                value = Long.parseLong((String) mProfileMap.get(key));
            } catch (Exception e) {

            }
        }
        return value;
    }

    /**
     * Warns about an unknown profile key and constructs it anyway. Constructs a new RSL element of
     * the format (key=value).
     *
     * @param key is the left-hand-side
     * @param value is the right hand side
     */
    public void unknownKey(String key, String value) {
        mLogger.log(
                "unknown profile " + namespaceName() + "." + key + ",  using anyway",
                LogManager.WARNING_MESSAGE_LEVEL);
        construct(key, value);
    }

    /**
     * Warns about a deprecated profile key. It constructs the corresponding replacement key.
     *
     * @param key is the left-hand-side
     * @param value is the right hand side
     * @see #deprecatedTable()
     */
    public void deprecatedKey(String key, String value) {
        String replacement = (String) deprecatedTable().get(key);
        if (replacement == null) {
            // no replacement key for the deprecated
            // profile! Fatal Internal Error
            StringBuffer error = new StringBuffer();
            error.append("No replacement key exists for deprecated profile ")
                    .append(namespaceName())
                    .append(".")
                    .append(key);
            throw new RuntimeException(error.toString());
        }
        mLogger.log(
                "profile "
                        + namespaceName()
                        + "."
                        + key
                        + " is deprecated. Replacing with "
                        + namespaceName()
                        + "."
                        + replacement,
                LogManager.WARNING_MESSAGE_LEVEL);

        if (containsKey(replacement)) {
            // replacement key already exists.
            // use that ! might break profile overriding ??
        } else {
            construct(replacement, value);
        }
    }

    /**
     * Warns about a namespace profile key that cannot be permitted.
     *
     * @param key is the key that induced the warning.
     */
    public void notPermitted(String key) {
        mLogger.log(
                "profile " + namespaceName() + "." + key + " is not permitted, ignoring!",
                LogManager.WARNING_MESSAGE_LEVEL);
    }

    /**
     * Deletes the key from the namespace.
     *
     * @param key the key with empty value
     */
    public void emptyKey(String key) {
        mLogger.log(
                "profile " + namespaceName() + "." + key + " is empty, Removing!",
                LogManager.WARNING_MESSAGE_LEVEL);
        this.removeKey(key);
    }

    /**
     * Warns about a namespace profile key-value pair that is malformed.
     *
     * @param key is the key that induced the warning.
     * @param value is the corresponding value of the key.
     */
    public void malformedKey(String key, String value) {
        mLogger.log(
                "profile "
                        + namespaceName()
                        + "."
                        + key
                        + " with value "
                        + value
                        + " is malformed, ignoring!",
                LogManager.WARNING_MESSAGE_LEVEL);
    }

    /**
     * Merges key value to an existing value if it exists
     *
     * @param key
     * @param value
     */
    public void mergeKey(String key, String value) {
        throw new UnsupportedOperationException(
                "Function mergeKey(String,String not supported for namespace "
                        + this.namespaceName());
    }

    /** Resets the namespace, removing all profiles associated */
    public void reset() {
        if (this.mProfileMap != null) {
            this.mProfileMap.clear();
        }
    }

    /**
     * Returns the clone of the object.
     *
     * @return the clone
     */
    public Object clone() {
        Namespace obj;
        try {
            obj = (Namespace) super.clone();
            for (Iterator it = this.getProfileKeyIterator(); it.hasNext(); ) {
                String key = (String) it.next();
                obj.construct(key, (String) this.get(key));
            }

        } catch (CloneNotSupportedException e) {
            // somewhere in the hierarch chain clone is not implemented
            throw new RuntimeException(
                    "Clone not implemented in the base class of " + this.getClass().getName(), e);
        }
        return obj;
    }

    /** An empty iterator that allows me to traverse in case of null objects. */
    protected class EmptyIterator implements Iterator {

        /**
         * Always returns false, as an empty iterator.
         *
         * @return false
         */
        public boolean hasNext() {
            return false;
        }

        /**
         * Returns a null as we are iterating over nothing.
         *
         * @return null
         */
        public Object next() {
            return null;
        }

        /** Returns a false, as no removal */
        public void remove() {}
    }
}
