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
package edu.isi.pegasus.planner.catalog.replica.impl;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.common.util.CommonProperties;
import edu.isi.pegasus.planner.catalog.ReplicaCatalog;
import edu.isi.pegasus.planner.catalog.replica.*;
import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * A multiple replica catalog implementation that allows users to query different multiple catalogs
 * at the same time.
 *
 * <p>To use it set
 *
 * <pre>
 * pegasus.catalog.replica MRC
 * </pre>
 *
 * Each associated replica catalog can be configured via properties as follows.
 *
 * <p>The user associates a variable name referred to as [value] for each of the catalogs, where
 * [value] is any legal identifier (concretely [A-Za-z][_A-Za-z0-9]*)
 *
 * <p>For each associated replica catalogs the user specifies the following properties.
 *
 * <pre>
 * pegasus.catalog.replica.mrc.[value]      to specify the type of replica catalog.
 * pegasus.catalog.replica.mrc.[value].key  to specify a property name key for a
 *                                          particular catalog
 * </pre>
 *
 * <p>For example, if a user wants to query two lrc's at the same time he/she can specify as follows
 *
 * <pre>
 *    pegasus.catalog.replica.mrc.lrc1 LRC
 *    pegasus.catalog.replica.mrc.lrc2.url rls://sukhna
 *
 *    pegasus.catalog.replica.mrc.lrc2 LRC
 *    pegasus.catalog.replica.mrc.lrc2.url rls://smarty
 *
 * </pre>
 *
 * <p>In the above example, lrc1, lrc2 are any valid identifier names and url is the property key
 * that needed to be specified.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class MRC implements ReplicaCatalog {

    /** The prefix for the property subset for connecting to the individual catalogs. */
    public static final String PROPERTY_PREFIX = "mrc";

    /** The property key that designates the type of replica catalog to connect to. */
    public static final String TYPE_KEY = "type";

    /** The list of replica catalogs that need to be queried for. */
    protected List mRCList;

    /** The handle to the logging manager. */
    protected LogManager mLogger;

    /** The default constructor. */
    public MRC() {
        mRCList = new LinkedList();
        mLogger = LogManagerFactory.loadSingletonInstance();
    }

    /**
     * Removes everything from the catalogs. Use with care!!!
     *
     * @return the number of removed entries.
     */
    public int clear() {
        int result = 0;
        for (Iterator it = this.rcIterator(); it.hasNext(); ) {
            ReplicaCatalog catalog = (ReplicaCatalog) it.next();
            result += catalog.clear();
        }
        return 0;
    }

    /** Explicitely free resources before the garbage collection hits. */
    public void close() {
        for (Iterator it = this.rcIterator(); it.hasNext(); ) {
            ReplicaCatalog catalog = (ReplicaCatalog) it.next();
            catalog.close();
        }
    }

    /**
     * Establishes a link between the implementation and the thing the implementation is build upon.
     *
     * @param props contains all necessary data to establish the link.
     * @return true if connected now, or false to indicate a failure.
     */
    public boolean connect(Properties props) {

        // get the subset for the properties
        Properties subset = CommonProperties.matchingSubset(props, PROPERTY_PREFIX, false);
        mLogger.log("MRC Properties are " + subset, LogManager.DEBUG_MESSAGE_LEVEL);

        // container for properties for each of the different catalogs
        Map propertiesMap = new HashMap();

        // put each of the keys in the correct bin
        for (Iterator it = subset.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry entry = (Map.Entry) it.next();
            String key = (String) entry.getKey();
            String value = (String) entry.getValue();
            String name = getName(key); // bin stores the user defined name specified
            // now determine the key
            key = getKey(key, name);

            // store the key, value in the correct properties object
            Properties p;
            if (propertiesMap.containsKey(name)) {
                p = (Properties) propertiesMap.get(name);
            } else {
                p = new Properties();
                propertiesMap.put(name, p);
            }
            p.setProperty(key, value);
        }

        // now that we have all the properties sorted accd to individual catalogs
        // try connecting to them one by one
        boolean result = true;
        for (Iterator it = propertiesMap.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry entry = (Map.Entry) it.next();
            result &= connect((String) entry.getKey(), (Properties) entry.getValue());

            // if unable to connect to any single
            // break out and exit
            if (!result) {
                mLogger.log(
                        "MRC unable to connect to replica catalog backend "
                                + entry.getKey()
                                + " with connection properties "
                                + entry.getValue(),
                        LogManager.ERROR_MESSAGE_LEVEL);
                break;
            }
            mLogger.log(
                    "MRC Successfully connect to replica catalog backend "
                            + entry.getKey()
                            + " with connection properties "
                            + entry.getValue(),
                    LogManager.DEBUG_MESSAGE_LEVEL);
        }

        // if the result is false, then disconnect from
        // already connected replica catalogs
        if (!result) {
            close();
        }

        return result;
    }

    /**
     * Connects to an individual replica catalog. Also adds the handle to the connected replica
     * catalog in the internal list.
     *
     * @param name the name given by the user in the properties file.
     * @param properties the properties to use for connecting.
     * @return boolean
     */
    protected boolean connect(String name, Properties properties) {

        // get the type first
        String type = properties.getProperty(this.TYPE_KEY);
        if (type == null) {
            StringBuffer message = new StringBuffer();
            message.append("No type associated with replica catalog of name ").append(name);
            message.append("Set the property ")
                    .append(ReplicaCatalog.c_prefix)
                    .append(".")
                    .append(name);
            mLogger.log(message.toString(), LogManager.DEBUG_MESSAGE_LEVEL);
            return false;
        }

        // try and connect
        ReplicaCatalog catalog = null;
        try {
            catalog = ReplicaFactory.loadInstance(type, properties);
        } catch (Exception e) {
            // log the connection error
            mLogger.log(
                    "Unable to connect to replica catalog of name " + name,
                    e,
                    LogManager.ERROR_MESSAGE_LEVEL);
            return false;
        }

        mRCList.add(catalog);
        return true;
    }

    /**
     * Returns an iterator to iterate through the list of ReplicaCatalogs that MRC is associated
     * with.
     *
     * @return Iterator
     */
    protected Iterator rcIterator() {
        return this.mRCList.iterator();
    }

    /**
     * Returns the name from the key. The name is first component of the key before the first dot
     * (.).
     *
     * @param key String
     * @return String
     */
    protected String getName(String key) {

        return (key.indexOf('.') == -1)
                ?
                // if there is no instance of . then the key is the name
                key
                :
                // else get the substring to first dot
                key.substring(0, key.indexOf('.'));
    }

    /**
     * Returns the key with the prefix stripped off. In the case, where the key is the prefix,
     * STYLE_KEY is returned. If the key does not start with the prefix, then null is returned.
     *
     * @param key the key
     * @param prefix String
     * @return key stripped off of the prefix
     * @see #TYPE_KEY
     */
    protected String getKey(String key, String prefix) {
        // sanity check
        if (!key.startsWith(prefix)) return null;

        // if the key and prefix are same length
        if (key.length() == prefix.length()) {
            return this.TYPE_KEY;
        }

        // if prefix does not end in a dot add a dot
        if (prefix.charAt(prefix.length() - 1) != '.') {
            prefix = prefix + '.';
        }

        // for a valid subsetting operation there should be . at prefix.length() - 1
        // allows us to distinguish between lrc1.url and lrc1a.url for prefix
        // lrc1
        return (key.charAt(prefix.length() - 1) != '.') ? null : key.substring(prefix.length());
    }

    /**
     * Deletes all PFN entries for a given LFN from the replica catalog where the PFN attribute is
     * found, and matches exactly the object value.
     *
     * @param lfn is the logical filename to look for.
     * @param name is the PFN attribute name to look for.
     * @param value is an exact match of the attribute value to match.
     * @return the number of removed entries.
     */
    public int delete(String lfn, String name, Object value) {
        int result = 0;
        for (Iterator it = this.rcIterator(); it.hasNext(); ) {
            ReplicaCatalog catalog = (ReplicaCatalog) it.next();
            result += catalog.delete(lfn, name, value);
        }
        return result;
    }

    /**
     * Deletes a very specific mapping from the replica catalog.
     *
     * @param lfn is the logical filename in the tuple.
     * @param tuple is a description of the PFN and its attributes.
     * @return the number of removed entries, either 0 or 1.
     */
    public int delete(String lfn, ReplicaCatalogEntry tuple) {
        int result = 0;
        for (Iterator it = this.rcIterator(); it.hasNext(); ) {
            ReplicaCatalog catalog = (ReplicaCatalog) it.next();
            result += catalog.delete(lfn, tuple);
        }
        return result;
    }

    /**
     * Deletes multiple mappings into the replica catalog.
     *
     * @param x is a map from logical filename string to list of replica catalog entries.
     * @param matchAttributes whether mapping should be deleted only if all attributes match.
     * @return the number of deletions.
     */
    public int delete(Map x, boolean matchAttributes) {
        int result = 0;
        for (Iterator it = this.rcIterator(); it.hasNext(); ) {
            ReplicaCatalog catalog = (ReplicaCatalog) it.next();
            result += catalog.delete(x, matchAttributes);
        }
        return result;
    }

    /**
     * Deletes a specific mapping from the replica catalog.
     *
     * @param lfn is the logical filename in the tuple.
     * @param pfn is the physical filename in the tuple.
     * @return the number of removed entries.
     */
    public int delete(String lfn, String pfn) {
        int result = 0;
        for (Iterator it = this.rcIterator(); it.hasNext(); ) {
            ReplicaCatalog catalog = (ReplicaCatalog) it.next();
            result += catalog.delete(lfn, pfn);
        }
        return result;
    }

    /**
     * Deletes all PFN entries for a given LFN from the replica catalog where the resource handle is
     * found.
     *
     * @param lfn is the logical filename to look for.
     * @param handle is the resource handle
     * @return the number of entries removed.
     */
    public int deleteByResource(String lfn, String handle) {
        int result = 0;
        for (Iterator it = this.rcIterator(); it.hasNext(); ) {
            ReplicaCatalog catalog = (ReplicaCatalog) it.next();
            result += catalog.delete(lfn, handle);
        }
        return result;
    }

    /**
     * Inserts a new mapping into the replica catalog.
     *
     * @param lfn is the logical filename under which to book the entry.
     * @param pfn is the physical filename associated with it.
     * @param handle is a resource handle where the PFN resides.
     * @return number of insertions, should always be 1. On failure, throw an exception, don't use
     *     zero.
     * @throws UnsupportedOperationException
     */
    public int insert(String lfn, String pfn, String handle) {
        throw new UnsupportedOperationException(
                "Method insert( String, String, String ) not supported in MRC");
    }

    /**
     * Inserts a new mapping into the replica catalog.
     *
     * @param lfn is the logical filename under which to book the entry.
     * @param tuple is the physical filename and associated PFN attributes.
     * @return number of insertions, should always be 1. On failure, throw exception
     * @throws UnsupportedOperationException
     */
    public int insert(String lfn, ReplicaCatalogEntry tuple) {
        throw new UnsupportedOperationException(
                "Method insert( String, ReplicaCatalogEntry ) not supported in MRC");
    }

    /**
     * Inserts multiple mappings into the replica catalog.
     *
     * @param x is a map from logical filename string to list of replica catalog entries.
     * @return the number of insertions.
     * @throws UnsupportedOperationException
     */
    public int insert(Map x) {
        throw new UnsupportedOperationException("Method insert( Map ) not supported in MRC");
    }

    /**
     * Predicate to check, if the connection with the catalog's implementation is still active.
     * Returns true only if the connections to all the associated replica catalogs is closed.
     *
     * @return true, if the implementation is disassociated, false otherwise.
     */
    public boolean isClosed() {
        boolean result = true;
        for (Iterator it = this.rcIterator(); it.hasNext(); ) {
            ReplicaCatalog catalog = (ReplicaCatalog) it.next();
            result &= catalog.isClosed();
        }
        return result;
    }

    /**
     * Lists a subset of all logical filenames in the catalog.
     *
     * @param constraint is a constraint for the logical filename only. It is a string that has some
     *     meaning to the implementing system. This can be a SQL wildcard for queries, or a regular
     *     expression for Java-based memory collections.
     * @return A set of logical filenames that match. The set may be empty
     */
    public Set list(String constraint) {
        Set result = new HashSet();
        for (Iterator it = this.rcIterator(); it.hasNext(); ) {
            ReplicaCatalog catalog = (ReplicaCatalog) it.next();
            result.addAll(catalog.list(constraint));
        }
        return result;
    }

    /**
     * Lists all logical filenames in the catalog.
     *
     * @return A set of all logical filenames known to the catalog.
     */
    public Set list() {
        Set result = new HashSet();
        for (Iterator it = this.rcIterator(); it.hasNext(); ) {
            ReplicaCatalog catalog = (ReplicaCatalog) it.next();
            result.addAll(catalog.list());
        }
        return result;
    }

    /**
     * Retrieves the entry for a given filename and resource handle from the replica catalog.
     *
     * @param lfn is the logical filename to obtain information for.
     * @param handle is the resource handle to obtain entries for.
     * @return the (first) matching physical filename, or <code>null</code> if no match was found.
     */
    public String lookup(String lfn, String handle) {
        String result = null;
        for (Iterator it = this.rcIterator(); it.hasNext(); ) {
            ReplicaCatalog catalog = (ReplicaCatalog) it.next();

            if ((result = catalog.lookup(lfn, handle)) != null) {
                return result;
            }
        }
        return result;
    }

    /**
     * Retrieves all entries for a given LFN from the replica catalog.
     *
     * @param lfn is the logical filename to obtain information for.
     * @return a collection of replica catalog entries
     */
    public Collection lookup(String lfn) {
        Collection result = new LinkedList();
        for (Iterator it = this.rcIterator(); it.hasNext(); ) {
            ReplicaCatalog catalog = (ReplicaCatalog) it.next();
            Collection l = catalog.lookup(lfn);
            if (l != null) {
                result.addAll(l);
            }
        }
        return result;
    }

    /**
     * Retrieves multiple entries for a given logical filename, up to the complete catalog.
     *
     * @param lfns is a set of logical filename strings to look up.
     * @param handle is the resource handle, restricting the LFNs.
     * @return a map indexed by the LFN. Each value is a collection of replica catalog entries (all
     *     attributes).
     */
    public Map lookup(Set lfns, String handle) {
        Map result = new HashMap();

        for (Iterator it = this.rcIterator(); it.hasNext(); ) {
            ReplicaCatalog catalog = (ReplicaCatalog) it.next();
            Map m = catalog.lookup(lfns, handle);

            // merge all the entries in the map into the result
            for (Iterator mit = m.entrySet().iterator(); mit.hasNext(); ) {
                Map.Entry entry = (Map.Entry) mit.next();
                // merge the entries into the main result
                String lfn = (String) entry.getKey(); // the lfn
                if (result.containsKey(lfn)) {
                    // right now no merging of RCE being done on basis
                    // on them having same pfns. duplicate might occur.
                    ((Set) result.get(lfn)).addAll((Set) entry.getValue());
                } else {
                    result.put(lfn, entry.getValue());
                }
            }
        }
        return result;
    }

    /**
     * Retrieves multiple entries for a given logical filename, up to the complete catalog.
     *
     * @param constraints is mapping of keys 'lfn', 'pfn', or any attribute name, e.g. the resource
     *     handle 'site', to a string that has some meaning to the implementing system. This can be
     *     a SQL wildcard for queries, or a regular expression for Java-based memory collections.
     *     Unknown keys are ignored. Using an empty map requests the complete catalog.
     * @return a map indexed by the LFN. Each value is a collection of replica catalog entries.
     */
    public Map lookup(Map constraints) {

        Map result = new HashMap();

        for (Iterator it = this.rcIterator(); it.hasNext(); ) {
            ReplicaCatalog catalog = (ReplicaCatalog) it.next();
            Map m = catalog.lookup(constraints);

            // merge all the entries in the map into the result
            for (Iterator mit = m.entrySet().iterator(); mit.hasNext(); ) {
                Map.Entry entry = (Map.Entry) mit.next();
                // merge the entries into the main result
                String lfn = (String) entry.getKey(); // the lfn
                if (result.containsKey(lfn)) {
                    // right now no merging of RCE being done on basis
                    // on them having same pfns. duplicate might occur.
                    ((Set) result.get(lfn)).addAll((Set) entry.getValue());
                } else {
                    result.put(lfn, entry.getValue());
                }
            }
        }
        return result;
    }

    /**
     * Retrieves multiple entries for a given logical filename, up to the complete catalog.
     *
     * @param lfns is a set of logical filename strings to look up.
     * @return a map indexed by the LFN. Each value is a collection of replica catalog entries for
     *     the LFN.
     */
    public Map lookup(Set lfns) {

        Map result = new HashMap();

        for (Iterator it = this.rcIterator(); it.hasNext(); ) {
            ReplicaCatalog catalog = (ReplicaCatalog) it.next();
            Map m = catalog.lookup(lfns);

            // merge all the entries in the map into the result
            for (Iterator mit = m.entrySet().iterator(); mit.hasNext(); ) {
                Map.Entry entry = (Map.Entry) mit.next();
                // merge the entries into the main result
                String lfn = (String) entry.getKey(); // the lfn
                if (result.containsKey(lfn)) {
                    // right now no merging of RCE being done on basis
                    // on them having same pfns. duplicate might occur.
                    ((Collection) result.get(lfn)).addAll((Collection) entry.getValue());
                } else {
                    result.put(lfn, entry.getValue());
                }
            }
        }
        return result;
    }

    /**
     * Retrieves all entries for a given LFN from the replica catalog.
     *
     * @param lfn is the logical filename to obtain information for.
     * @return a set of PFN strings
     */
    public Set lookupNoAttributes(String lfn) {
        Set result = new HashSet();
        for (Iterator it = this.rcIterator(); it.hasNext(); ) {
            ReplicaCatalog catalog = (ReplicaCatalog) it.next();
            result.addAll(catalog.lookupNoAttributes(lfn));
        }
        return result;
    }

    /**
     * Retrieves multiple entries for a given logical filename, up to the complete catalog.
     *
     * @param lfns is a set of logical filename strings to look up.
     * @return a map indexed by the LFN. Each value is a set of PFN strings.
     */
    public Map lookupNoAttributes(Set lfns) {
        Map result = new HashMap();
        for (Iterator it = lfns.iterator(); it.hasNext(); ) {
            String lfn = (String) it.next();
            result.put(lfn, this.lookupNoAttributes(lfn));
        }
        return result;
    }

    /**
     * Retrieves multiple entries for a given logical filename, up to the complete catalog.
     *
     * @param lfns is a set of logical filename strings to look up.
     * @param handle is the resource handle, restricting the LFNs.
     * @return a map indexed by the LFN. Each value is a set of physical filenames.
     */
    public Map lookupNoAttributes(Set lfns, String handle) {
        Map result = new HashMap();

        for (Iterator it = this.rcIterator(); it.hasNext(); ) {
            ReplicaCatalog catalog = (ReplicaCatalog) it.next();
            Map m = this.lookupNoAttributes(lfns, handle);

            // merge the map into the result
            for (Iterator mit = m.entrySet().iterator(); mit.hasNext(); ) {
                Map.Entry entry = (Map.Entry) mit.next();
                // merge the entries into the main result
                String key = (String) entry.getKey(); // the lfn
                if (result.containsKey(key)) {
                    // merge the results
                    ((Set) result.get(key)).addAll((Set) entry.getValue());
                } else {
                    result.put(key, entry.getValue());
                }
            }
        }
        return result;
    }

    /**
     * Removes all mappings for an LFN from the replica catalog.
     *
     * @param lfn is the logical filename to remove all mappings for.
     * @return the number of removed entries.
     */
    public int remove(String lfn) {
        int result = 0;
        for (Iterator it = this.rcIterator(); it.hasNext(); ) {
            ReplicaCatalog catalog = (ReplicaCatalog) it.next();
            result += catalog.remove(lfn);
        }
        return result;
    }

    /**
     * Removes all mappings for a set of LFNs.
     *
     * @param lfns is a set of logical filename to remove all mappings for.
     * @return the number of removed entries.
     */
    public int remove(Set lfns) {
        int result = 0;
        for (Iterator it = this.rcIterator(); it.hasNext(); ) {
            ReplicaCatalog catalog = (ReplicaCatalog) it.next();
            result += catalog.remove(lfns);
        }
        return result;
    }

    /**
     * Removes all entries from the replica catalog where the PFN attribute is found, and matches
     * exactly the object value.
     *
     * @param name is the PFN attribute name to look for.
     * @param value is an exact match of the attribute value to match.
     * @return the number of removed entries.
     */
    public int removeByAttribute(String name, Object value) {
        int result = 0;
        for (Iterator it = this.rcIterator(); it.hasNext(); ) {
            ReplicaCatalog catalog = (ReplicaCatalog) it.next();
            result += catalog.removeByAttribute(name, value);
        }
        return result;
    }

    /**
     * Removes all entries associated with a particular resource handle.
     *
     * @param handle is the site handle to remove all entries for.
     * @return the number of removed entries.
     */
    public int removeByAttribute(String handle) {
        int result = 0;
        for (Iterator it = this.rcIterator(); it.hasNext(); ) {
            ReplicaCatalog catalog = (ReplicaCatalog) it.next();
            result += catalog.removeByAttribute(handle);
        }
        return result;
    }

    /**
     * Returns the file source.
     *
     * @return the file source if it exists , else null
     */
    public java.io.File getFileSource() {
        return null;
    }

    /**
     * Set the catalog to read-only mode.
     *
     * @param readonly whether the catalog is read-only
     */
    @Override
    public void setReadOnly(boolean readonly) {
        // do nothing
    }
}
