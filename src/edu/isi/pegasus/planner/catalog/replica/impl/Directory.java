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

import edu.isi.pegasus.common.util.Boolean;
import edu.isi.pegasus.planner.catalog.ReplicaCatalog;
import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry;
import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogException;
import edu.isi.pegasus.planner.catalog.replica.ReplicaFactory;
import edu.isi.pegasus.planner.common.PegasusProperties;
import java.io.*;
import java.util.*;
import java.util.regex.Pattern;

/**
 * This class implements a replica catalog on top of a directory. This implementation does a
 * directory listing to build up the lfn, pfn mappings for the Replica Catalog.
 *
 * <p>To connect to this implementation, in Pegasus Properties set
 *
 * <p>pegasus.catalog.replica Directory
 *
 * <p>The site attribute defaults to local unless specified in Pegasus Properties by specifying the
 * property
 *
 * <pre>
 *      pegasus.catalog.replica.directory.site
 * </pre>
 *
 * The URL prefix for the PFN's defaults to file:// unless specified in Pegasus Properties by
 * specifying the property
 *
 * <pre>
 *      pegasus.catalog.replica.directory.url.prefix
 * </pre>
 *
 * By default, deep LFN's are constructed while traversing through the directory, unless the
 * following property is set to true
 *
 * <pre>
 *     pegasus.catalog.replica.directory.flat.lfn
 * </pre>
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class Directory implements ReplicaCatalog {

    /** The default site handle to use. */
    public static final String DEFAULT_SITE_HANDLE = "local";

    /** The default URL Prefix to use. */
    public static final String DEFAULT_URL_PREFIX = "file://";

    /**
     * The name of the key that disables writing back to the cache file. Designates a static file.
     * i.e. read only
     */
    // public static final String READ_ONLY_PROPERTY_KEY = "read.only";

    /** The name of the key that specifies the path to directory. */
    public static final String DIRECTORY_PROPERTY_KEY = "directory";

    /** The name of the key that specifies the site attribute to be associated */
    public static final String SITE_PROPERTY_KEY = "directory.site";

    /** The name of the key that specifies the url prefix to be associated with the PFN's */
    public static final String URL_PRFIX_PROPERTY_KEY = "directory.url.prefix";

    /**
     * the name of the key that specifies whether we want flat lfns or not. By default it is false,
     * i.e we construct deep lfn's when traversing through the directory hierarchy
     */
    public static final String FLAT_LFN_PROPERTY_KEY = "directory.flat.lfn";

    /** Records the name of the on-disk representation. */
    protected String mDirectory = null;

    /** Maintains a memory slurp of the file representation. */
    protected Map mLFNMap = null;

    /** A boolean indicating whether the catalog is read only or not. */
    boolean mReadOnly;

    /** A boolean indicating whether the catalog is to construct flat lfns or not */
    boolean mConstructFlatLFN;

    /** The site handle to use. */
    protected String mSiteHandle;

    /** The URL prefix to use */
    protected String mURLPrefix;

    /**
     * Default empty constructor creates an object that is not yet connected to any database. You
     * must use support methods to connect before this instance becomes usable.
     *
     * @see #connect( Properties )
     */
    public Directory() {
        // make connection defunc
        mLFNMap = null;
        mDirectory = null;
        // mReadOnly = false;
        mConstructFlatLFN = false;
        mSiteHandle = Directory.DEFAULT_SITE_HANDLE;
        mURLPrefix = Directory.DEFAULT_URL_PREFIX;
    }

    /**
     * Establishes a connection to the database from the properties. You will need to specify a
     * "file" property to point to the location of the on-disk instance. If the property "quote" is
     * set to a true value, LFNs and PFNs are always quoted. By default, and if false, LFNs and PFNs
     * are only quoted as necessary.
     *
     * @param props is the property table with sufficient settings to establish a link with the
     *     database.
     * @return true if connected, false if failed to connect.
     * @throws Error subclasses for runtime errors in the class loader.
     */
    public boolean connect(Properties props) {

        // update the m_writeable flag if specified
        if (props.containsKey(Directory.FLAT_LFN_PROPERTY_KEY)) {
            mConstructFlatLFN =
                    Boolean.parse(props.getProperty(Directory.FLAT_LFN_PROPERTY_KEY), false);
        }

        String value = props.getProperty(Directory.SITE_PROPERTY_KEY);
        if (value != null) {
            this.mSiteHandle = value;
        }

        value = props.getProperty(Directory.URL_PRFIX_PROPERTY_KEY);
        if (value != null) {
            this.mURLPrefix = value;
        }

        if (props.containsKey(Directory.DIRECTORY_PROPERTY_KEY)) {
            return connect(props.getProperty("directory"));
        }
        return false;
    }

    /**
     * Does the file listing on the directory to create the mappings in memory.
     *
     * @param directory is the name of the file to read.
     * @return true, if the in-memory data structures appear sound.
     */
    public boolean connect(String directory) {

        // sanity check
        if (directory == null) {
            return false;
        }
        mDirectory = directory;
        mLFNMap = new LinkedHashMap();

        try {
            File f = new File(directory);
            if (f.exists() && f.isDirectory()) {
                traverse(f, null);
            } else {
                return false;
            }
        } catch (Exception ioe) {
            mLFNMap = null;
            mDirectory = null;
            throw new RuntimeException(ioe); // re-throw
        }

        return true;
    }

    /**
     * Traverses a directory and populates the mappings in memory
     *
     * @param directory the directory to traverse.
     * @param prefix the LFN prefix to be applied
     */
    private void traverse(File directory, String prefix) {
        // sanity check, if we can read it
        if (!directory.canRead()) {
            // warn and return
            System.err.println("Ignoring. Unable to read directory " + directory);
            return;
        }

        for (File f : directory.listFiles()) {
            StringBuffer lfn = new StringBuffer();
            String name = f.getName();
            if (mConstructFlatLFN || prefix == null || prefix.isEmpty()) {
                lfn.append(name);
            } else {
                lfn.append(prefix).append(File.separator).append(name);
            }

            if (f.isDirectory()) {
                // the lfn is the prefix now
                traverse(f, lfn.toString());
            } else {
                // we have a mapping to populate
                String pfn = this.mURLPrefix + f.getAbsolutePath();
                // System.out.println( lfn + " => " + pfn );
                insert(lfn.toString(), new ReplicaCatalogEntry(pfn, mSiteHandle));
            }
        }

        return;
    }

    /**
     * This operation will dump the in-memory representation back onto disk. The store operation is
     * strict in what it produces. The LFN and PFN records are only quoted, if they require quotes,
     * because they contain special characters. The attributes are <b>always</b> quoted and thus
     * quote-escaped.
     */
    public void close() {
        mLFNMap.clear();
        mLFNMap = null;
        mDirectory = null;
    }

    /**
     * Predicate to check, if the connection with the catalog's implementation is still active. This
     * helps determining, if it makes sense to call <code>close()</code>.
     *
     * @return true, if the implementation is disassociated, false otherwise.
     * @see #close()
     */
    public boolean isClosed() {
        return (mLFNMap == null);
    }

    /**
     * Retrieves the entry for a given directory and site handle from the replica catalog.
     *
     * @param lfn is the logical directory to obtain information for.
     * @param handle is the resource handle to obtain entries for.
     * @return the (first) matching physical directory, or <code>null</code> if no match was found.
     */
    public String lookup(String lfn, String handle) {
        Collection c = (Collection) mLFNMap.get(lfn);
        if (c == null) {
            return null;
        }

        for (Iterator i = c.iterator(); i.hasNext(); ) {
            ReplicaCatalogEntry rce = (ReplicaCatalogEntry) i.next();
            String pool = rce.getResourceHandle();
            if (pool == null && handle == null
                    || pool != null && handle != null && pool.equals(handle)) {
                return rce.getPFN();
            }
        }
        return null;
    }

    /**
     * Retrieves all entries for a given LFN from the replica catalog. Each entry in the result set
     * is a tuple of a PFN and all its attributes.
     *
     * @param lfn is the logical directory to obtain information for.
     * @return a collection of replica catalog entries
     * @see ReplicaCatalogEntry
     */
    public Collection lookup(String lfn) {
        Collection c = (Collection) mLFNMap.get(lfn);
        if (c == null) {
            return new ArrayList();
        } else {
            return new ArrayList(c);
        }
    }

    /**
     * Retrieves all entries for a given LFN from the replica catalog. Each entry in the result set
     * is just a PFN string. Duplicates are reduced through the set paradigm.
     *
     * @param lfn is the logical directory to obtain information for.
     * @return a set of PFN strings
     */
    public Set lookupNoAttributes(String lfn) {
        Set result = new TreeSet();
        Collection c = (Collection) mLFNMap.get(lfn);

        if (c != null) {
            for (Iterator i = c.iterator(); i.hasNext(); ) {
                result.add(((ReplicaCatalogEntry) i.next()).getPFN());
            }
        }

        // done
        return result;
    }

    /**
     * Retrieves multiple entries for a given logical directory, up to the complete catalog.
     * Retrieving full catalogs should be harmful, but may be helpful in an online display or
     * portal.
     *
     * @param lfns is a set of logical directory strings to look up.
     * @return a map indexed by the LFN. Each value is a collection of replica catalog entries for
     *     the LFN.
     * @see edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry
     */
    public Map lookup(Set lfns) {
        Map result = new HashMap();
        if (lfns == null || lfns.size() == 0) {
            return result;
        }

        for (Iterator i = lfns.iterator(); i.hasNext(); ) {
            String lfn = (String) i.next();
            Collection c = (Collection) mLFNMap.get(lfn);
            if (c == null) {
                result.put(lfn, new ArrayList());
            } else {
                result.put(lfn, new ArrayList(c));
            }
        }

        // done
        return result;
    }

    /**
     * Retrieves multiple entries for a given logical directory, up to the complete catalog.
     * Retrieving full catalogs should be harmful, but may be helpful in an online display or
     * portal.
     *
     * @param lfns is a set of logical directory strings to look up.
     * @return a map indexed by the LFN. Each value is a set of PFN strings.
     */
    public Map lookupNoAttributes(Set lfns) {
        Map result = new HashMap();
        if (lfns == null || lfns.size() == 0) {
            return result;
        }

        for (Iterator i = lfns.iterator(); i.hasNext(); ) {
            Set value = new TreeSet();
            String lfn = (String) i.next();
            Collection c = (Collection) mLFNMap.get(lfn);
            if (c != null) {
                for (Iterator j = c.iterator(); j.hasNext(); ) {
                    value.add(((ReplicaCatalogEntry) j.next()).getPFN());
                }
            }
            result.put(lfn, value);
        }

        // done
        return result;
    }

    /**
     * Retrieves multiple entries for a given logical directory, up to the complete catalog.
     * Retrieving full catalogs should be harmful, but may be helpful in online display or portal.
     *
     * <p>
     *
     * @param lfns is a set of logical directory strings to look up.
     * @param handle is the resource handle, restricting the LFNs.
     * @return a map indexed by the LFN. Each value is a collection of replica catalog entries (all
     *     attributes).
     * @see ReplicaCatalogEntry
     */
    public Map lookup(Set lfns, String handle) {
        Map result = new HashMap();
        if (lfns == null || lfns.size() == 0) {
            return result;
        }

        for (Iterator i = lfns.iterator(); i.hasNext(); ) {
            String lfn = (String) i.next();
            Collection c = (Collection) mLFNMap.get(lfn);
            if (c != null) {
                List value = new ArrayList();

                for (Iterator j = c.iterator(); j.hasNext(); ) {
                    ReplicaCatalogEntry rce = (ReplicaCatalogEntry) j.next();
                    String pool = rce.getResourceHandle();
                    if (pool == null && handle == null
                            || pool != null && handle != null && pool.equals(handle)) {
                        value.add(rce);
                    }
                }

                // only put found LFNs into result
                result.put(lfn, value);
            }
        }

        // done
        return result;
    }

    /**
     * Retrieves multiple entries for a given logical directory, up to the complete catalog.
     * Retrieving full catalogs should be harmful, but may be helpful in online display or portal.
     *
     * <p>
     *
     * @param lfns is a set of logical directory strings to look up.
     * @param handle is the resource handle, restricting the LFNs.
     * @return a map indexed by the LFN. Each value is a set of physical filenames.
     */
    public Map lookupNoAttributes(Set lfns, String handle) {
        Map result = new HashMap();
        if (lfns == null || lfns.size() == 0) {
            return result;
        }

        for (Iterator i = lfns.iterator(); i.hasNext(); ) {
            String lfn = (String) i.next();
            Collection c = (Collection) mLFNMap.get(lfn);
            if (c != null) {
                List value = new ArrayList();

                for (Iterator j = c.iterator(); j.hasNext(); ) {
                    ReplicaCatalogEntry rce = (ReplicaCatalogEntry) j.next();
                    String pool = rce.getResourceHandle();
                    if (pool == null && handle == null
                            || pool != null && handle != null && pool.equals(handle)) {
                        value.add(rce.getPFN());
                    }
                }

                // only put found LFNs into result
                result.put(lfn, value);
            }
        }

        // done
        return result;
    }

    /**
     * Retrieves multiple entries for a given logical directory, up to the complete catalog.
     * Retrieving full catalogs should be harmful, but may be helpful in online display or portal.
     *
     * @param constraints is mapping of keys 'lfn', 'pfn', or any attribute name, e.g. the resource
     *     handle 'pool', to a string that has some meaning to the implementing system. This can be
     *     a SQL wildcard for queries, or a regular expression for Java-based memory collections.
     *     Unknown keys are ignored. Using an empty map requests the complete catalog.
     * @return a map indexed by the LFN. Each value is a collection of replica catalog entries.
     * @see ReplicaCatalogEntry
     */
    public Map lookup(Map constraints) {
        if (constraints == null || constraints.size() == 0) {
            // return everything
            return Collections.unmodifiableMap(mLFNMap);

        } else if (constraints.size() == 1 && constraints.containsKey("lfn")) {
            // return matching LFNs
            Pattern p = Pattern.compile((String) constraints.get("lfn"));
            Map result = new HashMap();
            for (Iterator i = mLFNMap.entrySet().iterator(); i.hasNext(); ) {
                Map.Entry e = (Map.Entry) i.next();
                String lfn = (String) e.getKey();
                if (p.matcher(lfn).matches()) {
                    result.put(lfn, e.getValue());
                }
            }
            return result;

        } else {
            // FIXME: Implement!
            throw new RuntimeException("method not implemented");
        }
    }

    /**
     * Lists all logical filenames in the catalog.
     *
     * @return A set of all logical filenames known to the catalog.
     */
    public Set list() {
        return new TreeSet(mLFNMap.keySet());
    }

    /**
     * Lists a subset of all logical filenames in the catalog.
     *
     * @param constraint is a constraint for the logical directory only. It is a string that has
     *     some meaning to the implementing system. This can be a SQL wildcard for queries, or a
     *     regular expression for Java-based memory collections.
     * @return A set of logical filenames that match. The set may be empty
     */
    public Set list(String constraint) {
        Set result = new TreeSet();
        Pattern p = Pattern.compile(constraint);

        for (Iterator i = mLFNMap.keySet().iterator(); i.hasNext(); ) {
            String lfn = (String) i.next();
            if (p.matcher(lfn).matches()) {
                result.add(lfn);
            }
        }

        // done
        return result;
    }

    /**
     * Inserts a new mapping into the replica catalog. Any existing mapping of the same LFN, PFN,
     * and HANDLE will be replaced, including all of its attributes.
     *
     * @param lfn is the logical directory under which to book the entry.
     * @param tuple is the physical directory and associated PFN attributes.
     * @return number of insertions, should always be 1. On failure, throw an exception, don't use
     *     zero.
     */
    public int insert(String lfn, ReplicaCatalogEntry tuple) {
        if (lfn == null || tuple == null) {
            throw new NullPointerException();
        }

        Collection c = null;
        if (mLFNMap.containsKey(lfn)) {

            if (this.mConstructFlatLFN) {
                // for flat LFN's we need to throw error if two files of the same
                // name exist
                StringBuffer error = new StringBuffer();
                error.append("Entry for lfn ")
                        .append(lfn)
                        .append("already exists ")
                        .append(mLFNMap.get(lfn));
                throw new ReplicaCatalogException(error.toString());
            }

            boolean seen = false;
            String pfn = tuple.getPFN();
            String handle = tuple.getResourceHandle();
            c = (Collection) mLFNMap.get(lfn);
            for (Iterator i = c.iterator(); i.hasNext() && !seen; ) {
                ReplicaCatalogEntry rce = (ReplicaCatalogEntry) i.next();
                if ((seen = pfn.equals(rce.getPFN())) && handle.equals(rce.getResourceHandle())) {
                    try {
                        i.remove();
                    } catch (UnsupportedOperationException uoe) {
                        return 0;
                    }
                }
            }
        } else {
            c = new ArrayList();
            mLFNMap.put(lfn, c);
        }
        c.add(tuple);

        return 1;
    }

    /**
     * Inserts a new mapping into the replica catalog. This is a convenience function exposing the
     * resource handle. Internally, the <code>ReplicaCatalogEntry</code> element will be contructed,
     * and passed to the appropriate insert function.
     *
     * @param lfn is the logical directory under which to book the entry.
     * @param pfn is the physical directory associated with it.
     * @param handle is a resource handle where the PFN resides.
     * @return number of insertions, should always be 1. On failure, throw an exception, don't use
     *     zero.
     * @see #insert( String, ReplicaCatalogEntry )
     * @see ReplicaCatalogEntry
     */
    public int insert(String lfn, String pfn, String handle) {
        if (lfn == null || pfn == null || handle == null) {
            throw new NullPointerException();
        }
        return insert(lfn, new ReplicaCatalogEntry(pfn, handle));
    }

    /**
     * Inserts multiple mappings into the replica catalog. The input is a map indexed by the LFN.
     * The value for each LFN key is a collection of replica catalog entries. Note that this
     * operation will replace existing entries.
     *
     * @param x is a map from logical directory string to list of replica catalog entries.
     * @return the number of insertions.
     * @see edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry
     */
    public int insert(Map x) {
        int result = 0;

        // shortcut sanity
        if (x == null || x.size() == 0) {
            return result;
        }

        for (Iterator i = x.keySet().iterator(); i.hasNext(); ) {
            String lfn = (String) i.next();
            Object val = x.get(lfn);
            if (val instanceof ReplicaCatalogEntry) {
                // permit misconfigured clients
                result += insert(lfn, (ReplicaCatalogEntry) val);
            } else {
                // this is how it should have been
                for (Iterator j = ((Collection) val).iterator(); j.hasNext(); ) {
                    ReplicaCatalogEntry rce = (ReplicaCatalogEntry) j.next();
                    result += insert(lfn, rce);
                }
            }
        }

        return result;
    }

    /**
     * Deletes a specific mapping from the replica catalog. We don't care about the resource handle.
     * More than one entry could theoretically be removed. Upon removal of an entry, all attributes
     * associated with the PFN also evaporate (cascading deletion).
     *
     * @param lfn is the logical directory in the tuple.
     * @param pfn is the physical directory in the tuple.
     * @return the number of removed entries.
     */
    public int delete(String lfn, String pfn) {
        throw new java.lang.UnsupportedOperationException(
                "delete( String, String ) not implemented as yet");
    }

    /**
     * Deletes multiple mappings into the replica catalog. The input is a map indexed by the LFN.
     * The value for each LFN key is a collection of replica catalog entries. On setting
     * matchAttributes to false, all entries having matching lfn pfn mapping to an entry in the Map
     * are deleted. However, upon removal of an entry, all attributes associated with the pfn also
     * evaporate (cascaded deletion).
     *
     * @param x is a map from logical directory string to list of replica catalog entries.
     * @param matchAttributes whether mapping should be deleted only if all attributes match.
     * @return the number of deletions.
     * @see ReplicaCatalogEntry
     */
    public int delete(Map x, boolean matchAttributes) {
        throw new java.lang.UnsupportedOperationException(
                "delete(Map,boolean) not implemented as yet");
    }

    /**
     * Attempts to see, if all keys in the partial replica catalog entry are contained in the full
     * replica catalog entry.
     *
     * @param full is the full entry to check against.
     * @param part is the partial entry to check with.
     * @return true, if contained, false if not contained.
     */
    private boolean matchMe(ReplicaCatalogEntry full, ReplicaCatalogEntry part) {
        if (full.getPFN().equals(part.getPFN())) {
            for (Iterator i = part.getAttributeIterator(); i.hasNext(); ) {
                if (!full.hasAttribute((String) i.next())) {
                    return false;
                }
            }
            return true;
        } else {
            return false;
        }
    }

    /**
     * Deletes a very specific mapping from the replica catalog. The LFN must be matches, the PFN,
     * and all PFN attributes specified in the replica catalog entry. More than one entry could
     * theoretically be removed. Upon removal of an entry, all attributes associated with the PFN
     * also evaporate (cascading deletion).
     *
     * @param lfn is the logical directory in the tuple.
     * @param tuple is a description of the PFN and its attributes.
     * @return the number of removed entries, either 0 or 1.
     */
    public int delete(String lfn, ReplicaCatalogEntry tuple) {
        int result = 0;
        if (lfn == null || tuple == null) {
            return result;
        }

        Collection c = (Collection) mLFNMap.get(lfn);
        if (c == null) {
            return result;
        }

        List l = new ArrayList();
        for (Iterator i = c.iterator(); i.hasNext(); ) {
            ReplicaCatalogEntry rce = (ReplicaCatalogEntry) i.next();
            if (!matchMe(rce, tuple)) {
                l.add(rce);
            }
        }

        // anything removed?
        if (l.size() != c.size()) {
            result = c.size() - l.size();
            mLFNMap.put(lfn, l);
        }

        // done
        return result;
    }

    /**
     * Looks for a match of an attribute value in a replica catalog entry.
     *
     * @param rce is the replica catalog entry
     * @param name is the attribute key to match
     * @param value is the value to match against
     * @return true, if a match was found.
     */
    private boolean hasMatchingAttr(ReplicaCatalogEntry rce, String name, Object value) {
        if (rce.hasAttribute(name)) {
            return rce.getAttribute(name).equals(value);
        } else {
            return value == null;
        }
    }

    /**
     * Deletes all PFN entries for a given LFN from the replica catalog where the PFN attribute is
     * found, and matches exactly the object value. This method may be useful to remove all replica
     * entries that have a certain MD5 sum associated with them. It may also be harmful overkill.
     *
     * @param lfn is the logical directory to look for.
     * @param name is the PFN attribute name to look for.
     * @param value is an exact match of the attribute value to match.
     * @return the number of removed entries.
     */
    public int delete(String lfn, String name, Object value) {
        throw new java.lang.UnsupportedOperationException(
                "delete( String, String, Object) not implemented as yet");
    }

    /**
     * Deletes all PFN entries for a given LFN from the replica catalog where the resource handle is
     * found. Karan requested this convenience method, which can be coded like
     *
     * <pre>
     *  delete( lfn, RESOURCE_HANDLE, handle )
     * </pre>
     *
     * @param lfn is the logical directory to look for.
     * @param handle is the resource handle
     * @return the number of entries removed.
     */
    public int deleteByResource(String lfn, String handle) {
        throw new java.lang.UnsupportedOperationException(
                "delete( String, String) not implemented as yet");
    }

    /**
     * Removes all mappings for an LFN from the replica catalog.
     *
     * @param lfn is the logical directory to remove all mappings for.
     * @return the number of removed entries.
     */
    public int remove(String lfn) {
        Collection c = (Collection) mLFNMap.remove(lfn);
        if (c == null) {
            return 0;
        } else {
            return c.size();
        }
    }

    /**
     * Removes all mappings for a set of LFNs.
     *
     * @param lfns is a set of logical directory to remove all mappings for.
     * @return the number of removed entries.
     * @see #remove( String )
     */
    public int remove(Set lfns) {
        int result = 0;

        // sanity checks
        if (lfns == null || lfns.size() == 0) {
            return result;
        }

        for (Iterator i = lfns.iterator(); i.hasNext(); ) {
            String lfn = (String) i.next();
            result += remove(lfn);
        }

        // done
        return result;
    }

    /**
     * Removes all entries from the replica catalog where the PFN attribute is found, and matches
     * exactly the object value.
     *
     * @param name is the PFN attribute key to look for.
     * @param value is an exact match of the attribute value to match.
     * @return the number of removed entries.
     */
    public int removeByAttribute(String name, Object value) {
        int result = 0;

        for (Iterator i = mLFNMap.keySet().iterator(); i.hasNext(); ) {
            String lfn = (String) i.next();
            Collection c = (Collection) mLFNMap.get(lfn);
            if (c != null) {
                List l = new ArrayList();
                for (Iterator j = c.iterator(); j.hasNext(); ) {
                    ReplicaCatalogEntry rce = (ReplicaCatalogEntry) j.next();
                    if (!hasMatchingAttr(rce, name, value)) {
                        l.add(rce);
                    }
                }
                if (l.size() != c.size()) {
                    result += (c.size() - l.size());
                    mLFNMap.put(lfn, l);
                }
            }
        }

        // done
        return result;
    }

    /**
     * Removes all entries associated with a particular resource handle. This is useful, if a site
     * goes offline. It is a convenience method, which calls the generic <code>removeByAttribute
     * </code> method.
     *
     * @param handle is the site handle to remove all entries for.
     * @return the number of removed entries.
     * @see #removeByAttribute( String, Object )
     */
    public int removeByAttribute(String handle) {
        return removeByAttribute(ReplicaCatalogEntry.RESOURCE_HANDLE, handle);
    }

    /**
     * Removes everything. Use with caution!
     *
     * @return the number of removed entries.
     */
    public int clear() {
        int result = mLFNMap.size();
        mLFNMap.clear();
        return result;
    }

    /**
     * The main program
     *
     * @param args the arguments
     */
    public static void main(String[] args) {
        String directory = "/lfs1/work/pegasus-features/PM-659";

        // Properties p = new Properties();
        // p.put( "directory", directory );

        // load the Pegasus Properties
        String prefix = ReplicaCatalog.c_prefix + "."; // pegasus.catalog.replica.
        PegasusProperties props = PegasusProperties.getInstance();

        // specify the implementor to load this class
        props.setProperty(ReplicaCatalog.c_prefix, "Directory");

        // specify the path to directory
        props.setProperty(prefix + Directory.DIRECTORY_PROPERTY_KEY, directory);

        // specify the optional site handle to associate
        // defaults to local
        props.setProperty(prefix + Directory.SITE_PROPERTY_KEY, "isi");

        // specify the optional URL prefix to associate with the URL's
        // defaults to file://
        props.setProperty(prefix + Directory.URL_PRFIX_PROPERTY_KEY, "gsiftp://myhost.domain.edu");

        ReplicaCatalog c = null;
        try {
            c = ReplicaFactory.loadInstance(props);
        } catch (Exception ex) {
            System.err.println(
                    "Unable to connect to the Replica Catlog Backend " + ex.getMessage());
            System.exit(1);
        }

        // do the listing
        Set<String> lfns = c.list();
        for (String lfn : lfns) {
            System.out.println(lfn + " " + c.lookup(lfn));
        }

        // disconnect
        c.list();
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
        this.mReadOnly = readonly;
    }
}
