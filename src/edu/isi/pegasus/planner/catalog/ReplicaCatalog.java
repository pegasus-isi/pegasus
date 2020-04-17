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
package edu.isi.pegasus.planner.catalog;

import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry;
import java.util.*;

/**
 * This interface describes a minimum set of essential tasks required from a replica catalog. The
 * method verbs have a steering meaning associated with them:
 *
 * <dl>
 *   <dt>lookup
 *   <dd>retrieves physical filenames or replica entries
 *   <dt>list
 *   <dd>retrieves only lists of logical filenames
 *   <dt>delete
 *   <dd>removes an entry specified by LFN and PFN
 *   <dt>remove
 *   <dd>removes en-bulk by LFN
 * </dl>
 *
 * @author Jens-S. Vöckler
 * @author Karan Vahi
 * @version $Revision$
 */
public interface ReplicaCatalog extends Catalog {

    /** Prefix for the property subset to use with this catalog. */
    public static final String c_prefix = "pegasus.catalog.replica";

    /** The DB Driver properties prefix. */
    public static final String DB_PREFIX = "pegasus.catalog.replica.db";

    /** The key that if set, specifies the proxy to be picked up while connecting to the RLS. */
    public static final String PROXY_KEY = "proxy";

    /**
     * The suffix for the property that if set, specifies the size of the chunk in which the
     * implementations handle multiple queries. The property that needs to be specified is
     * vds.rc.chunk.size.
     */
    public static final String BATCH_KEY = "chunk.size";

    /** Key name of property to set variable expansion */
    public static final String VARIABLE_EXPANSION_KEY = "expand";

    /**
     * Retrieves the entry for a given filename and resource handle from the replica catalog.
     *
     * @param lfn is the logical filename to obtain information for.
     * @param handle is the resource handle to obtain entries for.
     * @return the (first) matching physical filename, or <code>null</code> if no match was found.
     */
    public String lookup(String lfn, String handle);

    /**
     * Retrieves all entries for a given LFN from the replica catalog. Each entry in the result set
     * is a tuple of a PFN and all its attributes.
     *
     * @param lfn is the logical filename to obtain information for.
     * @return a collection of replica catalog entries
     * @see ReplicaCatalogEntry
     */
    public Collection lookup(String lfn);

    /**
     * Retrieves all entries for a given LFN from the replica catalog. Each entry in the result set
     * is just a PFN string. Duplicates are reduced through the set paradigm.
     *
     * @param lfn is the logical filename to obtain information for.
     * @return a set of PFN strings
     */
    public Set lookupNoAttributes(String lfn);

    /**
     * Retrieves multiple entries for a given logical filename, up to the complete catalog.
     * Retrieving full catalogs should be harmful, but may be helpful in an online display or
     * portal.
     *
     * @param lfns is a set of logical filename strings to look up.
     * @return a map indexed by the LFN. Each value is a collection of replica catalog entries for
     *     the LFN.
     * @see ReplicaCatalogEntry
     */
    public Map lookup(Set lfns);

    /**
     * Retrieves multiple entries for a given logical filename, up to the complete catalog.
     * Retrieving full catalogs should be harmful, but may be helpful in an online display or
     * portal.
     *
     * @param lfns is a set of logical filename strings to look up.
     * @return a map indexed by the LFN. Each value is a set of PFN strings.
     */
    public Map lookupNoAttributes(Set lfns);

    /**
     * Retrieves multiple entries for a given logical filename, up to the complete catalog.
     * Retrieving full catalogs should be harmful, but may be helpful in online display or portal.
     *
     * <p>
     *
     * @param lfns is a set of logical filename strings to look up.
     * @param handle is the resource handle, restricting the LFNs.
     * @return a map indexed by the LFN. Each value is a collection of replica catalog entries (all
     *     attributes).
     * @see ReplicaCatalogEntry
     */
    public Map lookup(Set lfns, String handle);

    /**
     * Retrieves multiple entries for a given logical filename, up to the complete catalog.
     * Retrieving full catalogs should be harmful, but may be helpful in online display or portal.
     *
     * <p>
     *
     * @param lfns is a set of logical filename strings to look up.
     * @param handle is the resource handle, restricting the LFNs.
     * @return a map indexed by the LFN. Each value is a set of physical filenames.
     */
    public Map lookupNoAttributes(Set lfns, String handle);

    /**
     * Retrieves multiple entries for a given logical filename, up to the complete catalog.
     * Retrieving full catalogs should be harmful, but may be helpful in online display or portal.
     *
     * @param constraints is mapping of keys 'lfn', 'pfn', or any attribute name, e.g. the resource
     *     handle 'site', to a string that has some meaning to the implementing system. This can be
     *     a SQL wildcard for queries, or a regular expression for Java-based memory collections.
     *     Unknown keys are ignored. Using an empty map requests the complete catalog.
     * @return a map indexed by the LFN. Each value is a collection of replica catalog entries.
     * @see ReplicaCatalogEntry
     */
    public Map lookup(Map constraints);

    /**
     * Lists all logical filenames in the catalog.
     *
     * @return A set of all logical filenames known to the catalog.
     */
    public Set list();

    /**
     * Lists a subset of all logical filenames in the catalog.
     *
     * @param constraint is a constraint for the logical filename only. It is a string that has some
     *     meaning to the implementing system. This can be a SQL wildcard for queries, or a regular
     *     expression for Java-based memory collections.
     * @return A set of logical filenames that match. The set may be empty
     */
    public Set list(String constraint);

    /**
     * Inserts a new mapping into the replica catalog.
     *
     * @param lfn is the logical filename under which to book the entry.
     * @param tuple is the physical filename and associated PFN attributes.
     * @return number of insertions, should always be 1. On failure, throw an exception, don't use
     *     zero.
     */
    public int insert(String lfn, ReplicaCatalogEntry tuple);

    /**
     * Inserts a new mapping into the replica catalog. This is a convenience function exposing the
     * resource handle. Internally, the <code>ReplicaCatalogEntry</code> element will be contructed,
     * and passed to the appropriate insert function.
     *
     * @param lfn is the logical filename under which to book the entry.
     * @param pfn is the physical filename associated with it.
     * @param handle is a resource handle where the PFN resides.
     * @return number of insertions, should always be 1. On failure, throw an exception, don't use
     *     zero.
     * @see #insert( String, ReplicaCatalogEntry )
     * @see ReplicaCatalogEntry
     */
    public int insert(String lfn, String pfn, String handle);
    // ^^ MARKER ^^

    /**
     * Inserts multiple mappings into the replica catalog. The input is a map indexed by the LFN.
     * The value for each LFN key is a collection of replica catalog entries.
     *
     * @param x is a map from logical filename string to list of replica catalog entries.
     * @return the number of insertions.
     * @see ReplicaCatalogEntry
     */
    public int insert(Map x);

    /**
     * Deletes multiple mappings into the replica catalog. The input is a map indexed by the LFN.
     * The value for each LFN key is a collection of replica catalog entries. On setting
     * matchAttributes to false, all entries having matching lfn pfn mapping to an entry in the Map
     * are deleted. However, upon removal of an entry, all attributes associated with the pfn also
     * evaporate (cascaded deletion).
     *
     * @param x is a map from logical filename string to list of replica catalog entries.
     * @param matchAttributes whether mapping should be deleted only if all attributes match.
     * @return the number of deletions.
     * @see ReplicaCatalogEntry
     */
    public int delete(Map<String, Collection<ReplicaCatalogEntry>> x, boolean matchAttributes);

    /**
     * Deletes a specific mapping from the replica catalog. We don't care about the resource handle.
     * More than one entry could theoretically be removed. Upon removal of an entry, all attributes
     * associated with the PFN also evaporate (cascading deletion).
     *
     * @param lfn is the logical filename in the tuple.
     * @param pfn is the physical filename in the tuple.
     * @return the number of removed entries.
     */
    public int delete(String lfn, String pfn);

    /**
     * Deletes a very specific mapping from the replica catalog. The LFN must be matches, the PFN,
     * and all PFN attributes specified in the replica catalog entry. More than one entry could
     * theoretically be removed. Upon removal of an entry, all attributes associated with the PFN
     * also evaporate (cascading deletion).
     *
     * @param lfn is the logical filename in the tuple.
     * @param tuple is a description of the PFN and its attributes.
     * @return the number of removed entries, either 0 or 1.
     */
    public int delete(String lfn, ReplicaCatalogEntry tuple);

    /**
     * Deletes all PFN entries for a given LFN from the replica catalog where the PFN attribute is
     * found, and matches exactly the object value. This method may be useful to remove all replica
     * entries that have a certain MD5 sum associated with them. It may also be harmful overkill.
     *
     * @param lfn is the logical filename to look for.
     * @param name is the PFN attribute name to look for.
     * @param value is an exact match of the attribute value to match.
     * @return the number of removed entries.
     */
    public int delete(String lfn, String name, Object value);
    // ^^ MARKER ^^

    /**
     * Deletes all PFN entries for a given LFN from the replica catalog where the resource handle is
     * found. Karan requested this convenience method, which can be coded like
     *
     * <pre>
     *  delete( lfn, RESOURCE_HANDLE, handle )
     * </pre>
     *
     * @param lfn is the logical filename to look for.
     * @param handle is the resource handle
     * @return the number of entries removed.
     */
    public int deleteByResource(String lfn, String handle);

    /**
     * Removes all mappings for an LFN from the replica catalog.
     *
     * @param lfn is the logical filename to remove all mappings for.
     * @return the number of removed entries.
     */
    public int remove(String lfn);

    /**
     * Removes all mappings for a set of LFNs.
     *
     * @param lfns is a set of logical filename to remove all mappings for.
     * @return the number of removed entries.
     */
    public int remove(Set lfns);

    /**
     * Removes all entries from the replica catalog where the PFN attribute is found, and matches
     * exactly the object value.
     *
     * @param name is the PFN attribute name to look for.
     * @param value is an exact match of the attribute value to match.
     * @return the number of removed entries.
     */
    public int removeByAttribute(String name, Object value);

    /**
     * Removes all entries associated with a particular resource handle. This is useful, if a site
     * goes offline. It is a convenience method, which calls the generic <code>removeByAttribute
     * </code> method.
     *
     * @param handle is the site handle to remove all entries for.
     * @return the number of removed entries.
     * @see #removeByAttribute( String, Object )
     */
    public int removeByAttribute(String handle);
    // ^^ MARKER ^^

    /**
     * Removes everything. Use with caution!
     *
     * @return the number of removed entries.
     */
    public int clear();

    /**
     * Returns the file source.
     *
     * @return the file source if it exists , else null
     */
    public java.io.File getFileSource();

    /**
     * Set the catalog to read-only mode.
     *
     * @param readonly whether the catalog is read-only
     */
    public void setReadOnly(boolean readonly);
}
