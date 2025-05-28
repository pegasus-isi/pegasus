/*
 * Copyright 2007-2014 University Of Southern California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.isi.pegasus.planner.catalog.replica.impl;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import edu.isi.pegasus.common.util.Boolean;
import edu.isi.pegasus.planner.catalog.CatalogException;
import edu.isi.pegasus.planner.catalog.ReplicaCatalog;
import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry;
import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogException;
import edu.isi.pegasus.planner.catalog.replica.classes.ReplicaCatalogJsonDeserializer;
import edu.isi.pegasus.planner.classes.ReplicaLocation;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.common.VariableExpansionReader;
import edu.isi.pegasus.planner.namespace.Metadata;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.yaml.snakeyaml.LoaderOptions;

/**
 * This class implements a replica catalog backend populated by parsing the meta files that are
 * created by pegasus-exitcode for each job.
 *
 * <p>A sample replica catalog description is indicated below.
 *
 * <p>
 *
 * <pre>
 * [
 * {
 * "_id": "f.b2",
 * "_type": "file",
 * "_attributes": {
 * "user": "bamboo",
 * "size": "56",
 * "ctime": "2020-05-15T10:05:04-07:00",
 * "checksum.type": "sha256",
 * "checksum.value": "a69fef1a4b597ea5e61ce403b6ef8bb5b4cd3aba19e734bf340ea00f5095c894",
 * "checksum.timing": "0.0"
 * }
 * },
 * {
 * "_id": "f.b1",
 * "_type": "file",
 * "_attributes": {
 * "user": "bamboo",
 * "size": "56",
 * "ctime": "2020-05-15T10:05:04-07:00",
 * "checksum.type": "sha256",
 * "checksum.value": "a69fef1a4b597ea5e61ce403b6ef8bb5b4cd3aba19e734bf340ea00f5095c894",
 * "checksum.timing": "0.0"
 * }
 * }
 * ]
 * </pre>
 *
 * <p>The class is strict when producing (storing) results. The LFN and PFN are only quoted and
 * escaped, if necessary. The attribute values are always quoted and escaped.
 *
 * @author Karan Vahi
 * @version $Revision: 5402 $
 */
@JsonDeserialize(using = Meta.CallbackJsonDeserializer.class)
public class Meta implements ReplicaCatalog {

    /**
     * Records the quoting mode for LFNs and PFNs. If false, only quote as necessary. If true,
     * always quote all LFNs and PFNs.
     */
    protected boolean mQuote = false;

    /** Records the name of the on-disk representation. */
    protected String mFilename = null;

    /** Maintains a memory slurp of the file representation. */
    protected Map<String, ReplicaLocation> mLFN = null;

    /** A boolean indicating whether the catalog is read only or not. */
    boolean m_readonly;

    private final int mMAXParsedDocSize;

    /**
     * Default empty constructor creates an object that is not yet connected to any database. You
     * must use support methods to connect before this instance becomes usable.
     *
     * @see #connect(Properties)
     */
    public Meta() {
        // make connection defunc
        mLFN = null;
        mFilename = null;
        m_readonly = false;

        PegasusProperties props = PegasusProperties.getInstance();

        mMAXParsedDocSize = props.getMaxSupportedYAMLDocSize();
    }

    /**
     * Reads the on-disk map file into memory.
     *
     * @param filename is the name of the file to read.
     * @return true, if the in-memory data structures appear sound.
     */
    public boolean connect(String filename) {
        // sanity check
        if (filename == null) {
            return false;
        }
        mFilename = filename;
        mLFN = new LinkedHashMap<String, ReplicaLocation>();

        File replicaFile = new File(filename);
        // first attempt to validate only if it exists
        if (replicaFile.exists()) {
            Reader reader = null;
            try {
                reader = new VariableExpansionReader(new FileReader(filename));
                // GH-2113 load the yaml factory with the right loader option
                // as picked up from properties
                LoaderOptions loaderOptions = new LoaderOptions();
                loaderOptions.setCodePointLimit(mMAXParsedDocSize * 1024 * 1024); // in MB
                YAMLFactory yamlFactory =
                        YAMLFactory.builder().loaderOptions(loaderOptions).build();

                ObjectMapper mapper = new ObjectMapper(yamlFactory);
                mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);
                // inject instance of this class to be used for deserialization
                mapper.setInjectableValues(injectCallback());
                mapper.readValue(reader, Meta.class);
            } catch (IOException ioe) {
                mLFN = null;
                mFilename = null;
                throw new CatalogException(ioe); // re-throw
            } finally {
                if (reader != null) {
                    try {
                        reader.close();
                    } catch (IOException ex) {
                    }
                }
            }
        } else {
            return false;
        }

        return true;
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
        // quote mode
        mQuote = Boolean.parse(props.getProperty("quote"));
        // update the m_writeable flag if specified
        if (props.containsKey(YAML.READ_ONLY_KEY)) {
            m_readonly = Boolean.parse(props.getProperty(YAML.READ_ONLY_KEY), false);
        }
        if (props.containsKey("file")) return connect(props.getProperty("file"));
        return false;
    }

    /**
     * This operation will dump the in-memory representation back onto disk. The store operation is
     * strict in what it produces. The LFN and PFN records are only quoted, if they require quotes,
     * because they contain special characters. The attributes are <b>always</b> quoted and thus
     * quote-escaped.
     */
    public void close() {
        String newline = System.getProperty("line.separator", "\r\n");
        // sanity check
        if (mLFN == null) return;
        // check if the file is writeable or not
        if (m_readonly) {
            if (mLFN != null) mLFN.clear();
            mLFN = null;
            mFilename = null;
            return;
        }

        // we don't write out anything as it is a read only RC
    }

    /**
     * Predicate to check, if the connection with the catalog's implementation is still active. This
     * helps determining, if it makes sense to call <code>close()</code>.
     *
     * @return true, if the implementation is disassociated, false otherwise.
     * @see #close()
     */
    public boolean isClosed() {
        return (mLFN == null);
    }

    /**
     * Retrieves the entry for a given filename and site handle from the replica catalog.
     *
     * @param lfn is the logical filename to obtain information for.
     * @param handle is the resource handle to obtain entries for.
     * @return the (first) matching physical filename, or <code>null</code> if no match was found.
     */
    public String lookup(String lfn, String handle) {
        Collection<ReplicaCatalogEntry> result = lookupWithHandle(lfn, handle);

        if (result == null || result.isEmpty()) {
            return null;
        }
        return result.iterator().next().getPFN();
    }

    public Collection<ReplicaCatalogEntry> lookupWithHandle(String lfn, String handle) {
        Collection<ReplicaCatalogEntry> c = new ArrayList<ReplicaCatalogEntry>();

        // Lookup regular LFN's
        ReplicaLocation tmp = mLFN.get(lfn);
        if (tmp != null) {
            for (ReplicaCatalogEntry rce : tmp.getPFNList()) {
                String pool = rce.getResourceHandle();
                if (pool == null && handle == null
                        || pool != null && handle != null && pool.equals(handle)) c.add(rce);
            }
        }

        return c;
    }

    /**
     * Retrieves all entries for a given LFN from the replica catalog. Each entry in the result set
     * is a tuple of a PFN and all its attributes.
     *
     * @param lfn is the logical filename to obtain information for.
     * @return a collection of replica catalog entries
     * @see ReplicaCatalogEntry
     */
    public Collection<ReplicaCatalogEntry> lookup(String lfn) {
        Collection<ReplicaCatalogEntry> c = new ArrayList<ReplicaCatalogEntry>();
        ReplicaLocation tmp;
        // Lookup regular LFN's
        tmp = mLFN.get(lfn);
        if (tmp != null) {
            c.addAll(tmp.getPFNList());
            // PM-1534 and PM-1523 add metadata at LFN level
            // in the Replica Location object to individual RCE's
            Metadata m = tmp.getAllMetadata();
            for (ReplicaCatalogEntry rce : c) {
                for (Iterator<String> it = m.getProfileKeyIterator(); it.hasNext(); ) {
                    String key = it.next();
                    rce.addAttribute(key, m.get(key));
                }
            }
        }

        return c;
    }

    private ReplicaCatalogEntry cloneRCE(ReplicaCatalogEntry e) {

        return (ReplicaCatalogEntry) e.clone();
    }

    /**
     * Retrieves all entries for a given LFN from the replica catalog. Each entry in the result set
     * is just a PFN string. Duplicates are reduced through the set paradigm.
     *
     * @param lfn is the logical filename to obtain information for.
     * @return a set of PFN strings
     */
    public Set<String> lookupNoAttributes(String lfn) {
        Collection<ReplicaCatalogEntry> input = lookup(lfn);
        Set<String> result = new HashSet<String>();
        if (input == null || input.size() == 0) return result;
        for (ReplicaCatalogEntry entry : input) {
            result.add(entry.getPFN());
        }
        // done
        return result;
    }

    /**
     * Retrieves multiple entries for a given logical filename, up to the complete catalog.
     * Retrieving full catalogs should be harmful, but may be helpful in an online display or
     * portal.
     *
     * @param lfns is a set of logical filename strings to look up.
     * @return a map indexed by the LFN. Each value is a collection of replica catalog entries for
     *     the LFN.
     * @see edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry
     */
    public Map lookup(Set lfns) {
        Map<String, Collection<ReplicaCatalogEntry>> result =
                new HashMap<String, Collection<ReplicaCatalogEntry>>();
        if (lfns == null || lfns.size() == 0) return result;
        Collection<ReplicaCatalogEntry> c = null;
        for (Iterator<String> i = lfns.iterator(); i.hasNext(); ) {
            String lfn = i.next();
            c = lookup(lfn);
            result.put(lfn, new ArrayList<ReplicaCatalogEntry>(c));
        }
        // done
        return result;
    }

    /**
     * Retrieves multiple entries for a given logical filename, up to the complete catalog.
     * Retrieving full catalogs should be harmful, but may be helpful in an online display or
     * portal.
     *
     * @param lfns is a set of logical filename strings to look up.
     * @return a map indexed by the LFN. Each value is a set of PFN strings.
     */
    public Map lookupNoAttributes(Set lfns) {
        Map<String, Collection<ReplicaCatalogEntry>> input = lookup(lfns);
        if (input == null || input.size() == 0) return input;
        Map<String, Collection<String>> result = new HashMap<String, Collection<String>>();
        for (Map.Entry<String, Collection<ReplicaCatalogEntry>> entry : input.entrySet()) {
            String lfn = entry.getKey();
            Collection<ReplicaCatalogEntry> c = entry.getValue();
            Set<String> value = new HashSet<String>();
            if (c != null) {
                for (Iterator<ReplicaCatalogEntry> j = c.iterator(); j.hasNext(); ) {
                    value.add(j.next().getPFN());
                }
            }
            result.put(lfn, value);
        }
        // done
        return result;
    }

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
    public Map lookup(Set lfns, String handle) {
        ReplicaLocation rl = null;
        Pattern p = null;
        Matcher m = null;
        String lfn = null;
        String pool = null;
        Map<String, Collection<ReplicaCatalogEntry>> result =
                new HashMap<String, Collection<ReplicaCatalogEntry>>();
        ReplicaCatalogEntry rce = null;
        for (Iterator<String> i = lfns.iterator(); i.hasNext(); ) {
            lfn = i.next(); // f.a - String file name
            List<ReplicaCatalogEntry> value = new ArrayList<ReplicaCatalogEntry>();
            // Lookup regular LFN's
            rl = mLFN.get(lfn);
            if (rl != null) {
                for (Iterator j = rl.pfnIterator(); j.hasNext(); ) {
                    rce = (ReplicaCatalogEntry) j.next();
                    pool = rce.getResourceHandle();
                    if (pool == null && handle == null
                            || pool != null && handle != null && pool.equals(handle))
                        value.add(rce);
                }
            }
            result.put(lfn, value);
        }
        // done
        return result;
    }

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
    public Map lookupNoAttributes(Set lfns, String handle) {
        Map<String, Collection<String>> result = new HashMap<String, Collection<String>>();
        if (lfns == null || lfns.size() == 0) return result;
        for (Iterator<String> i = lfns.iterator(); i.hasNext(); ) {
            String lfn = i.next();
            Collection<ReplicaCatalogEntry> c = lookupWithHandle(lfn, handle);
            if (c != null) {
                List<String> value = new ArrayList<String>();
                for (ReplicaCatalogEntry entry : c) {
                    value.add(entry.getPFN());
                }
                result.put(lfn, value);
            }
        }
        // done
        return result;
    }

    /**
     * Retrieves multiple entries for a given logical filename, up to the complete catalog.
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
            // Map<String, Collection<ReplicaCatalogEntry>> result =
            //        new HashMap<String, Collection<ReplicaCatalogEntry>>(mLFN);
            // result.putAll(mLFNRegex);
            Map<String, Collection<ReplicaCatalogEntry>> result =
                    new HashMap<String, Collection<ReplicaCatalogEntry>>();
            for (Map.Entry<String, ReplicaLocation> entry : mLFN.entrySet()) {
                ReplicaLocation rl = entry.getValue();
                // merge any metadata associated with the lfn
                // into the Replica Catalog Entry object
                Collection<ReplicaCatalogEntry> c = new LinkedList();
                for (ReplicaCatalogEntry rce : rl.getPFNList()) {
                    rce.addAttribute(rl.getAllMetadata());
                    c.add(rce);
                }
                result.put(entry.getKey(), c);
            }
            return result;
        } else if (constraints.size() == 1 && constraints.containsKey("lfn")) {
            // return matching LFNs
            Pattern p = Pattern.compile((String) constraints.get("lfn"));
            Map<String, Collection<ReplicaCatalogEntry>> result =
                    new HashMap<String, Collection<ReplicaCatalogEntry>>();
            for (Iterator<Entry<String, ReplicaLocation>> i = mLFN.entrySet().iterator();
                    i.hasNext(); ) {
                Entry<String, ReplicaLocation> e = i.next();
                String lfn = e.getKey();
                if (p.matcher(lfn).matches()) {
                    ReplicaLocation rl = e.getValue();
                    // merge any metadata associated with the lfn
                    // into the Replica Catalog Entry object
                    Collection<ReplicaCatalogEntry> c = new LinkedList();
                    for (ReplicaCatalogEntry rce : rl.getPFNList()) {
                        rce.addAttribute(rl.getAllMetadata());
                        c.add(rce);
                    }
                    result.put(lfn, c);
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
        Set<String> s = new HashSet<String>(mLFN.keySet());
        return s;
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
        Set<String> result = new HashSet<String>();
        Pattern p = Pattern.compile(constraint);
        for (Iterator<String> i = list().iterator(); i.hasNext(); ) {
            String lfn = i.next();
            if (p.matcher(lfn).matches()) result.add(lfn);
        }
        // done
        return result;
    }

    /**
     * Inserts a new mapping into the replica catalog. Any existing mapping of the same LFN, PFN,
     * and HANDLE will be replaced, including all of its attributes.
     *
     * @param lfn is the logical filename under which to book the entry.
     * @param tuple is the physical filename and associated PFN attributes.
     * @return number of insertions, should always be 1. On failure, throw an exception, don't use
     *     zero.
     */
    public int insert(String lfn, ReplicaCatalogEntry tuple) {
        if (lfn == null || tuple == null) throw new NullPointerException();

        boolean isRegex = tuple.isRegex();
        ReplicaLocation rl = null;

        // Collection<ReplicaCatalogEntry> c = null;

        String pfn = tuple.getPFN();
        String handle = tuple.getResourceHandle();

        if (mLFN.containsKey(lfn)) {
            rl = mLFN.get(lfn);
            Collection<ReplicaCatalogEntry> c = rl.getPFNList();

            for (Iterator<ReplicaCatalogEntry> i = c.iterator(); i.hasNext(); ) {
                ReplicaCatalogEntry rce = i.next();

                if (pfn.equals(rce.getPFN())
                        && ((handle == null && rce.getResourceHandle() == null)
                                || (handle != null && handle.equals(rce.getResourceHandle())))) {
                    try {
                        i.remove();
                        break;
                    } catch (UnsupportedOperationException uoe) {
                        return 0;
                    }
                }
            }
        }

        rl = mLFN.get(lfn);
        Collection<ReplicaCatalogEntry> c = null;
        if (rl != null) {
            c = rl.getPFNList();
        }
        c = (c == null) ? new ArrayList<ReplicaCatalogEntry>() : c;
        c.add(tuple);
        mLFN.put(lfn, new ReplicaLocation(lfn, c, false));

        return 1;
    }

    /**
     * Inserts a new mapping into the replica catalog. This is a convenience function exposing the
     * resource handle. Internally, the <code>ReplicaCatalogEntry</code> element will be
     * constructed, and passed to the appropriate insert function.
     *
     * @param lfn is the logical filename under which to book the entry.
     * @param pfn is the physical filename associated with it.
     * @param handle is a resource handle where the PFN resides.
     * @return number of insertions, should always be 1. On failure, throw an exception, don't use
     *     zero.
     * @see #insert(String, ReplicaCatalogEntry)
     * @see ReplicaCatalogEntry
     */
    public int insert(String lfn, String pfn, String handle) {
        if (lfn == null || pfn == null || handle == null) throw new NullPointerException();
        return insert(lfn, new ReplicaCatalogEntry(pfn, handle));
    }

    /**
     * Inserts multiple mappings into the replica catalog. The input is a map indexed by the LFN.
     * The value for each LFN key is a collection of replica catalog entries. Note that this
     * operation will replace existing entries.
     *
     * @param x is a map from logical filename string to list of replica catalog entries.
     * @return the number of insertions.
     * @see edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry
     */
    public int insert(Map x) {
        int result = 0;
        // shortcut sanity
        if (x == null || x.size() == 0) return result;
        for (Iterator<String> i = x.keySet().iterator(); i.hasNext(); ) {
            String lfn = i.next();
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
     * @param lfn is the logical filename in the tuple.
     * @param pfn is the physical filename in the tuple.
     * @return the number of removed entries.
     */
    public int delete(String lfn, String pfn) {
        throw new java.lang.UnsupportedOperationException(
                "delete(String,String) not implemented as yet");
    }

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
            for (Iterator<String> i = part.getAttributeIterator(); i.hasNext(); ) {
                if (!full.hasAttribute(i.next())) return false;
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
     * @param lfn is the logical filename in the tuple.
     * @param tuple is a description of the PFN and its attributes.
     * @return the number of removed entries, either 0 or 1.
     */
    public int delete(String lfn, ReplicaCatalogEntry tuple) {
        throw new java.lang.UnsupportedOperationException(
                "delete(String, ReplicaCatalogEntry) not implemented as yet");
    }

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
    public int delete(String lfn, String name, Object value) {
        throw new java.lang.UnsupportedOperationException(
                "delete (String lfn, String name, Object value) not implemented as yet");
    }

    /**
     * Deletes all PFN entries for a given LFN from the replica catalog where the resource handle is
     * found. Karan requested this convenience method, which can be coded like
     *
     * <p>
     *
     * <pre>
     * delete( lfn, RESOURCE_HANDLE, handle )
     * </pre>
     *
     * @param lfn is the logical filename to look for.
     * @param handle is the resource handle
     * @return the number of entries removed.
     */
    public int deleteByResource(String lfn, String handle) {
        throw new java.lang.UnsupportedOperationException(
                "deleteByResource (String lfn, String handle) not implemented as yet");
    }

    /**
     * Removes all mappings for an LFN from the replica catalog.
     *
     * @param lfn is the logical filename to remove all mappings for.
     * @return the number of removed entries.
     */
    public int remove(String lfn) {
        throw new java.lang.UnsupportedOperationException(
                "remove (String lfn) not implemented as yet");
    }

    /**
     * Removes all mappings for a set of LFNs.
     *
     * @param lfns is a set of logical filename to remove all mappings for.
     * @return the number of removed entries.
     * @see #remove(String)
     */
    public int remove(Set lfns) {
        throw new java.lang.UnsupportedOperationException(
                "remove (Set lfns) not implemented as yet");
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
        throw new java.lang.UnsupportedOperationException(
                "removeByAttribute (String lfn, Object value) not implemented as yet");
    }

    /**
     * Removes all entries associated with a particular resource handle. This is useful, if a site
     * goes offline. It is a convenience method, which calls the generic <code>removeByAttribute
     * </code> method.
     *
     * @param handle is the site handle to remove all entries for.
     * @return the number of removed entries.
     * @see #removeByAttribute(String, Object)
     */
    public int removeByAttribute(String handle) {
        throw new java.lang.UnsupportedOperationException(
                "removeByAttribute (String handle) not implemented as yet");
    }

    /**
     * Removes everything. Use with caution!
     *
     * @return the number of removed entries.
     */
    public int clear() {
        int result = mLFN.size();
        return result;
    }

    /**
     * Returns the file source.
     *
     * @return the file source if it exists , else null
     */
    public java.io.File getFileSource() {
        return new java.io.File(this.mFilename);
    }

    /**
     * Set the catalog to read-only mode.
     *
     * @param readonly whether the catalog is read-only
     */
    @Override
    public void setReadOnly(boolean readonly) {
        this.m_readonly = readonly;
    }

    /**
     * Set the Callback as an injectable value to insert into the Deserializer via Jackson.
     *
     * @return
     */
    private InjectableValues injectCallback() {
        return new InjectableValues.Std().addValue("callback", this);
    }

    /**
     * Custom deserializer for YAML representation of Replica Catalog that calls back to the class
     * that invoked the serializer. The deserialized object returned is the callback itself
     *
     * @author Karan Vahi
     */
    static class CallbackJsonDeserializer extends ReplicaCatalogJsonDeserializer<ReplicaCatalog> {

        /**
         * Deserializes a Transformation YAML description of the type
         *
         * <pre>
         *
         * {
         * "_id": "f.b2",
         * "_type": "file",
         * "_attributes": {
         * "user": "bamboo",
         * "size": "56",
         * "ctime": "2020-05-15T10:05:04-07:00",
         * "checksum.type": "sha256",
         * "checksum.value": "a69fef1a4b597ea5e61ce403b6ef8bb5b4cd3aba19e734bf340ea00f5095c894",
         * "checksum.timing": "0.0"
         * }
         * </pre>
         *
         * @param parser
         * @param dc
         * @return
         * @throws IOException
         * @throws JsonProcessingException
         */
        @Override
        public ReplicaCatalog deserialize(JsonParser parser, DeserializationContext dc)
                throws IOException, JsonProcessingException {
            ObjectCodec oc = parser.getCodec();
            JsonNode node = oc.readTree(parser);
            Meta metaRC = (Meta) dc.findInjectableValue("callback", null, null);
            if (metaRC == null) {
                throw new ReplicaCatalogException(
                        "Callback not initialized when parsing inititated");
            }
            if (!node.isArray()) {
                throw new ReplicaCatalogException("The meta file should be array of entries");
            }

            for (JsonNode replicaNode : node) {

                String lfn = null;
                String type = null;
                ReplicaCatalogEntry rce = null;
                for (Iterator<Map.Entry<String, JsonNode>> it = replicaNode.fields();
                        it.hasNext(); ) {
                    Map.Entry<String, JsonNode> e = it.next();
                    String key = e.getKey();
                    MetaKeywords reservedKey = MetaKeywords.getReservedKey(key);
                    if (reservedKey == null) {
                        this.complainForIllegalKey(MetaKeywords.META.getReservedName(), key, node);
                    }

                    switch (reservedKey) {
                        case ID:
                            lfn = replicaNode.get(key).asText();
                            break;

                        case ATTRIBUTES:
                            rce = this.createReplicaCatalogEntry(replicaNode.get(key));
                            break;

                        case TYPE:
                            type = replicaNode.get(key).asText();
                            break;

                        default:
                            this.complainForUnsupportedKey(
                                    MetaKeywords.META.getReservedName(), key, node);
                    }
                }
                if (lfn == null) {
                    throw new ReplicaCatalogException("LFN not specified for node " + replicaNode);
                }
                if (rce == null) {
                    throw new ReplicaCatalogException(
                            "Attributes not specified for node " + replicaNode);
                }
                metaRC.insert(lfn, rce);
            }

            return metaRC;
        }

        /**
         * Parses all the attributes back into a Replica Catalog Entry object
         *
         * @param node
         * @return
         */
        private ReplicaCatalogEntry createReplicaCatalogEntry(JsonNode node) {
            ReplicaCatalogEntry rce = new ReplicaCatalogEntry();
            for (Iterator<Map.Entry<String, JsonNode>> it = node.fields(); it.hasNext(); ) {
                Map.Entry<String, JsonNode> e = it.next();
                String key = e.getKey();
                String value = e.getValue().asText();
                rce.addAttribute(key, value);
            }
            return rce;
        }
    }
}

/**
 * Keywords used while parsing the meta files
 *
 * <pre>
 * {
 * "_id": "f.b2",
 * "_type": "file",
 * "_attributes": {
 * "user": "bamboo",
 * "size": "56",
 * "ctime": "2020-05-15T10:05:04-07:00",
 * "checksum.type": "sha256",
 * "checksum.value": "a69fef1a4b597ea5e61ce403b6ef8bb5b4cd3aba19e734bf340ea00f5095c894",
 * "checksum.timing": "0.0"
 * }
 * </pre>
 *
 * @author Karan Vahi
 */
enum MetaKeywords {
    META("meta"),
    ID("_id"),
    TYPE("_type"),
    ATTRIBUTES("_attributes");

    private String mName;

    private static Map<String, MetaKeywords> mKeywordsVsType = new HashMap<>();

    static {
        for (MetaKeywords key : MetaKeywords.values()) {
            mKeywordsVsType.put(key.getReservedName(), key);
        }
    }

    MetaKeywords(String name) {
        this.mName = name;
    }

    public String getReservedName() {
        return mName;
    }

    public static MetaKeywords getReservedKey(String key) {
        return mKeywordsVsType.get(key);
    }
}
