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
package edu.isi.pegasus.planner.catalog.replica.classes;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import edu.isi.pegasus.planner.catalog.classes.CatalogEntryJsonDeserializer;
import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry;
import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogException;
import edu.isi.pegasus.planner.classes.Data;
import edu.isi.pegasus.planner.classes.ReplicaLocation;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * A Replica Store that allows us to store the entries from a replica catalog. The store map is
 * indexed by LFN's and values stored are ReplicaLocation objects.
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 * @version $Revision$
 */
@JsonDeserialize(using = ReplicaStoreDeserializer.class)
public class ReplicaStore extends Data implements Cloneable {

    /** The replica store. */
    private Map<String, ReplicaLocation> mStore;

    /** The version for the Replica Catalog */
    private String mVersion;

    /** Default constructor. */
    public ReplicaStore() {
        mStore = new HashMap();
    }

    /**
     * Overloaded constructor. Intializes the member variables to the values passed.
     *
     * @param rces map indexed by LFN's and each value is a collection of replica catalog entries
     *     for the LFN.
     */
    public ReplicaStore(Map<String, Collection<ReplicaCatalogEntry>> rces) {
        mStore = new HashMap(rces.size());
        store(rces);
    }

    /**
     * Set the Catalog version
     *
     * @param version
     */
    public void setVersion(String version) {
        this.mVersion = version;
    }

    /**
     * Get the Catalog version
     *
     * @return version
     */
    public String getVersion() {
        return this.mVersion;
    }

    /**
     * Stores replica catalog entries into the store. It overwrites any existing entries with the
     * same LFN's. The <code>ReplicaCatlogEntry</code> ends up being stored as a <code>
     * ReplicaLocation</code> object.
     *
     * @param rces map indexed by LFN's and each value is a collection of replica catalog entries
     *     for the LFN.
     */
    public void store(Map rces) {
        String lfn;
        Map.Entry entry;
        Collection values;

        // traverse through all the entries and render them into
        // ReplicaLocation objects before storing in the store
        for (Iterator it = rces.entrySet().iterator(); it.hasNext(); ) {
            entry = (Map.Entry) it.next();
            lfn = (String) entry.getKey();
            values = (Collection) entry.getValue();
            // only put in if the values are not empty
            if (!values.isEmpty()) {
                put(lfn, new ReplicaLocation(lfn, values));
            }
        }
    }

    /**
     * Adds ReplicaCatalogEntries into the store. Any existing mapping of the same LFN and PFN will
     * be replaced, including all its attributes. The <code>ReplicaCatlogEntry</code> ends up being
     * stored as a <code>ReplicaLocation</code> object.
     *
     * @param rces map indexed by LFN's and each value is a collection of replica catalog entries
     *     for the LFN.
     */
    public void add(Map rces) {
        String lfn;
        Map.Entry entry;
        Collection values;
        ReplicaLocation rl;

        // traverse through all the entries and render them into
        // ReplicaLocation objects before storing in the store
        for (Iterator it = rces.entrySet().iterator(); it.hasNext(); ) {
            entry = (Map.Entry) it.next();
            lfn = (String) entry.getKey();
            values = (Collection) entry.getValue();
            add(lfn, values);
        }
    }

    /**
     * Adds replica catalog entries into the store. Any existing
     * mapping of the same LFN and PFN will be replaced, including all its
     * attributes.
     *
     * @param lfn     the lfn.
     * @param tuples  list of <code>ReplicaCatalogEntry<code> containing the PFN and the
     *                attributes.
     */
    public void add(String lfn, Collection<ReplicaCatalogEntry> tuples) {
        // add only if tuples is not empty
        if (tuples.isEmpty()) {
            return;
        }
        this.add(new ReplicaLocation(lfn, tuples));
    }

    /**
     * Adds replica catalog entry into the store. Any existing mapping of the same LFN and PFN will
     * be replaced, including all its attributes.
     *
     * @param lfn the lfn.
     * @param rce the replica catalog entry
     */
    public void add(String lfn, ReplicaCatalogEntry rce) {
        // add only if tuples is not empty
        if (rce == null) {
            return;
        }
        ReplicaLocation rl = new ReplicaLocation();
        rl.setLFN(lfn);
        rl.addPFN(rce);
        this.add(rl);
    }

    /**
     * Adds replica catalog entries into the store. Any existing mapping of the same LFN and PFN
     * will be replaced, including all its attributes.
     *
     * @param rl the <code>ReplicaLocation</code> containing a pfn and all the attributes.
     */
    public void add(ReplicaLocation rl) {
        String lfn = rl.getLFN();

        if (this.containsLFN(lfn)) {
            // add to the existing Replica Location
            ReplicaLocation existing = this.get(lfn);
            existing.addPFNs(rl.getPFNList());
        } else {
            // store directly in the store.
            put(lfn, rl);
        }
    }

    /**
     * Returns a <code>ReplicaLocation</code> corresponding to the LFN.
     *
     * @param lfn the lfn for which the ReplicaLocation is required.
     * @return <code>ReplicaLocation</code> if entry exists else null.
     */
    public ReplicaLocation getReplicaLocation(String lfn) {
        return get(lfn);
    }

    /**
     * Returns an iterator to the list of <code>ReplicaLocation</code> objects stored in the store.
     *
     * @return Iterator.
     */
    public Iterator replicaLocationIterator() {
        return this.mStore.values().iterator();
    }

    /**
     * Returns the set of LFN's for which the mappings are stored in the store.
     *
     * @return Set
     */
    public Set getLFNs() {
        return this.mStore.keySet();
    }

    /**
     * Returns a <code>Set</code> of lfns for which the mappings are stored in the store, amongst
     * the <code>Set</code> passed as input.
     *
     * @param lfns the collections of lfns
     * @return Set
     */
    public Set getLFNs(Set lfns) {
        Set s = new HashSet();
        String lfn;
        for (Iterator it = lfns.iterator(); it.hasNext(); ) {
            lfn = (String) it.next();
            if (this.containsLFN(lfn)) {
                s.add(lfn);
            }
        }
        return s;
    }

    /**
     * Returns a boolean indicating whether a store is empty or not.
     *
     * @return boolean
     */
    public boolean isEmpty() {
        return this.getLFNCount() == 0;
    }

    /**
     * Returns the number of LFN's for which the mappings are stored in the store.
     *
     * @return int
     */
    public int getLFNCount() {
        return this.mStore.size();
    }

    /**
     * Returns the clone of the object.
     *
     * @return the clone
     */
    public Object clone() {

        // clone is not implemented fully.
        throw new RuntimeException("Clone not implemented for " + this.getClass().getName());

        //        return rc;
    }

    /**
     * Returns the textual description of the data class.
     *
     * @return the textual description.
     */
    public String toString() {
        StringBuffer sb = new StringBuffer();
        for (Iterator it = this.replicaLocationIterator(); it.hasNext(); ) {
            sb.append(it.next());
            sb.append("\n");
        }
        return sb.toString();
    }

    /**
     * Returns a boolean indicating whether the store has a mapping for a particular LFN or not.
     *
     * @param lfn the logical file name of the file.
     * @return boolean
     */
    public boolean containsLFN(String lfn) {
        return mStore.containsKey(lfn);
    }

    /**
     * Inserts entry in the store overwriting any existing entry.
     *
     * @param key the key
     * @param value <code>ReplicaLocation</code> object.
     * @return Object
     */
    protected Object put(String key, ReplicaLocation value) {
        return mStore.put(key, value);
    }

    /**
     * Returns an entry corresponding to the LFN
     *
     * @param key the LFN
     * @return <code>ReplicaLocation</code> object if exists, else null.
     */
    protected ReplicaLocation get(String key) {
        Object result = mStore.get(key);
        return (result == null) ? null : (ReplicaLocation) result;
    }
}

/**
 * Custom deserializer for YAML representation of ReplicaStore
 *
 * @author Karan Vahi
 */
class ReplicaStoreDeserializer extends ReplicaCatalogJsonDeserializer<ReplicaStore> {

    /**
     * Deserializes a Transformation YAML description of the type
     *
     * <pre>
     *  pegasus: 5.0
     *  replicas:
     *    # matches "f.a"
     *    - lfn: "f.a"
     *      pfn: "file:///Volumes/data/input/f.a"
     *      site: "local"
     *
     *    # matches faa, f.a, f0a, etc.
     *    - lfn: "f.a"
     *    pfn: "file:///Volumes/data/input/f.a"
     *    site: "local"
     *    regex: true
     * </pre>
     *
     * @param parser
     * @param dc
     * @return
     * @throws IOException
     * @throws JsonProcessingException
     */
    @Override
    public ReplicaStore deserialize(JsonParser parser, DeserializationContext dc)
            throws IOException, JsonProcessingException {
        ObjectCodec oc = parser.getCodec();
        JsonNode node = oc.readTree(parser);
        ReplicaStore store = new ReplicaStore();
        for (Iterator<Map.Entry<String, JsonNode>> it = node.fields(); it.hasNext(); ) {
            Map.Entry<String, JsonNode> e = it.next();
            String key = e.getKey();
            ReplicaCatalogKeywords reservedKey = ReplicaCatalogKeywords.getReservedKey(key);
            if (reservedKey == null) {
                this.complainForIllegalKey(
                        ReplicaCatalogKeywords.REPLICAS.getReservedName(), key, node);
            }

            String keyValue = node.get(key).asText();
            switch (reservedKey) {
                case PEGASUS:
                    store.setVersion(keyValue);
                    break;

                case REPLICAS:
                    JsonNode replicaNodes = node.get(key);
                    if (replicaNodes != null) {
                        if (replicaNodes.isArray()) {
                            for (JsonNode replicaNode : replicaNodes) {
                                ReplicaLocation rl = this.createReplicaLocation(replicaNode);
                                int count = rl.getPFNCount();
                                if (count == 0 || count > 1) {
                                    throw new ReplicaCatalogException("ReplicaLocation for ReplicaLocation " + rl +
                                                " can only have one pfn. Found " + count);
                                }
                                ReplicaCatalogEntry rce = rl.getPFNList().get(0);
                                if (rce.isRegex()) {
                                    StringBuffer error = new StringBuffer();
                                    error.append("Unable to deserialize into Replica Store an entry")
                                            .append(" ").append("for lfn")
                                            .append(" ").append(rl).append(" ")
                                            .append("as it has regex attribute set to true. Please specify such entries in a replica catalog file.");
                                    throw new ReplicaCatalogException(error.toString());
                                }
                                store.add(rl);
                            }
                        }
                    }
                    break;

                default:
                    this.complainForUnsupportedKey(
                            ReplicaCatalogKeywords.REPLICAS.getReservedName(), key, node);
            }
        }

        return store;
    }

    
}
