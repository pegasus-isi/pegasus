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
package edu.isi.pegasus.planner.classes;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.node.ObjectNode;
import edu.isi.pegasus.planner.catalog.classes.CatalogEntryJsonDeserializer;
import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry;
import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogException;
import edu.isi.pegasus.planner.catalog.replica.classes.ReplicaCatalogKeywords;
import edu.isi.pegasus.planner.common.PegasusJsonSerializer;
import edu.isi.pegasus.planner.dax.PFN;
import edu.isi.pegasus.planner.namespace.Metadata;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * A Data Class that associates a LFN with the PFN's. Attributes associated with the LFN go here.
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 * @version $Revision$
 * @see edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry
 */
@JsonDeserialize(using = ReplicaLocation.JsonDeserializer.class)
@JsonSerialize(using = ReplicaLocation.JsonSerializer.class)
public class ReplicaLocation extends Data implements Cloneable {

    /**
     * The site name that is associated in the case the resource handle is not specified with the
     * PFN.
     */
    public static final String UNDEFINED_SITE_NAME = "UNDEFINED_SITE";

    /** The LFN associated with the entry. */
    private String mLFN;

    /** whether an entry is of type regex */
    private boolean mIsRegex;

    /**
     * A list of <code>ReplicaCatalogEntry</code> objects containing the PFN's and associated
     * attributes.
     */
    private List<ReplicaCatalogEntry> mPFNList;

    /** Metadata attributes associated with the file. */
    private Metadata mMetadata;

    /** Default constructor. */
    public ReplicaLocation() {
        this("", new ArrayList<ReplicaCatalogEntry>());
    }

    /**
     * Overloaded constructor. Initializes the member variables to the values passed.
     *
     * @param rl
     */
    public ReplicaLocation(ReplicaLocation rl) {
        this(rl.getLFN(), rl.getPFNList());
    }

    /**
     * Overloaded constructor. Initializes the member variables to the values passed.
     *
     * @param lfn the logical filename.
     * @param pfns the list of <code>ReplicaCatalogEntry</code> objects.
     */
    public ReplicaLocation(String lfn, Collection<ReplicaCatalogEntry> pfns) {
        this(lfn, pfns, true);
    }

    /**
     * Overloaded constructor. Initializes the member variables to the values passed.
     *
     * @param lfn the logical filename.
     * @param pfns the list of <code>ReplicaCatalogEntry</code> objects.
     * @param sanitize add site handle if not specified
     */
    public ReplicaLocation(String lfn, Collection<ReplicaCatalogEntry> pfns, boolean sanitize) {
        mLFN = lfn;
        mIsRegex = false;
        // PM-1001 always create a separate list only if required
        mPFNList = new ArrayList(pfns);
        mMetadata = this.removeMetadata(pfns);
        if (sanitize) {
            // sanitize pfns. add a default resource handle if not specified
            sanitize(mPFNList);
        }
    }

    /**
     * Adds a PFN specified in the DAX to the object
     *
     * @param pfn the PFN
     */
    public void addPFN(PFN pfn) {
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry();
        rce.setPFN(pfn.getURL());
        rce.setResourceHandle(pfn.getSite());
        this.mPFNList.add(rce);
    }

    /**
     * Add a PFN and it's attributes. Any existing mapping with the same PFN and site attribute will
     * be replaced, including all its attributes.
     *
     * @param tuples the collection of <code>ReplicaCatalogEntry</code> object containing the PFN
     *     and the attributes.
     */
    public void addPFN(Collection<ReplicaCatalogEntry> tuples) {
        for (ReplicaCatalogEntry tuple : tuples) {
            this.addPFN(tuple);
        }
    }

    /*
     * Add a PFN and it's attributes. Any existing mapping with the same PFN and site attribute will
     * be replaced, including all its attributes.
     *
     * @param tuple the <code>ReplicaCatalogEntry</code> object containing the PFN and the
     *     attributes.
     *
     * @param sanitize add site handle if not specified
     */
    public void addPFN(ReplicaCatalogEntry tuple) {
        this.addPFN(tuple, true);
    }

    /**
     * Add a PFN and it's attributes. Any existing mapping with the same PFN and site attribute will
     * be replaced, including all its attributes.
     *
     * @param tuple the <code>ReplicaCatalogEntry</code> object containing the PFN and the
     *     attributes.
     * @param sanitize add site handle if not specified
     */
    public void addPFN(ReplicaCatalogEntry tuple, boolean sanitize) {
        boolean seen = false;
        String pfn = tuple.getPFN();
        String site = tuple.getResourceHandle();

        if (sanitize) {
            sanitize(tuple);
        }
        // traverse through the existing PFN's to check for the
        // same pfn
        for (Iterator i = this.pfnIterator(); i.hasNext() && !seen; ) {
            ReplicaCatalogEntry rce = (ReplicaCatalogEntry) i.next();
            seen = pfn.equals(rce.getPFN()) && site.equals(rce.getResourceHandle());
            if (seen) {
                try {
                    i.remove();
                } catch (UnsupportedOperationException uoe) {
                    // ignore for time being
                }
            }
        }

        this.mPFNList.add(tuple);
    }

    /**
     * Add a PFN and it's attributes.
     *
     * @param tuples the <code>List</code> object of <code>ReplicaCatalogEntry</code> objects, each
     *     containing the PFN and the attributes.
     */
    public void addPFNs(List<ReplicaCatalogEntry> tuples) {
        for (Iterator it = tuples.iterator(); it.hasNext(); ) {
            addPFN((ReplicaCatalogEntry) it.next());
        }
    }

    /**
     * Add metadata to the object.
     *
     * @param key
     * @param value
     */
    public void addMetadata(String key, String value) {
        this.mMetadata.checkKeyInNS(key, value);
    }

    /**
     * Sets the LFN.
     *
     * @param lfn the lfn.
     */
    public void setLFN(String lfn) {
        this.mLFN = lfn;
    }

    /**
     * Returns the associated LFN.
     *
     * @return lfn
     */
    public String getLFN() {
        return this.mLFN;
    }

    /**
     * Sets the regex attribute
     *
     * @param regex
     */
    public void setRegex(boolean regex) {
        this.mIsRegex = regex;
    }

    /**
     * Checks if the 'regex' attribute is set to true for the given tuple
     *
     * @return true if regex attribute is set to true, false otherwise
     */
    public boolean isRegex() {
        return this.mIsRegex;
    }

    /**
     * Return a PFN as a <code>ReplicaCatalogEntry</code>
     *
     * @param index the pfn location.
     * @return the element at the specified position in this list.
     * @throws IndexOutOfBoundsException - if the index is out of range (index < 0 || index >=
     *     size()).
     */
    public ReplicaCatalogEntry getPFN(int index) {
        return (ReplicaCatalogEntry) this.mPFNList.get(index);
    }

    /**
     * Returns the list of pfn's as <code>ReplicaCatalogEntry</code> objects.
     *
     * @return List
     */
    public List<ReplicaCatalogEntry> getPFNList() {
        return this.mPFNList;
    }

    /**
     * Returns an iterator to the list of <code>ReplicaCatalogEntry</code> objects.
     *
     * @return Iterator.
     */
    public Iterator pfnIterator() {
        return this.mPFNList.iterator();
    }

    /**
     * Returns the number of pfn's associated with the lfn.
     *
     * @return int
     */
    public int getPFNCount() {
        return this.mPFNList.size();
    }

    /**
     * Returns metadata attribute for a particular key
     *
     * @param key
     * @return value returned else null if not found
     */
    public String getMetadata(String key) {
        return (String) mMetadata.get(key);
    }

    /**
     * Returns all metadata attributes for the file
     *
     * @return Metadata
     */
    public Metadata getAllMetadata() {
        return this.mMetadata;
    }

    /**
     * Returns the clone of the object.
     *
     * @return the clone
     */
    public Object clone() {
        ReplicaLocation rc;
        try {
            rc = (ReplicaLocation) super.clone();
        } catch (CloneNotSupportedException e) {
            // somewhere in the hierarch chain clone is not implemented
            throw new RuntimeException(
                    "Clone not implemented in the base class of " + this.getClass().getName(), e);
        }
        rc.mPFNList = new ArrayList();
        rc.setLFN(this.mLFN);
        rc.setRegex(this.mIsRegex);
        rc.mMetadata = (Metadata) this.mMetadata.clone();

        // add all the RCE's
        for (Iterator it = this.pfnIterator(); it.hasNext(); ) {
            // creating a shallow clone here.
            rc.addPFN((ReplicaCatalogEntry) it.next());
        }
        // clone is not implemented fully.
        // throw new RuntimeException( "Clone not implemented for " + this.getClass().getName() );
        return rc;
    }

    /**
     * Merges the <code>ReplicaLocation</code> object to the existing one, only if the logical
     * filenames match.
     *
     * @param location is another <code>ReplicaLocations</code> to merge with.
     * @return true if a merge was successful, false if the LFNs did not match.
     */
    /*
    public boolean merge(ReplicaLocation location) {
        String lfn1 = this.getLFN();
        String lfn2 = (location == null) ? null : location.getLFN();
        boolean result =
                (lfn1 == null && lfn2 == null || lfn1 != null && lfn2 != null && lfn1.equals(lfn2));

        // only merge if LFN match
        if (result) {
            this.addPFNs(location.getPFNList());

            for (Iterator it = location.getAllMetadata().getProfileKeyIterator(); it.hasNext(); ) {
                String key = (String) it.next();
                this.addMetadata(key, location.getMetadata(key));
            }
        }

        return result;
    }
    */

    /**
     * Merges content of the passed replica location into the existing one.During the merge, any
     * existing RCE that match pfn and site handle, are removed. If after removal, the pfn list is
     * empty, all the metadata is purged, else metadata is add to existing metadata.
     *
     * @param rl
     * @return number of PFN inserted
     */
    public int merge(ReplicaLocation rl) {
        return this.merge(rl, true);
    }

    /**
     * Merges content of the passed replica location into the existing one. During the merge, any
     * existing RCE that match pfn and site handle, are removed. If after removal, the pfn list is
     * empty, all the metadata is purged, else metadata is add to existing metadata.
     *
     * @param rl
     * @param sanitize add site handle if not specified
     * @return number of PFN inserted
     */
    public int merge(ReplicaLocation rl, boolean sanitize) {
        String lfn1 = this.getLFN();
        String lfn2 = (rl == null) ? null : rl.getLFN();
        boolean lfnMatch =
                (lfn1 == null && lfn2 == null || lfn1 != null && lfn2 != null && lfn1.equals(lfn2));
        int count = 0;
        if (!lfnMatch) {
            return count;
        }

        for (ReplicaCatalogEntry toInsert : rl.getPFNList()) {
            String pfn = toInsert.getPFN();
            String handle = toInsert.getResourceHandle();

            Collection<ReplicaCatalogEntry> c = this.getPFNList();
            for (Iterator<ReplicaCatalogEntry> i = c.iterator(); i.hasNext(); ) {
                ReplicaCatalogEntry rce = i.next();
                // loop through existing entries and see if they match
                // what we are trying to insert
                if (pfn.equals(rce.getPFN())
                        && ((handle == null && rce.getResourceHandle() == null)
                                || (handle != null && handle.equals(rce.getResourceHandle())))) {
                    try {
                        i.remove();
                    } catch (UnsupportedOperationException uoe) {
                        return 0;
                    }
                }
            }
        }
        // if existing is empty now, then also purge all metadata
        if (this.getPFNCount() == 0) {
            this.getAllMetadata().reset();
        }

        // now we can insert all the entries into the existing entry
        for (ReplicaCatalogEntry toInsert : rl.getPFNList()) {
            this.addPFN(toInsert, sanitize);
            count += 1;
        }
        // we just add in metadata overwriting existing
        Metadata m = rl.getAllMetadata();
        for (Iterator<String> it = m.getProfileKeyIterator(); it.hasNext(); ) {
            String key = it.next();
            rl.addMetadata(key, (String) m.get(key));
        }
        return count;
    }

    /**
     * Returns the textual description of the data class.
     *
     * @return the textual description.
     */
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(mLFN).append(" regex ").append(this.mIsRegex).append(" -> {");
        for (Iterator it = this.pfnIterator(); it.hasNext(); ) {
            sb.append(it.next());
            sb.append(",");
        }
        sb.append(this.getAllMetadata());
        sb.append("}");
        return sb.toString();
    }

    /**
     * Helper method to retrofit RCE into the metadata object For replica catalog, metadata is per
     * LFN
     *
     * @param rces
     * @return Metadata object
     */
    protected final Metadata removeMetadata(Collection<ReplicaCatalogEntry> rces) {
        Metadata m = new Metadata();
        for (ReplicaCatalogEntry rce : rces) {
            for (Iterator<String> it = rce.getAttributeIterator(); it.hasNext(); ) {
                String attribute = it.next();
                if (attribute.equals(ReplicaCatalogEntry.RESOURCE_HANDLE)
                        || attribute.equals(ReplicaCatalogEntry.DEPRECATED_RESOURCE_HANDLE)
                        || attribute.equals(ReplicaCatalogEntry.REGEX_KEY)) {
                    // skip
                    continue;
                }
                m.construct(attribute, (String) rce.getAttribute(attribute));
                it.remove();
            }
        }
        return m;
    }

    /**
     * Sanitizes a tuple list . Sets the resource handle to a default value if not specified.
     *
     * @param tuples the tuple to be sanitized.
     */
    private void sanitize(List tuples) {
        for (Iterator it = tuples.iterator(); it.hasNext(); ) {
            this.sanitize((ReplicaCatalogEntry) it.next());
        }
    }

    /**
     * Sanitizes a tuple . Sets the resource handle to a default value if not specified.
     *
     * @param tuple the tuple to be sanitized.
     */
    private void sanitize(ReplicaCatalogEntry tuple) {
        // sanity check
        if (tuple.getResourceHandle() == null) {
            tuple.setResourceHandle(this.UNDEFINED_SITE_NAME);
        }
    }

    /**
     * Custom deserializer for YAML representation of ReplicaLocation
     *
     * @author Karan Vahi
     */
    static class JsonDeserializer extends CatalogEntryJsonDeserializer<ReplicaLocation> {

        public JsonDeserializer() {}

        /**
         * The exception to be thrown while deserializing on error
         *
         * @param message the error message
         * @return
         */
        @Override
        public RuntimeException getException(String message) {
            return new ReplicaCatalogException(message);
        }

        /**
         * Deserializes a Replica YAML description of the type
         *
         * <pre>
         * - lfn: f1
         *   pfns:
         *     - site: local
         *       pfn: /path/to/file
         *     - site: condorpool
         *       pfn: /path/to/file
         *   checksum:
         *      sha256: abc123
         *   metadata:
         *     owner: vahi
         *     size: 1024
         * </pre>
         *
         * @param node the json node
         * @return ReplicaLocation
         */
        public ReplicaLocation deserialize(JsonParser parser, DeserializationContext dc)
                throws IOException, JsonProcessingException {
            ObjectCodec oc = parser.getCodec();
            JsonNode node = oc.readTree(parser);
            String lfn = null;
            ReplicaLocation rl = new ReplicaLocation();

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
                    case LFN:
                        lfn = keyValue;
                        break;

                    case PFNS:
                        JsonNode pfnNodes = node.get(key);
                        if (pfnNodes != null) {
                            if (pfnNodes.isArray()) {
                                for (JsonNode pfnNode : pfnNodes) {
                                    rl.addPFN(this.createPFN(pfnNode));
                                }
                            }
                        }
                        break;

                    case REGEX:
                        rl.setRegex(Boolean.parseBoolean(keyValue));
                        break;

                    case CHECKSUM:
                        addChecksum(rl, node.get(key));
                        break;

                    case METADATA:
                        addMetadata(rl, node.get(key));
                        break;

                    default:
                        this.complainForUnsupportedKey(
                                ReplicaCatalogKeywords.REPLICAS.getReservedName(), key, node);
                }
            }
            if (lfn == null) {
                throw getException("Replica needs to be defined with a lfn " + node);
            }
            rl.setLFN(lfn);

            if (rl.isRegex()) {
                // apply to all PFN entries
                if (rl.getPFNCount() > 1) {
                    throw getException(
                            "PFN count cannot be more than 1 for replicas with regex true " + node);
                }
                for (ReplicaCatalogEntry rce : rl.getPFNList()) {
                    rce.addAttribute(ReplicaCatalogEntry.REGEX_KEY, "true");
                }
            }
            return rl;
        }

        /**
         * Parses checksum information and adds it to the replica catalog entry object
         *
         * @param rl
         * @param node
         */
        private void addChecksum(ReplicaLocation rl, JsonNode node) {

            if (node instanceof ObjectNode) {
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
                        case SHA256:
                            rl.addMetadata(Metadata.CHECKSUM_TYPE_KEY, "sha256");
                            rl.addMetadata(Metadata.CHECKSUM_VALUE_KEY, keyValue);
                            break;

                        default:
                            this.complainForUnsupportedKey(
                                    ReplicaCatalogKeywords.CHECKSUM.getReservedName(), key, node);
                    }
                }
            } else {
                throw getException("Checksum needs to be object node. Found for replica" + node);
            }
        }

        /**
         * Deserializes a pfn of type below
         *
         * <pre>
         * - site: local
         *   pfn: /url/to/file
         * </pre>
         *
         * @param node
         * @return
         */
        private ReplicaCatalogEntry createPFN(JsonNode node) {
            ReplicaCatalogEntry rce = new ReplicaCatalogEntry();
            if (node instanceof ObjectNode) {
                for (Iterator<Map.Entry<String, JsonNode>> it = node.fields(); it.hasNext(); ) {
                    Map.Entry<String, JsonNode> e = it.next();
                    String key = e.getKey();
                    ReplicaCatalogKeywords reservedKey = ReplicaCatalogKeywords.getReservedKey(key);
                    if (reservedKey == null) {
                        this.complainForIllegalKey(
                                ReplicaCatalogKeywords.PFNS.getReservedName(), key, node);
                    }

                    String keyValue = node.get(key).asText();
                    switch (reservedKey) {
                        case PFN:
                            rce.setPFN(keyValue);
                            break;

                        case SITE:
                            rce.setResourceHandle(keyValue);
                            break;

                        default:
                            this.complainForUnsupportedKey(
                                    ReplicaCatalogKeywords.PFNS.getReservedName(), key, node);
                    }
                }
            } else {
                throw getException("PFN needs to be object node. Found for replica" + node);
            }

            return rce;
        }

        /**
         * Parses any metadata into the ReplicaLocation object
         *
         * @param rl
         * @param node
         */
        private void addMetadata(ReplicaLocation rl, JsonNode node) {
            for (Iterator<Map.Entry<String, JsonNode>> it = node.fields(); it.hasNext(); ) {
                Map.Entry<String, JsonNode> entry = it.next();
                rl.addMetadata(entry.getKey(), entry.getValue().asText());
            }
        }
    }

    /**
     * Custom serializer for YAML representation of ReplicaLocation
     *
     * @author Karan Vahi
     */
    static class JsonSerializer extends PegasusJsonSerializer<ReplicaLocation> {

        public JsonSerializer() {}

        /**
         * Serializes contents into YAML representation Sample representation below
         *
         * <pre>
         * lfn: "f2"
         * pfns:
         *   -
         *     pfn: "file:///path/to/file"
         *     site: "local"
         *   -
         *     pfn: "file:///path/to/file"
         *     site: "condorpool"
         * checksum:
         *   sha256: "991232132abc"
         * metadata:
         *   owner: "pegasus"
         *   abc: "123"
         *   size: "1024"
         *   k: "v"
         * </pre>
         *
         * @param r;
         * @param gen
         * @param sp
         * @throws IOException
         */
        public void serialize(ReplicaLocation rl, JsonGenerator gen, SerializerProvider sp)
                throws IOException {
            gen.writeStartObject();
            writeStringField(gen, ReplicaCatalogKeywords.LFN.getReservedName(), rl.getLFN());
            if (rl.isRegex()) {
                writeStringField(gen, ReplicaCatalogKeywords.REGEX.getReservedName(), "true");
            }
            if (rl.getPFNCount() > 0) {
                gen.writeArrayFieldStart(ReplicaCatalogKeywords.PFNS.getReservedName());
                for (ReplicaCatalogEntry rce : rl.getPFNList()) {
                    gen.writeStartObject();
                    // we don't quote or escape anything as serializer
                    // always adds enclosing quotes
                    writeStringField(
                            gen, ReplicaCatalogKeywords.PFN.getReservedName(), rce.getPFN());
                    writeStringField(
                            gen,
                            ReplicaCatalogKeywords.SITE.getReservedName(),
                            rce.getResourceHandle());
                    gen.writeEndObject();
                }
                gen.writeEndArray();
            }
            Metadata m = rl.getAllMetadata();
            if (m != null && !m.isEmpty()) {
                gen.writeObject(m);
            }

            gen.writeEndObject();
        }
    }
}
