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

import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry;
import edu.isi.pegasus.planner.dax.PFN;
import edu.isi.pegasus.planner.namespace.Metadata;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * A Data Class that associates a LFN with the PFN's. Attributes associated with the LFN go here.
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 * @version $Revision$
 * @see edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry
 */
public class ReplicaLocation extends Data implements Cloneable {

    /**
     * The site name that is associated in the case the resource handle is not specified with the
     * PFN.
     */
    public static final String UNDEFINED_SITE_NAME = "UNDEFINED_SITE";

    /** The LFN associated with the entry. */
    private String mLFN;

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

        // PM-1001 always create a separate list only if required
        mPFNList = new ArrayList(pfns);

        if (sanitize) {
            mMetadata = this.removeMetadata(pfns);
            // only remove metadata if sanitize on.
            // we need to ensure metadata is not removed when ReplicaLocation
            // is used in the YAML backed replica catalog store.
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

    /**
     * Add a PFN and it's attributes. Any existing mapping with the same PFN and site attribute will
     * be replaced, including all its attributes.
     *
     * @param tuple the <code>ReplicaCatalogEntry</code> object containing the PFN and the
     *     attributes.
     */
    public void addPFN(ReplicaCatalogEntry tuple) {
        boolean seen = false;
        String pfn = tuple.getPFN();
        String site = tuple.getResourceHandle();

        sanitize(tuple);

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

    /**
     * Returns the textual description of the data class.
     *
     * @return the textual description.
     */
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(mLFN).append(" -> {");
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
}
