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
package edu.isi.pegasus.planner.catalog.replica;

import edu.isi.pegasus.planner.catalog.classes.CatalogEntry;

import java.util.*;

/**
 * The entry is a high-level logical structure representing the physical filename, the site handle,
 * and optional attributes related to the PFN as one entity.
 *
 * <p>The resource handle is the most frequently used attribute. In reality, the resource handle may
 * be a relational attribute of the mapping relation between an LFN and a PFN - there is
 * disagreement among the developers on this issue. For simplicity purposes, it appears to be
 * sufficient to make the resource handle a regular PFN attribute.
 *
 * <p>
 *
 * @author Jens-S. Vöckler
 * @author Karan Vahi
 * @version $Revision$
 */
public class ReplicaCatalogEntry implements CatalogEntry, Cloneable {
    /** The (reserved) attribute name used for the resource handle. */
    public static final String RESOURCE_HANDLE = "site";

    /** The (reserved) attribute name used for the resource handle. */
    public static final String DEPRECATED_RESOURCE_HANDLE = "pool";
    
    /**
     * Whether the entry is regex or not
     */
    public static final String REGEX_KEY = "regex";
    
    /** The physical filename. */
    private String m_pfn;

    /** Any optional attributes associated with the PFN. */
    private Map m_attributeMap;

    /**
     * Default constructor for arrays. The PFN is initialized to
     * <code>null</null>, and thus must be explicitly set later. The map
     * of attributes associated with the PFN is initialized to be empty.
     * Thus, no resource handle is available.
     */
    public ReplicaCatalogEntry() {
        m_pfn = null;
        m_attributeMap = new TreeMap();
    }

    /**
     * Convenience constructor initializes the PFN. The map of attributes is initialized to be
     * empty. Thus, no resource handle is avaiable.
     *
     * @param pfn is the PFN to remember.
     */
    public ReplicaCatalogEntry(String pfn) {
        m_pfn = pfn;
        m_attributeMap = new TreeMap();
    }

    /**
     * Convenience constructor initializes the PFN and the resource handle. The resource handle is
     * stored as regular PFN attribute.
     *
     * @param pfn is the PFN to remember.
     * @param handle is the resource handle to remember.
     */
    public ReplicaCatalogEntry(String pfn, String handle) {
        m_pfn = pfn;
        m_attributeMap = new TreeMap();
        m_attributeMap.put(RESOURCE_HANDLE, handle);
    }

    /**
     * Standard constructor initializes the PFN and arbitrary attributes.
     *
     * @param pfn is the PFN to remember.
     * @param attributes is a map of arbitrary attributes related to the PFN.
     */
    public ReplicaCatalogEntry(String pfn, Map attributes) {
        m_pfn = pfn;
        m_attributeMap = new TreeMap(attributes);
        this.checkAndUpdateForPoolAttribute();
    }

    /**
     * Adds an attribute to the set of attributes. Note, this is identical to the {@link
     * #setAttribute( String, Object )} method of the same signature.
     *
     * @param key is the key denoting an attribute.
     * @param value is a value object to store.
     */
    public void addAttribute(String key, Object value) {
        this.m_attributeMap.put(key, value);
    }

    /**
     * Adds attributes to the existing attributes.
     *
     * @param attributes is a map of attributes to add.
     * @see #setAttribute(Map)
     * @see java.util.Map#putAll( Map )
     */
    public void addAttribute(Map attributes) {
        this.m_attributeMap.putAll(attributes);
    }

    /**
     * Obtains the attribute value for a given key.
     *
     * @param key is the key to look up
     * @return the object stored as value, may be null.
     * @see java.util.Map#get( Object )
     */
    public Object getAttribute(String key) {
        return this.m_attributeMap.get(key);
    }

    /**
     * Checks for the existence of an attribute key.
     *
     * @param key is the key to look up
     * @return true if the key is known, false otherwise.
     */
    public boolean hasAttribute(String key) {
        return this.m_attributeMap.containsKey(key);
    }

    /**
     * Counts the number of attributes known for the PFN.
     *
     * @return number of attributes, may be zero.
     * @see java.util.Map#size()
     */
    public int getAttributeCount() {
        return this.m_attributeMap.size();
    }

    /**
     * Provides an iterator to traverse the attributes by their keys.
     *
     * @return an iterator over the keys to walk the attribute list.
     */
    public Iterator getAttributeIterator() {
        return this.m_attributeMap.keySet().iterator();
    }

    /**
     * Merges the attribute maps of two entries in a controlled fashion. Entries are only merged
     * with another entry, if the physical filenames match.
     *
     * @param a is one replica catalog entry to merge.
     * @param b is the other replica catalog entry to merge.
     * @param overwrite resolves intersections. If true, uses rce's attribute to remain, if false,
     *     the original attribute remains.
     * @return the merged entry, if the PFNs matched, or <code>null</code> if the PFN mismatched.
     */
    public static ReplicaCatalogEntry merge(
            ReplicaCatalogEntry a, ReplicaCatalogEntry b, boolean overwrite) {
        ReplicaCatalogEntry result = null;

        String pfn1 = a.getPFN();
        String pfn2 = b.getPFN();
        if (pfn1 == null && pfn2 == null || pfn1 != null && pfn2 != null && pfn1.equals(pfn2)) {
            result = new ReplicaCatalogEntry(pfn1, a.m_attributeMap);
            result.merge(b, overwrite); // result cannot be false
        }

        // will return null on PFN mismatch
        return result;
    }

    /**
     * Merges the attribute maps in a controlled fashion. An entry is only merged with another
     * entry, if the physical filenames match.
     *
     * @param rce is another replica catalog entry to merge with.
     * @param overwrite resolves intersections. If true, uses rce's attribute to remain, if false,
     *     the original attribute remains.
     * @return true if a merge was attempted, false if the PFNs did not match.
     */
    public boolean merge(ReplicaCatalogEntry rce, boolean overwrite) {
        String pfn1 = this.m_pfn;
        String pfn2 = rce.getPFN();
        boolean result =
                (pfn1 == null && pfn2 == null || pfn1 != null && pfn2 != null && pfn1.equals(pfn2));

        // only merge if PFN match
        if (result) {
            String key;
            Object val;

            for (Iterator i = rce.getAttributeIterator(); i.hasNext(); ) {
                key = (String) i.next();
                val = rce.getAttribute(key);
                if (hasAttribute(key)) {
                    if (overwrite) setAttribute(key, val);
                } else {
                    setAttribute(key, val);
                }
            }
        }

        return result;
    }

    /**
     * Removes all attributes associated with a PFN.
     *
     * @see #removeAttribute( String )
     */
    public void removeAllAttribute() {
        this.m_attributeMap.clear();
    }

    /**
     * Removes a specific attribute.
     *
     * @param name is the name of the attribute to remove.
     * @return the value object that was removed, or <code>null</code>, if the key was not in the
     *     map.
     * @see #removeAllAttribute()
     */
    public Object removeAttribute(String name) {
        return this.m_attributeMap.remove(name);
    }

    /**
     * Adds a new or overwrites an existing attribute. Note, this is identical to the {@link
     * #addAttribute( String, Object)} method of the same signature.
     *
     * @param key is the name of the attribute
     * @param value is the value object associated with the attribute.
     */
    public void setAttribute(String key, Object value) {
        this.m_attributeMap.put(key, value);
    }

    /**
     * Replaces all existing attributes with new attributes. Existing attributes are removed before
     * attempting a shallow copy of the new attributes.
     *
     * @param attributes is the map of new attributes to remember.
     * @see #addAttribute(Map)
     */
    public void setAttribute(Map attributes) {
        this.m_attributeMap.clear();
        this.m_attributeMap.putAll(attributes);
    }

    /**
     * Obtains the resource handle from the attributes map. This is a convenience method.
     * Internally, the PFN attribute map is queried for the value of the resource handle.
     *
     * @return the resource handle, or <code>null</code> if unset.
     * @see #setResourceHandle( String )
     */
    public String getResourceHandle() {
        String site = (String) this.m_attributeMap.get(RESOURCE_HANDLE);

        return (site == null) ? (String) this.m_attributeMap.get(DEPRECATED_RESOURCE_HANDLE) : site;
    }

    /**
     * Sets a new resource handle to remember as PFN attribute. This is a convenience method.
     * Internally, the PFN attribute map is changed to remember the new resource handle.
     *
     * @param handle is the new resource handle.
     * @see #getResourceHandle()
     */
    public void setResourceHandle(String handle) {
        this.m_attributeMap.put(RESOURCE_HANDLE, handle);
    }

    /**
     * Accessor: Obtains the PFN portion from this entry.
     *
     * @return the physical filename, or <code>null</code> if unset.
     * @see #setPFN( String )
     */
    public String getPFN() {
        return m_pfn;
    }

    /**
     * Accessor: Sets a new PFN to remember.
     *
     * @param pfn is a new physical filename.
     * @see #getPFN()
     */
    public void setPFN(String pfn) {
        m_pfn = pfn;
    }

    /**
     * Checks if the 'regex' attribute is set to true for the given tuple
     *
     * @return true if regex attribute is set to true, false otherwise
     */
    public boolean isRegex() {
        return (this != null
                && this.getAttribute(REGEX_KEY) != null
                && ((String) this.getAttribute(REGEX_KEY)).equals("true"));
    }
    
    /**
     * Converts the contents into a string.
     *
     * @return a textual representation of the item content.
     */
    public String toString() {
        // return "(" + m_pfn + "," + m_attributeMap.toString() + ")";
        StringBuffer result = null;

        // save the formatted map content
        String save = m_attributeMap.toString();

        if (m_pfn == null) {
            result = new StringBuffer(10 + save.length());
            result.append("((null),");
        } else {
            result = new StringBuffer(4 + m_pfn.length() + save.length());
            result.append('(').append(m_pfn).append(',');
        }

        result.append(save);
        result.append(')');

        return result.toString();
    }

    /**
     * Matches two ReplicaCatalogEntry objects. The primary key in this case is the pfn and all the
     * attributes.
     *
     * @return true if the pfn and all the attributes match, false otherwise.
     */
    public boolean equals(Object obj) {
        // null check
        if (obj == null) return false;

        // see if type of objects match
        if (!(obj instanceof ReplicaCatalogEntry)) return false;

        ReplicaCatalogEntry rce = (ReplicaCatalogEntry) obj;
        String pfn1 = this.m_pfn;
        String pfn2 = rce.getPFN();

        // rce with null pfns are assumed to match
        boolean result =
                (pfn1 == null && pfn2 == null
                        || pfn1 != null
                                && pfn2 != null
                                && pfn1.equals(pfn2)
                                && this.getAttributeCount() == rce.getAttributeCount());

        if (result) {
            String key;
            Object val;

            // do the matching on attributes now
            for (Iterator it = rce.getAttributeIterator(); it.hasNext(); ) {
                key = (String) it.next();
                val = rce.getAttribute(key);
                if (hasAttribute(key)) {
                    if (!(getAttribute(key).equals(val))) {
                        result = false;
                        break;
                    }
                }
            }
        }

        return result;
    }

    public Object clone() {

        ReplicaCatalogEntry r;
        try {
            r = (ReplicaCatalogEntry) super.clone();
        } catch (CloneNotSupportedException e) {
            // somewhere in the hierarch chain clone is not implemented
            throw new RuntimeException(
                    "Clone not implemented in the base class of " + this.getClass().getName(), e);
        }
        r.m_attributeMap = new TreeMap();
        r.setPFN(getPFN());
        r.setResourceHandle(getResourceHandle());
        r.addAttribute(this.m_attributeMap);

        return r;
    }

    /**
     * Checks to see if pool attribute is specified. If specified it is set as the site attribute.
     * In case both are specified an exception is thrown
     */
    public final void checkAndUpdateForPoolAttribute() {
        if (m_attributeMap.containsKey(ReplicaCatalogEntry.DEPRECATED_RESOURCE_HANDLE)) {
            String pool =
                    (String) m_attributeMap.remove(ReplicaCatalogEntry.DEPRECATED_RESOURCE_HANDLE);
            // PM-813 update the site attribute with the pool value
            if (m_attributeMap.containsKey(ReplicaCatalogEntry.RESOURCE_HANDLE)) {
                throw new RuntimeException(
                        "Both site and pool attribute specified for entry " + this);
            }
            m_attributeMap.put(ReplicaCatalogEntry.RESOURCE_HANDLE, pool);
        }
    }
}
