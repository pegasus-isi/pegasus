/**
 * Copyright 2007-2012 University Of Southern California
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
package edu.isi.pegasus.planner.dax;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.common.util.XMLWriter;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Abstract Type for RC and TC Sections of the DAX. Extended by {@link Executable} and {@link File}
 *
 * @author gmehta
 * @version $Revision$
 * @see Executable
 * @see File
 */
public class CatalogType {

    protected List<Profile> mProfiles;
    protected Set<MetaData> mMetadata;
    protected List<PFN> mPFNs;
    protected LogManager mLogger;

    protected CatalogType() {
        mProfiles = new LinkedList<Profile>();
        mMetadata = new LinkedHashSet<MetaData>();
        mPFNs = new LinkedList<PFN>();
        mLogger = LogManagerFactory.loadSingletonInstance();
    }

    /**
     * Copy Constructor
     *
     * @param c
     */
    protected CatalogType(CatalogType c) {
        this.mProfiles = new LinkedList<Profile>(c.mProfiles);
        this.mMetadata = new LinkedHashSet<MetaData>(c.mMetadata);
        this.mPFNs = new LinkedList<PFN>(c.mPFNs);
        this.mLogger = c.mLogger;
    }

    /**
     * Add a pfn url to the Catalog
     *
     * @param url
     * @return CatalogType
     */
    public CatalogType addPhysicalFile(String url) {
        PFN p = new PFN(url);
        mPFNs.add(p);
        return this;
    }

    /**
     * Add a PFN url and a site id to the Catalog
     *
     * @param url
     * @param site
     * @return CatalogType
     */
    public CatalogType addPhysicalFile(String url, String site) {
        PFN p = new PFN(url, site);
        mPFNs.add(p);
        return this;
    }

    /**
     * Add a PFN object to the Catalog
     *
     * @param pfn
     * @return CatalogType
     * @see PFN
     */
    public CatalogType addPhysicalFile(PFN pfn) {
        mPFNs.add(pfn);
        return this;
    }

    /**
     * Add a list of PFN objects to the Catalog
     *
     * @param pfns
     * @return CatalogType
     * @see PFN
     */
    public CatalogType addPhysicalFiles(List<PFN> pfns) {
        mPFNs.addAll(pfns);
        return this;
    }

    /**
     * Returns a List of PFN objects associated with this Catalog entry
     *
     * @return List<PFN>
     * @see PFN
     */
    public List<PFN> getPhysicalFiles() {
        return mPFNs;
    }

    /**
     * Add a Metadata entry for the Catalog
     *
     * @param key String key for the metadata entry
     * @param value String value for the metadata entry
     * @return CatalogType
     */
    public CatalogType addMetaData(String key, String value) {
        MetaData m = new MetaData(key, value);
        mMetadata.add(m);
        return this;
    }

    /**
     * Add a {@link MetaData} object for the Catalog object
     *
     * @param metadata
     * @return CatalogType
     * @see MetaData
     */
    public CatalogType addMetaData(MetaData metadata) {
        mMetadata.add(metadata);
        return this;
    }

    /**
     * Add a List of {@link MetaData} objects to the Catalog entry object
     *
     * @param metadata
     * @return CatalogType
     * @see MetaData
     */
    public CatalogType addMetaData(List<MetaData> metadata) {
        mMetadata.addAll(metadata);
        return this;
    }

    /**
     * Returns the List of MetaData objects associated with this Catalog entry object
     *
     * @return Set<MetaData>
     * @see MetaData
     */
    public Set<MetaData> getMetaData() {
        return mMetadata;
    }

    /**
     * Add a profile to the catalog entry
     *
     * @param namespace String Namespace of the profile. See {@link Profile.NAMESPACE} for a list of
     *     valid namespaces
     * @param key String Key of the profile
     * @param value String Value of the profile
     * @return CatalogType
     * @see Profile.NAMESPACE
     */
    public CatalogType addProfile(String namespace, String key, String value) {
        mProfiles.add(new Profile(namespace, key, value));
        return this;
    }

    /**
     * Add a profile to the catalog entry
     *
     * @param namespace {@link Profile.NAMESPACE} Namespace of the profile
     * @param key String Key of the profile
     * @param value String Value of the profile
     * @return CatalogType
     * @see Profile.NAMESPACE
     */
    public CatalogType addProfile(Profile.NAMESPACE namespace, String key, String value) {
        mProfiles.add(new Profile(namespace, key, value));
        return this;
    }

    /**
     * Add a List of profile objects to this Catalog entry
     *
     * @param profiles List of Profile objects
     * @return CatalogType
     * @see Profile
     */
    public CatalogType addProfiles(List<Profile> profiles) {
        mProfiles.addAll(profiles);
        return this;
    }

    /**
     * Add a Profile object to this Catalog entry
     *
     * @param profile
     * @return CatalogType
     * @see Profile
     */
    public CatalogType addProfiles(Profile profile) {
        mProfiles.add(profile);
        return this;
    }

    /**
     * Return the List of {@link Profile} objects associated with this Catalog entry
     *
     * @return List<Profile>
     * @see Profile
     */
    public List<Profile> getProfiles() {
        return mProfiles;
    }

    public boolean isFile() {
        return false;
    }

    public boolean isExecutable() {
        return false;
    }

    /**
     * Write the XML representation of this object
     *
     * @param writer
     * @see XMLWriter
     */
    public void toXML(XMLWriter writer) {
        toXML(writer, 0);
    }

    /**
     * Write the XML representation of this object
     *
     * @param writer
     * @param indent
     * @see XMLwriter
     */
    public void toXML(XMLWriter writer, int indent) {
        for (Profile p : mProfiles) {
            p.toXML(writer, indent + 1);
        }
        for (MetaData m : mMetadata) {
            m.toXML(writer, indent + 1);
        }
        for (PFN f : mPFNs) {
            f.toXML(writer, indent + 1);
        }
    }
}
