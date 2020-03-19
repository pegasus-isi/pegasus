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
package edu.isi.pegasus.planner.catalog.transformation.classes;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import edu.isi.pegasus.common.util.Separator;
import edu.isi.pegasus.planner.catalog.classes.CatalogEntryJsonDeserializer;
import edu.isi.pegasus.planner.catalog.classes.Transformation;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogKeywords;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * A container data class that is used to store transformations. The transformation are stored
 * internally indexed by transformation name.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class TransformationStore {

    /**
     * The internal store map. The Map is indexed by transformation names. The corresponding value
     * is a Map that contains entries for all sites for a particular transformation . This map is
     * indexed by site name and corresponding values are Lists of TransformationCatalogEntry
     * objects.
     */
    private Map<String, Map<String, List<TransformationCatalogEntry>>> mTCStore;

    /** Containers indexed by their LFN */
    private Map<String, Container> mContainers;
    
     /** The version for the Transformation Catalog */
    private String mVersion;

    /** The default constructor. */
    public TransformationStore() {
        initialize();
    }

    /** Intializes the store. */
    private void initialize() {
        mTCStore = new TreeMap<String, Map<String, List<TransformationCatalogEntry>>>();
        mContainers = new TreeMap<String, Container>();
    }

    /** Clears all the entries in the store. */
    public void clear() {
        // more efficient to create a new object rather than relying on
        // the underlying map clear method
        initialize();
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
     * Adds an entry into the store. If the entry already exists i.e entry for a site and
     * corresponding PFN exists it's overriden.
     *
     * @param entry the transformation catalog object.
     */
    public void addEntry(TransformationCatalogEntry entry) {

        String completeName = entry.getLogicalTransformation();

        if (this.containsTransformation(completeName)) {
            // retrieve the associated map
            Map<String, List<TransformationCatalogEntry>> m = mTCStore.get(completeName);

            // check if the transformation is defined for a particular site
            if (m.containsKey(entry.getResourceId())) {
                // add an existing entry
                List<TransformationCatalogEntry> l = m.get(entry.getResourceId());

                boolean existing = false;
                for (TransformationCatalogEntry e : l) {
                    // PM-888 instead of only matching on PFN we now match on the
                    // whole transformation catalog entry. since, we can have
                    // two entries with same PFN but ( different osrelease i.e osrlease
                    // specified for one entry and not for the other)
                    if (e.equals(entry)) {
                        // lets overwrite the entry and break out
                        l.remove(e);
                        l.add(entry);
                        existing = true;
                        break;
                    }
                }
                if (!existing) {
                    // an entry with the same pfn does not exist for the site
                    l.add(entry);
                }
            } else {
                // no entries for the  transformation at the site entry.getResourceId()
                List<TransformationCatalogEntry> l = new LinkedList();
                l.add(entry);
                m.put(entry.getResourceId(), l);
            }
        } else {
            Map<String, List<TransformationCatalogEntry>> m = new HashMap();
            List l = new LinkedList();
            l.add(entry);
            m.put(entry.getResourceId(), l);
            mTCStore.put(completeName, m);
        }
    }

    /**
     * Add container specified.
     *
     * @param container add a container
     */
    public void addContainer(Container container) {
        String name = container == null ? null : container.getName();
        if (name == null || name.isEmpty()) {
            throw new RuntimeException("Invalid container passed " + container);
        }

        // check if we already have it
        if (this.mContainers.containsKey(name)) {
            throw new RuntimeException(
                    "Container " + container + " already exists as " + container);
        }
        this.mContainers.put(name, container);
    }

    /**
     * Goes through all the transformation catalog entries and associates with them to real
     * references of containers
     */
    public void resolveContainerReferences() {
        for (TransformationCatalogEntry entry : this.getAllEntries()) {
            Container c = entry.getContainer();
            if (c == null) {
                continue;
            }
            String name = c.getLFN();
            if (containsContainer(name)) {
                // clone before associating as multiple transformations
                // can be associated with one container entry
                Container cont = (Container) this.getContainer(name).clone();
                entry.setContainer(cont);
                // PM-1214 special handling of merging container ENV profiles with TC
                entry.incorporateContainerProfiles(cont);
            } else {
                throw new RuntimeException(
                        "Transformation Catalog Entry "
                                + entry
                                + " refers to non existent container "
                                + name);
            }
        }
    }

    /**
     * Returns List of TransformationCatalogEntry objects for a transformation on a particular site
     * and a type. If the site parameter passed is null, then all entries are returned corresponding
     * to a tranformation. If type is null, then all entries associated with a site are returned.
     *
     * @param completeName the complete name of the transformation
     * @param site the site on which to search for entries. null means all
     * @param type the type to match on . null means all types.
     * @return List if entries are found , else empty list.
     */
    public List<TransformationCatalogEntry> getEntries(
            String completeName, String site, TCType type) {

        // retrieve all entries for a site
        List<TransformationCatalogEntry> result = null;

        // check whether we need to filter on type ?
        if (type == null) {
            result = this.getEntries(completeName, site);
        } else {
            result = new LinkedList();
            for (TransformationCatalogEntry entry : this.getEntries(completeName, site)) {

                if (entry.getType().equals(type)) {
                    result.add(entry);
                }
            }
        }

        return result;
    }

    /**
     * Returns List of TransformationCatalogEntry objects for a transformation on a particular site.
     * If the site parameter passed is null, then all entries are returned corresponding to a
     * tranformation.
     *
     * @param completeName the complete name of the transformation
     * @param site the site on which to search for entries. null means all sites
     * @return List if entries are found , else empty list.
     */
    public List<TransformationCatalogEntry> getEntries(String completeName, String site) {
        List<TransformationCatalogEntry> result = new LinkedList();

        if (this.containsTransformation(completeName)) {
            Map<String, List<TransformationCatalogEntry>> m = mTCStore.get(completeName);
            if (site == null) {
                // return all entries
                for (Map.Entry<String, List<TransformationCatalogEntry>> entry : m.entrySet()) {
                    result.addAll(entry.getValue());
                }
            } else if (m.containsKey(site)) {
                // retrieve all the entries for the site.
                result.addAll(m.get(site));
            }
        }

        return result;
    }

    /**
     * Returns all the entries in the Transformation Store
     *
     * @return all entries.
     */
    public List<TransformationCatalogEntry> getAllEntries() {
        return this.getEntries((String) null, (TCType) null);
    }

    /**
     * Returns a list of TransformationCatalogEntry objects matching on a site and transformation
     * type.
     *
     * @param site the site on which to search for entries. null means all
     * @param type the type to match on . null means all types.
     * @return List if transformations exist
     */
    public List<TransformationCatalogEntry> getEntries(String site, TCType type) {
        List<TransformationCatalogEntry> result = new LinkedList();

        // retrieve list of all transformation names
        for (String name : mTCStore.keySet()) {
            result.addAll(this.getEntries(name, site, type));
        }

        return result;
    }

    /**
     * Returns a list of transformation names matching on a site and transformation type.
     *
     * @param site the site on which to search for entries. null means all
     * @param type the type to match on . null means all types.
     * @return List if transformations exist
     */
    public List<String> getTransformations(String site, TCType type) {
        List<String> result = new LinkedList();

        if (site == null && type == null) {
            // retrieve list of all transformation names
            for (String name : mTCStore.keySet()) {
                result.add(name);
            }
            return result;
        } else if (type == null) {
            // no matching on type required only on site
            for (String name : mTCStore.keySet()) {
                Map<String, List<TransformationCatalogEntry>> m = mTCStore.get(name);

                for (String s : m.keySet()) {
                    if (s.equals(site)) {
                        result.add(name);
                        break;
                    }
                }
            }
        } else {
            // (site == null  || site is not null ) and  match on type
            for (String name : mTCStore.keySet()) {
                Map<String, List<TransformationCatalogEntry>> m = mTCStore.get(name);

                for (String s : m.keySet()) {

                    boolean matchFound = false;
                    // either site name matches or we searching for all sites
                    if (site == null || site.equals(s)) {
                        // traverse through all entries and match on type
                        List<TransformationCatalogEntry> l = m.get(s);
                        for (TransformationCatalogEntry entry : l) {
                            if (entry.getType().equals(type)) {
                                result.add(name);
                                matchFound = true;
                                break;
                            }
                        }
                    }
                    if (matchFound) {
                        break;
                    }
                } // end of iterating entries for sites
            } // end of iteration over all transformation names
        }

        return result;
    }

    /**
     * Returns a boolean indicating whether the store contains an entry corresponding to a
     * particular transformation or not.
     *
     * @param namespace the namespace associated with the transformation
     * @param name the logical name
     * @param version the version of the transformation
     * @return boolean
     */
    public boolean containsTransformation(String namespace, String name, String version) {
        return this.mTCStore.containsKey(Separator.combine(namespace, name, version));
    }

    /**
     * Returns a boolean indicating whether the store contains an entry corresponding to a
     * particular transformation or not.
     *
     * @param completeName the complete name of the transformation as constructed from namespace,
     *     name and version
     * @return boolean
     */
    public boolean containsTransformation(String completeName) {
        return this.mTCStore.containsKey(completeName);
    }

    /**
     * Whether store contains a container or not.
     *
     * @param name
     * @return
     */
    public boolean containsContainer(String name) {
        return this.mContainers.containsKey(name);
    }

    /**
     * Return a container.
     *
     * @param name
     * @return
     */
    public Container getContainer(String name) {
        return this.mContainers.get(name);
    }
}



/**
 * Custom deserializer for YAML representation of TransformationCatalog
 *
 * @author Karan Vahi
 */
class TransformationStoreDeserializer extends CatalogEntryJsonDeserializer<TransformationStore> {

    /**
     * Deserializes a Transformation Catalog YAML description of the type
     *
     * <pre>
     * pegasus: "5.0"
     * transformations:
     *    - namespace: "example"
     *      name: "keg"
     *      version: "1.0"
     *      profiles:
     *          env:
     *              APP_HOME: "/tmp/myscratch"
     *              JAVA_HOME: "/opt/java/1.6"
     *          pegasus:
     *              clusters.num: "1"
     *
     *      requires:
     *          - anotherTr
     *
     *      sites:
     *       - name: "isi"
     *          type: "installed"
     *          pfn: "/path/to/keg"
     *          arch: "x86_64"
     *          os.type: "linux"
     *          os.release: "fc"
     *          os.version: "1.0"
     *          profiles:
     *            env:
     *                Hello: World
     *                JAVA_HOME: /bin/java.1.6
     *            condor:
     *                FOO: bar
     *          container: centos-pegasus
     *
     *    - namespace: example
     *      name: anotherTr
     *      version: "1.2.3"
     *
     *      sites:
     *          - name: isi
     *            type: installed
     *            pfn: /path/to/anotherTr
     *
     * containers:
     *    - name: centos-pegasus
     *      type: docker
     *      image: docker:///rynge/montage:latest
     *      mount: /Volumes/Work/lfs1:/shared-data/:ro
     *      mount: /Volumes/Work/lfs12:/shared-data1/:ro
     *      profiles:
     *          env:
     *              JAVA_HOME: /opt/java/1.6
     * </pre>
     *
     *
     * @param parser
     * @param dc
     * @return
     * @throws IOException
     * @throws JsonProcessingException
     */
    @Override
    public TransformationStore deserialize(JsonParser parser, DeserializationContext dc)
            throws IOException, JsonProcessingException {
        ObjectCodec oc = parser.getCodec();
        JsonNode node = oc.readTree(parser);
        TransformationStore store = new TransformationStore();

        for (Iterator<Map.Entry<String, JsonNode>> it = node.fields(); it.hasNext(); ) {
            Map.Entry<String, JsonNode> e = it.next();
            String key = e.getKey();
            TransformationCatalogKeywords reservedKey = TransformationCatalogKeywords.getReservedKey(key);
            if (reservedKey == null) {
                this.complainForIllegalKey(TransformationCatalogKeywords.TRANSFORMATIONS.getReservedName(), key, node);
            }

            switch (reservedKey) {
                case PEGASUS:
                    store.setVersion(e.getValue().asText());
                    break;

                case TRANSFORMATIONS:
                    JsonNode transformationNodes = node.get(key);
                    if (transformationNodes != null) {
                        if (transformationNodes.isArray()) {
                            for (JsonNode siteNode : transformationNodes) {
                                parser = siteNode.traverse(oc);
                                Transformation tx = parser.readValueAs(Transformation.class);
                                for(TransformationCatalogEntry entry: tx.getTransformationCatalogEntries()){
                                    store.addEntry(entry);
                                }
                            }
                        }
                    }
                    break;


                default:
                    this.complainForUnsupportedKey(
                            SiteCatalogKeywords.SITES.getReservedName(), key, node);
            }
        }

        return store;
    }
}

