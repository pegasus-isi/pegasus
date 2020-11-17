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

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This is a data class to store the TCMAP for a particular dag. This data class is populated and
 * maintained in the TCMapper and is queried from the Interpool Engine and site selectors.
 *
 * <p>The TCMAP is a hashmap which maps an lfn to a Map which contains keys as siteids and values as
 * List of TransformationCatalogEntry objects
 *
 * <p>TCMAP= lfn1 ---> MAP1 lfn2 ---> MAP2
 *
 * <p>MAP1 = site1 ---> List1 site2 ---> List2
 *
 * <p>List1 = TCE1 TCE2 TCEn
 *
 * @author Gaurang Mehta
 * @version $Revision$
 */
public class TCMap {

    /** The TCMap for a dag is stored in this HashMap. */
    private Map mTCMap;

    private LogManager mLogger;

    /** Default constructor. Initializes the tcmap to 10 lfns. */
    public TCMap() {
        mLogger = LogManagerFactory.loadSingletonInstance();
        mTCMap = new HashMap(10);
    }

    /**
     * Returns a HashMap of sites as keys and a List of TransformationCatalogEntry object as values.
     *
     * @param fqlfn String The fully qualified logical transformation name for which you want the
     *     map.
     * @return Map Returns <B>NULL</B> if the transformation does not exist in the map.
     * @see org.griphyn.common.catalog.TransformationCatalogEntry
     */
    public Map getSiteMap(String fqlfn) {
        return mTCMap.containsKey(fqlfn) ? (Map) mTCMap.get(fqlfn) : null;
    }

    /**
     * This method allows to associate a site map with a particular logical transformation
     *
     * @param fqlfn String The transformation for which the sitemap is to be stored
     * @param sitemap Map The sitemap that is to be stored. It is a hashmap with key as the siteid
     *     and value as a list of TranformationCatalogEntry objects
     * @return boolean
     * @see org.griphyn.common.catalog.TransformationCatalogEntry
     */
    public boolean setSiteMap(String fqlfn, Map sitemap) {
        mTCMap.put(fqlfn, sitemap);
        return true;
    }

    /**
     * Returns a List of siteid's that are valid for a particular lfn.
     *
     * @param fqlfn String
     * @return List
     */
    public List getSiteList(String fqlfn) {
        List results = null;
        if (mTCMap.containsKey(fqlfn)) {
            return new ArrayList(((Map) mTCMap.get(fqlfn)).keySet());
        }
        return results;
    }

    /**
     * Returns a list of siteid's that are valid for a particular lfn and among a list of input
     * sites
     *
     * @param fqlfn The logical name of the transformation
     * @param sites The list of siteids
     * @return the list of siteids which are valid.
     */
    public List getSiteList(String fqlfn, List sites) {
        List results = new ArrayList();
        if (mTCMap.containsKey(fqlfn)) {
            for (Iterator i = ((Map) mTCMap.get(fqlfn)).keySet().iterator(); i.hasNext(); ) {
                String site = (String) i.next();
                if (sites.contains(site)) {
                    results.add(site);
                }
            }
        }
        return results.isEmpty() ? null : results;
    }

    /**
     * This method returns a list of TransformationCatalogEntry objects for a given transformation
     * and siteid
     *
     * @param fqlfn String The fully qualified logical name of the transformation
     * @param siteid String The siteid for which the Entries are required
     * @return List returns NULL if no entries exist.
     */
    public List getSiteTCEntries(String fqlfn, String siteid) {
        Map sitemap = null;
        List tcentries = null;
        if (mTCMap.containsKey(fqlfn)) {
            sitemap = (Map) mTCMap.get(fqlfn);
            if (sitemap.containsKey(siteid)) {
                tcentries = (List) sitemap.get(siteid);
            } else {
                mLogger.log(
                        "The TCMap does not contain the site \""
                                + siteid
                                + "\" for the transformation \""
                                + fqlfn
                                + "\"",
                        LogManager.DEBUG_MESSAGE_LEVEL);
            }
        } else {
            mLogger.log(
                    "The TCMap does not contain the transformation \"" + fqlfn + "\"",
                    LogManager.DEBUG_MESSAGE_LEVEL);
        }
        return tcentries;
    }

    /**
     * Retrieves all the entries matching a particular lfn for the sites passed.
     *
     * @param fqlfn the fully qualified logical name
     * @param sites the list of siteID's for which the entries are required.
     * @return a map indexed by site names. Each value is a collection of <code>
     *     TransformationCatalogEntry</code> objects. Returns null in case of no entry being found.
     */
    public Map getSitesTCEntries(String fqlfn, List sites) {
        Map m = this.getSiteMap(fqlfn);
        Set siteIDS = new HashSet(sites);
        String site = null;
        if (m == null) {
            return null;
        }

        Map result = new HashMap(siteIDS.size());
        for (Iterator it = m.keySet().iterator(); it.hasNext(); ) {
            site = (String) it.next();
            if (siteIDS.contains(site)) {
                result.put(site, m.get(site));
            }
        }

        // returning NULL only to maintain semantics
        // for rest of mapper operations. Gaurang
        // should change to return empty map
        return result.isEmpty() ? null : result;
    }

    /**
     * This method allows to add a TransformationCatalogEntry object in the map to a particular
     * transformation for a particular site
     *
     * @param fqlfn String The fully qualified logical transformation
     * @param siteid String The site for which the TransformationCatalogEntry is valid
     * @param entry TransformationCatalogEntry The Transformation CatalogEntry object to be added.
     * @return boolean
     */
    public boolean setSiteTCEntries(String fqlfn, String siteid, TransformationCatalogEntry entry) {
        Map sitemap = null;
        List tcentries = null;
        if (mTCMap.containsKey(fqlfn)) {
            sitemap = (Map) mTCMap.get(fqlfn);
        } else {
            sitemap = new HashMap(10);
            setSiteMap(fqlfn, sitemap);
        }
        if (sitemap.containsKey(siteid)) {
            tcentries = (List) sitemap.get(siteid);
        } else {
            tcentries = new ArrayList(10);
            sitemap.put(siteid, tcentries);
        }
        tcentries.add(entry);
        return true;
    }

    /**
     * Returns the textual description of the contents of the object
     *
     * @return String
     */
    public String toString() {
        StringBuffer sb = new StringBuffer();
        for (Iterator i = mTCMap.keySet().iterator(); i.hasNext(); ) {
            sb.append(toString((String) i.next()));
        }
        return sb.toString();
    }

    /**
     * Returns a textual description of the object.
     *
     * @param lfn String
     * @return the textual description.
     */
    public String toString(String lfn) {
        StringBuffer sb = new StringBuffer();
        sb.append("LFN = " + lfn + "\n");
        sb.append("\tSite map\n");
        Map sitemap = (Map) mTCMap.get(lfn);
        for (Iterator j = sitemap.keySet().iterator(); j.hasNext(); ) {
            String site = (String) j.next();
            sb.append("\t\tSite=" + site + "\n");
            List tc = (List) sitemap.get(site);
            for (Iterator k = tc.iterator(); k.hasNext(); ) {
                TransformationCatalogEntry tcentry = (TransformationCatalogEntry) k.next();
                sb.append("\t\t\tPfn=" + tcentry.getPhysicalTransformation() + "\n");
                sb.append("\t\t\tPfn site=" + tcentry.getResourceId() + "\n");
            }
        }
        return sb.toString();
    }
}
