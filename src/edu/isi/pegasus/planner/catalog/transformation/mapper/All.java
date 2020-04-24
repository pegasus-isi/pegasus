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
package edu.isi.pegasus.planner.catalog.transformation.mapper;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.util.PegasusURL;
import edu.isi.pegasus.common.util.Separator;
import edu.isi.pegasus.planner.catalog.classes.SysInfo;
import edu.isi.pegasus.planner.catalog.transformation.Mapper;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.catalog.transformation.classes.Container;
import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;
import edu.isi.pegasus.planner.classes.PegasusBag;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * This implementation of the mapper generates maps for sites with installed as well as stageable
 * transformations.
 *
 * @author Gaurang Mehta
 * @version $Revision$
 */
public class All extends Mapper {

    /**
     * The private constructor.
     *
     * @param bag the bag of initialization objects
     */
    public All(PegasusBag bag) {
        super(bag);
    }

    /**
     * This method returns a Map of compute sites to List of TransformationCatalogEntry objects that
     * are valid for that site.
     *
     * @param namespace String The namespace of the logical transformation
     * @param name String The name of the logical transformation
     * @param version String The version of the logical transformation
     * @param siteids List The sites for which you want the map.
     * @return Map Key=String SiteId , Values = List of TransformationCatalogEntry object. Returns
     *     null if no entries are found.
     */
    public Map getSiteMap(String namespace, String name, String version, List siteids) {
        // stores the entries got from the TC
        List tcentries = null;
        // stores the string arrays mapping a site to an entry.
        Map sitemap = null;
        // stores the system information obtained from RIC
        Map<String, SysInfo> sysinfomap = null;

        // the fully qualified lfn
        String lfn = Separator.combine(namespace, name, version);

        boolean hassite = false;
        // check if the sitemap already exists in the TCMap
        if ((sitemap = mTCMap.getSiteMap(lfn)) != null) {
            if (!sitemap.isEmpty()) {
                hassite = true;
                for (Iterator i = siteids.iterator(); i.hasNext(); ) {
                    // check if the site exists in the sitemap if not then generate sitemap again
                    if (!sitemap.containsKey((String) i.next())) {
                        hassite = false;
                    }
                }
            }
            if (hassite) {
                return mTCMap.getSitesTCEntries(lfn, siteids);
            }
        }

        // since sitemap does not exist we need to generate and populate it.
        // get the TransformationCatalog entries from the TC.
        try {
            tcentries = mTCHandle.lookup(namespace, name, version, (List) null, null);
        } catch (Exception e) {
            mLogger.log(
                    "Getting physical names from TC in the TC Mapper\n",
                    e,
                    LogManager.FATAL_MESSAGE_LEVEL);
        }
        // get the system info for the sites from the SC
        if (tcentries != null) {
            sysinfomap = mSiteStore.getSysInfos(siteids);
        } else {
            throw new RuntimeException(
                    "There are no entries for the transformation \""
                            + lfn
                            + "\" in the TC for sites "
                            + siteids);
        }
        if (sysinfomap != null) {
            for (Iterator i = siteids.iterator(); i.hasNext(); ) {
                String site = (String) i.next();
                SysInfo sitesysinfo = (SysInfo) sysinfomap.get(site);
                for (Iterator j = tcentries.iterator(); j.hasNext(); ) {
                    // check for installed and static binary executables at each site.
                    TransformationCatalogEntry entry = (TransformationCatalogEntry) j.next();
                    if (match(entry, site, sitesysinfo)) {
                        // add the TC entries in the map.
                        mTCMap.setSiteTCEntries(lfn, site, entry);
                    }
                } // outside inner for loop
            } // outside outer for loop
        } else {
            mLogger.log(
                    "There are no entries in the site catalog for site" + siteids.toString(),
                    LogManager.FATAL_MESSAGE_LEVEL);
            System.exit(1);
        }

        return mTCMap.getSitesTCEntries(lfn, siteids);
    }

    /** Returns the mode description. */
    public String getMode() {
        return "All Mode - Handle both Installed and Stageable Executables on all sites";
    }

    /**
     * Return a boolean indicating whether a TC entry maps to a site with a particular sysinfo
     *
     * @param entry
     * @param site the execution site where a job may run
     * @param sitesysinfo
     * @return
     */
    private boolean match(TransformationCatalogEntry entry, String site, SysInfo sitesysinfo) {
        SysInfo txsysinfo = entry.getSysInfo();
        String txsiteid = entry.getResourceId();
        TCType txtype = entry.getType();
        boolean match = false;
        Container c = entry.getContainer();
        if (txsysinfo.equals(sitesysinfo)) {
            // system information match
            if ((txsiteid.equalsIgnoreCase(site)) && (txtype.equals(TCType.INSTALLED))) {
                // vanilla installed case where entry is installed on the site
                match = true;
            } else if (txtype.equals(TCType.STAGEABLE)) {
                // stageable entries are accepted
                match = true;
            } else if (c != null) {
                // PM-1530 check if job has a container associated with it
                // check for a non file URL since siteid don't match
                PegasusURL url = c.getImageURL();
                if (!url.getProtocol().equals(PegasusURL.FILE_URL_SCHEME)) {
                    // non file URL means can be staged remotely
                    match = true;
                }
            }
        }
        return match;
    }
}
