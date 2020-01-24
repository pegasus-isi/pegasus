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
package edu.isi.pegasus.planner.selector.replica;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.util.PegasusURL;
import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry;
import edu.isi.pegasus.planner.classes.ReplicaLocation;
import edu.isi.pegasus.planner.common.PegRandom;
import edu.isi.pegasus.planner.common.PegasusProperties;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

/**
 * A replica selector, that allows the user to specify good sites and bad sites for staging in data
 * to a compute site.
 *
 * <p>A good site for a compute site X, is a preferred site from which replicas should be staged to
 * site X. If there are more than one good sites having a particular replica, then a random siteis
 * selected amongst these preferred sites.
 *
 * <p>A bad site for a compute site X, is a site from which replica's should not be staged. The
 * reason of not accessing replica from a bad site can vary from the link being down, to the user
 * not having permissions on that site's data.
 *
 * <p>The good | bad sites are specified by the properties
 * pegasus.selector.replica.*.prefer.stagein.sites| pegasus.selector.replica.*.ignore.stagein.sites,
 * where the * in the property name denotes the name of the compute site. A * in the property key is
 * taken to mean all sites.
 *
 * <p>The pegasus.selector.replica.*.prefer.stagein.sites property takes precedence over
 * pegasus.selector.replica.*.ignore.stagein.sites property i.e. if for a site X, a site Y is
 * specified both in the ignored and the preferred set, then site Y is taken to mean as only a
 * preferred site for a site X.
 *
 * <p>In order to use the replica selector implemented by this class,
 *
 * <pre>
 *        - the property pegasus.selector.replica.selector must be set to value Restricted.
 * </pre>
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 * @version $Revision$
 */
public class Restricted extends Default {

    /** A short description of the replica selector. */
    private static final String mDescription = "Restricted";

    /** The property prefix for all properties used by this selector. */
    private static final String PROPERTY_PREFIX = "pegasus.selector.replica";

    /** The property suffix for determining the preferred sites for a site x. */
    private static final String PROPERTY_PREFER_SUFFIX = "prefer.stagein.sites";

    /** The property suffix for determining the ignored sites for a site x. */
    private static final String PROPERTY_IGNORE_SUFFIX = "ignore.stagein.sites";

    /**
     * A Map indexed by site handles, that contains a set of site handles. The sites in the set are
     * the sites from which to prefer data transfers to the site referred to by key of the map.
     */
    private Map mPreferredSitesMap;

    /**
     * The set of preferred sites, that are preferred stagein sites for all sites. Referred to by
     * "pegasus.selector.replica.*.prefer.sites" property.
     */
    private Set mGlobalPreferredSites;

    /**
     * A Map indexed by site handles, that contains a set of site handles. The sites in the set are
     * the sites from which to ignore data transfers to the site referred to by key of the map.
     */
    private Map mIgnoredSitesMap;

    /**
     * The Set of ignored sites, that are ignored for selecting replicas for all sites. Referred to
     * by "pegasus.selector.replica.*.default.sites" property.
     */
    private Set mGlobalIgnoredSites;

    /**
     * The overloaded constructor, that is called by load method.
     *
     * @param properties the <code>PegasusProperties</code> object containing all the properties
     *     required by Pegasus.
     */
    public Restricted(PegasusProperties properties) {
        super(properties);
        mIgnoredSitesMap = new HashMap(15);
        mPreferredSitesMap = new HashMap(15);
        mGlobalIgnoredSites = getSitesSet(mProps.getAllIgnoredSites());
        mGlobalPreferredSites = getSitesSet(mProps.getAllPreferredSites());
    }

    /**
     * This chooses a location amongst all the locations returned by the replica location service.
     * If a location is found with re attribute same as the preference pool, it is taken. Else a
     * random location is selected and returned. If more than one location for the lfn is found at
     * the preference pool, then also a random location amongst the ones at the preference pool is
     * selected.
     *
     * @param rl the <code>ReplicaLocation</code> object containing all the pfn's associated with
     *     that LFN.
     * @param preferredSite the preffered site for picking up the replicas.
     * @param allowLocalFileURLs indicates whether Replica Selector can select a replica on the
     *     local site / submit host.
     * @return <code>ReplicaCatalogEntry</code> corresponding to the location selected.
     * @see org.griphyn.cPlanner.classes.ReplicaLocation
     */
    public ReplicaCatalogEntry selectReplica(
            ReplicaLocation rl, String preferredSite, boolean allowLocalFileURLs) {

        String lfn = rl.getLFN();
        String site;
        ArrayList prefLocs = new ArrayList();
        int locSelected;

        // create a shallow clone as we will be removing
        // using Iterator.remove() methods
        rl = (ReplicaLocation) rl.clone();

        // build state on the basis of preferred sites
        populateSiteMaps(preferredSite);

        mLogger.log(
                "[RestrictedReplicaSelector] Selecting a pfn for lfn " + lfn + "\n amongst" + rl,
                LogManager.DEBUG_MESSAGE_LEVEL);

        ReplicaCatalogEntry rce;
        for (Iterator it = rl.pfnIterator(); it.hasNext(); ) {
            rce = (ReplicaCatalogEntry) it.next();
            site = rce.getResourceHandle();

            // check if equal to the execution site
            // or site is preferred to stage to execution site.
            if (prefer(site, preferredSite)) {

                // check for file URL
                if (this.removeFileURL(rce, preferredSite, allowLocalFileURLs)) {
                    it.remove();
                } else {
                    if (rce.getPFN().startsWith(PegasusURL.FILE_URL_SCHEME)) {
                        // this is the one which is reqd for ligo
                        // return the location instead of breaking
                        return rce;
                    }
                    prefLocs.add(rce);
                }

            }

            // remove a URL with a site that is
            // to be ignored for staging data to any site.
            // or if it is a file url
            else if (ignore(site, preferredSite)
                    || this.removeFileURL(rce, preferredSite, allowLocalFileURLs)) {
                it.remove();
            }
        }

        int noOfLocs = rl.getPFNCount();
        if (noOfLocs == 0) {
            // in all likelihood all the urls were file urls and
            // none were associated with the preference pool.
            // replica not selected
            throw new RuntimeException(
                    "Unable to select a Physical Filename (PFN) for the file with logical filename (LFN) as "
                            + lfn);
        }

        if (prefLocs.isEmpty()) {
            // select a random location from all the matching locations
            locSelected = PegRandom.getInteger(noOfLocs - 1);
            rce = rl.getPFN(locSelected);

        } else {
            // select a random location amongst all the preferred locations
            int preferredSize = prefLocs.size();
            locSelected = PegRandom.getInteger(preferredSize - 1);
            rce = (ReplicaCatalogEntry) prefLocs.get(locSelected);

            // create symbolic links instead of going through gridftp server
            // moved to Transfer Engine Karan June 8th, 2009
            /*
            if (mUseSymLinks) {
                rce = replaceProtocolFromURL( rce );
            }
            */
        }

        return rce;
    }

    /**
     * Returns a short description of the replica selector.
     *
     * @return string corresponding to the description.
     */
    public String description() {
        return mDescription;
    }

    /**
     * Returns a boolean indicating whether a source site is to be preffered for staging to a
     * destination site
     *
     * @param source the source site.
     * @param destination the destination site.
     * @return true if source is a preferred site for staging to destination, else false.
     */
    protected boolean prefer(String source, String destination) {
        boolean result = false;
        Set s;
        if (mPreferredSitesMap.containsKey(destination)) {
            s = (Set) mPreferredSitesMap.get(destination);
            result = s.contains(source);
        }

        if (!result) {
            // check for source in global preferred sites
            result = globallyPreferred(source);
        }
        return result;
    }

    /**
     * Returns a boolean indicating whether a source site is to be ignored for staging to a
     * destination site
     *
     * @param source the source site.
     * @param destination the destination site.
     * @return true if source is tp be ignored while staging to destination, else false.
     */
    protected boolean ignore(String source, String destination) {
        boolean result = false;
        Set s;
        if (mIgnoredSitesMap.containsKey(destination)) {
            s = (Set) mIgnoredSitesMap.get(destination);
            result = s.contains(source);
        }

        if (!result) {
            // check for source in global preferred sites
            result = globallyIgnored(source);
        }
        return result;
    }

    /**
     * Returns a boolean indicating whether a site is a preferred replica source for all compute
     * sites.
     *
     * @param site the site to test for.
     * @return boolean.
     */
    protected boolean globallyPreferred(String site) {
        return mGlobalPreferredSites.contains(site);
    }

    /**
     * Returns a boolean indicating whether a site is to be ignored as a replica source for all
     * compute sites.
     *
     * @param site the site to test for.
     * @return boolean.
     */
    protected boolean globallyIgnored(String site) {
        return mGlobalIgnoredSites.contains(site);
    }

    /**
     * Returns the name of the property, for a particular site X. The value of the property contains
     * a comma separated list of site handles that are to be ignored|preferred while selecting
     * replicas to stage to the site X.
     *
     * @param site the site X.
     * @param suffix the property suffix to be applied.
     * @return the name of the property.
     */
    protected String getProperty(String site, String suffix) {
        StringBuffer sb = new StringBuffer();
        sb.append(this.PROPERTY_PREFIX).append('.').append(site).append('.').append(suffix);
        return sb.toString();
    }

    /**
     * Builds up the set of preferred and ignored sites for a site.
     *
     * @param site the site for which to identify the preferred and ignored sites.
     */
    private void populateSiteMaps(String site) {
        // check to see if we already have an entry
        if (mPreferredSitesMap.containsKey(site)) {
            // we already have computed the site
            return;
        }

        // build up preferred sites for site
        String name = getProperty(site, this.PROPERTY_PREFER_SUFFIX);
        Set p = this.getSitesSet(mProps.getProperty(name));
        mPreferredSitesMap.put(site, p);

        // build up ignored sites for site
        name = getProperty(site, this.PROPERTY_IGNORE_SUFFIX);
        Set i = this.getSitesSet(mProps.getProperty(name));
        mIgnoredSitesMap.put(site, i);
    }

    /**
     * Returns a set of third party sites. An empty set is returned if value is null.
     *
     * @param value the comma separated list in the properties file.
     * @return Set containing the names of the pools.
     */
    private Set getSitesSet(String value) {
        Set set = new LinkedHashSet();
        String site;
        if (value == null || value.length() == 0) {
            return set;
        }

        for (StringTokenizer st = new StringTokenizer(value, ","); st.hasMoreTokens(); ) {
            site = (String) st.nextToken();
            set.add(site);
        }
        return set;
    }
}
