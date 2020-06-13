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
package edu.isi.pegasus.planner.mapper.output;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.util.Boolean;
import edu.isi.pegasus.planner.catalog.ReplicaCatalog;
import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry;
import edu.isi.pegasus.planner.catalog.replica.ReplicaFactory;
import edu.isi.pegasus.planner.catalog.site.classes.FileServer;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.NameValue;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.mapper.MapperException;
import edu.isi.pegasus.planner.mapper.OutputMapper;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * This class connects to a Replica Catalog backend to determine where an output file should be
 * placed on the output site. At present the location on the output site returned is the first
 * matching entry in the Replica Catalog.
 *
 * <p>By default, if no replica catalog backend is specified, the RC defaults to Regex replica
 * catalog backend.
 *
 * <p>To use this mapper, user needs to set the following properties
 *
 * <pre>
 * pegasus.dir.storage.mapper               Replica
 * pegasus.dir.storage.mapper.replica       <replica-catalog backend to use>
 * pegasus.dir.storage.mapper.replica.file  the RC file at the backend to use, \
 *                                          if using a file based RC
 * </pre>
 *
 * @author Karan Vahi
 */
public class Replica implements OutputMapper {

    /** The prefix for the property subset for connecting to the individual catalogs. */
    public static final String PROPERTY_PREFIX = "pegasus.dir.storage.mapper.replica";

    /** Short description. */
    private static final String DESCRIPTION = "Replica Catalog Mapper";

    /**
     * The name of the key that disables writing back to the cache file. Designates a static file.
     * i.e. read only
     */
    public static final String READ_ONLY_KEY = "read.only";

    /**
     * The name of the key that disables exception thown in case of unable to map a file. Instead
     * return null in that case.
     */
    public static final String DISABLE_EXCEPTIONS_KEY = "disable.exceptions";

    /** The short name for this backend. */
    private static final String SHORT_NAME = "Replica";

    /** The default replica catalog backend. */
    private String DEFAULT_REPLICA_BACKEND = "Regex";

    /** The handle to the logger. */
    protected LogManager mLogger;

    /** Handle to the Site Catalog contents. */
    protected SiteStore mSiteStore;

    /** The output sites where the data needs to be placed. */
    protected Set<String> mOutputSites;

    protected ReplicaCatalog mRCCatalog;

    protected boolean mThrowExceptionInCaseOfReplicaNotFound;

    /** The default constructor. */
    public Replica() {}

    /**
     * Initializes the mappers.
     *
     * @param bag the bag of objects that is useful for initialization.
     * @param workflow the workflow refined so far.
     */
    public void initialize(PegasusBag bag, ADag workflow) throws MapperException {
        PlannerOptions options = bag.getPlannerOptions();
        mLogger = bag.getLogger();
        mSiteStore = bag.getHandleToSiteStore();
        mOutputSites = (Set<String>) options.getOutputSites();
        boolean stageOut = ((this.mOutputSites != null) && (!this.mOutputSites.isEmpty()));

        if (!stageOut) {
            // no initialization and return
            mLogger.log(
                    "No initialization of StageOut Site Directory Factory",
                    LogManager.DEBUG_MESSAGE_LEVEL);
            return;
        }

        Properties props = bag.getPegasusProperties().matchingSubset(PROPERTY_PREFIX, false);

        mThrowExceptionInCaseOfReplicaNotFound =
                !Boolean.parse(props.getProperty(Replica.DISABLE_EXCEPTIONS_KEY), false);
        String catalogImplementor = bag.getPegasusProperties().getProperty(Replica.PROPERTY_PREFIX);

        // we only are reading not inserting any entries
        props.setProperty(Replica.READ_ONLY_KEY, "true");

        catalogImplementor =
                (catalogImplementor == null) ? DEFAULT_REPLICA_BACKEND : catalogImplementor;
        try {
            mRCCatalog = ReplicaFactory.loadInstance(catalogImplementor, bag, props);
        } catch (Exception e) {
            // log the connection error
            throw new MapperException(
                    "Unable to connect to replica catalog backend for output mapper "
                            + catalogImplementor,
                    e);
        }
    }

    /**
     * Maps a LFN to a location on the filsystem of a site and returns a single externally
     * accessible URL corresponding to that location. It queries the underlying Replica Catalog and
     * returns the first matching PFN.
     *
     * @param lfn the lfn
     * @param site the output site
     * @param operation whether we want a GET or a PUT URL
     * @return NameValue with name referring to the site and value as externally accessible URL to
     *     the mapped file
     * @throws MapperException if unable to construct URL for any reason
     */
    public NameValue map(String lfn, String site, FileServer.OPERATION operation)
            throws MapperException {
        // in this case we want to create an entry in factory namespace and use that addOn
        return this.map(lfn, site, operation, false);
    }

    /**
     * Maps a LFN to a location on the filsystem of a site and returns a single externally
     * accessible URL corresponding to that location. It queries the underlying Replica Catalog and
     * returns the first matching PFN.
     *
     * @param lfn the lfn
     * @param site the output site
     * @param operation whether we want a GET or a PUT URL
     * @param existing indicates whether to create a new location/placement for a file, or rely on
     *     existing placement on the site.
     * @return NameValue with name referring to the site and value as externally accessible URL to
     *     the mapped file
     * @throws MapperException if unable to construct URL for any reason and exception throwing is
     *     enabled.
     */
    public NameValue map(String lfn, String site, FileServer.OPERATION operation, boolean existing)
            throws MapperException {

        String url = null;
        if (site == null) {
            Collection<ReplicaCatalogEntry> c = mRCCatalog.lookup(lfn);
            if (c != null) {
                for (ReplicaCatalogEntry rce : c) {
                    url = rce.getPFN();
                    site = rce.getResourceHandle();
                    break;
                }
            }
        } else {
            // we just return the first matching URL
            url = mRCCatalog.lookup(lfn, site);
        }

        if (url == null && this.mThrowExceptionInCaseOfReplicaNotFound) {
            throw new MapperException(
                    this.getErrorMessagePrefix()
                            + "Unable to retrive location from Mapper Replica Backend for lfn "
                            + lfn
                            + " for site "
                            + site
                            + " and operation "
                            + operation);
        }

        return url == null ? null : new NameValue(site, url);
    }

    /**
     * Maps a LFN to a location on the filsystem of a site and returns all the possible equivalent
     * externally accessible URL corresponding to that location. In case of the replica backed only
     * one URL is returned and that is the first matching PFN for the output site.
     *
     * @param lfn the lfn
     * @param site the output site
     * @param operation whether we want a GET or a PUT URL
     * @return List of NameValue objects referring to mapped URL's along with their corresponding
     *     site information
     * @throws MapperException if unable to construct URL for any reason
     */
    public List<NameValue> mapAll(String lfn, String site, FileServer.OPERATION operation)
            throws MapperException {
        List result = new LinkedList();

        Collection<ReplicaCatalogEntry> c = mRCCatalog.lookup(lfn);
        if (c != null) {
            for (ReplicaCatalogEntry rce : c) {
                String s = rce.getResourceHandle();
                if (site == null || site.equals(s)) {
                    result.add(new NameValue(s, rce.getPFN()));
                }
            }
        }

        if (result.isEmpty() && this.mThrowExceptionInCaseOfReplicaNotFound) {
            throw new MapperException(
                    this.getErrorMessagePrefix()
                            + "Unable to retrive location from Mapper Replica Backend for lfn "
                            + lfn
                            + " for site "
                            + site
                            + " and operation "
                            + operation);
        }

        return result.isEmpty() ? null : result;
    }

    /**
     * Returns the prefix message to be attached to an error message
     *
     * @return
     */
    protected String getErrorMessagePrefix() {
        StringBuilder error = new StringBuilder();
        error.append("[").append(this.getShortName()).append("] ");
        return error.toString();
    }

    private String getShortName() {
        return Replica.SHORT_NAME;
    }

    /**
     * Returns a short description of the mapper.
     *
     * @return
     */
    public String description() {
        return this.DESCRIPTION;
    }
}
