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
import edu.isi.pegasus.planner.catalog.site.classes.FileServer;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.NameValue;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.mapper.MapperException;
import edu.isi.pegasus.planner.mapper.OutputMapper;
import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * A convenience mapper implementation that stages output files to a fixed directory, specified
 * using properties. The URL set for this needs to be logically consistent with the --output-site
 * option passed to the planner.
 *
 * <p>To use this mapper, user needs to set the following properties
 *
 * <pre>
 * pegasus.dir.storage.mapper            Fixed
 * pegasus.dir.storage.mapper.fixed.url  externally accessible URL to the directory
 *                                       where output files need to be placed.
 * </pre>
 *
 * @author Karan Vahi
 */
public class Fixed implements OutputMapper {

    /** The prefix for the property subset for this mapper implementation */
    public static final String PROPERTY_PREFIX = "pegasus.dir.storage.mapper.fixed";

    /** Short description. */
    private static final String DESCRIPTION = "Fixed Directory mapper";

    private static final String SHORT_NAME = "Fixed";

    /** The handle to the logger. */
    protected LogManager mLogger;

    /** Handle to the Site Catalog contents. */
    protected SiteStore mSiteStore;

    /** The output sites where the data needs to be placed. */
    protected Set<String> mOutputSites;

    /** Externally accessible URL */
    private String mDirectoryURL;

    /** The default constructor. */
    public Fixed() {}

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
                    "No initialization of Fixed Directory Mapper", LogManager.DEBUG_MESSAGE_LEVEL);
            return;
        }

        String property = Fixed.PROPERTY_PREFIX + ".url";
        mDirectoryURL = bag.getPegasusProperties().getProperty(property);
        if (mDirectoryURL == null) {
            throw new MapperException("Unspecified property " + property);
        }
    }

    /**
     * Maps a LFN to a location on the filsystem of a site and returns a single externally
     * accessible URL corresponding to that location.
     *
     * @param lfn the lfn
     * @param site the output site
     * @param operation whether we want a GET or a PUT URL
     * @return NameValue with name referring to the site and value as externally accessible URL to
     *     the mapped file
     * @throws MapperException if unable to construct URL for any reason
     */
    public NameValue<String, String> map(String lfn, String site, FileServer.OPERATION operation)
            throws MapperException {
        // in this case we want to create an entry in factory namespace and use that addOn
        return this.map(lfn, site, operation, false);
    }

    /**
     * Maps a LFN to a location on the filsystem of a site and returns a single externally
     * accessible URL corresponding to that location.
     *
     * @param lfn the lfn
     * @param site the output site
     * @param operation whether we want a GET or a PUT URL
     * @param existing indicates whether to create a new location/placement for a file, or rely on
     *     existing placement on the site.
     * @return NameValue with name referring to the site and value as externally accessible URL to
     *     the mapped file
     * @throws MapperException if unable to construct URL for any reason
     */
    public NameValue<String, String> map(
            String lfn, String site, FileServer.OPERATION operation, boolean existing)
            throws MapperException {
        StringBuilder url = new StringBuilder();
        url.append(this.mDirectoryURL).append(File.separator).append(lfn);
        return new NameValue(site, url.toString());
    }

    /**
     * Maps a LFN to a location on the filesystem of a site and returns all the possible equivalent
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
    public List<NameValue<String, String>> mapAll(
            String lfn, String site, FileServer.OPERATION operation) throws MapperException {
        NameValue nv = this.map(lfn, site, operation);
        List result = new LinkedList();
        result.add(nv);
        return result;
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
        return Fixed.SHORT_NAME;
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
