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

import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PegasusFile;
import edu.isi.pegasus.planner.mapper.MapperException;
import edu.isi.pegasus.planner.partitioner.graph.GraphNode;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.griphyn.vdl.euryale.FileFactory;
import org.griphyn.vdl.euryale.VirtualDecimalHashedFileFactory;

/**
 * Maps the output files in a Hashed Directory structure on the output site.
 *
 * @author Karan Vahi
 * @see org.griphyn.vdl.euryale.VirtualDecimalHashedFileFactory;
 */
public class Hashed extends AbstractFileFactoryBasedMapper {

    /** The short name for the mapper */
    public static final String SHORT_NAME = "Hashed";

    /** Short description. */
    private static final String DESCRIPTION = "Hashed Directory Mapper";

    /** A Map that tracks for each output site, the LFN to the Add on's */
    private Map<String, Map<String, String>> mSiteLFNAddOnMap;

    /** The maximum number of entries in the map, before the output site map is cleared. */
    private static final int MAX_CACHE_ENTRIES = 1000;

    private int mNumberOfExistingLFNS;

    /**
     * Initializes the mappers.
     *
     * @param bag the bag of objects that is useful for initialization.
     * @param workflow the workflow refined so far.
     */
    public void initialize(PegasusBag bag, ADag workflow) throws MapperException {
        super.initialize(bag, workflow);
        resetLFNAddOnCache();
    }

    /**
     * Method that instantiates the FileFactory
     *
     * @param bag the bag of objects that is useful for initialization.
     * @param workflow the workflow refined so far.
     * @return the handle to the File Factory to use
     */
    public FileFactory instantiateFileFactory(PegasusBag bag, ADag workflow) {
        FileFactory factory;

        // all file factories intialized with the addon component only
        try {

            String addOn = mSiteStore.getRelativeStorageDirectoryAddon();
            // get the total number of files that need to be stageout
            int totalFiles = 0;
            for (Iterator<GraphNode> it = workflow.jobIterator(); it.hasNext(); ) {
                GraphNode node = it.next();
                Job job = (Job) node.getContent();

                // traverse through all the job output files
                for (Iterator opIt = job.getOutputFiles().iterator(); opIt.hasNext(); ) {
                    if (!((PegasusFile) opIt.next()).getTransientTransferFlag()) {
                        // means we have to stage to output site
                        totalFiles++;
                    }
                }
            }

            factory = new VirtualDecimalHashedFileFactory(addOn, totalFiles);

            // each stageout file  has only 1 file associated with it
            ((VirtualDecimalHashedFileFactory) factory).setMultiplicator(1);
        } catch (IOException ioe) {
            throw new MapperException(
                    this.getErrorMessagePrefix() + "Unable to intialize the Flat File Factor ",
                    ioe);
        }
        return factory;
    }

    /**
     * Returns the addOn part that is retrieved from the File Factory. It creates a new file in the
     * factory for the LFN and returns it.
     *
     * @param lfn the LFN to be used
     * @param site the site at which the LFN resides
     * @param existing indicates whether to create a new location/placement for a file, or rely on
     *     existing placement on the site.
     * @return
     */
    public String createAndGetAddOn(String lfn, String site, boolean existing) {
        if (existing) {
            Map<String, String> lfnAddOn = this.mSiteLFNAddOnMap.get(site);
            if (lfnAddOn == null) {
                throw new MapperException(
                        this.getErrorMessagePrefix() + " LFN's not tracked for site " + site);
            }
            String addOn = (String) lfnAddOn.get(lfn);
            if (addOn == null) {
                throw new MapperException(
                        this.getErrorMessagePrefix()
                                + " LFN "
                                + lfn
                                + " is not tracked for site "
                                + site);
            }

            // check if we need to clear the addOnMap
            if (mNumberOfExistingLFNS == Hashed.MAX_CACHE_ENTRIES) {
                this.resetLFNAddOnCache();
            }

            return addOn;
        }

        // In the Flat hierarchy, all files are placed on the same directory.
        // we just let the factory create a new addOn space in the base directory
        // for the lfn
        String addOn = null;
        try {
            // the factory will give us the relative
            // add on part
            addOn = mFactory.createFile(lfn).toString();
            this.trackLFNAddOn(site, lfn, addOn);

        } catch (IOException e) {
            throw new MapperException("IOException ", e);
        }

        return addOn;
    }

    /**
     * Tracks the lfn with addOn's on the various sites.
     *
     * @param site
     * @param lfn
     * @param addOn
     */
    private void trackLFNAddOn(String site, String lfn, String addOn) {
        if (mSiteLFNAddOnMap.containsKey(site)) {
            // we know output site  it is initialized already
            Map m = mSiteLFNAddOnMap.get(site);
            m.put(lfn, addOn);
        } else {
            Map<String, String> m = new HashMap();
            m.put(lfn, addOn);
            mSiteLFNAddOnMap.put(site, m);
        }
        mNumberOfExistingLFNS++;
    }

    /**
     * Returns the short name for the implementation class.
     *
     * @return
     */
    public String getShortName() {
        return Hashed.SHORT_NAME;
    }

    /**
     * Returns a short description of the mapper.
     *
     * @return
     */
    public String description() {
        return this.DESCRIPTION;
    }

    /** Resets the internal cache. */
    private void resetLFNAddOnCache() {
        // this is also relying on the fact that registration URL's (for which existing = true)
        // are retrieved in conjuction with the PUT urls on the stageout site.
        mSiteLFNAddOnMap = new HashMap();
        if (mOutputSites != null) {
            // add a default lfn to add on map for the site
            Map<String, String> m = new HashMap();
            for (String outputSite : this.mOutputSites) {
                mSiteLFNAddOnMap.put(outputSite, m);
            }
        }
        mNumberOfExistingLFNS = 0;
    }
}
