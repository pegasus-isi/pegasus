/**
 * Copyright 2007-2016 University Of Southern California
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
package edu.isi.pegasus.planner.mapper.staging;

import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.mapper.MapperException;
import java.io.File;
import java.io.IOException;
import java.util.Properties;
import org.griphyn.vdl.euryale.VirtualFlatFileFactory;

/**
 * Maps the output files to a flat directory on the staging site.
 *
 * @author Karan Vahi
 */
public class Flat extends Abstract {

    /** The short name for the mapper */
    public static final String SHORT_NAME = "Flat";

    /** Short description. */
    private static final String DESCRIPTION = "Flat Directory Staging Mapper";

    private VirtualFlatFileFactory mFactory;

    /** The default constructor. */
    public Flat() {}

    /**
     * Initializes the submit mapper
     *
     * @param bag the bag of Pegasus objects
     * @param properties properties that can be used to control the behavior of the mapper
     */
    public void initialize(PegasusBag bag, Properties properties) {
        super.initialize(bag, properties);

        // all file factories intialized with the addon component only
        try {
            // Create a flat file factory
            mFactory = new VirtualFlatFileFactory("."); // minimum default
        } catch (IOException ioe) {
            throw new MapperException(
                    "Unable to intialize the Flat File Factory for Staging Mapper ", ioe);
        }
    }

    /**
     * Returns the addOn part that is retrieved from the File Factory. It creates a new file in the
     * factory for the LFN and returns it.
     *
     * @param job
     * @param lfn the LFN to be used
     * @param site the site at which the LFN resides
     * @return
     */
    public File mapToRelativeDirectory(Job job, SiteCatalogEntry site, String lfn) {
        // In the Flat hierarchy, all files are placed on the same directory.
        // we just let the factory create a new addOn space in the base directory
        // for the lfn
        File addOn = null;
        try {
            // the factory will give us the relative
            // add on part
            // PM-1131 figure out the last addon directory taking into
            // account deep lfns
            // addOn = mFactory.createFile( lfn ).getParentFile();
            File relative = mFactory.createFile(lfn);
            File deepLFN = new File(lfn);
            addOn = relative;
            while (deepLFN != null) {
                deepLFN = deepLFN.getParentFile();
                addOn = addOn.getParentFile();
            }
        } catch (IOException e) {
            throw new MapperException("IOException ", e);
        }

        return addOn;
    }

    /**
     * Returns a virtual relative directory for the job that has been mapped already.
     *
     * @param site
     * @param lfn the lfn
     * @return
     */
    public File getRelativeDirectory(String site, String lfn) {
        return new File(".");
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
