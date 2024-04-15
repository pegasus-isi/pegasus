/*
 *
 *   Copyright 2007-2008 University Of Southern California
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 */

package edu.isi.pegasus.planner.catalog.transformation.impl;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.util.Boolean;
import edu.isi.pegasus.common.util.PegasusURL;
import edu.isi.pegasus.planner.catalog.TransformationCatalog;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.catalog.transformation.TransformationFactory;
import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.common.PegasusProperties;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * An abstract base class that provides useful methods for all the TransformationCatalog
 * Implementations to use.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public abstract class Abstract implements TransformationCatalog {
    /**
     * The LogManager object which is used to log all the messages. It's values are set in the
     * CPlanner (the main toolkit) class.
     */
    protected LogManager mLogger;

    /** The handle to the properties object. */
    protected PegasusProperties mProps;

    /**
     * A boolean tracking whether a TC instance is transient i.e. used only internally for planner
     * use
     */
    protected boolean mTransient;

    /**
     * Initialize the implementation, and return an instance of the implementation. It should be in
     * the connect method, to be consistent with the other catalogs.
     *
     * @param bag the bag of Pegasus initialization objects.
     */
    @Override
    public void initialize(PegasusBag bag) {
        mProps = bag.getPegasusProperties();
        mLogger = bag.getLogger();
        mTransient =
                Boolean.parse(
                        mProps.getProperty(
                                PegasusProperties.PEGASUS_TRANSFORMATION_CATALOG_PROPERTY
                                        + "."
                                        + TransformationFactory.TRANSIENT_PROPERTY_KEY),
                        false);
        mLogger.log(
                "Transformation Catalog Type used " + this.getDescription(),
                LogManager.CONFIG_MESSAGE_LEVEL);
    }

    /**
     * Modifies a Transformation Catalog Entry to handle file URL's. A file URL if specified for the
     * physical path is converted to an absolute path if the type of entry is set to INSTALLED.
     *
     * <p>Alternately it modifies the TC to handle absolute file paths by converting them to file
     * URL if the type of entry is set to STAGEABLE.
     *
     * @param entry the transformation catalog entry object.
     * @return the TransformationCatalogEntry object.
     */
    public static TransformationCatalogEntry modifyForFileURLS(TransformationCatalogEntry entry) {
        // sanity checks
        if (entry == null || entry.getPhysicalTransformation() == null) {
            // return without modifying
            return entry;
        }

        String url = entry.getPhysicalTransformation();
        // convert file url appropriately for installed executables
        if (entry.getType().equals(TCType.INSTALLED)
                && url.startsWith(PegasusURL.FILE_URL_SCHEME)) {
            try {
                url = new URL(url).getFile();
                entry.setPhysicalTransformation(url);
            } catch (MalformedURLException ex) {
                throw new RuntimeException("Error while converting file url ", ex);
            }
        } else if (entry.getType().equals(TCType.STAGEABLE) && url.startsWith("/")) {
            url = PegasusURL.FILE_URL_SCHEME + "//" + url;
            entry.setPhysicalTransformation(url);
        }

        return entry;
    }

    /**
     * Modifies a Transformation Catalog Entry to handle file URL's. A file URL if specified for the
     * physical path is converted to an absolute path if the type of entry is set to INSTALLED.
     *
     * <p>Alternately it modifies the TC to handle absolute file paths by converting them to file
     * URL if the type of entry is set to STAGEABLE.
     *
     * @param pfn The PFN to modify
     * @param type The type of PFN
     * @return the Transformed PFN.
     */
    public static String modifyForFileURLS(String pfn, String type) {
        // sanity checks
        if (pfn == null) {
            // return without modifying
            return pfn;
        }

        if (TCType.valueOf(type) == TCType.INSTALLED
                && pfn.startsWith(PegasusURL.FILE_URL_SCHEME)) {
            try {
                pfn = new URL(pfn).getFile();
            } catch (MalformedURLException ex) {
                throw new RuntimeException("Error while converting file url ", ex);
            }
        } else if (TCType.valueOf(type) == TCType.STAGEABLE && pfn.startsWith("/")) {
            pfn = PegasusURL.FILE_URL_SCHEME + "//" + pfn;
        }

        return pfn;
    }
}
