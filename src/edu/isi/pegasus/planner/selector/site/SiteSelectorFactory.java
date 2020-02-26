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
package edu.isi.pegasus.planner.selector.site;

import edu.isi.pegasus.common.util.DynamicLoader;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.selector.SiteSelector;

/**
 * A factory class to load the appropriate type of Site Selector, as specified by the user at
 * runtime in properties. Each invocation of the factory results in a SiteSelector being
 * instantiated.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class SiteSelectorFactory {

    /** The default package where the all the implementing classes provided with the VDS reside. */
    public static final String DEFAULT_PACKAGE_NAME = "edu.isi.pegasus.planner.selector.site";

    /**
     * The name of the class in the DEFAULT package, that corresponds to the default site selector.
     */
    public static final String DEFAULT_SITE_SELECTOR = "Random";

    /**
     * Loads the implementing class corresponding to the mode specified by the user at runtime in
     * the properties file. A default replica selector is loaded if property is not specified in the
     * properties.
     *
     * @param bag the bag of objects that is required.
     * @return the instance of the class implementing this interface.
     * @exception SiteSelectorFactoryException that chains any error that might occur during the
     *     instantiation
     * @see #DEFAULT_PACKAGE_NAME
     * @see #DEFAULT_SITE_SELECTOR
     */
    public static SiteSelector loadInstance(PegasusBag bag) throws SiteSelectorFactoryException {

        PegasusProperties properties = (PegasusProperties) bag.get(PegasusBag.PEGASUS_PROPERTIES);
        String className = null;
        SiteSelector selector;

        // sanity check
        try {
            if (properties == null) {
                throw new RuntimeException("Invalid properties passed");
            }

            // figure out the implementing class
            // that needs to be instantiated.
            className = properties.getSiteSelectorMode();
            className =
                    (className == null || className.trim().length() < 2)
                            ? DEFAULT_SITE_SELECTOR
                            : className;

            // prepend the package name if required
            className =
                    (className.indexOf('.') == -1)
                            ?
                            // pick up from the default package
                            DEFAULT_PACKAGE_NAME + "." + className
                            :
                            // load directly
                            className;

            // try loading the class dynamically
            DynamicLoader dl = new DynamicLoader(className);
            selector = (SiteSelector) dl.instantiate(new Object[0]);
            selector.initialize(bag);
        } catch (Exception e) {
            // chain the exception caught into the appropriate Factory Exception
            throw new SiteSelectorFactoryException("Instantiating SiteSelector ", className, e);
        }

        return selector;
    }
}
