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

package edu.isi.pegasus.planner.catalog.site;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.common.util.Version;
import edu.isi.pegasus.planner.catalog.SiteCatalog;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.common.PegasusProperties;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * A Test program that shows how to load a Site Catalog, and query for all sites. The configuration
 * is picked from the Properties. The following properties need to be set
 *
 * <pre>
 *      pegasus.catalog.site       Text|XML|XML3
 *      pegasus.catalog.site.file  path to the site catalog.
 *  </pre>
 *
 * The Pegasus Properties can be picked from property files at various locations. The priorities are
 * explained below.
 *
 * <pre>
 *   - The default path for the properties file is $PEGASUS_HOME/etc/properties.
 *   - A properties file if found at ${user.home}/.pegasusrc has higher property.
 *   - Finally a user can specify the path to the properties file by specifying
 *     the JVM  property pegasus.user.properties . This has the higher priority.
 * </pre>
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class TestSiteCatalog {

    /**
     * The main program.
     *
     * @param args arguments passed at runtime
     */
    public static void main(String[] args) {
        SiteCatalog catalog = null;
        PegasusProperties properties = PegasusProperties.nonSingletonInstance();

        // setup the logger for the default streams.
        LogManager logger = LogManagerFactory.loadSingletonInstance(properties);
        logger.logEventStart(
                "event.pegasus.catalog.site.test",
                "planner.version",
                Version.instance().toString());

        // set debug level to maximum
        // set if something is going wrong
        // logger.setLevel( LogManager.DEBUG_MESSAGE_LEVEL );

        /* print out all the relevant site catalog properties that were specified*/
        Properties siteProperties = properties.matchingSubset("pegasus.catalog.site", true);
        System.out.println("Site Catalog Properties specified are " + siteProperties);

        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, PegasusProperties.nonSingletonInstance());
        /* load the catalog using the factory */
        try {
            catalog = SiteFactory.loadInstance(bag);
        } catch (SiteFactoryException e) {
            System.out.println(e.convertException());
            System.exit(2);
        }

        /* load all sites in site catalog */
        try {
            List s = new ArrayList(1);
            s.add("*");
            System.out.println("Loaded  " + catalog.load(s) + " number of sites ");

            /* query for the sites, and print them out */
            Set<String> sites = catalog.list();
            System.out.println("Sites loaded are " + sites);

            /* get detailed information about all the sites */
            for (String site : sites) {
                SiteCatalogEntry entry = catalog.lookup(site);
                System.out.println(entry);
            }

        } catch (SiteCatalogException e) {
            e.printStackTrace();
        } finally {
            /* close the connection */
            try {
                catalog.close();
            } catch (Exception e) {
            }
        }
    }
}
