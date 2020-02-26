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
import edu.isi.pegasus.planner.common.PegasusProperties;
import java.util.ArrayList;
import java.util.List;

/**
 * A Test program that shows how to load a Site Catalog, and query for all sites. The configuration
 * is picked from the Properties. The following properties need to be set
 *
 * <pre>
 *      pegasus.catalog.site       Text|XML|XML3
 *      pegasus.catalog.site.file  path to the site catalog.
 *  </pre>
 *
 * @author Karan Vahi
 * @version $Revision: 595 $
 */
public class TestVORSSiteCatalog {

    /**
     * The main program.
     *
     * @param args passed at runtime
     */
    public static void main(String[] args) {
        SiteCatalog catalog = null;
        /* Properties props = new Properties();
        	props.put("host", "vors.grid.iu.edu");
        props.put("port", "80");
        props.put("vo","all");
        props.put("grid","all");*/
        /* load the catalog using the factory */

        try {
            // catalog = SiteFactory.loadInstance("VORS", props);
            PegasusProperties p = PegasusProperties.nonSingletonInstance();
            p.setProperty("pegasus.catalog.site", "VORS");
            LogManager logger = LogManagerFactory.loadInstance(p);
            logger.setLevel(LogManager.DEBUG_MESSAGE_LEVEL);
            logger.logEventStart(
                    "event.pegasus.planner", "planner.version", Version.instance().toString());
            catalog = SiteFactory.loadInstance(p);

        } catch (SiteFactoryException e) {
            System.out.println(e.convertException());
            System.exit(2);
        }

        /* load all sites in site catalog */
        try {
            // catalog.connect(props);
            List s = new ArrayList(1);
            String handle = "CIT_CMS_T2";
            s.add(handle); // "*" );
            System.out.println("Loaded  " + catalog.load(s) + " number of sites ");

            /* query for the sites, and print them out */
            System.out.println("Sites loaded are " + catalog.list());
            System.out.println(catalog.lookup(handle));

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
