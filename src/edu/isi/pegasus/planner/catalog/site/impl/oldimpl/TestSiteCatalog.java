/**
 *  Copyright 2007-2008 University Of Southern California
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package edu.isi.pegasus.planner.catalog.site.impl.oldimpl;

import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.planner.catalog.site.impl.oldimpl.classes.SiteInfo;

import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.common.logging.LogManager;

import edu.isi.pegasus.planner.namespace.Condor;

import java.util.List;
import java.util.Iterator;

/**
 * A Test program that shows how to load a Site Catalog, and query for all sites.
 * The configuration is picked from the Properties. The following properties
 * need to be set
 *  <pre>
 *      pegasus.catalog.site       Text|XML
 *      pegasus.catalog.site.file  path to the site catalog.
 *  </pre>
 *
 * @author Karan Vahi
 * @version $Revision$
 */

public class TestSiteCatalog {

    /**
     * The main program.
     */
    public static void main( String[] args ) {
        PoolInfoProvider catalog = null;
        LogManager logger =  LogManagerFactory.loadSingletonInstance();

        /* load the catalog using the factory */
        try{
            catalog = SiteFactory.loadInstance( PegasusProperties.nonSingletonInstance(),
                                                false );
        }
        catch ( SiteFactoryException e ){
            logger.log( e.convertException() , LogManager.FATAL_MESSAGE_LEVEL);
            System.exit( 2 );
        }

        /* query for the sites, and print them out */
        List siteIDs = catalog.getPools();

        for( Iterator it = catalog.getPools().iterator(); it.hasNext(); ){
            String siteID = (String)it.next();
            SiteInfo site = catalog.getPoolEntry( siteID, Condor.VANILLA_UNIVERSE );
            //System.out.println( site.toXML() ); //for XML output
            System.out.println( site.toMultiLine() ); //for multiline text output
        }

    }
}
