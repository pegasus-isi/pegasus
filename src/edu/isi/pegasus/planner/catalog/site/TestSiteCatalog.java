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

import edu.isi.pegasus.planner.catalog.SiteCatalog;
import java.util.ArrayList;
import java.util.List;
import org.griphyn.cPlanner.common.LogManager;

import java.util.Properties;
import org.griphyn.cPlanner.common.PegasusProperties;

/**
 * A Test program that shows how to load a Site Catalog, and query for all sites.
 * The configuration is picked from the Properties. The following properties
 * need to be set
 *  <pre>
 *      pegasus.catalog.site       Text|XML|XML3
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
        SiteCatalog catalog = null;
        
        /* load the catalog using the factory */
        try{
            catalog = SiteFactory.loadInstance( PegasusProperties.nonSingletonInstance());
        }
        catch ( SiteFactoryException e ){
            System.out.println(  e.convertException() );
            System.exit( 2 );
        }

        /* load all sites in site catalog */
        try{
            List s = new ArrayList(1);
            s.add( "*" );
            System.out.println( "Loaded  " + catalog.load( s ) + " number of sites " );
        
            /* query for the sites, and print them out */
            System.out.println( "Sites loaded are "  + catalog.list( ) );
        }
        catch ( SiteCatalogException e ){
            e.printStackTrace();
        }
        finally{
            /* close the connection */
            try{
                catalog.close();
            }catch( Exception e ){}
        }

    }

}
