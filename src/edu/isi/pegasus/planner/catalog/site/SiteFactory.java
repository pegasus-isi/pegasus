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

import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;

import org.griphyn.cPlanner.classes.PegasusBag;

import org.griphyn.cPlanner.common.PegasusProperties;
import org.griphyn.cPlanner.common.LogManager;

import org.griphyn.common.catalog.*;
import org.griphyn.common.util.*;

import java.util.Properties;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * A factory class to load the appropriate implementation of Site Catalog
 * as specified by properties.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class SiteFactory {

   
    /**
     * The default package where all the implementations reside.
     */
    public static final String DEFAULT_PACKAGE_NAME =
        "edu.isi.pegasus.planner.catalog.site.impl";

    /**
     * 
     * @param sites
     * @param bag  the bag of pegasus objects
     * 
     * @return SiteStore object containing the information about the sites.
     */
    public static SiteStore loadSiteStore( Collection<String> sites , PegasusBag bag ) {
        LogManager logger = bag.getLogger();
        SiteStore result = new SiteStore();
        if( sites.isEmpty() ) {
            logger.log( "No sites given by user. Will use sites from the site catalog",
                        LogManager.DEBUG_MESSAGE_LEVEL);
            sites.add( "*" );
        }
        SiteCatalog catalog = null;
        
        /* load the catalog using the factory */
        catalog = SiteFactory.loadInstance( bag.getPegasusProperties() );
        
        /* always load local site */
        List<String> toLoad = new ArrayList<String>( sites );
        toLoad.add( "local" );

        
        /* load the sites in site catalog */
        try{
            catalog.load( toLoad );
        
            /* query for the sites, and print them out */
            logger.log( "Sites loaded are "  + catalog.list( ) ,
                         LogManager.DEBUG_MESSAGE_LEVEL );
            
            
            //load into SiteStore from the catalog.
            for( Iterator<String> it = toLoad.iterator(); it.hasNext(); ){
                SiteCatalogEntry s = catalog.lookup( it.next() );
                if( s != null ){
                    result.addEntry( s );
                }
            }
        }
        catch ( SiteCatalogException e ){
            throw new RuntimeException( "Unable to load from site catalog " , e );
        }
        finally{
            /* close the connection */
            try{
                catalog.close();
            }catch( Exception e ){}
        }

        return result;
    }

    
    /**
     * Connects the interface with the transformation catalog implementation. The
     * choice of backend is configured through properties. This method uses default
     * properties from the property singleton.
     *
     * @return handle to the Site Catalog.
     *
     * @throws SiteFactoryException that nests any error that
     *         might occur during the instantiation
     *
     * @see #DEFAULT_PACKAGE_NAME
     */
    public static SiteCatalog loadInstance() throws
                                            SiteFactoryException {
        return loadInstance( PegasusProperties.getInstance() );
    }

    /**
     * Connects the interface with the site catalog implementation. Tedu.isi.pegasus.catalog.site.impl.XML3he
     * choice of backend is configured through properties. 
     *
     * @param properties is an instance of properties to use.
     *
     * @return handle to the Site Catalog.
     *
     * @throws SiteFactoryException that nests any error that
     *         might occur during the instantiation
     *
     * @see #DEFAULT_PACKAGE_NAME
     */
    public static SiteCatalog loadInstance( PegasusProperties properties ) throws
        SiteFactoryException {

        if( properties == null ){
            throw new SiteFactoryException( "Invalid NULL properties passed" );
        }

        /* get the implementor from properties */
        String catalogImplementor = properties.getPoolMode().trim();

        /* prepend the package name if required */
        catalogImplementor = (catalogImplementor.indexOf('.') == -1) ?
            //pick up from the default package
            DEFAULT_PACKAGE_NAME + "." + catalogImplementor :
            //load directly
            catalogImplementor;

        Properties connect = properties.matchingSubset( SiteCatalog.c_prefix, false );
        
        // determine the class that implements the site catalog
        return loadInstance( properties.getProperty( SiteCatalog.c_prefix ),
                             connect );
    }

    /**
     * Connects the interface with the site catalog implementation. The
     * choice of backend is configured through properties. 
     *
     * 
     * @param catalogImplementor  the name of the class implementing catalog
     * @param properties             the connection properties.
     *
     * @return handle to the Site Catalog.
     *
     * @throws SiteFactoryException that nests any error that
     *         might occur during the instantiation
     *
     * @see #DEFAULT_PACKAGE_NAME
     */
    public  static SiteCatalog loadInstance( String catalogImplementor, Properties properties ) {
        if( properties == null ){
            throw new SiteFactoryException( "Invalid NULL properties passed" );
        }

        SiteCatalog catalog = null;
        try{
            if ( catalogImplementor == null ){
                throw new RuntimeException( "You need to specify the " +
                                        SiteCatalog.c_prefix + " property" );
            }

            /* prepend the package name if required */
            catalogImplementor = (catalogImplementor.indexOf('.') == -1) ?
                //pick up from the default package
                DEFAULT_PACKAGE_NAME + "." + catalogImplementor :
                //load directly
                catalogImplementor;

            DynamicLoader dl = new DynamicLoader( catalogImplementor );
            catalog = ( SiteCatalog ) dl.instantiate( new Object[0] );

            if ( catalog == null ){
                throw new RuntimeException( "Unable to load " + catalogImplementor );
            }


            if ( ! catalog.connect( properties ) )
                throw new RuntimeException( "Unable to connect to site catalog implementation" );
        }
        catch( Exception e ){
            throw new SiteFactoryException( "Unable to instantiate Site Catalog ",
                                            catalogImplementor,
                                            e );
        }
        
        return catalog;
    }

}
