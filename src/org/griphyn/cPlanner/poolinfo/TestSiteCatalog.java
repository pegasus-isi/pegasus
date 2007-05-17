/*
 * This file or a portion of this file is licensed under the terms of
 * the Globus Toolkit Public License, found in file GTPL, or at
 * http://www.globus.org/toolkit/download/license.html. This notice must
 * appear in redistributions of this file, with or without modification.
 *
 * Redistributions of this Software, with or without modification, must
 * reproduce the GTPL in: (1) the Software, or (2) the Documentation or
 * some other similar material which is provided with the Software (if
 * any).
 *
 * Copyright 1999-2004 University of Chicago and The University of
 * Southern California. All rights reserved.
 */
package org.griphyn.cPlanner.poolinfo;

import org.griphyn.cPlanner.classes.SiteInfo;

import org.griphyn.cPlanner.common.PegasusProperties;
import org.griphyn.cPlanner.common.LogManager;

import org.griphyn.cPlanner.namespace.Condor;

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
        LogManager logger = LogManager.getInstance();

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