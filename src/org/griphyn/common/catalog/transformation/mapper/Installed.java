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

package org.griphyn.common.catalog.transformation.mapper;

import org.griphyn.common.catalog.TransformationCatalogEntry;

import org.griphyn.common.catalog.transformation.Mapper;

import org.griphyn.common.classes.SysInfo;
import org.griphyn.common.classes.TCType;

import org.griphyn.cPlanner.common.LogManager;

import org.griphyn.common.util.Separator;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 *This class only generates maps for sites with installed transformations.
 *
 *@author Gaurang Mehta
 *@version $Revision: 1.6 $
 */
public class Installed
    extends Mapper {

    /**
     * This method returns a Map of compute sites to List of
     * TransformationCatalogEntry objects that are valid for that site.
     *
     * @param namespace String The namespace of the logical transformation
     * @param name String The name of the logical transformation
     * @param version String The version of the logical transformation
     * @param siteids List The sites for which you want the map.
     *
     * @return Map Key=String SiteId , Values = List of TransformationCatalogEntry object.
     *         null if no entries are found.
     */
    public Map getSiteMap( String namespace, String name, String version,
        List siteids ) {
        //stores the entries got from the TC
        List tcentries = null;
        //stores the string arrays mapping a site to an entry.
        Map sitemap = null;
        //stores the system information obtained from RIC
        Map sysinfomap = null;

        //the fully qualified lfn
        String lfn = Separator.combine( namespace, name, version );

        //check if the sitemap already exists in the TCMap
        if ( ( sitemap = mTCMap.getSiteMap( lfn ) ) != null ) {
            boolean hassite = true;
            for ( Iterator i = siteids.iterator(); i.hasNext(); ) {
                //check if the site exists in the sitemap if not then generate sitemap again
                // Need to check if this can be avoided by making sure Karan always sends me a list of sites rather then individual sites.

                if ( !sitemap.containsKey( ( String ) i.next() ) ) {
                    hassite = false;
                }
            }
            if ( hassite ) {
                // CANNOT RETURN THIS. YOU NEED ONLY RETURN THE RELEVANT
                // ENTRIES MATCHING THE SITES . KARAN SEPT 21, 2005
                //return sitemap;
                return mTCMap.getSitesTCEntries(lfn,siteids);
            }
        }

        //since sitemap does not exist we need to generate and populate it.
        //get the TransformationCatalog entries from the TC.
        try {
            tcentries = mTCHandle.getTCEntries( namespace, name, version,
                siteids,
                TCType.INSTALLED );
        } catch ( Exception e ) {
            mLogger.log(
                "Unable to get physical names from TC in the TC Mapper\n",
                e ,LogManager.ERROR_MESSAGE_LEVEL);
        }
        //get the system info for the sites from the RIC
        if ( tcentries != null ) {
            sysinfomap = mPoolHandle.getSysinfos( siteids );
        } else {
            throw new RuntimeException(
                "There are no entries for the transformation \"" + lfn +
                "\" of type \"" + TCType._INSTALLED + "\" the TC" );
        }
        if ( sysinfomap != null ) {
            for ( Iterator i = siteids.iterator(); i.hasNext(); ) {
                String site = ( String ) i.next();
                SysInfo sitesysinfo = ( SysInfo ) sysinfomap.get( site );
                for ( Iterator j = tcentries.iterator(); j.hasNext(); ) {
                    TransformationCatalogEntry entry = (
                        TransformationCatalogEntry ) j.next();
                    //get the required stuff from the TCentry.
                    String txsiteid = entry.getResourceId();
                    TCType txtype = entry.getType();
                    SysInfo txsysinfo = entry.getSysInfo();

                    //check for installed executables at each site.
                    if ( txsysinfo.equals( sitesysinfo ) &&
                        txsiteid.equalsIgnoreCase( site ) ) {
                        //add the installed executables in the map.
                        mTCMap.setSiteTCEntries( lfn, site, entry );
                    }
                } //outside inner for loop
            } //outside outer for loop
        } else {
            throw new RuntimeException(
                "There are no entries for the sites \n" + siteids.toString() +
                "\n" );
        }

        // CANNOT RETURN THIS. YOU NEED ONLY RETURN THE RELEVANT
        // ENTRIES MATCHING THE SITES . KARAN SEPT 21, 2005
        //    return mTCMap.getSiteMap( lfn );
        return mTCMap.getSitesTCEntries(lfn,siteids);
    }

    public String getMode() {
        return "Installed Mode : Only use Installed executables at the site";
    }
}
