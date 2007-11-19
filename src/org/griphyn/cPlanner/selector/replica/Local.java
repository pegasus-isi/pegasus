/**
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
package org.griphyn.cPlanner.selector.replica;

import org.griphyn.cPlanner.classes.ReplicaLocation;

import org.griphyn.cPlanner.selector.ReplicaSelector;

import org.griphyn.cPlanner.common.LogManager;
import org.griphyn.cPlanner.common.PegasusProperties;
import org.griphyn.cPlanner.common.PegRandom;

import org.griphyn.common.catalog.ReplicaCatalogEntry;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Vector;

/**
 * This replica selector only prefers replicas from the local host and that
 * start with a file: URL scheme. It is useful, when you want to stagin
 * files to a remote site from your submit host using the Condor file transfer
 * mechanism.
 *
 * <p>
 * In order to use the replica selector implemented by this class,
 * <pre>
 *        - the property pegasus.selector.replica must be set to value Local
 * </pre>
 *
 *
 * @see org.griphyn.cPlanner.transfer.implementation.Condor
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class Local implements ReplicaSelector {

    /**
     * A short description of the replica selector.
     */
    private static String mDescription = "Local from submit host";

    /**
     * The scheme name for file url.
     */
    protected static final String FILE_URL_SCHEME = "file:";


    /**
     * The handle to the logging object that is used to log the various debug
     * messages.
     */
    protected LogManager mLogger;

    /**
     * The properties object containing the properties passed to the planner.
     */
    protected PegasusProperties mProps;

    /**
     * The overloaded constructor, that is called by load method.
     *
     * @param properties the <code>PegasusProperties</code> object containing all
     *                   the properties required by Pegasus.
     *
     *
     */
    public Local( PegasusProperties properties ){
        mProps       = properties;
        mLogger      = LogManager.getInstance();
    }


    /**
     * Selects a random replica from all the replica's that have their
     * site handle set to local and the pfn's start with a file url scheme.
     *
     * @param rl         the <code>ReplicaLocation</code> object containing all
     *                   the pfn's associated with that LFN.
     * @param preferredSite the preffered site for picking up the replicas.
     *
     * @return <code>ReplicaCatalogEntry</code> corresponding to the location selected.
     *
     * @see org.griphyn.cPlanner.classes.ReplicaLocation
     */
    public ReplicaCatalogEntry selectReplica( ReplicaLocation rl,
                                              String preferredSite ){

        ReplicaCatalogEntry rce;
        ArrayList prefPFNs = new ArrayList();
        int locSelected;
        String site = null;

//        mLogger.log("Selecting a pfn for lfn " + lfn + "\n amongst" + locations ,
//                    LogManager.DEBUG_MESSAGE_LEVEL);

        for ( Iterator it = rl.pfnIterator(); it.hasNext(); ) {
            rce = ( ReplicaCatalogEntry ) it.next();
            site = rce.getResourceHandle();

            if( site == null ){
                //skip to next replica
                continue;
            }

            //check if has pool attribute as local, and at same time
            //start with a file url scheme
            if( site.equals( "local" ) && rce.getPFN().startsWith( FILE_URL_SCHEME ) ){
                prefPFNs.add( rce );
            }
        }

        if ( prefPFNs.isEmpty() ) {
            //select a random location from
            //all the matching locations
            //in all likelihood all the urls were file urls and none
            //were associated with the preference pool.
            throw new RuntimeException( "Unable to select any location on local site from " +
                                        "the list passed for lfn "  + rl.getLFN() );

        } else {
            //select a random location
            //amongst the locations
            //on the preference pool
            int length = prefPFNs.size();
            //System.out.println("No of locations found at pool " + prefPool + " are " + length);
            locSelected = PegRandom.getInteger( length - 1 );
            rce = ( ReplicaCatalogEntry ) prefPFNs.get( locSelected );

        }

        return rce;

    }

    /**
     * This chooses a location amongst all the locations returned by the
     * Replica Mechanism. If a location is found with re/pool attribute same
     * as the preference pool, it is taken. This returns all the locations which
     * match to the preference pool. This function is called to determine if a
     * file does exist on the output pool or not beforehand. We need all the
     * location to ensure that we are able to make a match if it so exists.
     * Else a random location is selected and returned
     *
     * @param rl         the <code>ReplicaLocation</code> object containing all
     *                   the pfn's associated with that LFN.
     * @param preferredSite the preffered site for picking up the replicas.
     *
     * @return <code>ReplicaLocation</code> corresponding to the replicas selected.
     *
     * @see org.griphyn.cPlanner.classes.ReplicaLocation
     */
    public ReplicaLocation selectReplicas( ReplicaLocation rl,
                                           String preferredSite ){

        String lfn = rl.getLFN();
        ReplicaLocation result = new ReplicaLocation();
        result.setLFN( rl.getLFN() );

        ReplicaCatalogEntry rce;
        String site;
        int noOfLocs = 0;

        for ( Iterator it = rl.pfnIterator(); it.hasNext(); ) {
            noOfLocs++;
            rce = ( ReplicaCatalogEntry ) it.next();
            site = rce.getResourceHandle();

            if ( site != null && site.equals( preferredSite )) {
                result.addPFN( rce );
            }
            else if ( site == null ){
                mLogger.log(
                    " pool attribute not specified for the location objects" +
                    " in the Replica Catalog", LogManager.WARNING_MESSAGE_LEVEL);
            }
        }

        if ( result.getPFNCount() == 0 ) {
            //means we have to choose a random location between 0 and (noOfLocs -1)
            int locSelected = PegRandom.getInteger( noOfLocs - 1 );
            rce = ( ReplicaCatalogEntry ) rl.getPFN(locSelected );
            result.addPFN( rce );
        }
        return result;

    }

    /**
     * Returns a short description of the replica selector.
     *
     * @return string corresponding to the description.
     */
    public String description(){
        return mDescription;
    }


}
