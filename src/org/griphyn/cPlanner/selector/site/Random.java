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
package org.griphyn.cPlanner.selector.site;

import org.griphyn.cPlanner.classes.SubInfo;

import org.griphyn.cPlanner.classes.PegasusBag;

import org.griphyn.cPlanner.common.LogManager;


import java.util.List;

/**
 * A random site selector that maps to a job to a random pool, amongst the subset
 * of pools where that particular job can be executed.
 *
 * @author Karan Vahi
 * @version $Revision$
 */

public class Random extends AbstractPerJob {



    /**
     * The default constructor. Should not be called. Call the overloaded one.
     */
    public Random() {
    }


    /**
     * Initializes the site selector.
     *
     * @param bag   the bag of objects that is useful for initialization.
     *
     */
    public void initialize( PegasusBag bag ){
        super.initialize( bag );
    }

    /**
     * Maps a job in the workflow to an execution site.
     *
     * @param job    the job to be mapped.
     * @param sites  the list of <code>String</code> objects representing the
     *               execution sites that can be used.
     *
     */
    public void mapJob( SubInfo job, List sites ){

        List rsites = mTCMapper.getSiteList( job.getTXNamespace(),job.getTXName(),
                                             job.getTXVersion(), sites );


        if( rsites == null || rsites.isEmpty() ){
            job.setSiteHandle( null );
        }
        else{
            job.setSiteHandle(selectRandomSite(rsites));
            mLogger.log( "[Random Selector] Mapped to " + job.getSiteHandle(),
                         LogManager.DEBUG_MESSAGE_LEVEL );
        }
    }

    /**
     * Returns a brief description of the site selection technique being used.
     *
     * @return String
     */
    public String description() {
        String st = "Random Site Selection";
        return st;
    }

    /**
     * The random selection that selects randomly one of the records returned by
     * the transformation catalog.
     *
     * @param  sites  List of <code>String</code>objects.
     *
     * @return String
     */
    private String selectRandomSite(List sites) {
        double randNo;
        int noOfRecs = sites.size();

        //means we have to choose a random location between 0 and (noOfLocs -1)
        randNo = Math.random() * noOfRecs;
        int recSelected = new Double(randNo).intValue();
        /*
        String message = "Random Site selected is " + (recSelected + 1) +
            " amongst " + noOfRecs + " possible";
        mLogger.logMessage(message, 1, false);
        */
        return (String)sites.get(recSelected);
    }
}
