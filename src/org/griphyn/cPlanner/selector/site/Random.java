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

import org.griphyn.cPlanner.common.LogManager;
import org.griphyn.cPlanner.common.PegasusProperties;

import org.griphyn.cPlanner.poolinfo.PoolInfoProvider;
import org.griphyn.cPlanner.poolinfo.PoolMode;

import org.griphyn.cPlanner.selector.SiteSelector;


import java.util.List;

/**
 * A random site selector that maps to a job to a random pool, amongst the subset
 * of pools where that particular job can be executed.
 *
 * @author Karan Vahi
 * @version $Revision: 1.4 $
 */

public class Random
    extends SiteSelector {

    /**
     * The handle to the pool configuration file.
     */
    private PoolInfoProvider mPoolHandle;

    /**
     * The handle to the transformation catalog.
     */
    //private TransformationCatalog mTCHandle;

    /**
     * The handle to the properties file.
     */
    private PegasusProperties mProps;

    /**
     * The handle to the logger.
     */
    private LogManager mLogger;

    /**
     * The default constructor. Should not be called. Call the overloaded one.
     */
    public Random() {
    }

    /**
     * The overloaded constructor.
     *
     * @param path  the path to the site selector. In this case it is null.
     */
    public Random(String path) {
        super(path);
        mProps = PegasusProperties.nonSingletonInstance();
        mLogger = LogManager.getInstance();
        String tcmode = mProps.getTCMode();
        //mTCHandle = TCMode.loadInstance();

        String poolClass = PoolMode.getImplementingClass(mProps.getPoolMode());
        mPoolHandle = PoolMode.loadPoolInstance(poolClass, mProps.getPoolFile(),
                                                PoolMode.SINGLETON_LOAD);
    }

    /**
     * The main method that maps a particular job to an execution pool.
     *
     * @param job SubInfo   the <code>SubInfo</code> object  corresponding to the
     *                  job whose execution pool we want to determine.
     *
     * @param pools     the list of <code>String</code> objects representing the
     *                  execution pools that can be used.
     *
     * @return if the pool is found to which the job can be mapped, a string of the
     *         form <code>executionpool:jobmanager</code> where the jobmanager can
     *          be null. If the pool is not found, then set poolhandle to NONE.
     *          null - if some error occured .
     */
    public String mapJob2ExecPool(SubInfo job, List pools) {
        List sites = null;

        sites = mTCMapper.getSiteList(job.namespace,job.logicalName,job.version,
                                      pools);
        String mapping = null;

        //for each of the record ,
        //check if the entry is for one of pools passed
        if (sites != null) {


            if (sites.isEmpty()) {
                mapping = SiteSelector.POOL_NOT_FOUND + ":null";
                return mapping;
            }

            String selectedSite=null;
            selectedSite = selectRandomSite(sites);
            mapping = selectedSite + ":";
            //assume no jobmanager selected
            mapping += null;
        } else {
            //means no pool has been found to which the job could be mapped to.
            mapping = SiteSelector.POOL_NOT_FOUND + ":null";
        }
        mLogger.log("[Random Selector] Mapped to " + mapping,
                    LogManager.DEBUG_MESSAGE_LEVEL);
        return mapping;
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
