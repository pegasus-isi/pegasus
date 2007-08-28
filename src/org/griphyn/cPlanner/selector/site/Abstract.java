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


import org.griphyn.cPlanner.classes.ADag;
import org.griphyn.cPlanner.classes.PegasusBag;

import org.griphyn.cPlanner.common.LogManager;

import org.griphyn.cPlanner.common.PegasusProperties;

import org.griphyn.cPlanner.partitioner.graph.Adapter;

import org.griphyn.cPlanner.selector.SiteSelector;


import org.griphyn.cPlanner.poolinfo.PoolInfoProvider;

import org.griphyn.common.catalog.transformation.Mapper;

import java.util.List;


/**
 * The Abstract Site selector.
 *
 * @author Karan Vahi
 * @author Jens-S. Vöckler
 * @author Gaurang Mehta
 *
 *
 * @version $Revision$
 */
public abstract class Abstract implements SiteSelector {

    /**
     * The properties passed to Pegasus at runtime.
     */
    protected PegasusProperties mProps;

    /**
     * The handle to the logger.
     */
    protected LogManager mLogger;

    /**
     * The handle to the site catalog.
     */
    protected PoolInfoProvider mSCHandle;

    /**
     * The handle to the TCMapper object.
     */
    protected Mapper mTCMapper;

    /**
     * The bag of Pegasus objects.
     */
    protected PegasusBag mBag;

    /**
     * Initializes the site selector.
     *
     * @param bag   the bag of objects that is useful for initialization.
     *
     */
    public void initialize( PegasusBag bag ){
        mBag   =  bag;
        mProps =  ( PegasusProperties )bag.get( PegasusBag.PEGASUS_PROPERTIES );
        mLogger   = ( LogManager )bag.get( PegasusBag.PEGASUS_LOGMANAGER );
        mSCHandle = ( PoolInfoProvider )bag.get( PegasusBag.SITE_CATALOG );
        mTCMapper = ( Mapper )bag.get( PegasusBag.TRANSFORMATION_MAPPER );
    }



    /**
     * Maps the jobs in the workflow to the various grid sites.
     * The jobs are mapped by setting the site handle for the jobs.
     *
     * @param workflow   the workflow.
     *
     * @param sites     the list of <code>String</code> objects representing the
     *                  execution sites that can be used.
     */
    public void mapWorkflow( ADag workflow, List sites ){
         mapWorkflow( Adapter.convert( workflow ), sites );
    }

}
