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

package edu.isi.pegasus.planner.ranking;

import org.griphyn.cPlanner.classes.PegasusBag;

import org.griphyn.cPlanner.selector.site.heft.Algorithm;

import org.griphyn.cPlanner.classes.ADag;

import org.griphyn.cPlanner.common.PegasusProperties;
import org.griphyn.cPlanner.common.LogManager;

import org.griphyn.cPlanner.parser.DaxParser;

import org.griphyn.cPlanner.parser.dax.DAXCallbackFactory;
import org.griphyn.cPlanner.parser.dax.Callback;

import java.util.Collection;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.List;

/**
 * The Rank class that ranks the DAX'es
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class Rank {

    /**
     * The handle to the ranking algorithm.
     */
    private Algorithm mHeft;

    /**
     * The pegasus bag.
     */
    private PegasusBag mBag;

    /**
     * The list of candidate grid sites.
     */
    private List mSites;

    /**
     * The handle to the logging object.
     */
    private LogManager mLogger;

    /**
     * The default constructor.
     */
    public Rank() {
        mLogger = LogManager.getInstance();
    }

    /**
     * Initializes the rank client.
     *
     * @param bag   the PegasusBag.
     * @param sites the sites where the wf can run potentially.
     */
    public void initialize( PegasusBag bag , List sites ){
        mBag = bag;
        mLogger = bag.getLogger();
        mHeft = new Algorithm( bag );
        mSites = sites;
    }


    /**
     * Ranks the daxes, and returns a sort collection of Ranking objects.
     *
     * @param daxes Collection
     *
     * @return a sorted collection according to the ranks.
     */
    public Collection<Ranking> rank( Collection<String> daxes ){

        Collection result = new TreeSet();

        //traverse through the DAX'es
        for( Iterator it = daxes.iterator(); it.hasNext(); ){
            String dax = ( String ) it.next();
            Callback cb = DAXCallbackFactory.loadInstance( mBag.getPegasusProperties(),
                                                           dax,
                                                           "DAX2CDAG" );

            mLogger.log( "Ranking dax " + dax, LogManager.DEBUG_MESSAGE_LEVEL );
            DaxParser daxParser = new DaxParser( dax,
                                                 mBag.getPegasusProperties(),
                                                 cb );

            ADag dag = (ADag)cb.getConstructedObject();
            mHeft.schedule( dag, mSites );
            result.add( new Ranking( dax, mHeft.getMakespan()) );
        }

        return result;
    }

}
