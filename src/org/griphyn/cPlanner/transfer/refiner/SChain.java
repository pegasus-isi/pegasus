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

package org.griphyn.cPlanner.transfer.refiner;

import org.griphyn.cPlanner.classes.ADag;
import org.griphyn.cPlanner.classes.PlannerOptions;

import org.griphyn.cPlanner.common.PegasusProperties;
import org.griphyn.cPlanner.common.LogManager;

import org.griphyn.common.util.FactoryException;

/**
 * This transfer refiner incorporates chaining for the impelementations that
 * can transfer only one file per transfer job, by delegating it to the Chain
 * refiner implementation.
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 *
 * @version $Revision: 1.3 $
 */
public class SChain extends SDefault {

    /**
     * The handle to the chain refiner.
     */
    protected Chain mChainRefiner;


    /**
     * A short description of the transfer refinement.
     */
    public static final String DESCRIPTION =
        "SChain Mode (the stage in jobs being chained together in bundles";

    /**
     * The overloaded constructor.
     *
     * @param dag        the workflow to which transfer nodes need to be added.
     * @param properties the <code>PegasusProperties</code> object containing all
     *                   the properties required by Pegasus.
     * @param options    the options passed to the planner.
     *
     */
    public SChain(ADag dag,PegasusProperties properties,PlannerOptions options){
        super(dag, properties,options);
        mChainRefiner = new Chain(dag,properties,options);
	try{
            //hmm we are bypassing the factory check!
            mChainRefiner.loadImplementations(properties, options);
        }
        catch(Exception e){
            throw new FactoryException( "While loading in SChain " , e );

        }
    }

    /**
     * Adds a new relation to the workflow. In the case when the parent is a
     * transfer job that is added, the parentNew should be set only the first
     * time a relation is added. For subsequent compute jobs that maybe
     * dependant on this, it needs to be set to false.
     *
     * @param parent    the jobname of the parent node of the edge.
     * @param child     the jobname of the child node of the edge.
     * @param site      the execution site where the transfer node is to be run.
     * @param parentNew the parent node being added, is the new transfer job
     *                  and is being called for the first time.
     */
    public void addRelation(String parent,
                            String child,
                            String site,
                            boolean parentNew){
        //delegate to the Chain refiner
        mChainRefiner.addRelation(parent,child,site,parentNew);
    }


    /**
     * Prints out the bundles and chains that have been constructed.
     */
    public void done(){
        //delegate to the Chain refiner
        mChainRefiner.done();
    }

    /**
     * Returns a textual description of the transfer mode.
     *
     * @return a short textual description
     */
    public  String getDescription(){
        return this.DESCRIPTION;
    }

}
