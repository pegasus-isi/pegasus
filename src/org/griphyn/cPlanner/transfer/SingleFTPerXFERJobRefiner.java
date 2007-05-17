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
package org.griphyn.cPlanner.transfer;

import org.griphyn.cPlanner.classes.ADag;
import org.griphyn.cPlanner.classes.SubInfo;
import org.griphyn.cPlanner.classes.PlannerOptions;

import org.griphyn.cPlanner.common.PegasusProperties;
import org.griphyn.cPlanner.common.LogManager;

import org.griphyn.cPlanner.transfer.implementation.ImplementationFactory;
import org.griphyn.cPlanner.transfer.implementation.TransferImplementationFactoryException;

import java.util.Collection;

import java.io.IOException;

import java.lang.reflect.InvocationTargetException;


/**
 * The refiner interface, that determines the functions that need to be
 * implemented to add various types of transfer nodes to the workflow.
 * The single in the name indicates that the refiner works with the
 * implementation that handles one file transfer per transfer job.
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 *
 * @version $Revision: 1.3 $
 */
public abstract class SingleFTPerXFERJobRefiner extends AbstractRefiner{

    /**
     * The overloaded constructor.
     *
     * @param dag        the workflow to which transfer nodes need to be added.
     * @param properties the <code>PegasusProperties</code> object containing all
     *                   the properties required by Pegasus.
     * @param options    the options passed to the planner.
     */
    public SingleFTPerXFERJobRefiner(ADag dag,
                                     PegasusProperties properties,
                                     PlannerOptions options){
          super(dag,properties,options);
    }



    /**
     * Loads the appropriate implementations that is required by this refinement
     * strategy for different types of transfer jobs. It calls to the factory
     * method to load the appropriate Implementor.
     *
     * Loads the implementing class corresponding to the mode specified by the user
     * at runtime in the properties file. The properties object passed should not
     * be null.
     *
     * @param properties the <code>PegasusProperties</code> object containing all
     *                   the properties required by Pegasus.
     * @param options    the options with which the planner was invoked.
     *
     * @exception TransferImplementationFactoryException that nests any error that
     *            might occur during the instantiation.
     */
    public void loadImplementations(PegasusProperties properties,
                                             PlannerOptions options)
        throws TransferImplementationFactoryException{

        //this can work with any Implementation Factory
        this.mTXStageInImplementation = ImplementationFactory.loadInstance(
                                              properties,options,
                                              ImplementationFactory.TYPE_STAGE_IN);
        this.mTXStageInImplementation.setRefiner(this);
        this.mTXInterImplementation = ImplementationFactory.loadInstance(
                                              properties,options,
                                              ImplementationFactory.TYPE_STAGE_INTER);
        this.mTXInterImplementation.setRefiner(this);
        this.mTXStageOutImplementation = ImplementationFactory.loadInstance(
                                               properties,options,
                                               ImplementationFactory.TYPE_STAGE_OUT);
        this.mTXStageOutImplementation.setRefiner(this);
        //log config messages message
        super.logConfigMessages();

    }
}
