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
 * The multiple in the name indicates that the refiner works only with the
 * implementation that handles multiple file transfer per transfer job.
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 *
 * @version $Revision$
 */
public abstract class MultipleFTPerXFERJobRefiner extends AbstractRefiner {


    /**
     * The overloaded constructor.
     *
     * @param dag        the workflow to which transfer nodes need to be added.
     * @param properties the <code>PegasusProperties</code> object containing all
     *                   the properties required by Pegasus.
     * @param options    the options passed to the planner.
     */
    public MultipleFTPerXFERJobRefiner(ADag dag,
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
     * @exception ClassCastException in case the incompatible implementation is
     *            loaded
     */
    public void loadImplementations(PegasusProperties properties,
                                    PlannerOptions options)
        throws TransferImplementationFactoryException{

        //load
        this.mTXStageInImplementation = ImplementationFactory.loadInstance(
                                              properties,options,
                                              ImplementationFactory.TYPE_STAGE_IN);
        this.mTXStageInImplementation.setRefiner(this);
        checkCompatibility(this.mTXStageInImplementation);

        this.mTXInterImplementation = ImplementationFactory.loadInstance(
                                              properties,options,
                                              ImplementationFactory.TYPE_STAGE_INTER);
        this.mTXInterImplementation.setRefiner(this);
        checkCompatibility(this.mTXInterImplementation);

        this.mTXStageOutImplementation = ImplementationFactory.loadInstance(
                                               properties,options,
                                               ImplementationFactory.TYPE_STAGE_OUT);
        this.mTXStageOutImplementation.setRefiner(this);
        checkCompatibility(this.mTXStageOutImplementation);

        //log config messages message
        super.logConfigMessages();
    }

    /**
     * Checks whether the implementation loaded is compatible with the refiner.
     * If not throws a ClassCastException.
     *
     * @param implementation  the implementation whose compatibility needs to
     *                        be checked.
     *
     * @exception ClassCastException in case the implementation is incompatible.
     */
    private void checkCompatibility(Implementation implementation)
        throws ClassCastException{
        //check if refiner loaded is of special type
        boolean condition1 = !this.getClass().getName().
            equalsIgnoreCase("org.griphyn.cPlanner.transfer.refiner.Chain");

        //check if implementation loaded is of right type
        if(condition1 &&  !(implementation instanceof MultipleFTPerXFERJob)){
            throw new ClassCastException("Wrong type of transfer implementation loaded " +
                                         implementation.getDescription() + " for refiner " +
                                         this.getDescription());
        }

    }
}
