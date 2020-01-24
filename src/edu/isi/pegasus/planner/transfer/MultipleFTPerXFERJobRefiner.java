/**
 * Copyright 2007-2008 University Of Southern California
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.isi.pegasus.planner.transfer;

import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.transfer.implementation.ImplementationFactory;
import edu.isi.pegasus.planner.transfer.implementation.TransferImplementationFactoryException;

/**
 * The refiner interface, that determines the functions that need to be implemented to add various
 * types of transfer nodes to the workflow. The multiple in the name indicates that the refiner
 * works only with the implementation that handles multiple file transfer per transfer job.
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 * @version $Revision$
 */
public abstract class MultipleFTPerXFERJobRefiner extends AbstractRefiner {

    /**
     * The overloaded constructor.
     *
     * @param dag the workflow to which transfer nodes need to be added.
     * @param bag the bag of initialization objects.
     */
    public MultipleFTPerXFERJobRefiner(ADag dag, PegasusBag bag) {
        super(dag, bag);
    }

    /**
     * Loads the appropriate implementations that is required by this refinement strategy for
     * different types of transfer jobs. It calls to the factory method to load the appropriate
     * Implementor.
     *
     * <p>Loads the implementing class corresponding to the mode specified by the user at runtime in
     * the properties file. The properties object passed should not be null.
     *
     * @param bag the bag of initialization objects.
     * @exception TransferImplementationFactoryException that nests any error that might occur
     *     during the instantiation.
     * @exception ClassCastException in case the incompatible implementation is loaded
     */
    public void loadImplementations(PegasusBag bag) throws TransferImplementationFactoryException {

        // load
        this.mTXStageInImplementation =
                ImplementationFactory.loadInstance(bag, ImplementationFactory.TYPE_STAGE_IN);
        this.mTXStageInImplementation.setRefiner(this);
        checkCompatibility(this.mTXStageInImplementation);

        this.mTXInterImplementation =
                ImplementationFactory.loadInstance(bag, ImplementationFactory.TYPE_STAGE_INTER);
        this.mTXInterImplementation.setRefiner(this);
        checkCompatibility(this.mTXInterImplementation);

        this.mTXStageOutImplementation =
                ImplementationFactory.loadInstance(bag, ImplementationFactory.TYPE_STAGE_OUT);
        this.mTXStageOutImplementation.setRefiner(this);
        checkCompatibility(this.mTXStageOutImplementation);

        this.mTXSymbolicLinkImplementation =
                ImplementationFactory.loadInstance(
                        bag, ImplementationFactory.TYPE_SYMLINK_STAGE_IN);
        this.mTXSymbolicLinkImplementation.setRefiner(this);
        checkCompatibility(this.mTXSymbolicLinkImplementation);

        // log config messages message
        super.logConfigMessages();
    }

    /**
     * Checks whether the implementation loaded is compatible with the refiner. If not throws a
     * ClassCastException.
     *
     * @param implementation the implementation whose compatibility needs to be checked.
     * @exception ClassCastException in case the implementation is incompatible.
     */
    private void checkCompatibility(Implementation implementation) throws ClassCastException {
        // check if refiner loaded is of special type
        boolean condition1 =
                !this.getClass()
                        .getName()
                        .equalsIgnoreCase("org.griphyn.cPlanner.transfer.refiner.Chain");

        // check if implementation loaded is of right type
        if (condition1 && !(implementation instanceof MultipleFTPerXFERJob)) {
            throw new ClassCastException(
                    "Wrong type of transfer implementation loaded "
                            + implementation.getDescription()
                            + " for refiner "
                            + this.getDescription());
        }
    }
}
