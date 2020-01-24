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
package edu.isi.pegasus.planner.transfer.refiner;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.util.DynamicLoader;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.transfer.Refiner;

/**
 * The factory class that loads an appropriate Transfer Refiner class, as specified by the
 * properties.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class RefinerFactory {

    /** The default package where the implementations reside, which this factory loads. */
    public static final String DEFAULT_PACKAGE_NAME = "edu.isi.pegasus.planner.transfer.refiner";

    /** The default refiner implementation that is picked up. */
    public static final String DEFAULT_REFINER_IMPLEMENTATION = "BalancedCluster";

    /**
     * Loads the implementing class corresponding to the value specified in the properties. If the
     * package name is not specified with the class, then class is assumed to be in the
     * DEFAULT_PACKAGE. The properties object passed should not be null.
     *
     * <p>In addition it ends up loading the appropriate Transfer Implementation that is required by
     * the refiner.
     *
     * @param dag the workflow that is being refined.
     * @param bag the bag of initialization objects
     * @return the instance of the class implementing this interface.
     * @exception TransferRefinerException that nests any error that might occur during the
     *     instantiation.
     * @see #DEFAULT_PACKAGE_NAME
     */
    public static Refiner loadInstance(ADag dag, PegasusBag bag)
            throws TransferRefinerFactoryException {

        return loadInstance(bag.getPegasusProperties().getTransferRefiner(), bag, dag);
    }

    /**
     * Loads the implementing class corresponding to the class. If the package name is not specified
     * with the class, then class is assumed to be in the DEFAULT_PACKAGE. The properties object
     * passed should not be null. In addition it ends up loading the appropriate Transfer
     * Implementation that is required by the refiner.
     *
     * @param className the name of the class that implements the mode.It can or cannot be with the
     *     package name.
     * @param bag the bag of initialization objects
     * @param dag the workflow that is being refined.
     * @return the instance of the class implementing this interface.
     * @exception TransferRefinerFactoryException that nests any error that might occur during the
     *     instantiation.
     * @see #DEFAULT_PACKAGE_NAME
     */
    public static Refiner loadInstance(String className, PegasusBag bag, ADag dag)
            throws TransferRefinerFactoryException {

        Refiner refiner = null;
        try {
            // sanity check
            if (bag.getPegasusProperties() == null) {
                throw new RuntimeException("Invalid properties passed");
            }
            if (dag == null) {
                throw new RuntimeException("Invalid workflow passed");
            }

            // set the refiner to default if required
            if (className == null) {
                className = RefinerFactory.DEFAULT_REFINER_IMPLEMENTATION;
            }

            // prepend the package name
            className =
                    (className.indexOf('.') == -1)
                            ?
                            // pick up from the default package
                            DEFAULT_PACKAGE_NAME + "." + className
                            :
                            // load directly
                            className;

            // log a warning for the old Bundle Refiner
            if (className.equals(Bundle.class.getName())) {
                bag.getLogger()
                        .log(
                                "Bundle Transfer Refiner is deprecated. Instead use "
                                        + RefinerFactory.DEFAULT_REFINER_IMPLEMENTATION,
                                LogManager.WARNING_MESSAGE_LEVEL);
            }

            // try loading the class dynamically
            DynamicLoader dl = new DynamicLoader(className);
            Object argList[] = new Object[2];
            argList[0] = dag;
            argList[1] = bag;
            refiner = (Refiner) dl.instantiate(argList);

            // we got the refiner try to load the appropriate
            // transfer implementation also
            refiner.loadImplementations(bag);
        } catch (Exception e) {
            throw new TransferRefinerFactoryException(
                    "Instantiating Transfer Refiner", className, e);
        }

        return refiner;
    }
}
