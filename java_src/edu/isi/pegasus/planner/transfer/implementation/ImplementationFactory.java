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
package edu.isi.pegasus.planner.transfer.implementation;

import edu.isi.pegasus.common.util.DynamicLoader;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.transfer.Implementation;

/**
 * The factory class that loads an appropriate Transfer Immplementation class, as specified by the
 * properties.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class ImplementationFactory {

    /** The default package where the implementations reside, which this factory loads. */
    public static final String DEFAULT_PACKAGE_NAME =
            "edu.isi.pegasus.planner.transfer.implementation";

    public static final String DEFAULT_TRANSFER_IMPLEMENTATION = "Transfer";

    public static final String DEFAULT_SETUP_TRANSFER_IMPLEMENTATION = "Transfer";

    /** The constant designating the implementation be loaded for stage in jobs. */
    public static final int TYPE_STAGE_IN = 0;

    /** The constant designating the implementation be loaded for inter pool jobs. */
    public static final int TYPE_STAGE_INTER = 1;

    /** The constant designating the implementation be loaded for stage out jobs. */
    public static final int TYPE_STAGE_OUT = 2;

    /** The constant designating the implementation to be loaded for setup tx jobs. */
    public static final int TYPE_SETUP = 3;

    /** The constant designating the implementation to be loaded for symbolic tx jobs. */
    public static final int TYPE_SYMLINK_STAGE_IN = 4;

    /**
     * Loads the implementing class corresponding to the type specified by the user. The type is
     * used to determine what property to be picked up from the properties file. The properties
     * object passed should not be null.
     *
     * @param bag the bag of initialization objects.
     * @param type the type.
     * @return the instance of the class implementing this interface.
     * @exception TransferImplementationFactoryException that nests any error that might occur
     *     during the instantiation.
     * @see #DEFAULT_PACKAGE_NAME
     */
    public static Implementation loadInstance(PegasusBag bag, int type)
            throws TransferImplementationFactoryException {

        String key = getPropertyKey(type);
        String implementingClass = bag.getPegasusProperties().getTransferImplementation(key);

        if (implementingClass == null) {
            // User did not define anything in the properties file.
            implementingClass =
                    (type == ImplementationFactory.TYPE_SETUP)
                            ? DEFAULT_SETUP_TRANSFER_IMPLEMENTATION
                            : DEFAULT_TRANSFER_IMPLEMENTATION;
        }

        return loadInstance(implementingClass, bag);
    }

    /**
     * Loads the implementing class corresponding to the mode specified by the user at runtime in
     * the properties file. The properties object passed should not be null.
     *
     * @param bag bag of initialization objects.
     * @return the instance of the class implementing this interface.
     * @exception TransferImplementationFactoryException that nests any error that might occur
     *     during the instantiation.
     * @see #DEFAULT_PACKAGE_NAME
     */
    /*public static  Implementation loadInstance( PegasusBag bag )
        throws TransferImplementationFactoryException{

        return loadInstance( bag.getPegasusProperties().getTransferImplementation(),
                             bag );
    }*/

    /**
     * Loads the implementing class corresponding to the class. If the package name is not specified
     * with the class, then class is assumed to be in the DEFAULT_PACKAGE. The properties object
     * passed should not be null.
     *
     * @param className the name of the class that implements the mode.It can or cannot be with the
     *     package name. Can be null to get the factory to load the default implementation
     * @param bag the bag of initialization objects.
     * @return the instance of the class implementing this interface.
     * @exception TransferImplementationFactoryException that nests any error that might occur
     *     during the instantiation.
     * @see #DEFAULT_PACKAGE_NAME
     */
    private static Implementation loadInstance(String className, PegasusBag bag)
            throws TransferImplementationFactoryException {

        Implementation implementation = null;
        try {
            // sanity check
            if (bag.getPegasusProperties() == null) {
                throw new RuntimeException("Invalid properties passed");
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

            // try loading the class dynamically
            DynamicLoader dl = new DynamicLoader(className);
            Object argList[] = new Object[1];
            argList[0] = bag;
            implementation = (Implementation) dl.instantiate(argList);
        } catch (Exception e) {
            throw new TransferImplementationFactoryException(
                    "Instantiating Transfer Impelmentation ", className, e);
        }
        return implementation;
    }

    /**
     * Returns the name of the property that needs to be loaded for a particular type.
     *
     * @param type the type of implementation to be loaded.
     * @return the name of the property
     * @throws IllegalArgumentException
     */
    private static String getPropertyKey(int type) throws IllegalArgumentException {
        String property;
        if (type == TYPE_STAGE_IN) {
            property = "pegasus.transfer.stagein.impl";
        } else if (type == TYPE_STAGE_INTER) {
            property = "pegasus.transfer.inter.impl";
        } else if (type == TYPE_STAGE_OUT) {
            property = "pegasus.transfer.stageout.impl";
        } else if (type == TYPE_SETUP) {
            property = "pegasus.transfer.setup.impl";
        } else if (type == TYPE_SYMLINK_STAGE_IN) {
            property = "pegasus.transfer.symlink.impl";
        } else {
            throw new java.lang.IllegalArgumentException(
                    "Invalid implementation type passed to factory " + type);
        }
        return property;
    }
}
