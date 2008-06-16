/**
 *  Copyright 2007-2008 University Of Southern California
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.griphyn.cPlanner.transfer.refiner;

import org.griphyn.cPlanner.classes.ADag;
import org.griphyn.cPlanner.classes.PlannerOptions;

import org.griphyn.cPlanner.common.PegasusProperties;

import org.griphyn.cPlanner.transfer.Implementation;
import org.griphyn.cPlanner.transfer.Refiner;

import org.griphyn.common.util.DynamicLoader;

import java.io.IOException;

import java.lang.reflect.InvocationTargetException;


/**
 * The factory class that loads an appropriate Transfer Refiner class,
 * as specified by the properties.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class RefinerFactory {

    /**
     * The default package where the implementations reside, which this factory
     * loads.
     */
    public static final String DEFAULT_PACKAGE_NAME =
        "org.griphyn.cPlanner.transfer.refiner";

    /**
     * Loads the implementing class corresponding to the value specified in the
     * properties. If the package name is not specified with the class, then
     * class is assumed to be in the DEFAULT_PACKAGE. The properties object passed
     * should not be null.
     * <p>
     * In addition it ends up loading the appropriate Transfer Implementation
     * that is required by the refiner.
     *
     * @param properties the <code>PegasusProperties</code> object containing all
     *                   the properties required by Pegasus.
     * @param dag        the workflow that is being refined.
     * @param options    the options with which the planner was invoked.
     *
     * @return the instance of the class implementing this interface.
     *
     * @exception TransferRefinerException that nests any error that
     *            might occur during the instantiation.
     *
     * @see #DEFAULT_PACKAGE_NAME
     */
    public static Refiner loadInstance(PegasusProperties properties,
                                       ADag dag,
                                       PlannerOptions options )
        throws TransferRefinerFactoryException{

        return loadInstance(properties.getTransferRefiner(),properties,dag,options);
    }


    /**
     * Loads the implementing class corresponding to the class. If the package
     * name is not specified with the class, then class is assumed to be
     * in the DEFAULT_PACKAGE. The properties object passed should not be null.
     * In addition it ends up loading the appropriate Transfer Implementation
     * that is required by the refiner.
     *
     * @param className  the name of the class that implements the mode.It can or
     *                   cannot be with the package name.
     * @param properties the <code>PegasusProperties</code> object containing all
     *                   the properties required by Pegasus.
     * @param dag        the workflow that is being refined.
     * @param options    the options with which the planner was invoked.
     *
     * @return the instance of the class implementing this interface.
     *
     * @exception TransferRefinerFactoryException that nests any error that
     *            might occur during the instantiation.
     *
     * @see #DEFAULT_PACKAGE_NAME
     */
    public static Refiner loadInstance(String className,
                                       PegasusProperties properties,
                                       ADag dag,
                                       PlannerOptions options )
        throws TransferRefinerFactoryException{

        Refiner refiner = null;
        try{
            //sanity check
            if (properties == null) {
                throw new RuntimeException("Invalid properties passed");
            }
            if (dag == null) {
                throw new RuntimeException("Invalid workflow passed");
            }

            //prepend the package name
            className = (className.indexOf('.') == -1) ?
                        //pick up from the default package
                        DEFAULT_PACKAGE_NAME + "." + className :
                        //load directly
                        className;

                //try loading the class dynamically
            DynamicLoader dl = new DynamicLoader(className);
            Object argList[] = new Object[3];
            argList[0] = dag;
            argList[1] = properties;
            argList[2] = options;
            refiner = (Refiner) dl.instantiate(argList);

            //we got the refiner try to load the appropriate
            //transfer implementation also
            refiner.loadImplementations(properties, options);
        }
        catch (Exception e){
            throw new TransferRefinerFactoryException("Instantiating Transfer Refiner",
                                                      className,
                                                      e);
        }

        return refiner;
    }

}
