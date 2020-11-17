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
package edu.isi.pegasus.planner.selector.replica;

import edu.isi.pegasus.common.util.DynamicLoader;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.selector.ReplicaSelector;

/**
 * A factory class to load the appropriate type of Replica Selector, as specified by the user at
 * runtime in properties. Each invocation of the factory results in a ReplicaSelector being
 * instantiated.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class ReplicaSelectorFactory {

    /** The default package where the all the implementing classes provided with the VDS reside. */
    public static final String DEFAULT_PACKAGE_NAME = "edu.isi.pegasus.planner.selector.replica";

    /**
     * The name of the class in the DEFAULT package, that corresponds to the default replica
     * selector.
     */
    public static final String DEFAULT_REPLICA_SELECTOR = "Default";

    /**
     * A no hassle factory method that loads the replica selector specified in the properties. The
     * properties are obtained from the property singleton. A default replica selector is loaded if
     * property is not specified in the properties.
     *
     * @return the instance of the class implementing this interface.
     * @exception ReplicaSelectorFactoryException that chains any error that might occur during the
     *     instantiation
     * @see #DEFAULT_PACKAGE_NAME
     * @see #DEFAULT_REPLICA_SELECTOR
     */
    public static ReplicaSelector loadInstance() throws ReplicaSelectorFactoryException {

        return loadInstance(PegasusProperties.getInstance());
    }

    /**
     * Loads the implementing class corresponding to the mode specified by the user at runtime in
     * the properties file. A default replica selector is loaded if property is not specified in the
     * properties.
     *
     * @param properties the <code>PegasusProperties</code> object containing all the properties
     *     required by Pegasus.
     * @return the instance of the class implementing this interface.
     * @exception ReplicaSelectorFactoryException that chains any error that might occur during the
     *     instantiation
     * @see #DEFAULT_PACKAGE_NAME
     * @see #DEFAULT_REPLICA_SELECTOR
     */
    public static ReplicaSelector loadInstance(PegasusProperties properties)
            throws ReplicaSelectorFactoryException {

        String className = null;
        // sanity check
        try {
            if (properties == null) {
                throw new RuntimeException("Invalid properties passed");
            }

            // figure out the implementing class
            // that needs to be instantiated.
            className = properties.getReplicaSelector();
            className =
                    (className == null || className.trim().length() < 2)
                            ? DEFAULT_REPLICA_SELECTOR
                            : className;
        } catch (Exception e) {
            throw new ReplicaSelectorFactoryException("Instantiating ReplicaSelector ", e);
        }

        return loadInstance(properties, className);
    }

    /**
     * Loads the implementing class corresponding to the class. If the package name is not specified
     * with the class, then class is assumed to be in the DEFAULT_PACKAGE. The properties object
     * passed should not be null.
     *
     * @param className the name of the class that implements the mode. It is the name of the class,
     *     not the complete name with package. That is added by itself.
     * @param properties the <code>PegasusProperties</code> object containing all the properties
     *     required by Pegasus.
     * @return the instance of the class implementing this interface.
     * @exception ReplicaSelectorFactoryException that chains any error that might occur during the
     *     instantiation
     * @see #DEFAULT_PACKAGE_NAME
     */
    public static ReplicaSelector loadInstance(PegasusProperties properties, String className)
            throws ReplicaSelectorFactoryException {

        ReplicaSelector rs = null;

        try {
            // some sanity checks
            if (properties == null) {
                throw new RuntimeException("Invalid properties passed");
            }
            if (className == null) {
                throw new RuntimeException("Invalid className specified");
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
            argList[0] = properties;
            rs = (ReplicaSelector) dl.instantiate(argList);
        } catch (Exception e) {
            // chain the exception caught into the appropriate Factory Exception
            throw new ReplicaSelectorFactoryException(
                    "Instantiating ReplicaSelector ", className, e);
        }

        return rs;
    }
}
