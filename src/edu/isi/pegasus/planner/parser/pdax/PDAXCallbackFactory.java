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
package edu.isi.pegasus.planner.parser.pdax;

import edu.isi.pegasus.common.util.DynamicLoader;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.common.PegasusProperties;

/**
 * A factory class to load the appropriate DAX callback implementations that need to be passed to
 * the DAX Parser.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class PDAXCallbackFactory {

    /** Package to prefix "just" class names with. */
    public static final String DEFAULT_PACKAGE_NAME = "edu.isi.pegasus.planner.parser.pdax";

    /**
     * Loads the implementing class corresponding to the type specified by the user. The properties
     * object passed should not be null. The callback that is loaded, is the one referred to in the
     * properties by the user.
     *
     * @param directory the base level directory in which the output files are to be generated.
     * @param properties the <code>PegasusProperties</code> object containing all the properties
     *     required by Pegasus.
     * @return the instance of the class implementing this interface.
     * @exception PDAXCallbackFactoryException that nests any error that might occur during the
     *     instantiation
     * @see #DEFAULT_PACKAGE_NAME
     * @see org.griphyn.cPlanner.common.PegasusProperties#getPartitionerDAXCallback()
     */
    public static Callback loadInstance(
            PegasusProperties properties, PlannerOptions options, String directory)
            throws PDAXCallbackFactoryException {

        String mode = options.getMegaDAGMode();
        if (mode == null || mode.equalsIgnoreCase("dag")) {
            // load the default one
            mode = "PDAX2MDAG";
        } else if (mode.equalsIgnoreCase("noop")) {
            mode = "PDAX2NOOP";
        } else if (mode.equalsIgnoreCase("daglite")) {
            mode = "PDAX2DAGLite";
        }
        // load the class stored in the mode
        return loadInstance(properties, options, directory, mode);
    }

    /**
     * Loads the implementing class corresponding to the type specified by the user. The properties
     * object passed should not be null. The callback that is loaded, is the one referred to by the
     * className parameter passed.
     *
     * @param properties the <code>PegasusProperties</code> object containing all the properties
     *     required by Pegasus. dag|noop|daglite.
     * @param options the <code>PlannerOptions</code> object containing the options passed to
     *     gencdag.
     * @param directory the directory where the pdax file and parititioned daxes reside.
     * @param className the name of the implementing class.
     * @return the instance of the class implementing this interface.
     * @exception PDAXCallbackFactoryException that nests any error that might occur during the
     *     instantiation
     * @see #DEFAULT_PACKAGE_NAME
     */
    public static Callback loadInstance(
            PegasusProperties properties,
            PlannerOptions options,
            String directory,
            String className)
            throws PDAXCallbackFactoryException {

        Callback callback = null;

        try {

            // sanity check
            if (properties == null) {
                throw new RuntimeException("Invalid properties passed");
            }
            if (className == null) {
                return loadInstance(properties, options, directory);
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
            Object argList[] = new Object[3];
            argList[0] = directory;
            argList[1] = properties;
            argList[2] = options;
            callback = (Callback) dl.instantiate(argList);
        } catch (Exception e) {
            throw new PDAXCallbackFactoryException("Instantiating PDAXCallback ", className, e);
        }

        return callback;
    }
}
