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
package edu.isi.pegasus.planner.catalog.transformation;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.common.util.DynamicLoader;
import edu.isi.pegasus.planner.catalog.TransformationCatalog;
import edu.isi.pegasus.planner.common.PegasusProperties;

/**
 * This class defines all the constants referring to the various interfaces to the transformation
 * catalog, and used by the Concrete Planner.
 *
 * @author Gaurang Mehta
 * @version $Revision$
 */
public class TCMode {

    /** Constants for backward compatibility. */
    public static final String SINGLE_READ = "single";

    public static final String MULTIPLE_READ = "multiple";

    public static final String OLDFILE_TC_CLASS = "OldFile";

    public static final String DEFAULT_TC_CLASS = "File";
    /** Default PACKAGE PATH for the TC implementing classes */
    public static final String PACKAGE_NAME = "org.griphyn.common.catalog.transformation.";

    private static LogManager mLogger = LogManagerFactory.loadSingletonInstance();

    // add your constants here.

    /**
     * This method just checks and gives the correct classname if a user provides the classname in a
     * different case.
     *
     * @param tcmode String
     * @return String
     */
    private static String getImplementingClass(String tcmode) {

        if (tcmode.trim().equalsIgnoreCase(SINGLE_READ)
                || tcmode.trim().equalsIgnoreCase(MULTIPLE_READ)) {
            return OLDFILE_TC_CLASS;
        } else {
            // no match to any predefined constant
            // assume that the value of readMode is the
            // name of the implementing class
            return tcmode;
        }
    }

    /**
     * The overloaded method which is to be used internally in Pegasus.
     *
     * @return TCMechanism
     */
    public static TransformationCatalog loadInstance() {
        PegasusProperties mProps = PegasusProperties.getInstance();
        TransformationCatalog tc = null;
        String tcClass = getImplementingClass(mProps.getTCMode());

        // if (tcClass.equals(FILE_TC_CLASS)) {
        //  String[] args = {mProps.getTCPath()};
        // return loadInstance(tcClass, args);
        // } else {
        String[] args = new String[0];
        tc = loadInstance(tcClass, args);
        if (tc == null) {
            mLogger.log("Unable to load TC", LogManager.FATAL_MESSAGE_LEVEL);
            System.exit(1);
        }
        return tc;
        //  }
    }

    /**
     * Loads the appropriate TC implementing Class with the given arguments.
     *
     * @param tcClass String
     * @param args String[]
     * @return TCMechanism
     */
    public static TransformationCatalog loadInstance(String tcClass, Object[] args) {

        TransformationCatalog tc = null;
        String methodName = "getInstance";
        // get the complete name including
        // the package if the package name not
        // specified
        if (tcClass.indexOf(".") == -1) {
            tcClass = PACKAGE_NAME + tcClass;
        }

        DynamicLoader d = new DynamicLoader(tcClass);

        try {
            tc = (TransformationCatalog) d.static_method(methodName, args);

            // This identifies the signature for
            // the method

        } catch (Exception e) {
            mLogger.log(d.convertException(e), LogManager.FATAL_MESSAGE_LEVEL);
            System.exit(1);
        }
        return tc;
    }
}
