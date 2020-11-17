/*
 *
 *   Copyright 2007-2008 University Of Southern California
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 */

package edu.isi.pegasus.common.logging;

import edu.isi.pegasus.common.util.DynamicLoader;
import edu.isi.pegasus.planner.common.PegasusProperties;
import java.util.Properties;

/**
 * A factory class to load the appropriate implementation of Logger API as specified by properties.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class LogManagerFactory {

    /** The default package where all the implementations reside. */
    public static final String DEFAULT_PACKAGE_NAME = "edu.isi.pegasus.common.logging.logger";

    /** Holds a singleton instance that is populated via the loadSingletonInstance() method. */
    private static LogManager mSingletonInstance;

    /**
     * Loads the appropriate LogManager class as specified by properties.
     *
     * @return handle to the Log Formatter.
     * @throws LogManagerFactoryException that nests any error that might occur during the
     *     instantiation
     * @see #DEFAULT_PACKAGE_NAME
     */
    public static LogManager loadSingletonInstance() throws LogManagerFactoryException {

        return (mSingletonInstance =
                (mSingletonInstance == null)
                        ? loadSingletonInstance(PegasusProperties.getInstance())
                        : mSingletonInstance);
    }

    /**
     * Loads the appropriate LogManager class as specified by properties.
     *
     * @param properties is an instance of properties to use.
     * @return handle to the Log Formatter.
     * @throws LogManagerFactoryException that nests any error that might occur during the
     *     instantiation
     * @see #DEFAULT_PACKAGE_NAME
     */
    public static LogManager loadSingletonInstance(PegasusProperties properties)
            throws LogManagerFactoryException {

        return (mSingletonInstance =
                (mSingletonInstance == null) ? loadInstance(properties) : mSingletonInstance);
    }

    /**
     * Loads the appropriate LogManager class as specified by properties.
     *
     * @param properties is an instance of properties to use.
     * @return handle to the Log Manager.
     * @throws LogManagerFactoryException that nests any error that might occur during the
     *     instantiation
     * @see #DEFAULT_PACKAGE_NAME
     */
    public static LogManager loadInstance(PegasusProperties properties)
            throws LogManagerFactoryException {

        if (properties == null) {
            throw new LogManagerFactoryException("Invalid NULL properties passed");
        }

        /* get the implementor from properties */
        String logImplementor = properties.getLogManager();
        String formatImplementor = properties.getLogFormatter();

        Properties initialize = properties.matchingSubset(LogManager.PROPERTIES_PREFIX, false);

        // determine the class that implements the site catalog
        return loadInstance(logImplementor, formatImplementor, initialize);
    }

    /**
     * Loads the Log Formatter specified.
     *
     * @param implementor the name of the class implementing LogManager
     * @param formatImplementor the name of the class implementing the formatting technique
     * @param properties properties
     * @return handle to the LogManager
     * @throws LogManagerFactoryException that nests any error that might occur during the
     *     instantiation
     * @see #DEFAULT_PACKAGE_NAME
     */
    public static LogManager loadInstance(
            String implementor, String formatImplementor, Properties properties)
            throws LogManagerFactoryException {

        // implementor = implementor == null ? "Default" : implementor;
        // formatImplementor = formatImplementor == null ? "Simple" : formatImplementor;

        LogManager result = null;
        try {
            if (implementor == null) {
                throw new RuntimeException("You need to specify the Logger implementor ");
            }

            /* prepend the package name if required */
            implementor =
                    (implementor.indexOf('.') == -1)
                            ?
                            // pick up from the default package
                            DEFAULT_PACKAGE_NAME + "." + implementor
                            :
                            // load directly
                            implementor;

            DynamicLoader dl = new DynamicLoader(implementor);
            result = (LogManager) dl.instantiate(new Object[0]);

            if (implementor == null) {
                throw new RuntimeException("Unable to load " + implementor);
            }

            /* load the log formatter and set it */
            result.initialize(LogFormatterFactory.loadInstance(formatImplementor), properties);
        } catch (Exception e) {
            throw new LogManagerFactoryException("Unable to instantiate Logger ", implementor, e);
        }

        /* store reference for singleton return */
        mSingletonInstance = result;

        return result;
    }
}
