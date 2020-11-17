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

/**
 * A factory class to load the appropriate implementation of LogFormatter as specified by
 * properties.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class LogFormatterFactory {

    /** The default package where all the implementations reside. */
    public static final String DEFAULT_PACKAGE_NAME = "edu.isi.pegasus.common.logging.format";

    /** Holds a singleton instance that is populated via the loadSingletonInstance() method. */
    private static LogFormatter mSingletonInstance;

    /**
     * Loads the appropriate LogFormatter class as specified by properties.
     *
     * @param implementor the name of the class implementing LogFormatter
     * @return handle to the Log Formatter.
     * @throws LogFormatterFactoryException that nests any error that might occur during the
     *     instantiation
     * @see #DEFAULT_PACKAGE_NAME
     */
    public static LogFormatter loadSingletonInstance(String implementor)
            throws LogFormatterFactoryException {

        return (mSingletonInstance =
                (mSingletonInstance == null) ? loadInstance(implementor) : mSingletonInstance);
    }

    /**
     * Loads the appropriate LogFormatter class as specified by properties.
     *
     * @param properties is an instance of properties to use.
     * @return handle to the Log Formatter.
     * @throws LogFormatterFactoryException that nests any error that might occur during the
     *     instantiation
     * @see #DEFAULT_PACKAGE_NAME
     */
    public static LogFormatter loadInstance(PegasusProperties properties)
            throws LogFormatterFactoryException {

        if (properties == null) {
            throw new LogFormatterFactoryException("Invalid NULL properties passed");
        }

        /* get the implementor from properties */
        String formatImplementor = properties.getLogFormatter().trim();

        /* prepend the package name if required */
        formatImplementor =
                (formatImplementor.indexOf('.') == -1)
                        ?
                        // pick up from the default package
                        DEFAULT_PACKAGE_NAME + "." + formatImplementor
                        :
                        // load directly
                        formatImplementor;

        // determine the class that implements the site catalog
        return loadInstance(formatImplementor);
    }

    /**
     * Loads the Log Formatter specified.
     *
     * @param implementor the name of the class implementing LogFormatter
     * @return handle to the Site Catalog.
     * @throws LogFormatterFactoryException that nests any error that might occur during the
     *     instantiation
     * @see #DEFAULT_PACKAGE_NAME
     */
    public static LogFormatter loadInstance(String implementor)
            throws LogFormatterFactoryException {

        LogFormatter result = null;
        try {
            if (implementor == null) {
                throw new RuntimeException("You need to specify the implementor ");
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
            result = (LogFormatter) dl.instantiate(new Object[0]);

            if (implementor == null) {
                throw new RuntimeException("Unable to load " + implementor);
            }

        } catch (Exception e) {
            throw new LogFormatterFactoryException(
                    "Unable to instantiate Log Formatter ", implementor, e);
        }

        return result;
    }
}
