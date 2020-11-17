/**
 * Copyright 2007-2013 University Of Southern California
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
package edu.isi.pegasus.planner.test;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.common.PegasusProperties;
import java.io.File;
import java.util.List;

/**
 * An interface to setup the the jnuit tests for the planner, the module tests setup implement.
 *
 * @author Karan Vahi
 */
public interface TestSetup {

    /** The relative directory for the junit tests in our repo. */
    public static final String RELATIVE_TESTS_DIR = "test" + File.separator + "junit";

    /**
     * Set the input directory for the test on the basis of the classname of test class
     *
     * @param testClass the test class.
     */
    public void setInputDirectory(Class testClass);

    /**
     * Set the input directory for the test.
     *
     * @param directory
     */
    public void setInputDirectory(String directory);

    /**
     * Returns the input directory set by the test.
     *
     * @return
     */
    public String getInputDirectory();

    /**
     * Loads up properties from the input directory for the test.
     *
     * @param sanitizeKeys list of keys to be sanitized
     * @return
     */
    public PegasusProperties loadProperties(List<String> sanitizeKeys);

    /**
     * Loads up properties from the input directory for the test.
     *
     * @param propertiesBasename basename of the properties file in the input directory.
     * @param sanitizeKeys list of keys to be sanitized . relative paths replaced with full path on
     *     basis of test input directory.
     * @return
     */
    public PegasusProperties loadPropertiesFromFile(
            String propertiesBasename, List<String> sanitizeKeys);

    /**
     * Loads the logger from the properties.
     *
     * @param properties
     * @return
     */
    public LogManager loadLogger(PegasusProperties properties);

    /**
     * Loads the planner options for the test
     *
     * @return
     */
    public PlannerOptions loadPlannerOptions();

    /**
     * Loads up the SiteStore with the sites passed in list of sites.
     *
     * @param props the properties
     * @param logger the logger
     * @param sites the list of sites to load
     * @return the SiteStore
     */
    public SiteStore loadSiteStore(PegasusProperties props, LogManager logger, List<String> sites);

    /**
     * Loads up the SiteStore with the sites passed in list of sites.
     *
     * @param props the properties
     * @param logger the logger
     * @param sites the list of sites to load
     * @return the SiteStore
     */
    public SiteStore loadSiteStoreFromFile(
            PegasusProperties props, LogManager logger, List<String> sites);
}
