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
import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.planner.catalog.site.SiteFactory;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.common.PegasusProperties;
import java.io.File;
import java.util.List;

/**
 * A default test setup implementation for the junit tests.
 *
 * @author Karan Vahi
 */
public class DefaultTestSetup implements TestSetup {

    /** The relative directory for the junit tests in our repo. */
    public static final String RELATIVE_TESTS_DIR = "test" + File.separator + "junit";

    /** The input directory for the test. */
    private String mTestInputDir;

    /** The default constructor. */
    public DefaultTestSetup() {
        mTestInputDir = ".";
    }

    /**
     * Set the input directory for the test on the basis of the classname of test class
     *
     * @param testClass the test class.
     */
    public void setInputDirectory(Class testClass) {
        StringBuilder dir = new StringBuilder();
        dir.append(new File(".").getAbsolutePath());
        dir.append(File.separator);
        dir.append(DefaultTestSetup.RELATIVE_TESTS_DIR);

        String packageName = testClass.getPackage().getName();
        for (String component : packageName.split("\\.")) {
            dir.append(File.separator);
            dir.append(component);
        }

        dir.append(File.separator).append("input");
        this.mTestInputDir = dir.toString();
    }

    /**
     * Set the input directory for the test.
     *
     * @param directory the directory
     */
    public void setInputDirectory(String directory) {
        mTestInputDir = directory;
    }

    /**
     * Returns the input directory set by the test.
     *
     * @return
     */
    public String getInputDirectory() {
        return this.mTestInputDir;
    }

    /**
     * Loads up PegasusProperties properties.
     *
     * @param sanitizeKeys list of keys to be sanitized
     * @return
     */
    public PegasusProperties loadProperties(List<String> sanitizeKeys) {
        PegasusProperties properties = PegasusProperties.nonSingletonInstance();

        for (String key : sanitizeKeys) {
            // check if the properties have a relative value for value property set
            String value = properties.getProperty(key);
            if (value != null && value.startsWith(".")) {
                // update the relative path with the input dir
                value = mTestInputDir + File.separator + value;
                properties.setProperty(key, value);
                System.out.println("Set property " + key + " to " + value);
            }
        }
        return properties;
    }

    /**
     * Loads up properties from the input directory for the test.
     *
     * @param propertiesBasename basename of the properties file in the input directory.
     * @param sanitizeKeys list of keys to be sanitized . relative paths replaced with full path on
     *     basis of test input directory.
     * @return
     */
    public PegasusProperties loadPropertiesFromFile(
            String propertiesBasename, List<String> sanitizeKeys) {
        String propsFile = new File(mTestInputDir, propertiesBasename).getAbsolutePath();

        System.out.println("Properties File for test is " + propsFile);
        PegasusProperties properties = PegasusProperties.getInstance(propsFile);

        for (String key : sanitizeKeys) {
            // check if the properties have a relative value for value property set
            String value = properties.getProperty(key);
            if (value != null && value.startsWith(".")) {
                // update the relative path with the input dir
                value = mTestInputDir + File.separator + value;
                properties.setProperty(key, value);
                System.out.println("Set property " + key + " to " + value);
            }
        }
        return properties;
    }

    /**
     * Loads the logger from the properties and sets default level to INFO
     *
     * @param properties
     * @return
     */
    public LogManager loadLogger(PegasusProperties properties) {
        LogManager logger = LogManagerFactory.loadSingletonInstance(properties);
        logger.setLevel(LogManager.INFO_MESSAGE_LEVEL);
        return logger;
    }

    /**
     * Loads the planner options for the test
     *
     * @return
     */
    @Override
    public PlannerOptions loadPlannerOptions() {
        throw new UnsupportedOperationException("Method loadPlannerOptions() not implemented ");
    }

    /**
     * Loads up the SiteStore with the sites passed in list of sites.
     *
     * @param props the properties
     * @param logger the logger
     * @param sites the list of sites to load
     * @return the SiteStore
     */
    @Override
    public SiteStore loadSiteStore(PegasusProperties props, LogManager logger, List<String> sites) {
        throw new UnsupportedOperationException(
                "Method loadSiteStore( PegasusProperties, LogManager, List) not implemented ");
    }

    /**
     * Loads up the SiteStore with the sites passed in list of sites.
     *
     * @param props the properties
     * @param logger the logger
     * @param sites the list of sites to load
     * @return the SiteStore
     */
    public SiteStore loadSiteStoreFromFile(
            PegasusProperties props, LogManager logger, List<String> sites) {
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, props);
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, logger);
        return SiteFactory.loadSiteStore(sites, bag);
    }
}
