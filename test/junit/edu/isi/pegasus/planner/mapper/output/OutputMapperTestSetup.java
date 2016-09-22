/**
 *  Copyright 2007-2013 University Of Southern California
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
package edu.isi.pegasus.planner.mapper.output;

import edu.isi.pegasus.planner.test.*;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.common.PegasusProperties;
import java.util.List;

/**
 * A default test setup implementation for the junit tests.
 * 
 * @author Karan Vahi
 */
public class OutputMapperTestSetup implements TestSetup {
   
    
    /**
     * The input directory for the test.
     */
    private String mTestInputDir;
    
    /**
     * The Default Testup that this uses
     */
    private DefaultTestSetup mDefaultTestSetup;
    
    /**
     * The default constructor.
     */
    public OutputMapperTestSetup(){
        mTestInputDir = ".";
        mDefaultTestSetup = new DefaultTestSetup();
        
    }
    
      
    /**
     * Set the input directory for the test on the basis of the classname of test class
     * 
     * @param testClass  the test class.
     */
    public void setInputDirectory( Class testClass ){
        mDefaultTestSetup.setInputDirectory(testClass);
    }
    
    /**
     * Set the input directory for the test. 
     * 
     * @param directory  the directory 
     */
    public void setInputDirectory( String directory ){
        mDefaultTestSetup.setInputDirectory(directory);
    }
    
    /**
     * Returns the input directory set by the test.
     * 
     * @return 
     */
    public String getInputDirectory(){
        return mDefaultTestSetup.getInputDirectory();
    }
    
    /**
     * Loads up PegasusProperties properties.
     * 
     * @param sanitizeKeys  list of keys to be sanitized 
     * 
     * @return 
     */
    public PegasusProperties loadProperties( List<String> sanitizeKeys ){
        return mDefaultTestSetup.loadProperties( sanitizeKeys );
    }
    
    /**
     * Loads up properties from the input directory for the test.
     * 
     * @param propertiesBasename  basename of the properties file in the input directory.
     * @param sanitizeKeys  list of keys to be sanitized . relative paths replaced 
     *                      with full path on basis of test input directory.
     * 
     * @return 
     */
    public  PegasusProperties loadPropertiesFromFile( String propertiesBasename, List<String> sanitizeKeys){
        return mDefaultTestSetup.loadPropertiesFromFile(propertiesBasename, sanitizeKeys);
    }
    
    /**
     * Loads the logger from the properties and sets default level to INFO
     * 
     * @param properties
     * 
     * @return 
     */
    public LogManager loadLogger( PegasusProperties properties ){
        return mDefaultTestSetup.loadLogger(properties);
    }
    
    /**
     * Loads the planner options for the test
     * 
     * @return 
     */
    public PlannerOptions loadPlannerOptions( ){
        PlannerOptions options = new PlannerOptions();
        options.setOutputSite( "local" );
        return options;
     
    }
            
    /**
     * Loads up the SiteStore with the sites passed in list of sites.
     * 
     * @param props   the properties
     * @param logger  the logger
     * @param sites   the list of sites to load
     * 
     * @return the SiteStore
     */
    public SiteStore loadSiteStore( PegasusProperties props , LogManager logger, List<String> sites ){
        return mDefaultTestSetup.loadSiteStore(props, logger, sites);
    }
    
    /**
     * Loads up the SiteStore with the sites passed in list of sites.
     * 
     * @param props   the properties
     * @param logger  the logger
     * @param sites   the list of sites to load
     * 
     * @return the SiteStore
     */
    public SiteStore loadSiteStoreFromFile( PegasusProperties props , LogManager logger, List<String> sites ){
        return mDefaultTestSetup.loadSiteStoreFromFile( props, logger, sites );
    }
}
