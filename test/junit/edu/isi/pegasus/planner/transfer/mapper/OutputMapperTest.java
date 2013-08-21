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
package edu.isi.pegasus.planner.transfer.mapper;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.planner.catalog.site.SiteFactory;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.test.RequiresProperties;
import edu.isi.pegasus.planner.test.RequiresSiteCatalog;

import java.io.File;
import java.util.LinkedList;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 * A JUnit Test to test the output mapper interface.
 *
 * @author Karan Vahi
 */
public abstract class OutputMapperTest implements
                                        RequiresSiteCatalog,
                                        RequiresProperties{
    
    /**
     * The relative directory for the junit tests in our repo.
     */
    public static final String RELATIVE_TESTS_DIR = "test" + File.separator + "junit" ;
    
    protected PegasusBag mBag;
    
    protected PegasusProperties mProps;
    
    protected LogManager mLogger;
   
    protected String mInputDir;
    
    public OutputMapperTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    /**
     * Setup the logger and properties that all test functions require
     */
    @Before
    public final void setUp() {
        mBag = new PegasusBag();
       
        mInputDir = getInputDir();
        System.out.println( "Input Test Dir is " + mInputDir );
        
        mProps = this.getProperties( mInputDir );
        mBag.add( PegasusBag.PEGASUS_PROPERTIES, mProps );
        LogManager logger = LogManagerFactory.loadSingletonInstance( mProps );
        mLogger = logger;
        mLogger.setLevel(  LogManager.INFO_MESSAGE_LEVEL );
        logger.logEventStart( "test.output.mapper", "setup", "0" );
        mBag.add( PegasusBag.PEGASUS_LOGMANAGER, logger );
        mBag.add( PegasusBag.PLANNER_OPTIONS, this.getPlannerOptions() );
        
        List<String> sites = new LinkedList();
        sites.add( "*" );
        SiteStore store = this.getSiteStore( mProps, logger, sites);
        mBag.add( PegasusBag.SITE_STORE, store );
        mLogger.logEventCompletion();
    }
    
    public PlannerOptions getPlannerOptions( ){
        PlannerOptions options = new PlannerOptions();
        options.setOutputSite( "local" );
        return options;
    }
    
    /**
     * Loads up the SiteStore with the sites passed in list of sites
     * 
     * @param props   the properties
     * @param logger  the logger
     * @param sites   the list of sites to load
     * 
     * @return the SiteStore
     */
    public final SiteStore getSiteStore( PegasusProperties props , LogManager logger, List<String> sites ){
        PegasusBag bag = new PegasusBag();
        bag.add( PegasusBag.PEGASUS_PROPERTIES, props );
        bag.add( PegasusBag.PEGASUS_LOGMANAGER, logger );
        return SiteFactory.loadSiteStore( sites, bag );
    }
    
    /**
     * Loads up properties from the input directory for the test.
     * 
     * @param inputdir
     * @return 
     */
    public final PegasusProperties getProperties( String inputdir ){
        String propertiesBasename = getPropertiesFileBasename( );
        String propsFile = new File( inputdir, propertiesBasename ).getAbsolutePath();
       
        System.out.println( "Properties File for test is " + propsFile );
        PegasusProperties properties = PegasusProperties.getInstance( propsFile );
        //check if the properties have a relative value for file property set
        String file = properties.getProperty( PegasusProperties.PEGASUS_SITE_CATALOG_FILE_PROPERTY ) ;
        if( file != null && file.startsWith( "." )){
            //update the relative path with the input dir
            file = inputdir + File.separator + file;
            properties.setProperty( PegasusProperties.PEGASUS_SITE_CATALOG_FILE_PROPERTY, file );
        }
        return properties;
    }
    
    @After
    public void tearDown() {
        mLogger = null;
        mProps  = null;
        mBag    = null;
    }


    /**
     * Returns the path to the input directory for the test.
     * 
     * @return 
     */
    protected String getInputDir() {
        StringBuilder dir = new StringBuilder();
        dir.append( new File(".").getAbsolutePath());
        dir.append( File.separator );
        dir.append( OutputMapperTest.RELATIVE_TESTS_DIR );
        
        String packageName  = this.getClass().getPackage().getName();
        for( String component : packageName.split( "\\." )){
            dir.append( File.separator );
            dir.append( component );
        }
        
        dir.append( File.separator ).append( "input" );
        return dir.toString();
    }

    /**
     * Returns the basename for the properties file
     * 
     * @return 
     */
    protected abstract String getPropertiesFileBasename() ;

    
    
    
    
}