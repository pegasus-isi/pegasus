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

import edu.isi.pegasus.planner.mapper.OutputMapperFactory;
import edu.isi.pegasus.planner.mapper.OutputMapper;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.site.classes.FileServerType;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.test.TestSetup;
import java.util.LinkedList;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;

import org.junit.Test;

import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 * A JUnit Test to test the output mapper interface.
 *
 * @author Karan Vahi
 */
public class FlatOutputMapperTest {
    
    /**
     * The properties used for this test.
     */
    private static final String PROPERTIES_BASENAME="flat.properties";
    
    private PegasusBag mBag;
    
    private PegasusProperties mProps;
    
    private LogManager mLogger;
    
    private TestSetup mTestSetup;
    
    private static int mTestNum;
    
    
    @BeforeClass
    public static void setUpClass() {
        mTestNum = 1;
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    /**
     * Setup the logger and properties that all test functions require
     */
    @Before
    public final void setUp() {
        mTestSetup = new OutputMapperTestSetup();
        mBag = new PegasusBag();
       
        mTestSetup.setInputDirectory( this.getClass() );
        System.out.println( "Input Test Dir is " + mTestSetup.getInputDirectory() );
        
        mProps = mTestSetup.loadPropertiesFromFile( PROPERTIES_BASENAME, this.getPropertyKeysForSanitization() );
        mBag.add( PegasusBag.PEGASUS_PROPERTIES, mProps );
        
        mLogger  = mTestSetup.loadLogger( mProps );
        mLogger.logEventStart( "test.output.mapper", "setup", "0" );
        mBag.add( PegasusBag.PEGASUS_LOGMANAGER, mLogger );
        
        mBag.add( PegasusBag.PLANNER_OPTIONS, mTestSetup.loadPlannerOptions() );
        
        List<String> sites = new LinkedList();
        sites.add( "*" );
        SiteStore store = mTestSetup.loadSiteStoreFromFile(mProps, mLogger, sites);
        mBag.add( PegasusBag.SITE_STORE, store );
        mLogger.logEventCompletion();
    }

    /**
     * Test of the Flat Output Mapper.
     */
    @Test
    public void testWithoutDeepStorage() {
        
        //test with no deep storage structure enabled
        mLogger.logEventStart( "test.output.mapper.Flat", "set", Integer.toString(mTestNum++) );
        mProps.setProperty( OutputMapperFactory.PROPERTY_KEY, "Flat" );
        OutputMapper mapper = OutputMapperFactory.loadInstance( new ADag(), mBag );
        
        String lfn    = "f.a";
        String pfn    = mapper.map( lfn, "local", FileServerType.OPERATION.put );
        assertEquals( lfn + " not mapped to right location " ,
                      "file:///test/junit/output/mapper/blackdiamond/outputs/f.a", pfn );
        
        pfn = mapper.map( lfn, "local", FileServerType.OPERATION.get );
        assertEquals( lfn + " not mapped to right location " ,
                      "gsiftp://sukhna.isi.edu/test/junit/output/mapper/blackdiamond/outputs/f.a", pfn );
        
        List<String> pfns = mapper.mapAll( lfn, "local", FileServerType.OPERATION.get);
        String[] expected = { "gsiftp://sukhna.isi.edu/test/junit/output/mapper/blackdiamond/outputs/f.a" };
        assertArrayEquals( expected, pfns.toArray() );
        mLogger.logEventCompletion();
        
        
    }
    
    @Test
    public void testWithDeepStorage(){
        
        ////test with  deep storage structure enabled
        //set property to enable deep storage, where relative submit directory is added
        mLogger.logEventStart( "test.output.mapper.Flat", "set", Integer.toString(mTestNum++) );
        PlannerOptions options = mBag.getPlannerOptions();
        
        //by default planner only sets relative submit directory internally
        String relativeDir = "relative-submit";
        options.setRelativeSubmitDirectory( relativeDir );
        options.setRelativeDirectory( null );
        
        mProps.setProperty( "pegasus.dir.storage.deep", "true" );
        String lfn    = "f.a";
        SiteStore store = mBag.getHandleToSiteStore();
        store.setForPlannerUse(mProps,mBag.getPlannerOptions() );
        OutputMapper mapper = OutputMapperFactory.loadInstance( new ADag(), mBag );
        
        String deepPFN    = mapper.map( lfn, "local", FileServerType.OPERATION.put );
        assertEquals( lfn + " not mapped to right location " ,
                      "file:///test/junit/output/mapper/blackdiamond/outputs/" + relativeDir + "/f.a", deepPFN );
        
        deepPFN = mapper.map( lfn, "local", FileServerType.OPERATION.get );
        assertEquals( lfn + " not mapped to right location " ,
                      "gsiftp://sukhna.isi.edu/test/junit/output/mapper/blackdiamond/outputs/" + relativeDir + "/f.a", deepPFN );
        
        List<String> deepPFNS = mapper.mapAll( lfn, "local", FileServerType.OPERATION.get);
        String[] expectedDeepPFNS = { "gsiftp://sukhna.isi.edu/test/junit/output/mapper/blackdiamond/outputs/" + relativeDir + "/f.a" };
        assertArrayEquals( expectedDeepPFNS, deepPFNS.toArray() );
        mLogger.logEventCompletion();
        
    }

    @Test
    public void testWithDeepStorageRelativeDir(){
        
        ////test with  deep storage structure enabled
        //set property to enable deep storage, where relative submit directory is added
        mLogger.logEventStart( "test.output.mapper.Flat", "set", Integer.toString(mTestNum++) );
        PlannerOptions options = mBag.getPlannerOptions();
        
        //by default planner only sets relative submit directory internally
        String relativeDir = "relative";
        options.setRelativeSubmitDirectory( null );
        options.setRelativeDirectory( relativeDir );
        
        mProps.setProperty( "pegasus.dir.storage.deep", "true" );
        String lfn    = "f.a";
        SiteStore store = mBag.getHandleToSiteStore();
        store.setForPlannerUse(mProps,mBag.getPlannerOptions() );
        OutputMapper mapper = OutputMapperFactory.loadInstance( new ADag(), mBag );
        
        String deepPFN    = mapper.map( lfn, "local", FileServerType.OPERATION.put );
        assertEquals( lfn + " not mapped to right location " ,
                      "file:///test/junit/output/mapper/blackdiamond/outputs/" + relativeDir + "/f.a", deepPFN );
        
        deepPFN = mapper.map( lfn, "local", FileServerType.OPERATION.get );
        assertEquals( lfn + " not mapped to right location " ,
                      "gsiftp://sukhna.isi.edu/test/junit/output/mapper/blackdiamond/outputs/" + relativeDir + "/f.a", deepPFN );
        
        List<String> deepPFNS = mapper.mapAll( lfn, "local", FileServerType.OPERATION.get);
        String[] expectedDeepPFNS = { "gsiftp://sukhna.isi.edu/test/junit/output/mapper/blackdiamond/outputs/" + relativeDir + "/f.a" };
        assertArrayEquals( expectedDeepPFNS, deepPFNS.toArray() );
        mLogger.logEventCompletion();
        
    }
    
    @Test
    public void testWithDeepStorageRelativeDirs(){
        
        ////test with  deep storage structure enabled
        //set property to enable deep storage, where relative submit directory is added
        mLogger.logEventStart( "test.output.mapper.Flat", "set", Integer.toString(mTestNum++) );
        PlannerOptions options = mBag.getPlannerOptions();
        
        //by default planner only sets relative submit directory internally
        //should pick up relative-exec
        String relativeDir = "relative-exec";
        options.setRelativeSubmitDirectory( "relative-submit" );
        options.setRelativeDirectory( relativeDir );
        
        mProps.setProperty( "pegasus.dir.storage.deep", "true" );
        String lfn    = "f.a";
        SiteStore store = mBag.getHandleToSiteStore();
        store.setForPlannerUse(mProps,mBag.getPlannerOptions() );
        OutputMapper mapper = OutputMapperFactory.loadInstance( new ADag(), mBag );
        
        String deepPFN    = mapper.map( lfn, "local", FileServerType.OPERATION.put );
        assertEquals( lfn + " not mapped to right location " ,
                      "file:///test/junit/output/mapper/blackdiamond/outputs/" + relativeDir + "/f.a", deepPFN );
        
        deepPFN = mapper.map( lfn, "local", FileServerType.OPERATION.get );
        assertEquals( lfn + " not mapped to right location " ,
                      "gsiftp://sukhna.isi.edu/test/junit/output/mapper/blackdiamond/outputs/" + relativeDir + "/f.a", deepPFN );
        
        List<String> deepPFNS = mapper.mapAll( lfn, "local", FileServerType.OPERATION.get);
        String[] expectedDeepPFNS = { "gsiftp://sukhna.isi.edu/test/junit/output/mapper/blackdiamond/outputs/" + relativeDir + "/f.a" };
        assertArrayEquals( expectedDeepPFNS, deepPFNS.toArray() );
        mLogger.logEventCompletion();
        
    }
    
    @After
    public void tearDown() {
        mLogger = null;
        mProps  = null;
        mBag    = null;
        mTestSetup = null;
    }

    /**
     * Returns the list of property keys that should be sanitized
     * 
     * @return List<String>
     */
    protected List<String> getPropertyKeysForSanitization(){
        List<String> keys = new LinkedList();
        keys.add( PegasusProperties.PEGASUS_SITE_CATALOG_FILE_PROPERTY );
        return keys;
    }

    
    
    
}