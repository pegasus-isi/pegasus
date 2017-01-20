/**
 *  Copyright 2007-2008 University Of Southern California
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
package edu.isi.pegasus.planner.parser.dax;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.site.classes.FileServerType;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.mapper.OutputMapper;
import edu.isi.pegasus.planner.mapper.OutputMapperFactory;
import edu.isi.pegasus.planner.parser.DAXParserFactory;
import edu.isi.pegasus.planner.parser.Parser;
import edu.isi.pegasus.planner.test.DefaultTestSetup;
import edu.isi.pegasus.planner.test.TestSetup;
import java.io.File;
import java.util.LinkedList;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit tests for DAXParser
 * 
 * @author Karan Vahi
 */
public class DAXParserTest {
    
    /**
     * The properties used for this test.
     */
    private static final String PROPERTIES_BASENAME="properties";
    
    private PegasusBag mBag;
    
    private PegasusProperties mProps;
    
    private LogManager mLogger;
    
    private TestSetup mTestSetup;
    
    /**
     * The parsed DAX file
     */
    private ADag mParsedDAX;
    
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
        mTestSetup = new DefaultTestSetup();
        mBag = new PegasusBag();
       
        mTestSetup.setInputDirectory( this.getClass() );
        System.out.println( "Input Test Dir is " + mTestSetup.getInputDirectory() );
        
        mProps = mTestSetup.loadPropertiesFromFile( PROPERTIES_BASENAME, new LinkedList() );
        mBag.add( PegasusBag.PEGASUS_PROPERTIES, mProps );
        
        mLogger  = mTestSetup.loadLogger( mProps );
        mLogger.logEventStart( "test.planner.parser.dax", "setup", "0" );
        mBag.add( PegasusBag.PEGASUS_LOGMANAGER, mLogger );
        mBag.add( PegasusBag.PLANNER_OPTIONS, mTestSetup.loadPlannerOptions() );
     
        /* instantiate the DAX Parser and start parsing */
        String dax = new File( mTestSetup.getInputDirectory(), "blackdiamond.dax" ).getAbsolutePath();
        Parser p = (Parser)DAXParserFactory.loadDAXParser( mBag, "DAX2CDAG", dax );
        Callback cb = ((DAXParser)p).getDAXCallback();
        p.startParser( dax );
        mParsedDAX = (ADag)cb.getConstructedObject();
        mLogger.logEventCompletion();
    }

    /**
     * Test of the Flat Output Mapper.
     */
    @Test
    public void test() {
        
        int set = 1;
        //test with no deep storage structure enabled
        mLogger.logEventStart( "test.output.mapper.Fixed", "set", Integer.toString(set++) );
        mProps.setProperty( OutputMapperFactory.PROPERTY_KEY, "Fixed" );
        OutputMapper mapper = OutputMapperFactory.loadInstance( new ADag(), mBag );
        
        String lfn    = "f.a";
        String pfn    = mapper.map( lfn, "local", FileServerType.OPERATION.put );
        assertEquals( lfn + " not mapped to right location " ,
                      "gsiftp://outputs.isi.edu/shared/outputs/f.a", pfn );
        
        pfn = mapper.map( lfn, "local", FileServerType.OPERATION.get );
        assertEquals( lfn + " not mapped to right location " ,
                      "gsiftp://outputs.isi.edu/shared/outputs/f.a", pfn );
        
        List<String> pfns = mapper.mapAll( lfn, "local", FileServerType.OPERATION.get);
        String[] expected = { "gsiftp://outputs.isi.edu/shared/outputs/f.a" };
        assertArrayEquals( expected, pfns.toArray() );
        mLogger.logEventCompletion();
        
        
    }

    
    @After
    public void tearDown() {
        mLogger = null;
        mProps  = null;
        mBag    = null;
        mTestSetup = null;
    }


}
