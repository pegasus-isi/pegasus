/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.isi.pegasus.planner.parser.dax;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.parser.DAXParserFactory;
import edu.isi.pegasus.planner.parser.Parser;
import edu.isi.pegasus.planner.partitioner.graph.GraphNode;
import edu.isi.pegasus.planner.test.DefaultTestSetup;
import edu.isi.pegasus.planner.test.TestSetup;
import java.io.File;
import java.util.LinkedList;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author vahi
 */
public class DAXParser3Test {
    
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
     
        /* instantiate the DAX Parser and start parsing */
        String dax = new File( mTestSetup.getInputDirectory(), "blackdiamond.dax" ).getAbsolutePath();
        Parser p = (Parser)DAXParserFactory.loadDAXParser( mBag, "DAX2CDAG", dax );
        Callback cb = ((DAXParser)p).getDAXCallback();
        p.startParser( dax );
        mParsedDAX = (ADag)cb.getConstructedObject();
        mLogger.logEventCompletion();
    }

    /**
     * 
     */
    @Test
    public void testNoArgumentsTag() {
        
        int set = 1;
        //test with no deep storage structure enabled
        mLogger.logEventStart( "test.planner.parser.dax", "set", Integer.toString(set++) );
        GraphNode n  =  mParsedDAX.getNode("preprocess_j1");
        Job j        =  (Job)n.getContent();
        String args = j.getArguments();
        //System.out.println( "@" + args + "@" + args.length() + args.charAt( 0 ));
        assertEquals( "", args);
        mLogger.logEventCompletion();
    }
    
    /**
     * 
     */
    @Test
    public void testEmptyArgumentsTag() {
        
        int set = 1;
        //test with no deep storage structure enabled
        mLogger.logEventStart( "test.planner.parser.dax", "set", Integer.toString(set++) );
        GraphNode n  =  mParsedDAX.getNode("findrange_j2");
        Job j        =  (Job)n.getContent();
        String args = j.getArguments();
        //System.out.println( "@" + args + "@" + args.length() + args.charAt( 0 ));
        assertEquals( "", args);
        mLogger.logEventCompletion();
    }

    /**
     * 
     */
    @Test
    public void testArgumentsWithFileTagWithSpaces() {
        
        int set = 1;
        //test with no deep storage structure enabled
        mLogger.logEventStart( "test.planner.parser.dax", "set", Integer.toString(set++) );
        GraphNode n  =  mParsedDAX.getNode("findrange_j3");
        Job j        =  (Job)n.getContent();
        String args = j.getArguments();
        System.out.println( "@" + args + "@" + args.length()  );
        assertEquals( "-a findrange -T 60 -i f.b2 -o f.c2", args);
        mLogger.logEventCompletion();
    }

    /**
     * 
     */
    @Test
    public void testArgumentsWithFileTagWithNoSpaces() {
        
        int set = 1;
        //test with no deep storage structure enabled
        mLogger.logEventStart( "test.planner.parser.dax", "set", Integer.toString(set++) );
        GraphNode n  =  mParsedDAX.getNode("analyze_j4");
        Job j        =  (Job)n.getContent();
        String args = j.getArguments();
        System.out.println( "@" + args + "@" + args.length()  );
        assertEquals( "-a analyze -T 60 -if.c1,f.c2 -of.d", args);
        mLogger.logEventCompletion();
    }

    /**
     * 
     */
    @Test
    public void testLIGOArgumentsWithFileTagWithNoSpaces() {
        
        int set = 1;
        //test with no deep storage structure enabled
        mLogger.logEventStart( "test.planner.parser.dax", "set", Integer.toString(set++) );
        GraphNode n  =  mParsedDAX.getNode("inspiral-full-data_j5");
        Job j        =  (Job)n.getContent();
        String args = j.getArguments();
        System.out.println( "@" + args + "@" + args.length()  );
        assertEquals( "--frame-files --V1:V-HrecV2-967640000-10000.gwf --L1:L-L1_LDAS_C02_L2-967648128-128.gwf --L1:L-L1_LDAS_C02_L2-967648256-128.gwf --output V1H1L1-INSPIRAL_FULL_DATA_JOB0-967648572-1105.xml.gz --bank-file H1L1V1-PREGEN_TMPLTBANK_SPLITTABLE_BANK0-967593543-86400.xml.gz --user-tag FULL_DATA",
                      args);
        mLogger.logEventCompletion();
    }
    
    /**
     * 
     */
    @Test
    public void testDAGManCompliantWithNoSpecialChar() {
        
        int set = 1;
        //test with no deep storage structure enabled
        mLogger.logEventStart( "test.planner.parser.dax", "set", Integer.toString(set++) );
        DAXParser3 p = new DAXParser3( mBag, null);
        String name = "name";
        assertEquals( name,
                      p.makeDAGManCompliant(name) );
        mLogger.logEventCompletion();
    }
    
    /**
     * 
     */
    @Test
    public void testDAGManCompliantWithDotSpecialChar() {
        
        int set = 1;
        //test with no deep storage structure enabled
        mLogger.logEventStart( "test.planner.parser.dax", "set", Integer.toString(set++) );
        DAXParser3 p = new DAXParser3( mBag, null);
        String name = "blackdiamond.dax";
        assertEquals( "blackdiamond_dax",
                      p.makeDAGManCompliant(name) );
        mLogger.logEventCompletion();
    }
    
    /**
     * 
     */
    @Test
    public void testDAGManCompliantWithPlusSpecialChar() {
        
        int set = 1;
        //test with no deep storage structure enabled
        mLogger.logEventStart( "test.planner.parser.dax", "set", Integer.toString(set++) );
        DAXParser3 p = new DAXParser3( mBag, null);
        String name = "blackdiamond+dax";
        assertEquals( "blackdiamond_dax",
                      p.makeDAGManCompliant(name) );
        mLogger.logEventCompletion();
    }
    
    
    /**
     * 
     */
    @Test
    public void testDAGManCompliantWithMultipleSpecialChar() {
        
        int set = 1;
        //test with no deep storage structure enabled
        mLogger.logEventStart( "test.planner.parser.dax", "set", Integer.toString(set++) );
        DAXParser3 p = new DAXParser3( mBag, null);
        String name = "black.diamond+dax";
        assertEquals( "black_diamond_dax",
                      p.makeDAGManCompliant(name) );
        mLogger.logEventCompletion();
    }
    
    /**
     * 
     */
    @Test
    public void testDAGManCompliantWithRepeatedMultipleSpecialChar() {
        
        int set = 1;
        //test with no deep storage structure enabled
        mLogger.logEventStart( "test.planner.parser.dax", "set", Integer.toString(set++) );
        DAXParser3 p = new DAXParser3( mBag, null);
        String name = "black.diam.ond+dax++";
        assertEquals( "black_diam_ond_dax__",
                      p.makeDAGManCompliant(name) );
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
