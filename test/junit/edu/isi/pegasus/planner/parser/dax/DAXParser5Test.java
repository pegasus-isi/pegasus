/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.isi.pegasus.planner.parser.dax;

import static org.junit.Assert.assertEquals;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.WorkflowMetrics;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.parser.DAXParserFactory;
import edu.isi.pegasus.planner.test.DefaultTestSetup;
import edu.isi.pegasus.planner.test.TestSetup;
import java.io.File;
import java.util.LinkedList;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** @author vahi */
public class DAXParser5Test {

    /** The properties used for this test. */
    private static final String PROPERTIES_BASENAME = "properties";

    private PegasusBag mBag;

    private PegasusProperties mProps;

    private LogManager mLogger;

    private TestSetup mTestSetup;

    /** The parsed DAX file */
    private ADag mParsedDAX;

    @BeforeClass
    public static void setUpClass() {}

    @AfterClass
    public static void tearDownClass() {}

    /** Setup the logger and properties that all test functions require */
    @Before
    public final void setUp() {
        mTestSetup = new DefaultTestSetup();
        mBag = new PegasusBag();

        mTestSetup.setInputDirectory(this.getClass());
        // we pick up from the parser
        System.out.println("Input Test Dir is " + mTestSetup.getInputDirectory());

        mProps = mTestSetup.loadPropertiesFromFile(PROPERTIES_BASENAME, new LinkedList());
        mBag.add(PegasusBag.PEGASUS_PROPERTIES, mProps);

        mLogger = mTestSetup.loadLogger(mProps);
        mLogger.logEventStart("test.planner.parser.dax", "setup", "0");
        mBag.add(PegasusBag.PEGASUS_LOGMANAGER, mLogger);

        /* instantiate the DAX YAML and start parsing */
        String dax = new File(mTestSetup.getInputDirectory(), "workflow.yml").getAbsolutePath();
        DAXParser p =
                (DAXParser)
                        DAXParserFactory.loadDAXParser(
                                mBag, DAXParserFactory.DEFAULT_CALLBACK_CLASS, dax);
        Callback cb = ((DAXParser) p).getDAXCallback();
        p.parse(dax);
        mParsedDAX = (ADag) cb.getConstructedObject();
        mLogger.logEventCompletion();
    }

    @Test
    public void testWorkflowTaskMetrics() {
        WorkflowMetrics metrics = mParsedDAX.getWorkflowMetrics();
        // System.err.println(metrics);
        assertEquals("number of compute tasks ", 4, metrics.getTaskCount(Job.COMPUTE_JOB));
        assertEquals("number of dag tasks ", 0, metrics.getTaskCount(Job.DAG_JOB));
        assertEquals("number of dax tasks ", 0, metrics.getTaskCount(Job.DAX_JOB));
    }

    @Test
    public void testWorkflowFileMetrics() {
        WorkflowMetrics metrics = mParsedDAX.getWorkflowMetrics();
        assertEquals(
                "number of raw input files ",
                1,
                metrics.getNumDAXFiles(WorkflowMetrics.FILE_TYPE.input));
        assertEquals(
                "number of intermediate files ",
                4,
                metrics.getNumDAXFiles(WorkflowMetrics.FILE_TYPE.intermediate));
        assertEquals(
                "number of output files ",
                1,
                metrics.getNumDAXFiles(WorkflowMetrics.FILE_TYPE.output));
        assertEquals(
                "number of total files ",
                6,
                metrics.getNumDAXFiles(WorkflowMetrics.FILE_TYPE.total));
    }

    @After
    public void tearDown() {
        mLogger = null;
        mProps = null;
        mBag = null;
        mTestSetup = null;
    }
}
