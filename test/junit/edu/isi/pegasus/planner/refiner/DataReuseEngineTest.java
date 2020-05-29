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
package edu.isi.pegasus.planner.refiner;

import static org.junit.Assert.*;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PegasusFile;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.parser.DAXParserFactory;
import edu.isi.pegasus.planner.parser.XMLParser;
import edu.isi.pegasus.planner.parser.dax.Callback;
import edu.isi.pegasus.planner.parser.dax.DAXParser;
import edu.isi.pegasus.planner.partitioner.graph.GraphNode;
import edu.isi.pegasus.planner.test.DefaultTestSetup;
import edu.isi.pegasus.planner.test.TestSetup;
import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * A JUnit Test to test the DataReuseEngine
 *
 * @author Karan Vahi
 */
public class DataReuseEngineTest {

    /** The properties used for this test. */
    private static final String PROPERTIES_BASENAME = "properties";

    private PegasusBag mBag;

    private PegasusProperties mProps;

    private LogManager mLogger;

    private TestSetup mTestSetup;

    private static int mTestNumber = 1;

    @BeforeClass
    public static void setUpClass() {}

    @AfterClass
    public static void tearDownClass() {}

    public DataReuseEngineTest() {}

    /** Setup the logger and properties that all test functions require */
    @Before
    public final void setUp() {
        mTestSetup = new DataReuseEngineTestSetup();
        mBag = new PegasusBag();
        mTestSetup.setInputDirectory(this.getClass());
        System.out.println("Input Test Dir is " + mTestSetup.getInputDirectory());

        mProps =
                mTestSetup.loadPropertiesFromFile(
                        PROPERTIES_BASENAME, this.getPropertyKeysForSanitization());
        mBag.add(PegasusBag.PEGASUS_PROPERTIES, mProps);

        mLogger = mTestSetup.loadLogger(mProps);
        mLogger.setLevel(LogManager.DEBUG_MESSAGE_LEVEL);
        mLogger.logEventStart("test.refiner.datareuse", "setup", "0");
        mBag.add(PegasusBag.PEGASUS_LOGMANAGER, mLogger);

        mBag.add(PegasusBag.PLANNER_OPTIONS, mTestSetup.loadPlannerOptions());

        mLogger.logEventCompletion();
    }

    /** Test for cascading of data reuse. */
    @Test
    public void testCascading() {

        mLogger.logEventStart("test.refiner.datareuse", "set", Integer.toString(mTestNumber++));
        ADag dax = ((DataReuseEngineTestSetup) mTestSetup).loadDAX(mBag, "pipeline.dax");
        MyReplicaCatalogBridge rcb = new MyReplicaCatalogBridge(dax, mBag);

        Set<String> filesInRC = new HashSet();
        filesInRC.add("HN001_addrepl.bai");
        filesInRC.add("HN001_addrepl.bam");
        filesInRC.add("HN001_indel_realigned.bai");
        filesInRC.add("HN001_indel_realigned.bam");
        filesInRC.add("HN001_aligned_reads.sam");
        filesInRC.add("HN001_reduced_reads.bai");
        filesInRC.add("HN001_reduced_reads.bam");
        filesInRC.add("raw_indel.vcf");
        filesInRC.add("raw_snp.vcf");
        rcb.addFilesInReplica(filesInRC);

        DataReuseEngine engine = new DataReuseEngine(dax, mBag);
        engine.reduceWorkflow(dax, rcb);
        Job[] actualDeletedJobs = (Job[]) engine.getDeletedJobs().toArray(new Job[0]);

        String[] expectedDeletedJobs = {
            "add_replace_ID0000005", "alignment_to_reference_ID0000008", "dedup_ID0000006",
            "indel_realign_ID0000003", "realign_target_creator_ID0000004", "reduce_reads_ID0000002",
            "sort_sam_ID0000007", "unified_genotyper_indel_ID0000011",
                    "unified_genotyper_snp_ID0000009",
        };
        assertArrayEquals(
                "Deleted Jobs don't match ",
                expectedDeletedJobs,
                toSortedStringArray(actualDeletedJobs));
        mLogger.logEventCompletion();
        System.out.println("\n");
    }

    /**
     * Tests the cascading of deletion of jobs upwards, where the job to be deleted because of
     * cascading has an intermediate output file that exists in the RC Normally, the job will not be
     * deleted in the cascading phase as the output file is required by the user, unless it was
     * already identified for deletion in the first pass.
     */
    @Test
    public void testCascadingIntermediateOutputInRC() {

        mLogger.logEventStart("test.refiner.datareuse", "set", Integer.toString(mTestNumber++));
        ADag dax = ((DataReuseEngineTestSetup) mTestSetup).loadDAX(mBag, "blackdiamond.dax");
        MyReplicaCatalogBridge rcb = new MyReplicaCatalogBridge(dax, mBag);

        // retrieve the right findrange job and make sure
        // one of the output files has transfer set to true
        // and other has it set to false.
        GraphNode n = dax.getNode("findrange_ID0000003");
        Job findrange = (Job) n.getContent();
        for (PegasusFile pf : findrange.getOutputFiles()) {

            System.out.println(pf);
            if (pf.getLFN().equals("f.c2")) {
                pf.setTransferFlag("true");
            } else if (pf.getLFN().equals("f.c2'")) {
                pf.setTransferFlag("false");
            }
            System.out.println(pf);
        }

        Set<String> filesInRC = new HashSet();
        filesInRC.add("f.d");
        filesInRC.add("f.c2"); // only the output file with transfer set to true is in RC
        rcb.addFilesInReplica(filesInRC);

        DataReuseEngine engine = new DataReuseEngine(dax, mBag);
        engine.reduceWorkflow(dax, rcb);
        Job[] actualDeletedJobs = (Job[]) engine.getDeletedJobs().toArray(new Job[0]);

        // findrange_ID0000003 is deleted in the cascading phase
        // because user only wants f.c2 staged to output site and
        // that exists in the RC somewhere
        String[] expectedDeletedJobs = {
            "analyze_ID0000004", "findrange_ID0000003",
        };
        assertArrayEquals(
                "Deleted Jobs don't match ",
                expectedDeletedJobs,
                toSortedStringArray(actualDeletedJobs));
        mLogger.logEventCompletion();
        System.out.println("\n");
    }

    /**
     * Test for reducing the whole workflow. In this test, some of intermediate jobs, have output
     * files marked with transfer set to true. Hence, those jobs are only removed, if the
     * intermediate files also exist in the Replica Catalog
     */
    @Test
    public void testFullReduction() {
        mLogger.logEventStart(
                "test.refiner.datareuse.fullreduction", "set", Integer.toString(mTestNumber++));
        ADag dax = ((DataReuseEngineTestSetup) mTestSetup).loadDAX(mBag, "pipeline.dax");
        MyReplicaCatalogBridge rcb = new MyReplicaCatalogBridge(dax, mBag);

        Set<String> filesInRC = new HashSet();
        filesInRC.add("HN001_addrepl.bai");
        filesInRC.add("HN001_addrepl.bam");
        filesInRC.add("HN001_indel_realigned.bai");
        filesInRC.add("HN001_indel_realigned.bam");
        filesInRC.add("HN001_aligned_reads.sam");
        filesInRC.add("HN001_reduced_reads.bai");
        filesInRC.add("HN001_reduced_reads.bam");
        filesInRC.add("raw_indel.vcf");
        filesInRC.add("raw_snp.vcf");
        filesInRC.add("filtered_indel.vcf");
        filesInRC.add("filtered_snp.vcf");
        rcb.addFilesInReplica(filesInRC);

        DataReuseEngine engine = new DataReuseEngine(dax, mBag);
        ADag reducedDAG = engine.reduceWorkflow(dax, rcb);
        Job[] actualDeletedJobs = (Job[]) engine.getDeletedJobs().toArray(new Job[0]);

        String[] expectedDeletedJobs = {
            "add_replace_ID0000005", "alignment_to_reference_ID0000008", "dedup_ID0000006",
            "filtering_indel_ID0000012", "filtering_snp_ID0000010", "indel_realign_ID0000003",
            "realign_target_creator_ID0000004", "reduce_reads_ID0000002", "sort_sam_ID0000007",
            "unified_genotyper_indel_ID0000011", "unified_genotyper_snp_ID0000009",
        };
        assertArrayEquals(
                "Deleted Jobs don't match ",
                expectedDeletedJobs,
                toSortedStringArray(actualDeletedJobs));
        mLogger.logEventCompletion();
        System.out.println("\n");
    }

    /**
     * Test for reducing the whole workflow.
     *
     * <p>In this test, only the leaf jobs, have output files marked with transfer set to true.
     * Hence for full reduction, only the outputs of the leaf jobs need to be present in the Replica
     * Catalog. All other intermediate files in the workflow have transfer set to false
     */
    @Test
    public void testFullReductionLeafDAX() {

        mLogger.logEventStart(
                "test.refiner.datareuse.fullreduction-leaf",
                "set",
                Integer.toString(mTestNumber++));
        // only the leaf jobs have the transfer set to true for output files
        ADag dax = ((DataReuseEngineTestSetup) mTestSetup).loadDAX(mBag, "pipeline-leaf.dax");
        MyReplicaCatalogBridge rcb = new MyReplicaCatalogBridge(dax, mBag);

        Set<String> filesInRC = new HashSet();
        filesInRC.add("filtered_indel.vcf");
        filesInRC.add("filtered_snp.vcf");
        rcb.addFilesInReplica(filesInRC);

        DataReuseEngine engine = new DataReuseEngine(dax, mBag);
        ADag reducedDAG = engine.reduceWorkflow(dax, rcb);
        Job[] actualDeletedJobs = (Job[]) engine.getDeletedJobs().toArray(new Job[0]);

        String[] expectedDeletedJobs = {
            "add_replace_ID0000005", "alignment_to_reference_ID0000008", "dedup_ID0000006",
            "filtering_indel_ID0000012", "filtering_snp_ID0000010", "indel_realign_ID0000003",
            "realign_target_creator_ID0000004", "reduce_reads_ID0000002", "sort_sam_ID0000007",
            "unified_genotyper_indel_ID0000011", "unified_genotyper_snp_ID0000009",
        };
        assertArrayEquals(
                "Deleted Jobs don't match ",
                expectedDeletedJobs,
                toSortedStringArray(actualDeletedJobs));
        mLogger.logEventCompletion();
        System.out.println("\n");
    }

    /** Test for partial data reuse. */
    @Test
    public void testPartialDataReuse() {

        mLogger.logEventStart("test.refiner.datareuse", "set", Integer.toString(mTestNumber++));
        ADag dax = ((DataReuseEngineTestSetup) mTestSetup).loadDAX(mBag, "blackdiamond.dax");
        MyReplicaCatalogBridge rcb = new MyReplicaCatalogBridge(dax, mBag);

        // turn on partial data reuse
        mProps.setProperty("pegasus.data.reuse.scope", "partial");

        // all output files are in the replica catalog
        // however findrange_ID0000002 output file needs to be checked for
        // in the RC for datareuse. PM-774
        Set<String> filesInRC = new HashSet();
        filesInRC.add("f.a");
        filesInRC.add("f.b1");
        filesInRC.add("f.b2");
        filesInRC.add("f.c1");
        filesInRC.add("f.c2");
        filesInRC.add("f.d");
        rcb.addFilesInReplica(filesInRC);

        DataReuseEngine engine = new DataReuseEngine(dax, mBag);
        engine.reduceWorkflow(dax, rcb);
        Job[] actualDeletedJobs = (Job[]) engine.getDeletedJobs().toArray(new Job[0]);

        String[] expectedDeletedJobs = {
            "findrange_ID0000002",
        };
        assertArrayEquals(
                "Deleted Jobs don't match ",
                expectedDeletedJobs,
                toSortedStringArray(actualDeletedJobs));
        mLogger.logEventCompletion();
        System.out.println("\n");

        mProps.removeProperty("pegasus.data.reuse.scope");
    }

    @After
    public void tearDown() {
        mLogger = null;
        mProps = null;
        mBag = null;
        mTestSetup = null;
    }

    /**
     * Convenience method
     *
     * @param array
     * @return
     */
    protected String[] toSortedStringArray(Job[] array) {
        String[] result = new String[array.length];
        int i = 0;
        for (Job job : array) {
            result[i++] = job.getID();
        }
        Arrays.sort(result);
        return result;
    }

    /**
     * Returns the list of property keys that should be sanitized
     *
     * @return List<String>
     */
    protected List<String> getPropertyKeysForSanitization() {
        List<String> keys = new LinkedList();
        return keys;
    }

    private static class MyReplicaCatalogBridge extends ReplicaCatalogBridge {
        private final PegasusBag bag;
        private Set<String> mFiles;

        public MyReplicaCatalogBridge(ADag dax, PegasusBag bag) {
            super(dax, bag);
            this.bag = bag;
        }

        public void addFilesInReplica(Set<String> files) {
            this.mFiles = files;
        }

        public Set getFilesInReplica() {
            return this.mFiles;
        }
    }
}
/**
 * A default test setup implementation for the junit tests.
 *
 * @author Karan Vahi
 */
class DataReuseEngineTestSetup implements TestSetup {

    /** The input directory for the test. */
    private String mTestInputDir;

    /** The Default Testup that this uses */
    private DefaultTestSetup mDefaultTestSetup;

    /** The default constructor. */
    public DataReuseEngineTestSetup() {
        mTestInputDir = ".";
        mDefaultTestSetup = new DefaultTestSetup();
    }

    /**
     * Set the input directory for the test on the basis of the classname of test class
     *
     * @param testClass the test class.
     */
    public void setInputDirectory(Class testClass) {
        mDefaultTestSetup.setInputDirectory(testClass);
        // append dataruse to the input directory
        mDefaultTestSetup.setInputDirectory(
                mDefaultTestSetup.getInputDirectory() + File.separator + "datareuse");
    }

    /**
     * Set the input directory for the test.
     *
     * @param directory the directory
     */
    public void setInputDirectory(String directory) {
        mDefaultTestSetup.setInputDirectory(directory);
    }

    /**
     * Returns the input directory set by the test.
     *
     * @return
     */
    public String getInputDirectory() {
        return mDefaultTestSetup.getInputDirectory();
    }

    /**
     * Loads up PegasusProperties properties.
     *
     * @param sanitizeKeys list of keys to be sanitized
     * @return
     */
    public PegasusProperties loadProperties(List<String> sanitizeKeys) {
        return mDefaultTestSetup.loadProperties(sanitizeKeys);
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
        return mDefaultTestSetup.loadPropertiesFromFile(propertiesBasename, sanitizeKeys);
    }

    /**
     * Loads the logger from the properties and sets default level to INFO
     *
     * @param properties
     * @return
     */
    public LogManager loadLogger(PegasusProperties properties) {
        return mDefaultTestSetup.loadLogger(properties);
    }

    /**
     * Loads the planner options for the test
     *
     * @return
     */
    public PlannerOptions loadPlannerOptions() {
        PlannerOptions options = new PlannerOptions();
        options.addOutputSite("local");
        return options;
    }

    /**
     * Parses and loads the DAX
     *
     * @param dax the dax file basename in the input directory
     * @return
     */
    public ADag loadDAX(PegasusBag bag, String dax) {
        dax = this.getInputDirectory() + File.separator + dax;
        // load the parser and parse the dax
        XMLParser p =
                (XMLParser)
                        DAXParserFactory.loadDAXParser(
                                bag, DAXParserFactory.DEFAULT_CALLBACK_CLASS, dax);
        Callback cb = ((DAXParser) p).getDAXCallback();
        p.startParser(dax);

        return (ADag) cb.getConstructedObject();
    }

    /**
     * Loads up the SiteStore with the sites passed in list of sites.
     *
     * @param props the properties
     * @param logger the logger
     * @param sites the list of sites to load
     * @return the SiteStore
     */
    public SiteStore loadSiteStore(PegasusProperties props, LogManager logger, List<String> sites) {
        return mDefaultTestSetup.loadSiteStore(props, logger, sites);
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
        return mDefaultTestSetup.loadSiteStoreFromFile(props, logger, sites);
    }
}
