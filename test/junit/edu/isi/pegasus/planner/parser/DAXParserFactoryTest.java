/*
 *
 *   Copyright 2007-2020 University Of Southern California
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
package edu.isi.pegasus.planner.parser;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.parser.dax.DAXParser;
import edu.isi.pegasus.planner.parser.dax.DAXParser3;
import edu.isi.pegasus.planner.parser.dax.DAXParser3Test;
import edu.isi.pegasus.planner.parser.dax.DAXParser5;
import edu.isi.pegasus.planner.test.DefaultTestSetup;
import edu.isi.pegasus.planner.test.TestSetup;
import java.io.File;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** @author Karan Vahi */
public class DAXParserFactoryTest {

    private PegasusBag mBag;

    private LogManager mLogger;

    private TestSetup mTestSetup;

    public DAXParserFactoryTest() {}

    @BeforeClass
    public static void setUpClass() {}

    @AfterClass
    public static void tearDownClass() {}

    @Before
    public void setUp() {
        mTestSetup = new DefaultTestSetup();
        mBag = new PegasusBag();
        //the input dir is from the dax subpackage
        mTestSetup.setInputDirectory(DAXParser3Test.class);
       
        System.out.println("Input Test Dir is " + mTestSetup.getInputDirectory());
        PegasusProperties properties = PegasusProperties.nonSingletonInstance();
        mLogger = mTestSetup.loadLogger(properties);
        mLogger.setLevel(LogManager.DEBUG_MESSAGE_LEVEL);
        mLogger.logEventStart("test.planner.parser.DAXParserFactory", "setup", "0");
        mBag.add(PegasusBag.PEGASUS_PROPERTIES, properties);
        mBag.add(PegasusBag.PEGASUS_LOGMANAGER, mLogger);
        mLogger.logEventCompletion();
    }

    @Test
    public void testXMLDAXLoad() {

        mLogger.logEventStart("test.planner.parser.DAXParserFactory", "load", "0");
        String dax = new File(mTestSetup.getInputDirectory(), "blackdiamond.dax").getAbsolutePath();
        DAXParser parser =
                DAXParserFactory.loadDAXParser(mBag, DAXParserFactory.DEFAULT_CALLBACK_CLASS, dax);
        assertThat(parser, instanceOf(DAXParser3.class));
        mLogger.logEventCompletion();
    }

    @Test
    public void testYAMLDAXLoad() {

        mLogger.logEventStart("test.planner.parser.DAXParserFactory", "load", "0");
        String dax = new File(mTestSetup.getInputDirectory(), "workflow.yml").getAbsolutePath();
        DAXParser parser =
                DAXParserFactory.loadDAXParser(mBag, DAXParserFactory.DEFAULT_CALLBACK_CLASS, dax);
        assertThat(parser, instanceOf(DAXParser5.class));
        mLogger.logEventCompletion();
    }

    @After
    public void tearDown() {}
}
