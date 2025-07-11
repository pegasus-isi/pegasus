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
package edu.isi.pegasus.common.util;

import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.test.DefaultTestSetup;
import edu.isi.pegasus.planner.test.TestSetup;
import java.util.LinkedList;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test class to test the PegasusURL class and check if it parses the various URL combinations
 *
 * @author Karan Vahi
 */
public class PegasusURLTest {

    private static int mTestNumber = 1;

    private TestSetup mTestSetup;
    private LogManager mLogger;

    public PegasusURLTest() {}

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {
        mTestSetup = new DefaultTestSetup();

        mTestSetup.setInputDirectory(this.getClass());
        System.out.println("Input Test Dir is " + mTestSetup.getInputDirectory());

        mLogger =
                mTestSetup.loadLogger(
                        mTestSetup.loadPropertiesFromFile(".properties", new LinkedList()));
        mLogger.logEventStart("test.pegasus.url", "setup", "0");
    }

    @Test
    public void testGridFTPURL() {
        // should print
        // protocol -> gsiftp , host -> sukhna.isi.edu , path -> /tmp/test.file , url-prefix ->
        // gsiftp://sukhna.isi.edu
        mLogger.logEventStart(
                "test.common.util.PegasusURL", "set", Integer.toString(mTestNumber++));
        testURL(
                "gsiftp://sukhna.isi.edu/tmp/test.file",
                "gsiftp",
                "sukhna.isi.edu",
                "/tmp/test.file",
                "gsiftp://sukhna.isi.edu");
        mLogger.logEventCompletion();
    }

    @Test
    public void testGridFTPULRWithHome() {
        // protocol -> gsiftp , host -> dataserver.phys.uwm.edu , path ->
        // /~/griphyn_test/ligodemo_output/ , url-prefix -> gsiftp://dataserver.phys.uwm.edu
        // "gsiftp://dataserver.phys.uwm.edu/~/griphyn_test/ligodemo_output/" ;
        mLogger.logEventStart(
                "test.common.util.PegasusURL", "set", Integer.toString(mTestNumber++));
        testURL(
                "gsiftp://dataserver.phys.uwm.edu/~/griphyn_test/ligodemo_output/",
                "gsiftp",
                "dataserver.phys.uwm.edu",
                "/~/griphyn_test/ligodemo_output/",
                "gsiftp://dataserver.phys.uwm.edu");
        mLogger.logEventCompletion();
    }

    @Test
    public void testFileURL() {
        // protocol -> file , host ->  , path -> /tmp/test/k , url-prefix -> file://
        mLogger.logEventStart(
                "test.common.util.PegasusURL", "set", Integer.toString(mTestNumber++));
        testURL("file:///tmp/test/k", "file", "", "/tmp/test/k", "file://");
        mLogger.logEventCompletion();
    }

    @Test
    public void testAbsolutePath() {
        // protocol -> file , host ->  , path -> /tmp/path/to/input/file , url-prefix -> file://
        // url =  "/tmp/path/to/input/file"
        mLogger.logEventStart(
                "test.common.util.PegasusURL", "set", Integer.toString(mTestNumber++));
        testURL("/tmp/path/to/input/file", "file", "", "/tmp/path/to/input/file", "file://");
        mLogger.logEventCompletion();
    }

    @Test
    public void testHttpURLTrailing() {
        // url =  "http://isis.isi.edu" ;
        mLogger.logEventStart(
                "test.common.util.PegasusURL", "set", Integer.toString(mTestNumber++));
        testURL("http://isis.isi.edu", "http", "isis.isi.edu", "", "http://isis.isi.edu");
        mLogger.logEventCompletion();
    }

    @Test
    public void testHttpURLTrailingSlash() {
        // url =  "http://isis.isi.edu/" ;
        mLogger.logEventStart(
                "test.common.util.PegasusURL", "set", Integer.toString(mTestNumber++));
        testURL("http://isis.isi.edu/", "http", "isis.isi.edu", "/", "http://isis.isi.edu");
        mLogger.logEventCompletion();
    }

    @Test
    public void testMoveToURL() {
        mLogger.logEventStart(
                "test.common.util.PegasusURL", "set", Integer.toString(mTestNumber++));
        testURL(
                "moveto:///test/dir",
                PegasusURL.MOVETO_PROTOCOL,
                "",
                "/test/dir",
                PegasusURL.MOVETO_PROTOCOL_SCHEME + "//");
        mLogger.logEventCompletion();
    }

    @Test
    public void testHTTPURLWithSpace() {
        mLogger.logEventStart(
                "test.common.util.PegasusURL", "set", Integer.toString(mTestNumber++));
        testURL(
                "http://pegasus.isi.edu/data/f a",
                "http",
                "pegasus.isi.edu",
                "/data/f a",
                "http://pegasus.isi.edu");
        mLogger.logEventCompletion();
    }

    @Test
    public void testFileURLWithSpace() {
        mLogger.logEventStart(
                "test.common.util.PegasusURL", "set", Integer.toString(mTestNumber++));
        testURL("file:///tmp/test/f a", "file", "", "/tmp/test/f a", "file://");
        mLogger.logEventCompletion();
    }

    @AfterEach
    public void tearDown() {
        mLogger = null;
        mTestSetup = null;
    }

    private void testURL(String url, String protocol, String host, String path, String urlPrefix) {
        System.out.println("Testing URL " + url);
        PegasusURL pURL = new PegasusURL(url);
        assertEquals(protocol, pURL.getProtocol(), url + " unable to parse protocol ");
        assertEquals(host, pURL.getHost(), url + " unable to parse host ");
        assertEquals(path, pURL.getPath(), url + " unable to parse path ");
        assertEquals(urlPrefix, pURL.getURLPrefix(), url + " unable to parse urlprefix ");
    }
}
