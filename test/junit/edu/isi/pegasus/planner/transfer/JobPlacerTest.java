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
package edu.isi.pegasus.planner.transfer;

import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.util.FactoryException;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.classes.ADag;
import edu.isi.pegasus.planner.classes.FileTransfer;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PegasusFile;
import edu.isi.pegasus.planner.classes.PlannerOptions;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.test.DefaultTestSetup;
import edu.isi.pegasus.planner.test.TestSetup;
import edu.isi.pegasus.planner.transfer.refiner.RefinerFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

// import org.junit.jupiter.api.Test;
/** @author Karan Vahi */
public class JobPlacerTest {

    private TestSetup mTestSetup;

    private PegasusBag mBag;

    private PegasusProperties mProps;

    private LogManager mLogger;

    private static int mTestNumber = 1;

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    private Refiner mTXRefiner;

    @BeforeEach
    public void setUp() {

        mTestSetup = new DefaultTestSetup();
        mTestSetup.setInputDirectory(this.getClass());
        mBag = new PegasusBag();
        mTestSetup.setInputDirectory(this.getClass());
        System.out.println("Input Test Dir is " + mTestSetup.getInputDirectory());

        mProps = PegasusProperties.nonSingletonInstance();
        mBag.add(PegasusBag.PEGASUS_PROPERTIES, mProps);

        PlannerOptions options = new PlannerOptions();
        mBag.add(PegasusBag.PLANNER_OPTIONS, options);

        mLogger = mTestSetup.loadLogger(mProps);
        mLogger.setLevel(LogManager.DEBUG_MESSAGE_LEVEL);
        mLogger.logEventStart("test.transfer.refiner.JobPlacer", "setup", "0");
        mBag.add(PegasusBag.PEGASUS_LOGMANAGER, mLogger);

        try {
            mTXRefiner = RefinerFactory.loadInstance(new ADag(), mBag);
        } catch (Exception e) {
            // wrap all the exceptions into a factory exception
            throw new FactoryException("Transfer Engine ", e);
        }
    }

    @AfterEach
    public void tearDown() {}

    @ParameterizedTest
    @CsvSource(
            value = {
                // ftForContainerToSubmitHost forSymlink isLocalTransfer isRemoteTransfer
                // if ftForContainerToSubmitHost is set, then return always false
                "true, false, false, false, false",
                "true, false, false, true, false",
                "true, false, true, false, false",
                "true, false, true, true, false",
                "true, true, false, false, false",
                "true, true, false, true, false",
                "true, true, true, false, false",
                "true, true, true, true, false",
                // symlink being true means run remotely
                "false, true, true, true, true",
                "false, true, false, false, true",
                "false, true, false, true, true",
                "false, true, true, false, true",
                // set true for local transfer by default, but isRemoteTx set based on source URL
                "false, false, true, false, false",
                "false, false, true, true, true",
                // set false for local transfer by default, but isRemoteTx set based on source URL
                "false, false, false, true, true",
                "false, false, false, false, true",
            },
            nullValues = {"null"})
    public void testRunTransferRemotely(
            String ftForContainerToSubmitHost,
            String forSymlink,
            String isLocalTransfer,
            String isRemoteTransfer,
            String expectedValue) {

        JobPlacer jp = new JobPlacer(mTXRefiner);
        boolean expected = Boolean.parseBoolean(expectedValue);

        assertEquals(
                expected,
                jp.runTransferRemotely(
                        Boolean.parseBoolean(ftForContainerToSubmitHost),
                        Boolean.parseBoolean(forSymlink),
                        Boolean.parseBoolean(isLocalTransfer),
                        Boolean.parseBoolean(isRemoteTransfer)));
    }

    @ParameterizedTest
    @CsvSource(
            value = {
                // sourceSite sourceURL stagingSite stagingVisibleToLocal expectedValue
                // for non file source urls, always run locally (i.e. transfer is false)
                "remote, http://storage.example.com/f.in,  staging, false, false",
                "remote, http://storage.example.com/f.in,  staging, true, false",
                "remote, http://storage.example.com/f.in,  local, false, false",
                "remote, http://storage.example.com/f.in,  local, true, false",
                "local, http://storage.example.com/f.in,  staging, false, false",
                "local, http://storage.example.com/f.in,  staging, true, false",
                "local, http://storage.example.com/f.in,  local, false, false",
                "local, http://storage.example.com/f.in,  local, true, false",
                "staging, http://storage.example.com/f.in,  staging, false, false",
                "staging, http://storage.example.com/f.in,  staging, true, false",
                "staging, http://storage.example.com/f.in,  local, false, false",
                "staging, http://storage.example.com/f.in,  local, true, false",
                // for file URL's matching of source and dest site comes into play
                // if they match, then to be run locally, if stagingIsVisible set to true, else
                // false
                "staging, file:///storage/web/f.in,  staging, true, false",
                "staging, file:///storage/web/f.in,  staging, false, true",
                "local, file:///storage/web/f.in,  staging, true, false",
                "local, file:///storage/web/f.in,  staging, false, false",
                "local, file:///storage/web/f.in,  local, false, true",
                "local, file:///storage/web/f.in,  local, true, false",
            })
    public void testRunTransferRemotelyBasedonSourceURL(
            String sourceSite,
            String sourceURL,
            String stagingSite,
            String stagingVisibleToLocal,
            String expectedValue) {
        JobPlacer jp = new JobPlacer(mTXRefiner);
        boolean expected = Boolean.parseBoolean(expectedValue);

        SiteCatalogEntry stagingSiteEntry = new SiteCatalogEntry(stagingSite);
        stagingSiteEntry.setIsVisibleToLocalSite(Boolean.parseBoolean(stagingVisibleToLocal));

        FileTransfer ft = new FileTransfer(new PegasusFile("f.in"));
        ft.addSource(sourceSite, sourceURL);
        ft.addDestination(stagingSite, "scp://example.com/scratch/f.in");

        assertEquals(expected, jp.runTransferRemotely(stagingSiteEntry, ft));
    }
    /*
    @Test
    public void testSomeMethod() {
        assertEquals(1, 1);
    }
     */
}
