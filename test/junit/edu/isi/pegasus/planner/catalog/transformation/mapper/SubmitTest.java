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
package edu.isi.pegasus.planner.catalog.transformation.mapper;

import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.test.DefaultTestSetup;
import edu.isi.pegasus.planner.test.TestSetup;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for the Submit TC mapper. */
public class SubmitTest {

    private PegasusBag mBag;
    private TestSetup mTestSetup;
    private Submit mMapper;

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {
        mTestSetup = new DefaultTestSetup();
        mBag = new PegasusBag();
        PegasusProperties properties = PegasusProperties.nonSingletonInstance();
        mBag.add(PegasusBag.PEGASUS_PROPERTIES, properties);
        LogManager logger = mTestSetup.loadLogger(properties);
        mBag.add(PegasusBag.PEGASUS_LOGMANAGER, logger);
        mMapper = new Submit(mBag);
    }

    @AfterEach
    public void tearDown() {}

    @Test
    public void testSubmitMapperCanBeInstantiated() {
        assertNotNull(mMapper);
    }

    @Test
    public void testGetModeReturnsNonNullString() {
        assertNotNull(mMapper.getMode());
    }

    @Test
    public void testGetModeContainsLocalKeyword() {
        String mode = mMapper.getMode();
        assertTrue(
                mode.toLowerCase().contains("local"),
                "Mode description should mention 'local', got: " + mode);
    }

    @Test
    public void testGetModeContainsStageableKeyword() {
        String mode = mMapper.getMode();
        assertTrue(
                mode.toLowerCase().contains("stageable"),
                "Mode description should mention 'stageable', got: " + mode);
    }

    @Test
    public void testIsStageableMapper() {
        // Submit extends Mapper and should be detected as stageable
        assertTrue(mMapper.isStageableMapper());
    }

    @Test
    public void testIsInstanceOfMapper() {
        assertTrue(mMapper instanceof edu.isi.pegasus.planner.catalog.transformation.Mapper);
    }

    @Test
    public void testGetModeIsNonEmpty() {
        assertFalse(mMapper.getMode().isEmpty());
    }
}
