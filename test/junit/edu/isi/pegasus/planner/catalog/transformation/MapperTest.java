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
package edu.isi.pegasus.planner.catalog.transformation;

import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.test.DefaultTestSetup;
import edu.isi.pegasus.planner.test.TestSetup;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for the abstract Mapper class — exercised via an anonymous concrete subclass and the static
 * constants.
 */
public class MapperTest {

    private TestSetup mTestSetup;
    private PegasusBag mBag;

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
    }

    @AfterEach
    public void tearDown() {}

    @Test
    public void testPackageNameConstantIsCorrect() {
        assertEquals("edu.isi.pegasus.planner.catalog.transformation.mapper", Mapper.PACKAGE_NAME);
    }

    @Test
    public void testPackageNameConstantIsNonEmpty() {
        assertNotNull(Mapper.PACKAGE_NAME);
        assertFalse(Mapper.PACKAGE_NAME.isEmpty());
    }

    @Test
    public void testAnonymousSubclassCanBeInstantiated() {
        Mapper mapper = createMinimalMapper();
        assertNotNull(mapper);
    }

    @Test
    public void testGetModeReturnedBySubclass() {
        Mapper mapper = createMinimalMapper();
        assertEquals("TestMode", mapper.getMode());
    }

    @Test
    public void testIsStageableMapperReturnsFalseForNonStageableMapper() {
        Mapper mapper = createMinimalMapper();
        // The anonymous subclass is neither Staged nor Submit, so should be false
        assertFalse(mapper.isStageableMapper());
    }

    // Helper that constructs a trivial concrete Mapper for testing abstract class methods.
    private Mapper createMinimalMapper() {
        return new Mapper(mBag) {
            @Override
            public Map getSiteMap(String namespace, String name, String version, List siteids) {
                return null;
            }

            @Override
            public String getMode() {
                return "TestMode";
            }
        };
    }
}
