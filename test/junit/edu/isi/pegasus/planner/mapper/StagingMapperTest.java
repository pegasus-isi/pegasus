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
package edu.isi.pegasus.planner.mapper;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for the StagingMapper interface structure. */
public class StagingMapperTest {

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testStagingMapperIsInterface() {
        assertTrue(StagingMapper.class.isInterface());
    }

    @Test
    public void testPropertyPrefixConstant() {
        assertEquals("pegasus.dir.staging.mapper", StagingMapper.PROPERTY_PREFIX);
    }

    @Test
    public void testVersionConstant() {
        assertEquals("1.0", StagingMapper.VERSION);
    }

    @Test
    public void testStagingMapperExtendsMapper() {
        assertTrue(Mapper.class.isAssignableFrom(StagingMapper.class));
    }

    @Test
    public void testInitializeMethodExists() throws NoSuchMethodException {
        assertNotNull(
                StagingMapper.class.getMethod(
                        "initialize",
                        edu.isi.pegasus.planner.classes.PegasusBag.class,
                        java.util.Properties.class));
    }
}
