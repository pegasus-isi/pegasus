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
package edu.isi.pegasus.planner.selector.site.heft;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for the HEFT Algorithm class constants. */
public class AlgorithmTest {
    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testAverageBandwidth() {
        assertEquals(5.0f, Algorithm.AVERAGE_BANDWIDTH, 0.001f);
    }

    @Test
    public void testAverageDataSizeBetweenJobs() {
        assertEquals(2.0f, Algorithm.AVERAGE_DATA_SIZE_BETWEEN_JOBS, 0.001f);
    }

    @Test
    public void testDefaultNumberOfFreeNodes() {
        assertEquals(10, Algorithm.DEFAULT_NUMBER_OF_FREE_NODES);
    }

    @Test
    public void testMaximumFinishTime() {
        assertEquals(Long.MAX_VALUE, Algorithm.MAXIMUM_FINISH_TIME);
    }

    @Test
    public void testRuntimeProfileKeyNotNull() {
        assertNotNull(Algorithm.RUNTIME_PROFILE_KEY);
    }

    @Test
    public void testAlgorithmClassIsNotAbstract() {
        assertFalse(java.lang.reflect.Modifier.isAbstract(Algorithm.class.getModifiers()));
    }

    @Test
    public void testAlgorithmClassIsNotInterface() {
        assertFalse(Algorithm.class.isInterface());
    }
}
