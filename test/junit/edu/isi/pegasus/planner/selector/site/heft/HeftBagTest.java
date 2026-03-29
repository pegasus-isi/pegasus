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

import edu.isi.pegasus.planner.partitioner.graph.Bag;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for the HeftBag class constants and interface. */
public class HeftBagTest {
    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testHeftBagImplementsBag() {
        assertTrue(Bag.class.isAssignableFrom(HeftBag.class));
    }

    @Test
    public void testAvgComputeTimeConstant() {
        assertNotNull(HeftBag.AVG_COMPUTE_TIME);
        assertEquals(0, HeftBag.AVG_COMPUTE_TIME.intValue());
    }

    @Test
    public void testDownwardRankConstant() {
        assertNotNull(HeftBag.DOWNWARD_RANK);
        assertEquals(1, HeftBag.DOWNWARD_RANK.intValue());
    }

    @Test
    public void testUpwardRankConstant() {
        assertNotNull(HeftBag.UPWARD_RANK);
        assertEquals(2, HeftBag.UPWARD_RANK.intValue());
    }

    @Test
    public void testActualStartTimeConstant() {
        assertNotNull(HeftBag.ACTUAL_START_TIME);
        assertEquals(3, HeftBag.ACTUAL_START_TIME.intValue());
    }

    @Test
    public void testActualFinishTimeConstant() {
        assertNotNull(HeftBag.ACTUAL_FINISH_TIME);
        assertEquals(4, HeftBag.ACTUAL_FINISH_TIME.intValue());
    }

    @Test
    public void testScheduledSiteConstant() {
        assertNotNull(HeftBag.SCHEDULED_SITE);
        assertEquals(5, HeftBag.SCHEDULED_SITE.intValue());
    }

    @Test
    public void testHeftInfoArrayLength() {
        assertNotNull(HeftBag.HEFTINFO);
        assertEquals(6, HeftBag.HEFTINFO.length);
    }
}
