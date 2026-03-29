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

/** Tests for the Processor class. */
public class ProcessorTest {
    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testInstantiation() {
        Processor p = new Processor();
        assertNotNull(p);
    }

    @Test
    public void testGetAvailableTimeInitiallyReturnsStart() {
        Processor p = new Processor();
        // When no job scheduled, endTime=0. If start > endTime, return start
        assertEquals(5L, p.getAvailableTime(5L));
    }

    @Test
    public void testGetAvailableTimeWhenEndTimeGreaterThanStart() {
        Processor p = new Processor();
        p.scheduleJob(0L, 100L);
        // endTime=100, start=50: since 100 > 50, return endTime=100
        assertEquals(100L, p.getAvailableTime(50L));
    }

    @Test
    public void testGetAvailableTimeWhenStartGreaterThanEndTime() {
        Processor p = new Processor();
        p.scheduleJob(0L, 10L);
        // endTime=10, start=50: since 10 <= 50, return start=50
        assertEquals(50L, p.getAvailableTime(50L));
    }

    @Test
    public void testScheduleJob() {
        Processor p = new Processor();
        p.scheduleJob(10L, 20L);
        // After scheduling, end time is 20. start=0 < 20, so available time = 20
        assertEquals(20L, p.getAvailableTime(0L));
    }

    @Test
    public void testProcessorClassIsNotAbstract() {
        assertFalse(java.lang.reflect.Modifier.isAbstract(Processor.class.getModifiers()));
    }
}
