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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** Tests for the Processor class. */
public class ProcessorTest {

    @Test
    public void testInstantiation() {
        Processor p = new Processor();
        assertThat(p, notNullValue());
    }

    @Test
    public void testGetAvailableTimeInitiallyReturnsStart() {
        Processor p = new Processor();
        // When no job scheduled, endTime=0. If start > endTime, return start
        assertThat(p.getAvailableTime(5L), is(5L));
    }

    @Test
    public void testGetAvailableTimeWhenEndTimeGreaterThanStart() {
        Processor p = new Processor();
        p.scheduleJob(0L, 100L);
        // endTime=100, start=50: since 100 > 50, return endTime=100
        assertThat(p.getAvailableTime(50L), is(100L));
    }

    @Test
    public void testGetAvailableTimeWhenStartGreaterThanEndTime() {
        Processor p = new Processor();
        p.scheduleJob(0L, 10L);
        // endTime=10, start=50: since 10 <= 50, return start=50
        assertThat(p.getAvailableTime(50L), is(50L));
    }

    @Test
    public void testScheduleJob() {
        Processor p = new Processor();
        p.scheduleJob(10L, 20L);
        // After scheduling, end time is 20. start=0 < 20, so available time = 20
        assertThat(p.getAvailableTime(0L), is(20L));
    }

    @Test
    public void testConstructorInitializesPrivateFieldsToZero() throws Exception {
        Processor p = new Processor();
        assertThat((Long) ReflectionTestUtils.getField(p, "mStartTime"), is(0L));
        assertThat((Long) ReflectionTestUtils.getField(p, "mEndTime"), is(0L));
    }

    @Test
    public void testGetAvailableTimeReturnsStartWhenEqualToEndTime() {
        Processor p = new Processor();
        p.scheduleJob(10L, 20L);

        assertThat(p.getAvailableTime(20L), is(20L));
    }

    @Test
    public void testScheduleJobUpdatesPrivateStartAndEndTimeFields() throws Exception {
        Processor p = new Processor();
        p.scheduleJob(7L, 13L);
        assertThat((Long) ReflectionTestUtils.getField(p, "mStartTime"), is(7L));
        assertThat((Long) ReflectionTestUtils.getField(p, "mEndTime"), is(13L));
    }
}
