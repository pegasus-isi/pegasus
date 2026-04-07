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
import static org.junit.jupiter.api.Assertions.assertThrows;

import edu.isi.pegasus.planner.partitioner.graph.Bag;
import org.junit.jupiter.api.Test;

/** Tests for the HeftBag class constants and interface. */
public class HeftBagTest {

    @Test
    public void testHeftBagImplementsBag() {
        assertThat(Bag.class.isAssignableFrom(HeftBag.class), is(true));
    }

    @Test
    public void testAvgComputeTimeConstant() {
        assertThat(HeftBag.AVG_COMPUTE_TIME, notNullValue());
        assertThat(HeftBag.AVG_COMPUTE_TIME.intValue(), equalTo(0));
    }

    @Test
    public void testDownwardRankConstant() {
        assertThat(HeftBag.DOWNWARD_RANK, notNullValue());
        assertThat(HeftBag.DOWNWARD_RANK.intValue(), equalTo(1));
    }

    @Test
    public void testUpwardRankConstant() {
        assertThat(HeftBag.UPWARD_RANK, notNullValue());
        assertThat(HeftBag.UPWARD_RANK.intValue(), equalTo(2));
    }

    @Test
    public void testActualStartTimeConstant() {
        assertThat(HeftBag.ACTUAL_START_TIME, notNullValue());
        assertThat(HeftBag.ACTUAL_START_TIME.intValue(), equalTo(3));
    }

    @Test
    public void testActualFinishTimeConstant() {
        assertThat(HeftBag.ACTUAL_FINISH_TIME, notNullValue());
        assertThat(HeftBag.ACTUAL_FINISH_TIME.intValue(), equalTo(4));
    }

    @Test
    public void testScheduledSiteConstant() {
        assertThat(HeftBag.SCHEDULED_SITE, notNullValue());
        assertThat(HeftBag.SCHEDULED_SITE.intValue(), equalTo(5));
    }

    @Test
    public void testHeftInfoArrayLength() {
        assertThat(HeftBag.HEFTINFO, notNullValue());
        assertThat(HeftBag.HEFTINFO.length, equalTo(6));
    }

    @Test
    public void testDefaultConstructorInitializesZeroAndEmptyValues() {
        HeftBag bag = new HeftBag();

        assertThat(
                (double) ((Float) bag.get(HeftBag.AVG_COMPUTE_TIME)).floatValue(),
                closeTo(0.0, 0.001));
        assertThat(
                (double) ((Float) bag.get(HeftBag.DOWNWARD_RANK)).floatValue(),
                closeTo(0.0, 0.001));
        assertThat(
                (double) ((Float) bag.get(HeftBag.UPWARD_RANK)).floatValue(), closeTo(0.0, 0.001));
        assertThat(((Long) bag.get(HeftBag.ACTUAL_START_TIME)).longValue(), equalTo(0L));
        assertThat(((Long) bag.get(HeftBag.ACTUAL_FINISH_TIME)).longValue(), equalTo(0L));
        assertThat(bag.get(HeftBag.SCHEDULED_SITE), equalTo(""));
    }

    @Test
    public void testAddAndGetForAllSupportedKeys() {
        HeftBag bag = new HeftBag();

        assertThat(bag.add(HeftBag.AVG_COMPUTE_TIME, Float.valueOf(3.5f)), is(true));
        assertThat(bag.add(HeftBag.DOWNWARD_RANK, Float.valueOf(7.5f)), is(true));
        assertThat(bag.add(HeftBag.UPWARD_RANK, Float.valueOf(2.5f)), is(true));
        assertThat(bag.add(HeftBag.ACTUAL_START_TIME, Long.valueOf(11L)), is(true));
        assertThat(bag.add(HeftBag.ACTUAL_FINISH_TIME, Long.valueOf(19L)), is(true));
        assertThat(bag.add(HeftBag.SCHEDULED_SITE, "condorpool"), is(true));

        assertThat(
                (double) ((Float) bag.get(HeftBag.AVG_COMPUTE_TIME)).floatValue(),
                closeTo(3.5, 0.001));
        assertThat(
                (double) ((Float) bag.get(HeftBag.DOWNWARD_RANK)).floatValue(),
                closeTo(7.5, 0.001));
        assertThat(
                (double) ((Float) bag.get(HeftBag.UPWARD_RANK)).floatValue(), closeTo(2.5, 0.001));
        assertThat(((Long) bag.get(HeftBag.ACTUAL_START_TIME)).longValue(), equalTo(11L));
        assertThat(((Long) bag.get(HeftBag.ACTUAL_FINISH_TIME)).longValue(), equalTo(19L));
        assertThat(bag.get(HeftBag.SCHEDULED_SITE), equalTo("condorpool"));
    }

    @Test
    public void testContainsKeyCurrentRangeBehavior() {
        HeftBag bag = new HeftBag();

        assertThat(bag.containsKey(HeftBag.AVG_COMPUTE_TIME), is(true));
        assertThat(bag.containsKey(HeftBag.DOWNWARD_RANK), is(true));
        assertThat(bag.containsKey(HeftBag.UPWARD_RANK), is(true));
        assertThat(bag.containsKey(HeftBag.ACTUAL_START_TIME), is(false));
        assertThat(bag.containsKey(HeftBag.ACTUAL_FINISH_TIME), is(false));
        assertThat(bag.containsKey(HeftBag.SCHEDULED_SITE), is(false));
        assertThat(bag.containsKey("not-an-integer"), is(false));
    }

    @Test
    public void testAddInvalidKeyReturnsFalseAndGetInvalidKeyThrows() {
        HeftBag bag = new HeftBag();

        assertThat(bag.add(Integer.valueOf(99), Float.valueOf(1.0f)), is(false));

        RuntimeException exception =
                assertThrows(RuntimeException.class, () -> bag.get(Integer.valueOf(99)));
        assertThat(exception.getMessage(), containsString("Wrong Heft key"));
    }
}
