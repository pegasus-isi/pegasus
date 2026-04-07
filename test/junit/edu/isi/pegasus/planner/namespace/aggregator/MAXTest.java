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
package edu.isi.pegasus.planner.namespace.aggregator;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for the MAX aggregator. */
public class MAXTest {

    private MAX mAggregator;

    @BeforeEach
    public void setUp() {
        mAggregator = new MAX();
    }

    @AfterEach
    public void tearDown() {
        mAggregator = null;
    }

    @Test
    public void testMaxReturnsLargerNewValue() {
        String result = mAggregator.compute("3", "7", "0");
        assertThat(result, is("7"));
    }

    @Test
    public void testMaxReturnsLargerOldValue() {
        String result = mAggregator.compute("10", "4", "0");
        assertThat(result, is("10"));
    }

    @Test
    public void testMaxWithEqualValues() {
        String result = mAggregator.compute("5", "5", "0");
        assertThat(result, is("5"));
    }

    @Test
    public void testMaxWithNullOldValueUsesDefault() {
        // null old value => parseInt returns default(0), newValue=5 => max(0,5)=5
        String result = mAggregator.compute(null, "5", "0");
        assertThat(result, is("5"));
    }

    @Test
    public void testMaxWithNonNumericOldValueUsesDefault() {
        // "abc" is NaN => falls back to default(0), newValue=10 => max(0,10)=10
        String result = mAggregator.compute("abc", "10", "0");
        assertThat(result, is("10"));
    }

    @Test
    public void testMaxWithNegativeNumbers() {
        String result = mAggregator.compute("-5", "-2", "0");
        assertThat(result, is("-2"));
    }

    @Test
    public void testMaxImplementsAggregatorInterface() {
        assertThat(mAggregator instanceof Aggregator, is(true));
    }

    @Test
    public void testMaxExtendsAbstract() {
        assertThat(mAggregator instanceof Abstract, is(true));
    }

    @Test
    public void testMaxWithNullNewValueKeepsOldValue() {
        String result = mAggregator.compute("9", null, "0");
        assertThat(result, is("9"));
    }

    @Test
    public void testMaxWithBothInvalidValuesUsesDefault() {
        String result = mAggregator.compute("bad", "worse", "6");
        assertThat(result, is("6"));
    }

    @Test
    public void testMaxWithInvalidDefaultThrows() {
        assertThrows(NumberFormatException.class, () -> mAggregator.compute("1", "2", "bad"));
    }
}
