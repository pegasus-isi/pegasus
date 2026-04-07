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

/** Unit tests for the MIN aggregator. */
public class MINTest {

    private MIN mAggregator;

    @BeforeEach
    public void setUp() {
        mAggregator = new MIN();
    }

    @AfterEach
    public void tearDown() {
        mAggregator = null;
    }

    @Test
    public void testMinReturnsSmallerNewValue() {
        String result = mAggregator.compute("7", "3", "0");
        assertThat(result, is("3"));
    }

    @Test
    public void testMinReturnsSmallerOldValue() {
        String result = mAggregator.compute("2", "10", "0");
        assertThat(result, is("2"));
    }

    @Test
    public void testMinWithEqualValues() {
        String result = mAggregator.compute("5", "5", "0");
        assertThat(result, is("5"));
    }

    @Test
    public void testMinWithNullOldValueUsesDefault() {
        // null old value => parseInt returns default(0), newValue=5 => min(0,5)=0
        String result = mAggregator.compute(null, "5", "0");
        assertThat(result, is("0"));
    }

    @Test
    public void testMinWithNonNumericNewValueUsesDefault() {
        // "xyz" is NaN => falls back to default(0), oldValue=10 => min(10,0)=0
        String result = mAggregator.compute("10", "xyz", "0");
        assertThat(result, is("0"));
    }

    @Test
    public void testMinWithNegativeNumbers() {
        String result = mAggregator.compute("-2", "-5", "0");
        assertThat(result, is("-5"));
    }

    @Test
    public void testMinWithNullNewValueKeepsOldValue() {
        String result = mAggregator.compute("4", null, "9");
        assertThat(result, is("4"));
    }

    @Test
    public void testMinWithBothInvalidValuesUsesDefault() {
        String result = mAggregator.compute("bad", "worse", "6");
        assertThat(result, is("6"));
    }

    @Test
    public void testMinWithInvalidDefaultThrows() {
        assertThrows(NumberFormatException.class, () -> mAggregator.compute("1", "2", "bad"));
    }

    @Test
    public void testMinImplementsAggregatorInterface() {
        assertThat(mAggregator instanceof Aggregator, is(true));
    }

    @Test
    public void testMinExtendsAbstract() {
        assertThat(mAggregator instanceof Abstract, is(true));
    }
}
