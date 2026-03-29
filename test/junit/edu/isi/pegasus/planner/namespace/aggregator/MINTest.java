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

import static org.junit.jupiter.api.Assertions.*;

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
        assertEquals("3", result, "MIN should return the smaller of 7 and 3");
    }

    @Test
    public void testMinReturnsSmallerOldValue() {
        String result = mAggregator.compute("2", "10", "0");
        assertEquals("2", result, "MIN should return old value when it is smaller");
    }

    @Test
    public void testMinWithEqualValues() {
        String result = mAggregator.compute("5", "5", "0");
        assertEquals("5", result, "MIN should return the value when both are equal");
    }

    @Test
    public void testMinWithNullOldValueUsesDefault() {
        // null old value => parseInt returns default(0), newValue=5 => min(0,5)=0
        String result = mAggregator.compute(null, "5", "0");
        assertEquals(
                "0", result, "MIN with null old value should pick default(0) over newValue(5)");
    }

    @Test
    public void testMinWithNonNumericNewValueUsesDefault() {
        // "xyz" is NaN => falls back to default(0), oldValue=10 => min(10,0)=0
        String result = mAggregator.compute("10", "xyz", "0");
        assertEquals("0", result, "MIN with non-numeric new value should use default(0)");
    }

    @Test
    public void testMinWithNegativeNumbers() {
        String result = mAggregator.compute("-2", "-5", "0");
        assertEquals("-5", result, "MIN should return -5 as it is smaller than -2");
    }

    @Test
    public void testMinImplementsAggregatorInterface() {
        assertTrue(
                mAggregator instanceof Aggregator, "MIN should implement the Aggregator interface");
    }

    @Test
    public void testMinExtendsAbstract() {
        assertTrue(mAggregator instanceof Abstract, "MIN should extend the Abstract class");
    }
}
