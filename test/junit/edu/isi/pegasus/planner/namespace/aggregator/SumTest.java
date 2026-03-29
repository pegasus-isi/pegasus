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

/** Unit tests for the Sum aggregator. */
public class SumTest {

    private Sum mAggregator;

    @BeforeEach
    public void setUp() {
        mAggregator = new Sum();
    }

    @AfterEach
    public void tearDown() {
        mAggregator = null;
    }

    @Test
    public void testSumOfTwoPositiveNumbers() {
        String result = mAggregator.compute("3", "7", "0");
        assertEquals("10", result, "Sum of 3 and 7 should be 10");
    }

    @Test
    public void testSumWithZero() {
        String result = mAggregator.compute("5", "0", "0");
        assertEquals("5", result, "Sum of 5 and 0 should be 5");
    }

    @Test
    public void testSumWithBothZero() {
        String result = mAggregator.compute("0", "0", "0");
        assertEquals("0", result, "Sum of 0 and 0 should be 0");
    }

    @Test
    public void testSumWithNullOldValueUsesDefault() {
        // null old value => parseInt(null, "0")=0, newValue=5 => 0+5=5
        String result = mAggregator.compute(null, "5", "0");
        assertEquals(
                "5", result, "Sum with null old value should use default(0) and add newValue(5)");
    }

    @Test
    public void testSumWithNonNumericValueUsesDefault() {
        // "abc" => parseInt returns default(2), newValue=3 => 2+3=5
        String result = mAggregator.compute("abc", "3", "2");
        assertEquals("5", result, "Sum with non-numeric old value should use default(2)");
    }

    @Test
    public void testSumWithNegativeNumbers() {
        String result = mAggregator.compute("-3", "10", "0");
        assertEquals("7", result, "Sum of -3 and 10 should be 7");
    }

    @Test
    public void testSumImplementsAggregatorInterface() {
        assertTrue(
                mAggregator instanceof Aggregator, "Sum should implement the Aggregator interface");
    }

    @Test
    public void testSumExtendsAbstract() {
        assertTrue(mAggregator instanceof Abstract, "Sum should extend the Abstract class");
    }
}
