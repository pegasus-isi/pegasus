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
package edu.isi.pegasus.planner.common;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

/** Unit tests for the PegRandom utility class. */
public class PegRandomTest {

    @Test
    public void testNextDoubleIsBetweenZeroAndOne() {
        double value = PegRandom.nextDouble();
        assertTrue(value >= 0.0 && value < 1.0, "nextDouble should return a value in [0.0, 1.0)");
    }

    @Test
    public void testGetIntegerWithUpperBoundReturnsValueInRange() {
        int upperIndex = 9;
        int value = PegRandom.getInteger(upperIndex);
        assertTrue(
                value >= 0 && value <= upperIndex, "getInteger(9) should return a value in [0, 9]");
    }

    @Test
    public void testGetIntegerWithBothBoundsReturnsValueInRange() {
        int lower = 5;
        int upper = 10;
        int value = PegRandom.getInteger(lower, upper);
        assertTrue(
                value >= lower && value <= upper,
                "getInteger(5, 10) should return a value in [5, 10]");
    }

    @Test
    public void testGetIntegerWithSameLowerAndUpperBound() {
        int value = PegRandom.getInteger(3, 3);
        assertEquals(3, value, "getInteger(3, 3) should always return 3");
    }

    @RepeatedTest(20)
    public void testGetIntegerNeverExceedsUpperBound() {
        int upperIndex = 5;
        int value = PegRandom.getInteger(upperIndex);
        assertTrue(value <= upperIndex, "getInteger should never exceed the upper index");
    }

    @RepeatedTest(20)
    public void testGetIntegerNeverFallsBelowLowerBound() {
        int lower = 3;
        int upper = 7;
        int value = PegRandom.getInteger(lower, upper);
        assertTrue(value >= lower, "getInteger should never be below lower bound");
    }

    @Test
    public void testGetIntegerWithUpperBoundZero() {
        int value = PegRandom.getInteger(0);
        assertEquals(0, value, "getInteger(0) should always return 0");
    }

    @Test
    public void testNextGaussianReturnsDouble() {
        // Just assert it doesn't throw and returns a double
        double value = PegRandom.nextGaussian();
        assertNotNull(value, "nextGaussian should return a value without throwing");
    }
}
