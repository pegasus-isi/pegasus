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

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Random;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** Unit tests for the PegRandom utility class. */
public class PegRandomTest {

    private static class FixedRandom extends Random {
        private final double mDouble;
        private final double mGaussian;

        FixedRandom(double value, double gaussian) {
            mDouble = value;
            mGaussian = gaussian;
        }

        @Override
        public double nextDouble() {
            return mDouble;
        }

        @Override
        public double nextGaussian() {
            return mGaussian;
        }
    }

    private Random swapRandom(Random random) throws Exception {
        Random original = (Random) ReflectionTestUtils.getField(PegRandom.class, "mRandom");
        ReflectionTestUtils.setField(PegRandom.class, "mRandom", random);
        return original;
    }

    @Test
    public void testNextDoubleIsBetweenZeroAndOne() {
        double value = PegRandom.nextDouble();
        assertThat(value >= 0.0 && value < 1.0, is(true));
    }

    @Test
    public void testGetIntegerWithUpperBoundReturnsValueInRange() {
        int upperIndex = 9;
        int value = PegRandom.getInteger(upperIndex);
        assertThat(value >= 0 && value <= upperIndex, is(true));
    }

    @Test
    public void testGetIntegerWithBothBoundsReturnsValueInRange() {
        int lower = 5;
        int upper = 10;
        int value = PegRandom.getInteger(lower, upper);
        assertThat(value >= lower && value <= upper, is(true));
    }

    @Test
    public void testGetIntegerWithSameLowerAndUpperBound() {
        int value = PegRandom.getInteger(3, 3);
        assertThat(value, is(3));
    }

    @RepeatedTest(20)
    public void testGetIntegerNeverExceedsUpperBound() {
        int upperIndex = 5;
        int value = PegRandom.getInteger(upperIndex);
        assertThat(value <= upperIndex, is(true));
    }

    @RepeatedTest(20)
    public void testGetIntegerNeverFallsBelowLowerBound() {
        int lower = 3;
        int upper = 7;
        int value = PegRandom.getInteger(lower, upper);
        assertThat(value >= lower, is(true));
    }

    @Test
    public void testGetIntegerWithUpperBoundZero() {
        int value = PegRandom.getInteger(0);
        assertThat(value, is(0));
    }

    @Test
    public void testNextGaussianReturnsDouble() {
        // Just assert it doesn't throw and returns a double
        double value = PegRandom.nextGaussian();
        assertThat(value, notNullValue());
    }

    @Test
    public void testGetIntegerClampsValueAtUpperBoundWhenRandomIsNearOne() throws Exception {
        Random original = swapRandom(new FixedRandom(0.999999999999d, 0.0));
        try {
            assertThat(PegRandom.getInteger(0, 9), is(9));
        } finally {
            swapRandom(original);
        }
    }

    @Test
    public void testGetIntegerReturnsLowerBoundWhenRandomIsZero() throws Exception {
        Random original = swapRandom(new FixedRandom(0.0, 0.0));
        try {
            assertThat(PegRandom.getInteger(4, 9), is(4));
        } finally {
            swapRandom(original);
        }
    }

    @Test
    public void testGetIntegerSupportsNegativeBounds() throws Exception {
        Random original = swapRandom(new FixedRandom(0.50, 0.0));
        try {
            assertThat(PegRandom.getInteger(-2, 2), is(0));
        } finally {
            swapRandom(original);
        }
    }

    @Test
    public void testNextGaussianUsesSingletonRandomInstance() throws Exception {
        Random original = swapRandom(new FixedRandom(0.0, -3.5));
        try {
            assertThat(PegRandom.nextGaussian(), is(-3.5));
        } finally {
            swapRandom(original);
        }
    }
}
