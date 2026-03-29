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

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import org.junit.jupiter.api.Test;

/** Tests structural properties of the Aggregator interface via reflection. */
public class AggregatorTest {

    @Test
    public void testAggregatorIsInterface() {
        assertTrue(Aggregator.class.isInterface(), "Aggregator should be an interface");
    }

    @Test
    public void testAggregatorVersionConstant() {
        assertEquals("1.0", Aggregator.VERSION, "Aggregator VERSION constant should be 1.0");
    }

    @Test
    public void testAggregatorHasComputeMethod() throws NoSuchMethodException {
        Method compute =
                Aggregator.class.getMethod("compute", String.class, String.class, String.class);
        assertNotNull(
                compute, "Aggregator should declare a compute(String, String, String) method");
    }

    @Test
    public void testComputeMethodIsPublic() throws NoSuchMethodException {
        Method compute =
                Aggregator.class.getMethod("compute", String.class, String.class, String.class);
        assertTrue(Modifier.isPublic(compute.getModifiers()), "compute method should be public");
    }

    @Test
    public void testComputeMethodReturnsString() throws NoSuchMethodException {
        Method compute =
                Aggregator.class.getMethod("compute", String.class, String.class, String.class);
        assertEquals(String.class, compute.getReturnType(), "compute method should return String");
    }

    @Test
    public void testMaxImplementsAggregator() {
        assertTrue(Aggregator.class.isAssignableFrom(MAX.class), "MAX should implement Aggregator");
    }

    @Test
    public void testMINImplementsAggregator() {
        assertTrue(Aggregator.class.isAssignableFrom(MIN.class), "MIN should implement Aggregator");
    }

    @Test
    public void testSumImplementsAggregator() {
        assertTrue(Aggregator.class.isAssignableFrom(Sum.class), "Sum should implement Aggregator");
    }
}
