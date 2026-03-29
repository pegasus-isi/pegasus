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

import java.lang.reflect.Modifier;
import org.junit.jupiter.api.Test;

/**
 * Tests structural properties of the Abstract aggregator base class via reflection, since the class
 * itself cannot be instantiated directly.
 */
public class AbstractTest {

    @Test
    public void testAbstractClassIsActuallyAbstract() {
        assertTrue(
                Modifier.isAbstract(Abstract.class.getModifiers()),
                "Abstract should be declared abstract");
    }

    @Test
    public void testAbstractImplementsAggregatorInterface() {
        assertTrue(
                Aggregator.class.isAssignableFrom(Abstract.class),
                "Abstract should implement the Aggregator interface");
    }

    @Test
    public void testConcreteSubclassMAXIsNotAbstract() {
        assertFalse(
                Modifier.isAbstract(MAX.class.getModifiers()),
                "MAX (concrete subclass) should not be abstract");
    }

    @Test
    public void testConcreteSubclassMINIsNotAbstract() {
        assertFalse(
                Modifier.isAbstract(MIN.class.getModifiers()),
                "MIN (concrete subclass) should not be abstract");
    }

    @Test
    public void testConcreteSubclassSumIsNotAbstract() {
        assertFalse(
                Modifier.isAbstract(Sum.class.getModifiers()),
                "Sum (concrete subclass) should not be abstract");
    }

    @Test
    public void testSumExtendsAbstract() {
        assertEquals(
                Abstract.class, Sum.class.getSuperclass(), "Sum should directly extend Abstract");
    }

    @Test
    public void testMAXExtendsAbstract() {
        assertEquals(
                Abstract.class, MAX.class.getSuperclass(), "MAX should directly extend Abstract");
    }

    @Test
    public void testParseIntViaConcreteSubclass() {
        // Test parseInt behavior through a concrete subclass (Sum uses it)
        Sum sum = new Sum();
        // parseInt("5", "0") + parseInt("3", "0") = 8
        String result = sum.compute("5", "3", "0");
        assertEquals("8", result, "Sum(5+3) should equal 8, validating parseInt works");
    }
}
