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

import java.lang.reflect.Modifier;
import org.junit.jupiter.api.Test;

/**
 * Tests structural properties of the Abstract aggregator base class via reflection, since the class
 * itself cannot be instantiated directly.
 */
public class AbstractTest {

    @Test
    public void testAbstractClassIsActuallyAbstract() {
        assertThat(Modifier.isAbstract(Abstract.class.getModifiers()), is(true));
    }

    @Test
    public void testAbstractImplementsAggregatorInterface() {
        assertThat(Aggregator.class.isAssignableFrom(Abstract.class), is(true));
    }

    @Test
    public void testConcreteSubclassMAXIsNotAbstract() {
        assertThat(Modifier.isAbstract(MAX.class.getModifiers()), is(false));
    }

    @Test
    public void testConcreteSubclassMINIsNotAbstract() {
        assertThat(Modifier.isAbstract(MIN.class.getModifiers()), is(false));
    }

    @Test
    public void testConcreteSubclassSumIsNotAbstract() {
        assertThat(Modifier.isAbstract(Sum.class.getModifiers()), is(false));
    }

    @Test
    public void testSumExtendsAbstract() {
        assertThat(Sum.class.getSuperclass(), is(Abstract.class));
    }

    @Test
    public void testMAXExtendsAbstract() {
        assertThat(MAX.class.getSuperclass(), is(Abstract.class));
    }

    @Test
    public void testParseIntViaConcreteSubclass() {
        // Test parseInt behavior through a concrete subclass (Sum uses it)
        Sum sum = new Sum();
        // parseInt("5", "0") + parseInt("3", "0") = 8
        String result = sum.compute("5", "3", "0");
        assertThat(result, is("8"));
    }

    @Test
    public void testParseIntReturnsDefaultForNullValue() {
        TestAggregator aggregator = new TestAggregator();

        assertThat(aggregator.parse("null", "7"), is(7));
    }

    @Test
    public void testParseIntReturnsDefaultForNonNumericValue() {
        TestAggregator aggregator = new TestAggregator();

        assertThat(aggregator.parse("not-a-number", "9"), is(9));
    }

    @Test
    public void testParseIntParsesNegativeValue() {
        TestAggregator aggregator = new TestAggregator();

        assertThat(aggregator.parse("-3", "0"), is(-3));
    }

    @Test
    public void testParseIntThrowsWhenDefaultIsInvalid() {
        TestAggregator aggregator = new TestAggregator();

        assertThrows(NumberFormatException.class, () -> aggregator.parse("5", "bad-default"));
    }

    private static final class TestAggregator extends Abstract {
        int parse(String value, String dflt) {
            return this.parseInt("null".equals(value) ? null : value, dflt);
        }

        @Override
        public String compute(String v1, String v2, String dflt) {
            return Integer.toString(this.parse(v1, dflt) + this.parse(v2, dflt));
        }
    }
}
