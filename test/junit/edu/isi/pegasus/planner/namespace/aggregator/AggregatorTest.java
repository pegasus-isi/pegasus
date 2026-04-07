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

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import org.junit.jupiter.api.Test;

/** Tests structural properties of the Aggregator interface via reflection. */
public class AggregatorTest {

    @Test
    public void testAggregatorVersionConstant() {
        assertThat(Aggregator.VERSION, is("1.0"));
    }

    @Test
    public void testAggregatorHasComputeMethod() throws NoSuchMethodException {
        Method compute =
                Aggregator.class.getMethod("compute", String.class, String.class, String.class);
        assertThat(compute, is(org.hamcrest.Matchers.notNullValue()));
    }

    @Test
    public void testComputeMethodIsPublic() throws NoSuchMethodException {
        Method compute =
                Aggregator.class.getMethod("compute", String.class, String.class, String.class);
        assertThat(Modifier.isPublic(compute.getModifiers()), is(true));
    }

    @Test
    public void testComputeMethodReturnsString() throws NoSuchMethodException {
        Method compute =
                Aggregator.class.getMethod("compute", String.class, String.class, String.class);
        assertThat(compute.getReturnType(), is(String.class));
    }

    @Test
    public void testMaxImplementsAggregator() {
        assertThat(Aggregator.class.isAssignableFrom(MAX.class), is(true));
    }

    @Test
    public void testMINImplementsAggregator() {
        assertThat(Aggregator.class.isAssignableFrom(MIN.class), is(true));
    }

    @Test
    public void testSumImplementsAggregator() {
        assertThat(Aggregator.class.isAssignableFrom(Sum.class), is(true));
    }

    @Test
    public void testAggregatorIsInterface() {
        assertThat(Aggregator.class.isInterface(), is(true));
    }

    @Test
    public void testComputeMethodIsAbstract() throws NoSuchMethodException {
        Method compute =
                Aggregator.class.getMethod("compute", String.class, String.class, String.class);
        assertThat(Modifier.isAbstract(compute.getModifiers()), is(true));
    }

    @Test
    public void testAggregatorDeclaresOnlyComputeMethod() {
        assertThat(Aggregator.class.getDeclaredMethods().length, is(1));
    }

    @Test
    public void testUpdateImplementsAggregator() {
        assertThat(Aggregator.class.isAssignableFrom(Update.class), is(true));
    }
}
