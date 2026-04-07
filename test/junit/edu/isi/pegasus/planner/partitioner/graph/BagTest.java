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
package edu.isi.pegasus.planner.partitioner.graph;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Method;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for the Bag interface and its LabelBag implementation. */
public class BagTest {

    private LabelBag mBag;

    @BeforeEach
    public void setUp() {
        mBag = new LabelBag();
    }

    @Test
    public void testBagHasGetMethod() throws NoSuchMethodException {
        Method m = Bag.class.getMethod("get", Object.class);
        assertThat(m, is(notNullValue()));
    }

    @Test
    public void testBagHasAddMethod() throws NoSuchMethodException {
        Method m = Bag.class.getMethod("add", Object.class, Object.class);
        assertThat(m, is(notNullValue()));
    }

    @Test
    public void testBagHasContainsKeyMethod() throws NoSuchMethodException {
        Method m = Bag.class.getMethod("containsKey", Object.class);
        assertThat(m, is(notNullValue()));
    }

    @Test
    public void testLabelBagImplementsBag() {
        assertThat(mBag instanceof Bag, is(true));
    }

    @Test
    public void testLabelBagAddAndGetLabel() {
        mBag.add(LabelBag.LABEL_KEY, "partition1");
        Object value = mBag.get(LabelBag.LABEL_KEY);
        assertThat(value, is("partition1"));
    }

    @Test
    public void testLabelBagContainsKeyAfterAdd() {
        mBag.add(LabelBag.LABEL_KEY, "myLabel");
        assertThat(mBag.containsKey(LabelBag.LABEL_KEY), is(true));
    }

    @Test
    public void testLabelBagDefaultGetReturnsNull() {
        assertThat(mBag.get(LabelBag.LABEL_KEY), is(nullValue()));
    }

    @Test
    public void testBagIsInterface() {
        assertThat(Bag.class.isInterface(), is(true));
    }

    @Test
    public void testBagMethodReturnTypes() throws NoSuchMethodException {
        assertThat(Bag.class.getMethod("get", Object.class).getReturnType(), is(Object.class));
        assertThat(
                Bag.class.getMethod("add", Object.class, Object.class).getReturnType(),
                is(boolean.class));
        assertThat(
                Bag.class.getMethod("containsKey", Object.class).getReturnType(),
                is(boolean.class));
    }

    @Test
    public void testBagDeclaresOnlyExpectedMethods() {
        assertThat(Bag.class.getDeclaredMethods().length, is(3));
    }
}
