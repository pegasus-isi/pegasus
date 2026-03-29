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
    public void testBagIsInterface() {
        assertTrue(Bag.class.isInterface(), "Bag should be an interface");
    }

    @Test
    public void testBagHasGetMethod() throws NoSuchMethodException {
        Method m = Bag.class.getMethod("get", Object.class);
        assertNotNull(m, "Bag should declare a get(Object) method");
    }

    @Test
    public void testBagHasAddMethod() throws NoSuchMethodException {
        Method m = Bag.class.getMethod("add", Object.class, Object.class);
        assertNotNull(m, "Bag should declare an add(Object, Object) method");
    }

    @Test
    public void testBagHasContainsKeyMethod() throws NoSuchMethodException {
        Method m = Bag.class.getMethod("containsKey", Object.class);
        assertNotNull(m, "Bag should declare a containsKey(Object) method");
    }

    @Test
    public void testLabelBagImplementsBag() {
        assertTrue(mBag instanceof Bag, "LabelBag should implement Bag");
    }

    @Test
    public void testLabelBagAddAndGetLabel() {
        mBag.add(LabelBag.LABEL_KEY, "partition1");
        Object value = mBag.get(LabelBag.LABEL_KEY);
        assertEquals("partition1", value, "LabelBag should return the added label value");
    }

    @Test
    public void testLabelBagContainsKeyAfterAdd() {
        mBag.add(LabelBag.LABEL_KEY, "myLabel");
        assertTrue(
                mBag.containsKey(LabelBag.LABEL_KEY),
                "LabelBag.containsKey should return true after adding LABEL_KEY");
    }

    @Test
    public void testLabelBagDefaultGetReturnsNull() {
        assertNull(
                mBag.get(LabelBag.LABEL_KEY),
                "Default LabelBag should return null for LABEL_KEY before any add");
    }
}
