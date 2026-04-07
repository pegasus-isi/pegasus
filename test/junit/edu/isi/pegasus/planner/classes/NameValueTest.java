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
package edu.isi.pegasus.planner.classes;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class NameValueTest {

    @Test
    public void testDefaultConstructorYieldsNullKeyAndValue() {
        NameValue<String, String> nv = new NameValue<>();
        assertThat(nv.getKey(), is(nullValue()));
        assertThat(nv.getValue(), is(nullValue()));
    }

    @Test
    public void testParameterizedConstructorSetsKeyAndValue() {
        NameValue<String, String> nv = new NameValue<>("myKey", "myValue");
        assertThat(nv.getKey(), is("myKey"));
        assertThat(nv.getValue(), is("myValue"));
    }

    @Test
    public void testSetKeyAndGetKey() {
        NameValue<String, String> nv = new NameValue<>();
        nv.setKey("testKey");
        assertThat(nv.getKey(), is("testKey"));
    }

    @Test
    public void testSetValueAndGetValue() {
        NameValue<String, String> nv = new NameValue<>();
        nv.setValue("testValue");
        assertThat(nv.getValue(), is("testValue"));
    }

    @Test
    public void testToStringReturnsKeyEqualsValue() {
        NameValue<String, String> nv = new NameValue<>("PATH", "/usr/bin");
        assertThat(nv.toString(), is("PATH=/usr/bin"));
    }

    @Test
    public void testEqualsSameKeyAndValue() {
        NameValue<String, String> nv1 = new NameValue<>("key", "value");
        NameValue<String, String> nv2 = new NameValue<>("key", "value");
        assertThat(nv1, is(nv2));
    }

    @Test
    public void testEqualsDifferentValueNotEqual() {
        NameValue<String, String> nv1 = new NameValue<>("key", "value1");
        NameValue<String, String> nv2 = new NameValue<>("key", "value2");
        assertNotEquals(nv1, nv2);
    }

    @Test
    public void testEqualsDifferentKeyNotEqual() {
        NameValue<String, String> nv1 = new NameValue<>("key1", "value");
        NameValue<String, String> nv2 = new NameValue<>("key2", "value");
        assertNotEquals(nv1, nv2);
    }

    @Test
    public void testEqualsNonNameValueReturnsFalse() {
        NameValue<String, String> nv = new NameValue<>("key", "value");
        assertThat(nv.equals("notANameValue"), is(false));
    }

    @Test
    public void testEqualsNullReturnsFalse() {
        NameValue<String, String> nv = new NameValue<>("key", "value");
        assertThat(nv.equals(null), is(false));
    }

    @Test
    public void testCloneCreatesEqualCopy() {
        NameValue<String, String> original = new NameValue<>("k", "v");
        NameValue<String, String> clone = (NameValue<String, String>) original.clone();
        assertThat(clone.getKey(), is(original.getKey()));
        assertThat(clone.getValue(), is(original.getValue()));
    }

    @Test
    public void testCloneIsIndependentObject() {
        NameValue<String, String> original = new NameValue<>("k", "v");
        NameValue<String, String> clone = (NameValue<String, String>) original.clone();
        assertThat(clone, is(not(sameInstance(original))));
    }

    @Test
    public void testCompareToEqualKeysReturnsZero() {
        NameValue<String, String> nv1 = new NameValue<>("alpha", "x");
        NameValue<String, String> nv2 = new NameValue<>("alpha", "y");
        assertThat(nv1.compareTo(nv2), is(0));
    }

    @Test
    public void testCompareToLesserKeyReturnsNegative() {
        NameValue<String, String> nv1 = new NameValue<>("aaa", "x");
        NameValue<String, String> nv2 = new NameValue<>("bbb", "y");
        assertThat(nv1.compareTo(nv2), lessThan(0));
    }

    @Test
    public void testCompareToGreaterKeyReturnsPositive() {
        NameValue<String, String> nv1 = new NameValue<>("bbb", "x");
        NameValue<String, String> nv2 = new NameValue<>("aaa", "y");
        assertThat(nv1.compareTo(nv2), greaterThan(0));
    }

    @Test
    public void testCompareToNonNameValueThrowsClassCastException() {
        NameValue<String, String> nv = new NameValue<>("key", "value");
        assertThrows(ClassCastException.class, () -> nv.compareTo("not-a-name-value"));
    }

    @Test
    public void testCompareToWithNonComparableKeyThrowsIllegalArgumentException() {
        NameValue<Object, String> nv1 = new NameValue<>(new Object(), "value1");
        NameValue<Object, String> nv2 = new NameValue<>(new Object(), "value2");

        assertThrows(IllegalArgumentException.class, () -> nv1.compareTo(nv2));
    }

    @Test
    public void testSetKeyOverwritesPreviousKey() {
        NameValue<String, String> nv = new NameValue<>("oldKey", "val");
        nv.setKey("newKey");
        assertThat(nv.getKey(), is("newKey"));
    }

    @Test
    public void testSetValueOverwritesPreviousValue() {
        NameValue<String, String> nv = new NameValue<>("key", "oldVal");
        nv.setValue("newVal");
        assertThat(nv.getValue(), is("newVal"));
    }

    @Test
    public void testToStringWithNullKeyAndValueUsesStringNulls() {
        NameValue<String, String> nv = new NameValue<>();

        assertThat(nv.toString(), is("null=null"));
    }
}
