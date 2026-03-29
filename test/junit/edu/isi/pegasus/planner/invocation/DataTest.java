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
package edu.isi.pegasus.planner.invocation;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** Tests for Data invocation class. */
public class DataTest {

    @Test
    public void testExtendsInvocation() {
        assertTrue(Invocation.class.isAssignableFrom(Data.class));
    }

    @Test
    public void testImplementsHasText() {
        assertTrue(HasText.class.isAssignableFrom(Data.class));
    }

    @Test
    public void testDefaultConstructorNullValue() {
        Data d = new Data();
        assertNull(d.getValue());
        assertFalse(d.getTruncated());
    }

    @Test
    public void testConstructorWithValue() {
        Data d = new Data("content");
        assertEquals("content", d.getValue());
        assertFalse(d.getTruncated());
    }

    @Test
    public void testConstructorWithValueAndTruncated() {
        Data d = new Data("partial", true);
        assertEquals("partial", d.getValue());
        assertTrue(d.getTruncated());
    }

    @Test
    public void testConstructorNullValueThrows() {
        assertThrows(NullPointerException.class, () -> new Data(null));
    }

    @Test
    public void testSetAndGetTruncated() {
        Data d = new Data();
        d.setTruncated(true);
        assertTrue(d.getTruncated());
    }

    @Test
    public void testAppendValue() {
        Data d = new Data();
        d.appendValue("hello");
        d.appendValue(" world");
        assertEquals("hello world", d.getValue());
    }

    @Test
    public void testToXMLContainsTruncatedAttribute() {
        Data d = new Data("some content", false);
        String xml = d.toXML("");
        assertTrue(xml.contains("truncated=\"false\""));
        assertTrue(xml.contains("some content"));
    }

    @Test
    public void testToXMLEmptyWhenValueNull() {
        Data d = new Data();
        String xml = d.toXML("");
        assertEquals("", xml);
    }
}
