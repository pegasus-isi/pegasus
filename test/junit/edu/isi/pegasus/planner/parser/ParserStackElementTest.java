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
package edu.isi.pegasus.planner.parser;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class ParserStackElementTest {

    /*
    @Test
    public void testSomeMethod() {
        org.hamcrest.MatcherAssert.assertThat(1, org.hamcrest.Matchers.is(1));
    }
    */

    @Test
    public void testConstructorStoresElementNameAndObject() {
        Object payload = new Object();
        ParserStackElement element = new ParserStackElement("site", payload);

        assertThat("Element name should be preserved", element.getElementName(), is("site"));
        assertThat(
                "Element object should be preserved",
                element.getElementObject(),
                sameInstance(payload));
    }

    @Test
    public void testConstructorAllowsNullObject() {
        ParserStackElement element = new ParserStackElement("profile", null);

        assertThat("Element name should still be stored", element.getElementName(), is("profile"));
        assertThat("Null payloads should be preserved", element.getElementObject(), nullValue());
    }

    @Test
    public void testConstructorAllowsNullName() {
        Map<String, String> payload = new HashMap<String, String>();
        payload.put("k", "v");
        ParserStackElement element = new ParserStackElement(null, payload);

        assertThat("Null names are currently accepted", element.getElementName(), nullValue());
        assertThat(
                "Payload should still be stored",
                element.getElementObject(),
                sameInstance(payload));
    }

    @Test
    public void testGetElementObjectPreservesExactReferenceType() {
        Map<String, Integer> payload = new HashMap<String, Integer>();
        payload.put("x", 1);
        ParserStackElement element = new ParserStackElement("metadata", payload);

        assertThat(
                "Stored payload type should be retrievable without conversion",
                element.getElementObject(),
                instanceOf(Map.class));
        assertThat(
                "Getter should return the exact same reference",
                element.getElementObject(),
                sameInstance(payload));
    }
}
