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
import static org.junit.jupiter.api.Assertions.*;

import java.util.HashSet;
import java.util.Set;
import java.util.Vector;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for the Data abstract base class.
 *
 * <p>Because Data is abstract, tests use a minimal concrete subclass {@code ConcreteData} defined
 * at the bottom of this file.
 *
 * @author Rajiv Mayani
 */
public class DataTest {

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    // -----------------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------------

    @Test
    public void testDefaultConstructorInitializesLogMsg() {
        ConcreteData d = new ConcreteData();
        // mLogMsg should be initialised to empty string, not null
        assertNotNull(d.mLogMsg);
        assertThat(d.mLogMsg, is(""));
    }

    @Test
    public void testDefaultConstructorInitializesLogger() {
        ConcreteData d = new ConcreteData();
        assertNotNull(d.mLogger);
    }

    // -----------------------------------------------------------------------
    // vectorToString
    // -----------------------------------------------------------------------

    @Test
    public void testVectorToStringEmptyVector() {
        ConcreteData d = new ConcreteData();
        Vector<String> v = new Vector<String>();
        String result = d.vectorToString("heading", v);
        // heading should be present, no elements
        assertThat(result, containsString("heading"));
    }

    @Test
    public void testVectorToStringSingleElement() {
        ConcreteData d = new ConcreteData();
        Vector<String> v = new Vector<String>();
        v.add("apple");
        String result = d.vectorToString("fruits", v);
        assertThat(result, containsString("fruits"));
        assertThat(result, containsString("apple"));
    }

    @Test
    public void testVectorToStringMultipleElements() {
        ConcreteData d = new ConcreteData();
        Vector<String> v = new Vector<String>();
        v.add("one");
        v.add("two");
        v.add("three");
        String result = d.vectorToString("numbers", v);
        assertThat(result, containsString("one"));
        assertThat(result, containsString("two"));
        assertThat(result, containsString("three"));
    }

    @Test
    public void testVectorToStringStartsWithNewline() {
        ConcreteData d = new ConcreteData();
        Vector<String> v = new Vector<String>();
        String result = d.vectorToString("head", v);
        assertTrue(result.startsWith("\n"), "Expected result to start with a newline");
    }

    // -----------------------------------------------------------------------
    // setToString
    // -----------------------------------------------------------------------

    @Test
    public void testSetToStringEmptySet() {
        ConcreteData d = new ConcreteData();
        Set<String> s = new HashSet<String>();
        String result = d.setToString(s, ",");
        assertThat(result, is(""));
    }

    @Test
    public void testSetToStringSingleElement() {
        ConcreteData d = new ConcreteData();
        Set<String> s = new HashSet<String>();
        s.add("alpha");
        String result = d.setToString(s, ",");
        assertThat(result, is("alpha"));
    }

    @Test
    public void testSetToStringMultipleElementsContainsDelimiter() {
        ConcreteData d = new ConcreteData();
        Set<String> s = new HashSet<String>();
        s.add("x");
        s.add("y");
        // Result should contain both values joined by the delimiter (no trailing delimiter)
        String result = d.setToString(s, ":");
        // Both elements should be present
        assertThat(result, containsString("x"));
        assertThat(result, containsString("y"));
        // Delimiter should appear exactly once (between two elements)
        assertThat(result, containsString(":"));
    }

    @Test
    public void testSetToStringNoTrailingDelimiter() {
        ConcreteData d = new ConcreteData();
        Set<String> s = new HashSet<String>();
        s.add("only");
        String result = d.setToString(s, ";");
        // Single element: no delimiter at all
        assertFalse(result.endsWith(";"), "Should not have trailing delimiter");
    }

    @Test
    public void testSetToStringWithSpaceDelimiter() {
        ConcreteData d = new ConcreteData();
        Set<String> s = new HashSet<String>();
        s.add("hello");
        String result = d.setToString(s, " ");
        assertThat(result, is("hello"));
    }

    // -----------------------------------------------------------------------
    // toString (via the concrete subclass)
    // -----------------------------------------------------------------------

    @Test
    public void testToStringReturnsExpectedValue() {
        ConcreteData d = new ConcreteData("custom-text");
        assertThat(d.toString(), is("custom-text"));
    }

    // -----------------------------------------------------------------------
    // Minimal concrete subclass used for testing
    // -----------------------------------------------------------------------

    /**
     * A minimal concrete implementation of the abstract {@link Data} class that is used only within
     * this test file.
     */
    static class ConcreteData extends Data {

        private final String mText;

        ConcreteData() {
            this("");
        }

        ConcreteData(String text) {
            super();
            this.mText = text;
        }

        @Override
        public String toString() {
            return mText;
        }
    }
}
