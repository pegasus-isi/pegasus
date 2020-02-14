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
package edu.isi.pegasus.common.util;

import static org.junit.Assert.assertEquals;

import org.junit.*;

/**
 * Test class to test the GLiteEscape class
 *
 * @author Rajiv Mayani
 */
public class GLiteEscapeTest {

    private GliteEscape ge = null;

    public GLiteEscapeTest() {}

    @Before
    public void setUp() {
        ge = new GliteEscape();
    }

    @Test
    public void testBasic() {
        String value = "AB";
        assertEquals(value, ge.escape(value));
    }

    @Test
    public void testValWithSpaces() {
        String value = "A B";
        assertEquals("A\\ B", ge.escape(value));
    }

    @Test
    public void testValWithDoubleQuotes() {
        String value = "A\"B";
        assertEquals("A\\\\\"B", ge.escape(value));
    }

    @Test(expected = RuntimeException.class)
    public void testValWithSingleQuotes() {
        String value = "A'B";
        ge.escape(value);
    }

    @Test(expected = RuntimeException.class)
    public void testValWithNewLine() {
        String value = "A\nB";
        ge.escape(value);
    }

    @Test(expected = RuntimeException.class)
    public void testValWithTab() {
        String value = "A\tB";
        ge.escape(value);
    }
}
