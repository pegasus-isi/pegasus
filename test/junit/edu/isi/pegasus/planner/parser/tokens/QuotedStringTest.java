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
package edu.isi.pegasus.planner.parser.tokens;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for {@link QuotedString} token. */
public class QuotedStringTest {

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testImplementsToken() {
        QuotedString qs = new QuotedString("hello");
        assertInstanceOf(Token.class, qs);
    }

    @Test
    public void testGetValue() {
        QuotedString qs = new QuotedString("hello world");
        assertEquals("hello world", qs.getValue());
    }

    @Test
    public void testEmptyStringValue() {
        QuotedString qs = new QuotedString("");
        assertEquals("", qs.getValue());
    }

    @Test
    public void testValueWithSpecialCharacters() {
        String content = "/path/to/some file with spaces";
        QuotedString qs = new QuotedString(content);
        assertEquals(content, qs.getValue());
    }

    @Test
    public void testValuePreservesWhitespace() {
        String content = "  spaces  ";
        QuotedString qs = new QuotedString(content);
        assertEquals(content, qs.getValue());
    }
}
