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

/** Tests for {@link Identifier} token. */
public class IdentifierTest {

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
        Identifier id = new Identifier("myJob");
        assertInstanceOf(Token.class, id);
    }

    @Test
    public void testGetValue() {
        Identifier id = new Identifier("myJob");
        assertEquals("myJob", id.getValue());
    }

    @Test
    public void testToString() {
        Identifier id = new Identifier("pegasus");
        assertEquals("pegasus", id.toString());
    }

    @Test
    public void testGetValueMatchesConstructorInput() {
        String value = "transformation-name";
        Identifier id = new Identifier(value);
        assertEquals(value, id.getValue());
    }

    @Test
    public void testEmptyValue() {
        Identifier id = new Identifier("");
        assertEquals("", id.getValue());
    }

    @Test
    public void testToStringEqualsGetValue() {
        Identifier id = new Identifier("someToken");
        assertEquals(id.getValue(), id.toString());
    }
}
