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

/**
 * Tests for the {@link Token} interface and its concrete implementations as Token instances. Token
 * is an empty marker interface; tests verify all concrete types implement it.
 */
public class TokenTest {

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testIdentifierIsToken() {
        Token t = new Identifier("someId");
        assertInstanceOf(Token.class, t);
    }

    @Test
    public void testQuotedStringIsToken() {
        Token t = new QuotedString("quoted");
        assertInstanceOf(Token.class, t);
    }

    @Test
    public void testOpenBraceIsToken() {
        Token t = new OpenBrace();
        assertInstanceOf(Token.class, t);
    }

    @Test
    public void testCloseBraceIsToken() {
        Token t = new CloseBrace();
        assertInstanceOf(Token.class, t);
    }

    @Test
    public void testOpenParanthesisIsToken() {
        Token t = new OpenParanthesis();
        assertInstanceOf(Token.class, t);
    }

    @Test
    public void testCloseParanthesisIsToken() {
        Token t = new CloseParanthesis();
        assertInstanceOf(Token.class, t);
    }

    @Test
    public void testTransformationCatalogReservedWordIsToken() {
        Token t = TransformationCatalogReservedWord.symbolTable().get("tr");
        assertNotNull(t);
        assertInstanceOf(Token.class, t);
    }
}
