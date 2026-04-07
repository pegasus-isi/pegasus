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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/**
 * Tests for the {@link Token} interface and its concrete implementations as Token instances. Token
 * is an empty marker interface; tests verify all concrete types implement it.
 */
public class TokenTest {

    @Test
    public void testIdentifierIsToken() {
        Token t = new Identifier("someId");
        assertThat(t, instanceOf(Token.class));
    }

    @Test
    public void testQuotedStringIsToken() {
        Token t = new QuotedString("quoted");
        assertThat(t, instanceOf(Token.class));
    }

    @Test
    public void testOpenBraceIsToken() {
        Token t = new OpenBrace();
        assertThat(t, instanceOf(Token.class));
    }

    @Test
    public void testCloseBraceIsToken() {
        Token t = new CloseBrace();
        assertThat(t, instanceOf(Token.class));
    }

    @Test
    public void testOpenParanthesisIsToken() {
        Token t = new OpenParanthesis();
        assertThat(t, instanceOf(Token.class));
    }

    @Test
    public void testCloseParanthesisIsToken() {
        Token t = new CloseParanthesis();
        assertThat(t, instanceOf(Token.class));
    }

    @Test
    public void testTransformationCatalogReservedWordIsToken() {
        Token t = TransformationCatalogReservedWord.symbolTable().get("tr");
        assertThat(t, is(notNullValue()));
        assertThat(t, instanceOf(Token.class));
    }

    @Test
    public void testTokenIsInterface() {
        assertThat(Token.class.isInterface(), is(true));
    }

    @Test
    public void testTokenDeclaresNoMethods() {
        assertThat(Token.class.getDeclaredMethods().length, is(0));
    }

    @Test
    public void testTokenDeclaresNoFields() {
        assertThat(Token.class.getDeclaredFields().length, is(0));
    }
}
