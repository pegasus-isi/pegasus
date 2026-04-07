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

/** Tests for {@link Identifier} token. */
public class IdentifierTest {

    @Test
    public void testImplementsToken() {
        Identifier id = new Identifier("myJob");
        assertThat(id, instanceOf(Token.class));
    }

    @Test
    public void testGetValue() {
        Identifier id = new Identifier("myJob");
        assertThat(id.getValue(), is("myJob"));
    }

    @Test
    public void testToString() {
        Identifier id = new Identifier("pegasus");
        assertThat(id.toString(), is("pegasus"));
    }

    @Test
    public void testGetValueMatchesConstructorInput() {
        String value = "transformation-name";
        Identifier id = new Identifier(value);
        assertThat(id.getValue(), is(value));
    }

    @Test
    public void testEmptyValue() {
        Identifier id = new Identifier("");
        assertThat(id.getValue(), is(""));
    }

    @Test
    public void testToStringEqualsGetValue() {
        Identifier id = new Identifier("someToken");
        assertThat(id.toString(), is(id.getValue()));
    }

    @Test
    public void testNullValueIsPreserved() {
        Identifier id = new Identifier(null);
        assertThat(id.getValue(), is(nullValue()));
        assertThat(id.toString(), is(nullValue()));
    }

    @Test
    public void testDeclaresSinglePrivateStringField() throws Exception {
        assertThat(Identifier.class.getDeclaredFields().length, is(1));
        assertThat(Identifier.class.getDeclaredField("m_value").getType(), is(String.class));
    }

    @Test
    public void testDeclaresExpectedPublicMethods() {
        assertThat(Identifier.class.getDeclaredMethods().length, is(2));
    }
}
