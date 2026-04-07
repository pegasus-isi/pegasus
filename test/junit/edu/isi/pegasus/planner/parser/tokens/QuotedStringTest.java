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

/** Tests for {@link QuotedString} token. */
public class QuotedStringTest {

    @Test
    public void testImplementsToken() {
        QuotedString qs = new QuotedString("hello");
        assertThat(qs, instanceOf(Token.class));
    }

    @Test
    public void testGetValue() {
        QuotedString qs = new QuotedString("hello world");
        assertThat(qs.getValue(), is("hello world"));
    }

    @Test
    public void testEmptyStringValue() {
        QuotedString qs = new QuotedString("");
        assertThat(qs.getValue(), is(""));
    }

    @Test
    public void testValueWithSpecialCharacters() {
        String content = "/path/to/some file with spaces";
        QuotedString qs = new QuotedString(content);
        assertThat(qs.getValue(), is(content));
    }

    @Test
    public void testValuePreservesWhitespace() {
        String content = "  spaces  ";
        QuotedString qs = new QuotedString(content);
        assertThat(qs.getValue(), is(content));
    }

    @Test
    public void testNullValueIsPreserved() {
        QuotedString qs = new QuotedString(null);
        assertThat(qs.getValue(), is(nullValue()));
    }

    @Test
    public void testDeclaresSinglePrivateStringField() throws Exception {
        assertThat(QuotedString.class.getDeclaredFields().length, is(1));
        assertThat(QuotedString.class.getDeclaredField("m_value").getType(), is(String.class));
    }

    @Test
    public void testDeclaresOnlyGetterMethod() {
        assertThat(QuotedString.class.getDeclaredMethods().length, is(1));
    }
}
