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
package edu.isi.ikcap.workflows.util.logging;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** @author Rajiv Mayani */
public class EscapeTest {

    @Test
    public void testDefaultConstructorEscapesQuotesAndBackslashes() {
        Escape escape = new Escape();

        assertThat(escape.escape("a\"b\\c'd"), is("a\\\"b\\\\c\\'d"));
        assertThat(escape.unescape("a\\\"b\\\\c\\'d"), is("a\"b\\c'd"));
    }

    @Test
    public void testCustomConstructorCanSkipEscapingTheEscapeCharacterItself() {
        Escape escape = new Escape("xy", '!', false);

        assertThat(escape.escape("axby!"), is("a!xb!y!"));
        assertThat(escape.unescape("a!xb!y!"), is("axby"));
    }

    @Test
    public void testEscapeCharacterIsAddedWhenEscapeEscapeIsEnabled() throws Exception {
        Escape escape = new Escape("xy", '!', true);

        assertThat(escape.escape("axby!"), is("a!xb!y!!"));

        assertThat(
                ((String) ReflectionTestUtils.getField(escape, "m_escapable")).indexOf('!') != -1,
                is(true));
    }

    @Test
    public void testNullHandlingAndUnknownEscapeRoundTrip() {
        Escape escape = new Escape();

        assertThat(escape.escape(null), is(nullValue()));
        assertThat(escape.unescape(null), is(nullValue()));
        assertThat(escape.unescape("abc\\n"), is("abc\\n"));
    }

    @Test
    public void testMainPrintsCurrentEscapeAndUnescapeViews() {
        PrintStream originalOut = System.out;
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try {
            System.setOut(new PrintStream(output));
            Escape.main(new String[] {"a\"b"});
        } finally {
            System.setOut(originalOut);
        }

        String text = output.toString();
        assertThat(text, containsString("raw s  > a\"b"));
        assertThat(text, containsString("e(s)   > a\\\"b"));
        assertThat(text, containsString("u(e(s))> a\"b"));
        assertThat(text, containsString("u(s)   > a\"b"));
    }
}
