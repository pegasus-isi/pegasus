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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class EscapeTest {

    // -----------------------------------------------------------------------
    // escape() — default constructor (escapable = "'\ , escape = \)
    // -----------------------------------------------------------------------

    @Test
    public void testEscape_nullReturnsNull() {
        Escape e = new Escape();
        assertNull(e.escape(null));
    }

    @Test
    public void testEscape_emptyString() {
        Escape e = new Escape();
        assertThat(e.escape(""), is(""));
    }

    @Test
    public void testEscape_plainStringUnchanged() {
        Escape e = new Escape();
        assertThat(e.escape("hello world"), is("hello world"));
    }

    @Test
    public void testEscape_doubleQuote() {
        Escape e = new Escape();
        assertThat(e.escape("a\"b"), is("a\\\"b"));
    }

    @Test
    public void testEscape_singleQuote() {
        Escape e = new Escape();
        assertThat(e.escape("it's"), is("it\\'s"));
    }

    @Test
    public void testEscape_backslash() {
        Escape e = new Escape();
        assertThat(e.escape("a\\b"), is("a\\\\b"));
    }

    @Test
    public void testEscape_onlyDoubleQuote() {
        Escape e = new Escape();
        assertThat(e.escape("\""), is("\\\""));
    }

    @Test
    public void testEscape_onlySingleQuote() {
        Escape e = new Escape();
        assertThat(e.escape("'"), is("\\'"));
    }

    @Test
    public void testEscape_onlyBackslash() {
        Escape e = new Escape();
        assertThat(e.escape("\\"), is("\\\\"));
    }

    @Test
    public void testEscape_multipleSpecialChars() {
        Escape e = new Escape();
        assertThat(e.escape("a'b\"c\\"), is("a\\'b\\\"c\\\\"));
    }

    @Test
    public void testEscape_allSpecialCharsContiguous() {
        // Input:  "\'\ (double-quote, single-quote, backslash)
        // Output: \"\'\\ (each prefixed with backslash)
        Escape e = new Escape();
        assertThat(e.escape("\"'\\"), is("\\\"\\'\\\\"));
    }

    @Test
    public void testEscape_repeatedBackslashes() {
        // Two backslashes → four backslashes
        Escape e = new Escape();
        assertThat(e.escape("\\\\"), is("\\\\\\\\"));
    }

    @Test
    public void testEscape_specialCharsAtStartAndEnd() {
        Escape e = new Escape();
        assertThat(e.escape("\"hello\""), is("\\\"hello\\\""));
    }

    @Test
    public void testEscape_digitsAndLettersUnchanged() {
        Escape e = new Escape();
        String input = "abc123 !@#$%^&*()-+=[]{}|<>,./? \t\n";
        assertThat(e.escape(input), is(input));
    }

    // -----------------------------------------------------------------------
    // unescape() — default constructor
    // -----------------------------------------------------------------------

    @Test
    public void testUnescape_nullReturnsNull() {
        Escape e = new Escape();
        assertNull(e.unescape(null));
    }

    @Test
    public void testUnescape_emptyString() {
        Escape e = new Escape();
        assertThat(e.unescape(""), is(""));
    }

    @Test
    public void testUnescape_plainStringUnchanged() {
        Escape e = new Escape();
        assertThat(e.unescape("hello"), is("hello"));
    }

    @Test
    public void testUnescape_escapedDoubleQuote() {
        Escape e = new Escape();
        assertThat(e.unescape("a\\\"b"), is("a\"b"));
    }

    @Test
    public void testUnescape_escapedSingleQuote() {
        Escape e = new Escape();
        assertThat(e.unescape("it\\'s"), is("it's"));
    }

    @Test
    public void testUnescape_escapedBackslash() {
        Escape e = new Escape();
        // "a\\\\b" in Java source = a\\b as a string (a, \, \, b)
        // unescape strips one level: a\b
        assertThat(e.unescape("a\\\\b"), is("a\\b"));
    }

    @Test
    public void testUnescape_escapedBackslashAlone() {
        Escape e = new Escape();
        assertThat(e.unescape("\\\\"), is("\\"));
    }

    @Test
    public void testUnescape_multipleEscapedChars() {
        Escape e = new Escape();
        // \\'\\\" → '\
        assertThat(e.unescape("'\\\\\\\""), is("'\\\""));
    }

    /**
     * When the escape character appears at the very end of the string, it has nothing to escape.
     * The implementation silently drops it — the state machine ends in state=1 without emitting.
     */
    @Test
    public void testUnescape_trailingEscapeCharIsDropped() {
        Escape e = new Escape();
        // "abc\" (4 chars: a,b,c,\) → "abc" (trailing backslash silently dropped)
        assertThat(e.unescape("abc\\"), is("abc"));
    }

    @Test
    public void testUnescape_trailingEscapeAlone() {
        Escape e = new Escape();
        // A single backslash → empty string
        assertThat(e.unescape("\\"), is(""));
    }

    /**
     * When an escape character is followed by a character that is NOT in the escapable set, the
     * escape character is re-emitted verbatim (the "found escape but not escapable" branch).
     */
    @Test
    public void testUnescape_escapeFollowedByNonEscapableCharReEmitsEscape() {
        Escape e = new Escape();
        // "\\a" in Java source = backslash + 'a' (2 chars)
        // 'a' is not in escapable ("\"'\\"), so backslash is re-emitted → "\\a" (backslash + 'a')
        assertThat(e.unescape("\\a"), is("\\a"));
    }

    @Test
    public void testUnescape_escapeFollowedByDigitReEmitsEscape() {
        Escape e = new Escape();
        // "\\1" → backslash + '1'; '1' not in escapable → re-emit backslash → "\\1"
        assertThat(e.unescape("\\1"), is("\\1"));
    }

    @Test
    public void testUnescape_escapeFollowedBySpaceReEmitsEscape() {
        Escape e = new Escape();
        assertThat(e.unescape("\\ "), is("\\ "));
    }

    // -----------------------------------------------------------------------
    // Round-trip: unescape(escape(s)) === s
    // -----------------------------------------------------------------------

    @Test
    public void testRoundTrip_escape_unescape() {
        Escape e = new Escape();
        String original = "say \"hello\" it's a\\b";
        assertThat(e.unescape(e.escape(original)), is(original));
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "",
                "plain text",
                "\"quoted\"",
                "it's",
                "back\\slash",
                "all: \"'\\",
                "mixed \"it's\" a\\b\\c",
                "\\\\\\\\"
            })
    public void testRoundTrip_unescapeOfEscapeIsIdentity(String original) {
        Escape e = new Escape();
        assertEquals(
                original,
                e.unescape(e.escape(original)),
                "unescape(escape(s)) must equal s for: " + original);
    }

    @Test
    public void testRoundTrip_null() {
        Escape e = new Escape();
        assertNull(e.unescape(e.escape(null)));
    }

    // -----------------------------------------------------------------------
    // Custom constructor — escape char already in escapable set
    // -----------------------------------------------------------------------

    @Test
    public void testCustomConstructor_escapesCustomChar() {
        // escapable="|;", escape='|'; '|' is already in escapable
        Escape e = new Escape("|;", '|');
        assertThat(e.escape("a;b"), is("a|;b"));
    }

    @Test
    public void testCustomConstructor_escapesEscapeCharItself() {
        // '|' is the escape char and also in the escapable set → escaped as "||"
        Escape e = new Escape("|;", '|');
        assertThat(e.escape("a|b"), is("a||b"));
    }

    @Test
    public void testCustomConstructor_plainCharsUnchanged() {
        Escape e = new Escape("|;", '|');
        assertThat(e.escape("hello world"), is("hello world"));
    }

    @Test
    public void testCustomConstructor_unescape() {
        Escape e = new Escape("|;", '|');
        assertThat(e.unescape("a|;b"), is("a;b"));
    }

    @Test
    public void testCustomConstructor_roundTrip() {
        Escape e = new Escape("|;", '|');
        String original = "field1;field2|pipe|;semi";
        assertThat(e.unescape(e.escape(original)), is(original));
    }

    // -----------------------------------------------------------------------
    // Custom constructor — escape char NOT in escapable set (gets auto-added)
    // -----------------------------------------------------------------------

    @Test
    public void testCustomConstructor_escapeCharAddedToEscapable() {
        // escape char '\\' not initially in ";", so it gets added
        Escape e = new Escape(";", '\\');
        // backslash must now be escaped
        assertThat(e.escape("a\\b"), is("a\\\\b"));
    }

    @Test
    public void testCustomConstructor_escapeCharAutoAdded_semicolonEscaped() {
        Escape e = new Escape(";", '\\');
        assertThat(e.escape("a;b"), is("a\\;b"));
    }

    @Test
    public void testCustomConstructor_escapeCharAutoAdded_roundTrip() {
        Escape e = new Escape(";", '\\');
        String original = "a;b\\c;d";
        assertThat(e.unescape(e.escape(original)), is(original));
    }

    @Test
    public void testCustomConstructor_escapeCharAutoAdded_unescapeTrailingEscape() {
        Escape e = new Escape(";", '\\');
        // Trailing backslash silently dropped
        assertThat(e.unescape("abc\\"), is("abc"));
    }
}
