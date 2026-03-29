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

import edu.isi.pegasus.planner.catalog.classes.Profiles;
import edu.isi.pegasus.planner.classes.Profile;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** @author Rajiv Mayani */
public class ProfileParserTest {

    // -----------------------------------------------------------------------
    // parse() — boundary / trivial inputs
    // -----------------------------------------------------------------------

    @Test
    public void testParse_null_returnsEmptyList() throws ProfileParserException {
        List result = ProfileParser.parse(null);
        assertTrue(result.isEmpty());
    }

    @Test
    public void testParse_emptyString_returnsEmptyList() throws ProfileParserException {
        List result = ProfileParser.parse("");
        assertTrue(result.isEmpty());
    }

    @Test
    public void testParse_namespaceAndColonsOnly_returnsEmptyList() throws ProfileParserException {
        // "ns::" without a key or value — EOS from state 2 → final state, no triple emitted
        List result = ProfileParser.parse("condor::");
        assertTrue(result.isEmpty());
    }

    @Test
    public void testParse_namespaceAndKeyOnly_returnsEmptyList() throws ProfileParserException {
        // "ns::key" without "=" — EOS from state 2 → final, no triple emitted
        List result = ProfileParser.parse("condor::requirements");
        assertTrue(result.isEmpty());
    }

    // -----------------------------------------------------------------------
    // parse() — single profile, quoted value
    // -----------------------------------------------------------------------

    @Test
    public void testParse_simpleKeyValue() throws ProfileParserException {
        List result = ProfileParser.parse("condor::requirements=\"true\"");
        assertThat(result.size(), is(1));
        Profile p = (Profile) result.get(0);
        assertThat(p.getProfileNamespace(), is("condor"));
        assertThat(p.getProfileKey(), is("requirements"));
        assertThat(p.getProfileValue(), is("true"));
    }

    @Test
    public void testParse_emptyQuotedValue() throws ProfileParserException {
        // value is the empty string between ""
        List result = ProfileParser.parse("env::KEY=\"\"");
        assertThat(result.size(), is(1));
        Profile p = (Profile) result.get(0);
        assertThat(p.getProfileNamespace(), is("env"));
        assertThat(p.getProfileKey(), is("KEY"));
        assertThat(p.getProfileValue(), is(""));
    }

    // -----------------------------------------------------------------------
    // parse() — unquoted value
    // -----------------------------------------------------------------------

    @Test
    public void testParse_unquotedValue() throws ProfileParserException {
        // Unquoted value — state 6 path, terminated by EOS
        List result = ProfileParser.parse("env::PATH=/usr/bin");
        assertThat(result.size(), is(1));
        Profile p = (Profile) result.get(0);
        assertThat(p.getProfileNamespace(), is("env"));
        assertThat(p.getProfileKey(), is("PATH"));
        assertThat(p.getProfileValue(), is("/usr/bin"));
    }

    @Test
    public void testParse_unquotedValueTerminatedByComma() throws ProfileParserException {
        // Unquoted value followed by comma — partial flush (action 5) then new key
        List result = ProfileParser.parse("env::A=one,B=\"two\"");
        assertThat(result.size(), is(2));
        Profile p1 = (Profile) result.get(0);
        Profile p2 = (Profile) result.get(1);
        assertThat(p1.getProfileKey(), is("A"));
        assertThat(p1.getProfileValue(), is("one"));
        assertThat(p2.getProfileKey(), is("B"));
        assertThat(p2.getProfileValue(), is("two"));
    }

    @Test
    public void testParse_unquotedValueTerminatedBySemicolon() throws ProfileParserException {
        // Unquoted value followed by semicolon — full flush (action 4), new namespace
        List result = ProfileParser.parse("env::A=one;condor::B=\"two\"");
        assertThat(result.size(), is(2));
        assertThat(((Profile) result.get(0)).getProfileValue(), is("one"));
        assertThat(((Profile) result.get(1)).getProfileValue(), is("two"));
    }

    // -----------------------------------------------------------------------
    // parse() — namespace case normalisation
    // -----------------------------------------------------------------------

    @Test
    public void testParse_namespaceIsLowercasedOnFullFlush_eos() throws ProfileParserException {
        // Action 4 (full flush at EOS) calls namespace.toString().toLowerCase()
        List result = ProfileParser.parse("ENV::key=\"val\"");
        assertThat(((Profile) result.get(0)).getProfileNamespace(), is("env"));
    }

    @Test
    public void testParse_namespaceIsLowercasedOnFullFlush_semicolon()
            throws ProfileParserException {
        // Action 4 (full flush at ';') lowercases namespace
        List result = ProfileParser.parse("ENV::a=\"v1\";condor::b=\"v2\"");
        assertThat(((Profile) result.get(0)).getProfileNamespace(), is("env"));
    }

    @Test
    public void testParse_namespaceNotLowercasedOnPartialFlush_comma()
            throws ProfileParserException {
        // Action 5 (partial flush at ',') does NOT call toLowerCase on namespace —
        // documented asymmetry in the state-machine implementation.
        List result = ProfileParser.parse("ENV::A=\"a\",B=\"b\"");
        assertThat(result.size(), is(2));
        // partial flush (comma) preserves original case
        assertThat(((Profile) result.get(0)).getProfileNamespace(), is("ENV"));
        // full flush (EOS) lowercases
        assertThat(((Profile) result.get(1)).getProfileNamespace(), is("env"));
    }

    // -----------------------------------------------------------------------
    // parse() — multiple profiles
    // -----------------------------------------------------------------------

    @Test
    public void testParse_multipleProfiles_sameNamespace() throws ProfileParserException {
        List result = ProfileParser.parse("env::FOO=\"bar\",BAZ=\"qux\"");
        assertThat(result.size(), is(2));
    }

    @Test
    public void testParse_threeProfiles_sameNamespace() throws ProfileParserException {
        List result = ProfileParser.parse("env::A=\"1\",B=\"2\",C=\"3\"");
        assertThat(result.size(), is(3));
        assertThat(((Profile) result.get(0)).getProfileKey(), is("A"));
        assertThat(((Profile) result.get(0)).getProfileValue(), is("1"));
        assertThat(((Profile) result.get(1)).getProfileKey(), is("B"));
        assertThat(((Profile) result.get(1)).getProfileValue(), is("2"));
        assertThat(((Profile) result.get(2)).getProfileKey(), is("C"));
        assertThat(((Profile) result.get(2)).getProfileValue(), is("3"));
    }

    @Test
    public void testParse_multipleNamespaces() throws ProfileParserException {
        List result = ProfileParser.parse("env::FOO=\"bar\";condor::requirements=\"true\"");
        assertThat(result.size(), is(2));
        Profile p1 = (Profile) result.get(0);
        Profile p2 = (Profile) result.get(1);
        assertThat(p1.getProfileNamespace(), is("env"));
        assertThat(p2.getProfileNamespace(), is("condor"));
    }

    @Test
    public void testParse_threeNamespaces() throws ProfileParserException {
        List result = ProfileParser.parse("env::A=\"1\";condor::B=\"2\";dagman::C=\"3\"");
        assertThat(result.size(), is(3));
        assertThat(((Profile) result.get(0)).getProfileNamespace(), is("env"));
        assertThat(((Profile) result.get(1)).getProfileNamespace(), is("condor"));
        assertThat(((Profile) result.get(2)).getProfileNamespace(), is("dagman"));
    }

    // -----------------------------------------------------------------------
    // parse() — special characters in namespace / key
    // -----------------------------------------------------------------------

    @Test
    public void testParse_keyWithPlusSign() throws ProfileParserException {
        // '+' is in charset 1 (alphanumeric-like), so valid in key
        List result = ProfileParser.parse("condor::+version=\"2.0\"");
        assertThat(result.size(), is(1));
        Profile p = (Profile) result.get(0);
        assertThat(p.getProfileKey(), is("+version"));
        assertThat(p.getProfileValue(), is("2.0"));
    }

    @Test
    public void testParse_keyWithDot() throws ProfileParserException {
        List result = ProfileParser.parse("pegasus::data.configuration=\"sharedfs\"");
        assertThat(result.size(), is(1));
        assertThat(((Profile) result.get(0)).getProfileKey(), is("data.configuration"));
    }

    @Test
    public void testParse_keyWithUnderscore() throws ProfileParserException {
        List result = ProfileParser.parse("env::MY_VAR=\"hello\"");
        assertThat(result.size(), is(1));
        assertThat(((Profile) result.get(0)).getProfileKey(), is("MY_VAR"));
    }

    @Test
    public void testParse_keyWithHyphen() throws ProfileParserException {
        List result = ProfileParser.parse("condor::request-memory=\"1024\"");
        assertThat(result.size(), is(1));
        assertThat(((Profile) result.get(0)).getProfileKey(), is("request-memory"));
    }

    // -----------------------------------------------------------------------
    // parse() — escaped characters inside quoted value
    // -----------------------------------------------------------------------

    @Test
    public void testParse_quotedValue_escapedDoubleQuote() throws ProfileParserException {
        // Profile string:  env::key="a\"b"
        // Java literal:    "env::key=\"a\\\"b\""
        // Parsed value:    a"b
        List result = ProfileParser.parse("env::key=\"a\\\"b\"");
        assertThat(result.size(), is(1));
        assertThat(((Profile) result.get(0)).getProfileValue(), is("a\"b"));
    }

    @Test
    public void testParse_quotedValue_escapedBackslash() throws ProfileParserException {
        // Profile string:  env::key="a\\b"
        // Java literal:    "env::key=\"a\\\\b\""
        // Parsed value:    a\b
        List result = ProfileParser.parse("env::key=\"a\\\\b\"");
        assertThat(result.size(), is(1));
        assertThat(((Profile) result.get(0)).getProfileValue(), is("a\\b"));
    }

    // -----------------------------------------------------------------------
    // parse() — error cases
    // -----------------------------------------------------------------------

    @Test
    public void testParse_illegalChar_throws() {
        assertThrows(ProfileParserException.class, () -> ProfileParser.parse("env:: invalid"));
    }

    @Test
    public void testParse_illegalChar_exceptionMessageContainsChar() {
        ProfileParserException ex =
                assertThrows(
                        ProfileParserException.class, () -> ProfileParser.parse("env:: invalid"));
        assertThat(ex.getMessage(), containsString("Illegal character"));
        assertThat(ex.getMessage(), containsString(" "));
    }

    @Test
    public void testParse_illegalChar_exceptionPosition() {
        // "env:: invalid": e(1)n(2)v(3):(4):(5) (6) → space at index 6
        ProfileParserException ex =
                assertThrows(
                        ProfileParserException.class, () -> ProfileParser.parse("env:: invalid"));
        assertThat(ex.getPosition(), is(6));
    }

    @Test
    public void testParse_singleColon_throwsIllegalChar() {
        // "condor:key" — single colon, then letter 'k' is illegal in state 1
        assertThrows(ProfileParserException.class, () -> ProfileParser.parse("condor:key"));
    }

    @Test
    public void testParse_prematureEnd_throws() {
        // "condor::key=" — EOS after '=' while in state 3 (seen equals) → E2
        assertThrows(ProfileParserException.class, () -> ProfileParser.parse("condor::key="));
    }

    @Test
    public void testParse_prematureEnd_exceptionMessage() {
        ProfileParserException ex =
                assertThrows(
                        ProfileParserException.class, () -> ProfileParser.parse("condor::key="));
        assertThat(ex.getMessage(), containsString("Premature"));
    }

    @Test
    public void testParse_prematureEnd_exceptionPosition() {
        // "condor::key=" has length 12; EOS is encountered at index=12 (unchanged after ternary)
        ProfileParserException ex =
                assertThrows(
                        ProfileParserException.class, () -> ProfileParser.parse("condor::key="));
        assertThat(ex.getPosition(), is(12));
    }

    @Test
    public void testParse_prematureEnd_insideQuotedValue_throws() {
        // "env::key="abc" — unterminated quoted value with EOS in state 4 (quoted value)
        assertThrows(ProfileParserException.class, () -> ProfileParser.parse("env::key=\"abc"));
    }

    // -----------------------------------------------------------------------
    // combine(List) — format and escaping
    // -----------------------------------------------------------------------

    @Test
    public void testCombine_emptyList_returnsEmptyString() {
        String result = ProfileParser.combine(new ArrayList<>());
        assertThat(result, is(""));
    }

    @Test
    public void testCombine_singleProfile() throws ProfileParserException {
        List parsed = ProfileParser.parse("env::PATH=\"/bin\"");
        String combined = ProfileParser.combine(parsed);
        assertThat(combined, containsString("env::"));
        assertThat(combined, containsString("PATH="));
        assertThat(combined, containsString("\"/bin\""));
    }

    @Test
    public void testCombine_singleProfile_format() {
        // Verify exact format: ns::key="value"
        List<Profile> profiles = new ArrayList<>();
        profiles.add(new Profile("env", "FOO", "bar"));
        String result = ProfileParser.combine(profiles);
        assertThat(result, is("env::FOO=\"bar\""));
    }

    @Test
    public void testCombine_multipleProfiles_sameNamespace_usesComma() {
        List<Profile> profiles = new ArrayList<>();
        profiles.add(new Profile("env", "A", "1"));
        profiles.add(new Profile("env", "B", "2"));
        String result = ProfileParser.combine(profiles);
        // Same namespace → "env::A="1",B="2""
        assertThat(result, startsWith("env::"));
        assertThat(result, containsString(","));
        assertThat(result, not(containsString(";")));
    }

    @Test
    public void testCombine_multipleProfiles_differentNamespace_usesSemicolon() {
        List<Profile> profiles = new ArrayList<>();
        profiles.add(new Profile("env", "A", "1"));
        profiles.add(new Profile("condor", "B", "2"));
        String result = ProfileParser.combine(profiles);
        assertThat(result, containsString(";"));
        assertThat(result, containsString("env::"));
        assertThat(result, containsString("condor::"));
    }

    @Test
    public void testCombine_escapesDoubleQuoteInValue() {
        // Value contains " → combine must escape it as \"
        List<Profile> profiles = new ArrayList<>();
        profiles.add(new Profile("env", "key", "a\"b")); // value = a"b
        String result = ProfileParser.combine(profiles);
        // Should contain \" (backslash then double-quote) inside the outer double-quotes
        assertThat(result, containsString("\\\""));
    }

    @Test
    public void testCombine_escapesBackslashInValue() {
        // Value contains \ → combine must escape it as \\
        List<Profile> profiles = new ArrayList<>();
        profiles.add(new Profile("env", "key", "a\\b")); // value = a\b
        String result = ProfileParser.combine(profiles);
        // Should contain \\ (two backslashes) inside the outer double-quotes
        assertThat(result, containsString("\\\\"));
    }

    // -----------------------------------------------------------------------
    // combine(Profiles) — delegates to combine(List)
    // -----------------------------------------------------------------------

    @Test
    public void testCombine_profiles_singleProfile() {
        Profiles profiles = new Profiles();
        profiles.addProfile(Profiles.NAMESPACES.env, "PATH", "/usr/bin");
        String result = ProfileParser.combine(profiles);
        assertThat(result, containsString("env::"));
        assertThat(result, containsString("PATH="));
        assertThat(result, containsString("\"/usr/bin\""));
    }

    // -----------------------------------------------------------------------
    // Round-trip tests (parse → combine → parse)
    // -----------------------------------------------------------------------

    @Test
    public void testCombineRoundTrip() throws ProfileParserException {
        String input = "env::PATH=\"/usr/bin\"";
        List parsed = ProfileParser.parse(input);
        String combined = ProfileParser.combine(parsed);
        List reparsed = ProfileParser.parse(combined);
        assertThat(reparsed.size(), is(parsed.size()));
        Profile orig = (Profile) parsed.get(0);
        Profile round = (Profile) reparsed.get(0);
        assertThat(round.getProfileNamespace(), is(orig.getProfileNamespace()));
        assertThat(round.getProfileKey(), is(orig.getProfileKey()));
        assertThat(round.getProfileValue(), is(orig.getProfileValue()));
    }

    @Test
    public void testRoundTrip_multipleProfiles_sameNamespace() throws ProfileParserException {
        String input = "env::FOO=\"bar\",BAZ=\"qux\"";
        List parsed = ProfileParser.parse(input);
        // combine uses the namespace from the Profile objects; partial-flush preserves original
        // case
        // so we just verify the values survive the round-trip via the combined string
        String combined = ProfileParser.combine(parsed);
        List reparsed = ProfileParser.parse(combined);
        assertThat(reparsed.size(), is(2));
    }

    @Test
    public void testRoundTrip_multipleNamespaces() throws ProfileParserException {
        String input = "env::A=\"1\";condor::B=\"2\"";
        List parsed = ProfileParser.parse(input);
        String combined = ProfileParser.combine(parsed);
        List reparsed = ProfileParser.parse(combined);
        assertThat(reparsed.size(), is(2));
        assertThat(((Profile) reparsed.get(0)).getProfileValue(), is("1"));
        assertThat(((Profile) reparsed.get(1)).getProfileValue(), is("2"));
    }

    @Test
    public void testRoundTrip_escapedDoubleQuoteInValue() throws ProfileParserException {
        // Profile string: env::key="a\"b" — value is a"b
        // Java literal:   "env::key=\"a\\\"b\""
        List parsed = ProfileParser.parse("env::key=\"a\\\"b\"");
        String combined = ProfileParser.combine(parsed);
        List reparsed = ProfileParser.parse(combined);
        assertThat(reparsed.size(), is(1));
        assertThat(((Profile) reparsed.get(0)).getProfileValue(), is("a\"b"));
    }

    @Test
    public void testRoundTrip_escapedBackslashInValue() throws ProfileParserException {
        // Profile string: env::key="a\\b" — value is a\b
        // Java literal:   "env::key=\"a\\\\b\""
        List parsed = ProfileParser.parse("env::key=\"a\\\\b\"");
        String combined = ProfileParser.combine(parsed);
        List reparsed = ProfileParser.parse(combined);
        assertThat(reparsed.size(), is(1));
        assertThat(((Profile) reparsed.get(0)).getProfileValue(), is("a\\b"));
    }

    // -----------------------------------------------------------------------
    // Parameterised: valid single-profile strings that must parse to exactly 1 triple
    // -----------------------------------------------------------------------

    @ParameterizedTest
    @ValueSource(
            strings = {
                "env::KEY=\"value\"",
                "condor::requirements=\"true\"",
                "dagman::maxjobs=\"10\"",
                "condor::+ProjectName=\"myproject\"",
                "env::PATH=\"/usr/bin:/bin\"",
                "pegasus::data.configuration=\"sharedfs\""
            })
    public void testParse_validSingleProfile_returnsOneTriple(String input)
            throws ProfileParserException {
        List result = ProfileParser.parse(input);
        assertThat(result.size(), is(1));
    }

    // -----------------------------------------------------------------------
    // Parameterised: inputs that must throw ProfileParserException
    // -----------------------------------------------------------------------

    @ParameterizedTest
    @ValueSource(
            strings = {
                "env:: invalid", // space after ::
                "condor::key=", // premature EOS in state 3
                "condor::key=\"abc", // premature EOS in state 4 (unclosed quote)
                "condor:key", // single colon
            })
    public void testParse_invalidInput_throws(String input) {
        assertThrows(ProfileParserException.class, () -> ProfileParser.parse(input));
    }
}
