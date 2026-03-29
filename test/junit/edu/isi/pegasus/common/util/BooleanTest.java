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
import org.junit.jupiter.params.provider.CsvSource;

/** @author Rajiv Mayani */
public class BooleanTest {

    // -----------------------------------------------------------------------
    // Constants
    // -----------------------------------------------------------------------

    @Test
    public void testTrueConstantValue() {
        assertEquals("true", Boolean.TRUE, "Boolean.TRUE constant should equal \"true\"");
    }

    @Test
    public void testFalseConstantValue() {
        assertEquals("false", Boolean.FALSE, "Boolean.FALSE constant should equal \"false\"");
    }

    // -----------------------------------------------------------------------
    // print()
    // -----------------------------------------------------------------------

    @Test
    public void testPrint_true() {
        assertThat(Boolean.print(true), is("true"));
    }

    @Test
    public void testPrint_false() {
        assertThat(Boolean.print(false), is("false"));
    }

    @Test
    public void testPrint_trueReturnsTrueConstant() {
        assertSame(
                Boolean.TRUE,
                Boolean.print(true),
                "print(true) should return the Boolean.TRUE constant instance");
    }

    @Test
    public void testPrint_falseReturnsFalseConstant() {
        assertSame(
                Boolean.FALSE,
                Boolean.print(false),
                "print(false) should return the Boolean.FALSE constant instance");
    }

    // -----------------------------------------------------------------------
    // parse(String) — true representations
    // -----------------------------------------------------------------------

    @ParameterizedTest
    @CsvSource({"true", "True", "TRUE", "yes", "Yes", "on", "On", "1", "42"})
    public void testParse_trueRepresentations(String input) {
        assertTrue(Boolean.parse(input));
    }

    @Test
    public void testParse_trueWithSurroundingWhitespace() {
        assertTrue(Boolean.parse("  true  "), "\"  true  \" should parse to true after trimming");
    }

    @Test
    public void testParse_yesWithSurroundingWhitespace() {
        assertTrue(
                Boolean.parse("  YES  "), "\"  YES  \" should parse to true after trim+lowercase");
    }

    @Test
    public void testParse_onUpperCase() {
        assertTrue(Boolean.parse("ON"), "\"ON\" should parse to true");
    }

    @Test
    public void testParse_largePositiveNumber() {
        assertTrue(Boolean.parse("9999"), "A large positive number string should parse to true");
    }

    // -----------------------------------------------------------------------
    // parse(String) — false representations
    // -----------------------------------------------------------------------

    @ParameterizedTest
    @CsvSource({"false", "False", "FALSE", "no", "No", "off", "Off", "0"})
    public void testParse_falseRepresentations(String input) {
        assertFalse(Boolean.parse(input));
    }

    @Test
    public void testParse_falseWithSurroundingWhitespace() {
        assertFalse(
                Boolean.parse("  false  "), "\"  false  \" should parse to false after trimming");
    }

    @Test
    public void testParse_noUpperCase() {
        assertFalse(Boolean.parse("NO"), "\"NO\" should parse to false");
    }

    @Test
    public void testParse_offUpperCase() {
        assertFalse(Boolean.parse("OFF"), "\"OFF\" should parse to false");
    }

    @Test
    public void testParse_numberZeroIsFalse() {
        assertFalse(Boolean.parse("0"));
    }

    @Test
    public void testParse_numberNonZeroIsTrue() {
        assertTrue(Boolean.parse("5"));
    }

    // -----------------------------------------------------------------------
    // parse(String) — default fallback (no-default overload defaults to false)
    // -----------------------------------------------------------------------

    @Test
    public void testParse_nullDefaultsFalse() {
        assertFalse(Boolean.parse(null));
    }

    @Test
    public void testParse_emptyStringDefaultsFalse() {
        assertFalse(Boolean.parse(""));
    }

    @Test
    public void testParse_whitespaceOnlyDefaultsFalse() {
        assertFalse(
                Boolean.parse("   "),
                "Whitespace-only string trims to empty and should default to false");
    }

    @Test
    public void testParse_unknownStringDefaultsFalse() {
        assertFalse(Boolean.parse("maybe"));
    }

    @Test
    public void testParse_randomWordDefaultsFalse() {
        assertFalse(Boolean.parse("pegasus"), "An unrecognised word should default to false");
    }

    // -----------------------------------------------------------------------
    // parse(String) — negative numbers fall to keyword branch, return default
    // -----------------------------------------------------------------------

    @Test
    public void testParse_negativeNumberDefaultsFalse() {
        // '-' is not a digit so the numeric branch is not entered;
        // "-1" matches no keyword, so the default (false) is returned
        assertFalse(
                Boolean.parse("-1"),
                "Negative number string does not enter numeric branch; should default to false");
    }

    @Test
    public void testParse_negativeNumberRespectsDefaultTrue() {
        assertTrue(
                Boolean.parse("-1", true),
                "Negative number string should return the supplied default (true)");
    }

    // -----------------------------------------------------------------------
    // parse(String) — digit-leading non-integer triggers NumberFormatException path
    // -----------------------------------------------------------------------

    @Test
    public void testParse_digitLeadingNonIntegerWithDefaultFalse() {
        // "1.5" starts with a digit, enters numeric branch, Long.parseLong throws NFE,
        // value = deflt ? 1 : 0 = 0, returns false
        assertFalse(
                Boolean.parse("1.5"),
                "Digit-leading non-integer should use default (false) via NFE path");
    }

    @Test
    public void testParse_digitLeadingNonIntegerWithDefaultTrue() {
        // Same NFE path but deflt=true → value = 1 → returns true
        assertTrue(
                Boolean.parse("1.5", true),
                "Digit-leading non-integer should use default (true) via NFE path");
    }

    @Test
    public void testParse_digitLeadingAlphaWithDefaultFalse() {
        // "1abc" starts with digit, NFE path, deflt=false → value=0 → false
        assertFalse(
                Boolean.parse("1abc"),
                "\"1abc\" should trigger NFE path and return default (false)");
    }

    @Test
    public void testParse_digitLeadingAlphaWithDefaultTrue() {
        assertTrue(
                Boolean.parse("1abc", true),
                "\"1abc\" should trigger NFE path and return default (true)");
    }

    // -----------------------------------------------------------------------
    // parse(String, boolean) — explicit default
    // -----------------------------------------------------------------------

    @Test
    public void testParseWithDefault_nullUsesDefault() {
        assertTrue(Boolean.parse(null, true));
        assertFalse(Boolean.parse(null, false));
    }

    @Test
    public void testParseWithDefault_emptyStringUsesDefault() {
        assertTrue(Boolean.parse("", true), "Empty string with default=true should return true");
        assertFalse(
                Boolean.parse("", false), "Empty string with default=false should return false");
    }

    @Test
    public void testParseWithDefault_whitespaceOnlyUsesDefault() {
        assertTrue(
                Boolean.parse("   ", true), "Whitespace-only with default=true should return true");
        assertFalse(
                Boolean.parse("   ", false),
                "Whitespace-only with default=false should return false");
    }

    @Test
    public void testParseWithDefault_unknownUsesDefault() {
        assertTrue(Boolean.parse("unknown", true));
        assertFalse(Boolean.parse("unknown", false));
    }

    @Test
    public void testParseWithDefault_knownOverridesDefault() {
        assertTrue(Boolean.parse("yes", false));
        assertFalse(Boolean.parse("no", true));
    }

    @Test
    public void testParseWithDefault_numericZeroReturnsFalseRegardlessOfDefault() {
        // "0" is a valid numeric → value=0 → always false, default is ignored
        assertFalse(
                Boolean.parse("0", true),
                "Numeric \"0\" should return false even when default is true");
    }

    @Test
    public void testParseWithDefault_numericNonZeroReturnsTrueRegardlessOfDefault() {
        // "1" is a valid numeric → value=1 → always true, default is ignored
        assertTrue(
                Boolean.parse("1", false),
                "Numeric \"1\" should return true even when default is false");
    }

    @Test
    public void testParseWithDefault_trueKeywordOverridesDefaultFalse() {
        assertTrue(
                Boolean.parse("true", false),
                "\"true\" keyword should override a default of false");
    }

    @Test
    public void testParseWithDefault_falseKeywordOverridesDefaultTrue() {
        assertFalse(
                Boolean.parse("false", true),
                "\"false\" keyword should override a default of true");
    }

    @Test
    public void testParseWithDefault_onKeywordOverridesDefaultFalse() {
        assertTrue(Boolean.parse("on", false), "\"on\" should override a default of false");
    }

    @Test
    public void testParseWithDefault_offKeywordOverridesDefaultTrue() {
        assertFalse(Boolean.parse("off", true), "\"off\" should override a default of true");
    }
}
