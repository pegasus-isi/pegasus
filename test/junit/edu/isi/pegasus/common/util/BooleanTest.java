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

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
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
        assertThat(Boolean.TRUE, is("true"));
    }

    @Test
    public void testFalseConstantValue() {
        assertThat(Boolean.FALSE, is("false"));
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
        assertThat(Boolean.print(true), is(Boolean.TRUE));
    }

    @Test
    public void testPrint_falseReturnsFalseConstant() {
        assertThat(Boolean.print(false), is(Boolean.FALSE));
    }

    // -----------------------------------------------------------------------
    // parse(String) — true representations
    // -----------------------------------------------------------------------

    @ParameterizedTest
    @CsvSource({"true", "True", "TRUE", "yes", "Yes", "on", "On", "1", "42"})
    public void testParse_trueRepresentations(String input) {
        assertThat(Boolean.parse(input), is(true));
    }

    @Test
    public void testParse_trueWithSurroundingWhitespace() {
        assertThat(Boolean.parse("  true  "), is(true));
    }

    @Test
    public void testParse_yesWithSurroundingWhitespace() {
        assertThat(Boolean.parse("  YES  "), is(true));
    }

    @Test
    public void testParse_onUpperCase() {
        assertThat(Boolean.parse("ON"), is(true));
    }

    @Test
    public void testParse_largePositiveNumber() {
        assertThat(Boolean.parse("9999"), is(true));
    }

    // -----------------------------------------------------------------------
    // parse(String) — false representations
    // -----------------------------------------------------------------------

    @ParameterizedTest
    @CsvSource({"false", "False", "FALSE", "no", "No", "off", "Off", "0"})
    public void testParse_falseRepresentations(String input) {
        assertThat(Boolean.parse(input), is(false));
    }

    @Test
    public void testParse_falseWithSurroundingWhitespace() {
        assertThat(Boolean.parse("  false  "), is(false));
    }

    @Test
    public void testParse_noUpperCase() {
        assertThat(Boolean.parse("NO"), is(false));
    }

    @Test
    public void testParse_offUpperCase() {
        assertThat(Boolean.parse("OFF"), is(false));
    }

    @Test
    public void testParse_numberZeroIsFalse() {
        assertThat(Boolean.parse("0"), is(false));
    }

    @Test
    public void testParse_numberNonZeroIsTrue() {
        assertThat(Boolean.parse("5"), is(true));
    }

    // -----------------------------------------------------------------------
    // parse(String) — default fallback (no-default overload defaults to false)
    // -----------------------------------------------------------------------

    @Test
    public void testParse_nullDefaultsFalse() {
        assertThat(Boolean.parse(null), is(false));
    }

    @Test
    public void testParse_emptyStringDefaultsFalse() {
        assertThat(Boolean.parse(""), is(false));
    }

    @Test
    public void testParse_whitespaceOnlyDefaultsFalse() {
        assertThat(Boolean.parse("   "), is(false));
    }

    @Test
    public void testParse_unknownStringDefaultsFalse() {
        assertThat(Boolean.parse("maybe"), is(false));
    }

    @Test
    public void testParse_randomWordDefaultsFalse() {
        assertThat(Boolean.parse("pegasus"), is(false));
    }

    // -----------------------------------------------------------------------
    // parse(String) — negative numbers fall to keyword branch, return default
    // -----------------------------------------------------------------------

    @Test
    public void testParse_negativeNumberDefaultsFalse() {
        // '-' is not a digit so the numeric branch is not entered;
        // "-1" matches no keyword, so the default (false) is returned
        assertThat(Boolean.parse("-1"), is(false));
    }

    @Test
    public void testParse_negativeNumberRespectsDefaultTrue() {
        assertThat(Boolean.parse("-1", true), is(true));
    }

    // -----------------------------------------------------------------------
    // parse(String) — digit-leading non-integer triggers NumberFormatException path
    // -----------------------------------------------------------------------

    @Test
    public void testParse_digitLeadingNonIntegerWithDefaultFalse() {
        // "1.5" starts with a digit, enters numeric branch, Long.parseLong throws NFE,
        // value = deflt ? 1 : 0 = 0, returns false
        assertThat(Boolean.parse("1.5"), is(false));
    }

    @Test
    public void testParse_digitLeadingNonIntegerWithDefaultTrue() {
        // Same NFE path but deflt=true → value = 1 → returns true
        assertThat(Boolean.parse("1.5", true), is(true));
    }

    @Test
    public void testParse_digitLeadingAlphaWithDefaultFalse() {
        // "1abc" starts with digit, NFE path, deflt=false → value=0 → false
        assertThat(Boolean.parse("1abc"), is(false));
    }

    @Test
    public void testParse_digitLeadingAlphaWithDefaultTrue() {
        assertThat(Boolean.parse("1abc", true), is(true));
    }

    // -----------------------------------------------------------------------
    // parse(String, boolean) — explicit default
    // -----------------------------------------------------------------------

    @Test
    public void testParseWithDefault_nullUsesDefault() {
        assertThat(Boolean.parse(null, true), is(true));
        assertThat(Boolean.parse(null, false), is(false));
    }

    @Test
    public void testParseWithDefault_emptyStringUsesDefault() {
        assertThat(Boolean.parse("", true), is(true));
        assertThat(Boolean.parse("", false), is(false));
    }

    @Test
    public void testParseWithDefault_whitespaceOnlyUsesDefault() {
        assertThat(Boolean.parse("   ", true), is(true));
        assertThat(Boolean.parse("   ", false), is(false));
    }

    @Test
    public void testParseWithDefault_unknownUsesDefault() {
        assertThat(Boolean.parse("unknown", true), is(true));
        assertThat(Boolean.parse("unknown", false), is(false));
    }

    @Test
    public void testParseWithDefault_knownOverridesDefault() {
        assertThat(Boolean.parse("yes", false), is(true));
        assertThat(Boolean.parse("no", true), is(false));
    }

    @Test
    public void testParseWithDefault_numericZeroReturnsFalseRegardlessOfDefault() {
        // "0" is a valid numeric → value=0 → always false, default is ignored
        assertThat(Boolean.parse("0", true), is(false));
    }

    @Test
    public void testParseWithDefault_numericNonZeroReturnsTrueRegardlessOfDefault() {
        // "1" is a valid numeric → value=1 → always true, default is ignored
        assertThat(Boolean.parse("1", false), is(true));
    }

    @Test
    public void testParseWithDefault_trueKeywordOverridesDefaultFalse() {
        assertThat(Boolean.parse("true", false), is(true));
    }

    @Test
    public void testParseWithDefault_falseKeywordOverridesDefaultTrue() {
        assertThat(Boolean.parse("false", true), is(false));
    }

    @Test
    public void testParseWithDefault_onKeywordOverridesDefaultFalse() {
        assertThat(Boolean.parse("on", false), is(true));
    }

    @Test
    public void testParseWithDefault_offKeywordOverridesDefaultTrue() {
        assertThat(Boolean.parse("off", true), is(false));
    }

    @Test
    public void testParse_plusSignedNumberUsesDefaultFalse() {
        assertThat(Boolean.parse("+1"), is(false));
    }

    @Test
    public void testParse_plusSignedNumberUsesDefaultTrue() {
        assertThat(Boolean.parse("+1", true), is(true));
    }

    @Test
    public void testParse_leadingZeroNumberIsFalseOnlyForZero() {
        assertThat(Boolean.parse("000"), is(false));
        assertThat(Boolean.parse("0007"), is(true));
    }

    @Test
    public void testBooleanDeclaresOnlyExpectedStaticMethods() {
        Method[] methods = Boolean.class.getDeclaredMethods();
        assertThat(methods.length, is(3));
        for (Method method : methods) {
            assertThat(Modifier.isStatic(method.getModifiers()), is(true));
        }
    }
}
