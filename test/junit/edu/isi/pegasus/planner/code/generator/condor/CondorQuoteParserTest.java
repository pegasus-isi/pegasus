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
package edu.isi.pegasus.planner.code.generator.condor;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/** Tests for the CondorQuoteParser utility class. */
public class CondorQuoteParserTest {

    @Test
    public void testQuotePlainString() throws CondorQuoteParserException {
        String result = CondorQuoteParser.quote("hello");
        assertThat(result, is("hello"));
    }

    @Test
    public void testQuoteEmptyString() throws CondorQuoteParserException {
        String result = CondorQuoteParser.quote("");
        assertThat(result, is(""));
    }

    @Test
    public void testQuoteStringWithSingleQuotes() throws CondorQuoteParserException {
        // single quotes in input should be preserved
        String result = CondorQuoteParser.quote("'test'");
        assertThat(result, notNullValue());
    }

    @Test
    public void testQuoteStringWithDoubleQuoteConvertsToSingleQuote()
            throws CondorQuoteParserException {
        // " not enclosed in single quotes => converted to '
        String result = CondorQuoteParser.quote("Karan \"Vahi\"");
        assertThat(result, containsString("'Vahi'"));
    }

    @Test
    public void testQuoteWithEncloseAddsOuterQuotes() throws CondorQuoteParserException {
        String result = CondorQuoteParser.quote("hello", true);
        assertThat(result, notNullValue());
        assertThat(result.startsWith("\""), is(true));
    }

    @Test
    public void testQuoteWithoutEncloseNoOuterQuotes() throws CondorQuoteParserException {
        String result = CondorQuoteParser.quote("hello", false);
        assertThat(result, notNullValue());
        assertThat(result.startsWith("\""), is(false));
    }

    @Test
    public void testQuoteClassExists() {
        assertThat(CondorQuoteParser.class, notNullValue());
    }

    @Test
    public void testEscapedSingleQuotesBecomeDoubledSingleQuotes()
            throws CondorQuoteParserException {
        assertThat(CondorQuoteParser.quote("\\'Test Input\\'"), is("''Test Input''"));
    }

    @Test
    public void testEscapedDoubleQuotesBecomeDoubledDoubleQuotes()
            throws CondorQuoteParserException {
        assertThat(CondorQuoteParser.quote("\\\"Test Input\\\""), is("\"\"Test Input\"\""));
    }

    @Test
    public void testDoubleQuotesInsideSingleQuotesAreDoubled() throws CondorQuoteParserException {
        assertThat(CondorQuoteParser.quote("'Test \"Input\"'"), is("'Test \"\"Input\"\"'"));
    }

    @Test
    public void testQuoteEmptyStringWithEncloseAddsBothQuotes() throws CondorQuoteParserException {
        assertThat(CondorQuoteParser.quote("", true), is("\"\""));
    }

    @Test
    public void testQuoteThrowsForTrailingBackslash() {
        CondorQuoteParserException exception =
                assertThrows(CondorQuoteParserException.class, () -> CondorQuoteParser.quote("\\"));

        assertThat(exception.getMessage(), containsString("Unexpected end of input"));
        assertThat(exception.getPosition(), is(1));
    }

    @Test
    public void testQuoteThrowsForUnmatchedSingleQuotes() {
        CondorQuoteParserException exception =
                assertThrows(
                        CondorQuoteParserException.class,
                        () -> CondorQuoteParser.quote("'unterminated"));

        assertThat(exception.getMessage(), containsString("Unmatched Single Quotes"));
        assertThat(exception.getPosition(), is(13));
    }
}
