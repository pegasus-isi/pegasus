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
package edu.isi.pegasus.planner.parser;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.parser.tokens.CloseBrace;
import edu.isi.pegasus.planner.parser.tokens.Identifier;
import edu.isi.pegasus.planner.parser.tokens.OpenBrace;
import edu.isi.pegasus.planner.parser.tokens.QuotedString;
import edu.isi.pegasus.planner.parser.tokens.Token;
import edu.isi.pegasus.planner.parser.tokens.TransformationCatalogReservedWord;
import java.io.StringReader;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class TransformationCatalogTextScannerTest {

    /*
    @Test
    public void testSomeMethod() {
        org.hamcrest.MatcherAssert.assertThat(1, org.hamcrest.Matchers.is(1));
    }
    */

    @Test
    public void testHasMoreTokensSkipsWhitespaceAndComments() throws Exception {
        TransformationCatalogTextScanner scanner =
                new TransformationCatalogTextScanner(
                        new StringReader("   # comment only\n   tr"), false);

        assertThat(
                "Scanner should skip whitespace and comments", scanner.hasMoreTokens(), is(true));

        Token token = scanner.nextToken();
        assertThat(
                "The first real token should be the reserved word tr",
                token,
                org.hamcrest.CoreMatchers.instanceOf(TransformationCatalogReservedWord.class));
        assertThat(
                "Reserved word token should match TRANSFORMATION",
                ((TransformationCatalogReservedWord) token).getValue(),
                is(TransformationCatalogReservedWord.TRANSFORMATION));
    }

    @Test
    public void testScannerAllowsExpandedTransformationAndSiteIdentifiers() throws Exception {
        TransformationCatalogTextScanner scanner =
                new TransformationCatalogTextScanner(
                        new StringReader("tr ns::keg-1.0+/bin site local-site.example@grid"),
                        false);

        Token tr = scanner.nextToken();
        Token transformationName = scanner.nextToken();
        Token site = scanner.nextToken();
        Token siteName = scanner.nextToken();

        assertThat(
                "First token should be the transformation reserved word",
                ((TransformationCatalogReservedWord) tr).getValue(),
                is(TransformationCatalogReservedWord.TRANSFORMATION));
        assertThat(
                "Transformation identifiers should allow :, -, ., / and + after tr",
                ((Identifier) transformationName).getValue(),
                is("ns::keg-1.0+/bin"));
        assertThat(
                "Third token should be the site reserved word",
                ((TransformationCatalogReservedWord) site).getValue(),
                is(TransformationCatalogReservedWord.SITE));
        assertThat(
                "Site identifiers should allow -, . and @ after site",
                ((Identifier) siteName).getValue(),
                is("local-site.example@grid"));
    }

    @Test
    public void testScannerParsesBracesAndQuotedStringsWithEscapes() throws Exception {
        TransformationCatalogTextScanner scanner =
                new TransformationCatalogTextScanner(
                        new StringReader("{ \"a\\\"b\\\\c\" }"), false);

        Token open = scanner.nextToken();
        Token quoted = scanner.nextToken();
        Token close = scanner.nextToken();
        Token eof = scanner.nextToken();

        assertThat(
                "Open brace should tokenize correctly",
                open,
                org.hamcrest.CoreMatchers.instanceOf(OpenBrace.class));
        assertThat(
                "Quoted string should tokenize correctly",
                quoted,
                org.hamcrest.CoreMatchers.instanceOf(QuotedString.class));
        assertThat(
                "Escaped characters inside quoted strings should be preserved as content",
                ((QuotedString) quoted).getValue(),
                is("a\"b\\c"));
        assertThat(
                "Close brace should tokenize correctly",
                close,
                org.hamcrest.CoreMatchers.instanceOf(CloseBrace.class));
        assertThat("Scanner should return null at EOF", eof, nullValue());
    }

    @Test
    public void testScannerTreatsReservedWordsCaseInsensitively() throws Exception {
        TransformationCatalogTextScanner scanner =
                new TransformationCatalogTextScanner(
                        new StringReader("TR PROFILE PFN TYPE"), false);

        assertThat(
                "TR should be recognized case-insensitively",
                ((TransformationCatalogReservedWord) scanner.nextToken()).getValue(),
                is(TransformationCatalogReservedWord.TRANSFORMATION));
        assertThat(
                "PROFILE should be recognized case-insensitively",
                ((TransformationCatalogReservedWord) scanner.nextToken()).getValue(),
                is(TransformationCatalogReservedWord.PROFILE));
        assertThat(
                "PFN should be recognized case-insensitively",
                ((TransformationCatalogReservedWord) scanner.nextToken()).getValue(),
                is(TransformationCatalogReservedWord.PFN));
        assertThat(
                "TYPE should be recognized case-insensitively",
                ((TransformationCatalogReservedWord) scanner.nextToken()).getValue(),
                is(TransformationCatalogReservedWord.TYPE));
    }

    @Test
    public void testScannerRejectsUnterminatedQuotedString() throws Exception {
        TransformationCatalogTextScanner scanner =
                new TransformationCatalogTextScanner(new StringReader("\"unterminated"), false);

        ScannerException exception =
                assertThrows(
                        ScannerException.class,
                        scanner::nextToken,
                        "Scanner should reject unterminated quoted strings");

        assertThat(
                "Error message should mention the unterminated quoted string",
                exception.getMessage(),
                containsString("unterminated quoted string"));
    }
}
