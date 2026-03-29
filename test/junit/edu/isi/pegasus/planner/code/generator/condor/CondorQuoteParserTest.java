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

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for the CondorQuoteParser utility class. */
public class CondorQuoteParserTest {
    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testQuotePlainString() throws CondorQuoteParserException {
        String result = CondorQuoteParser.quote("hello");
        assertNotNull(result);
        assertEquals("hello", result);
    }

    @Test
    public void testQuoteEmptyString() throws CondorQuoteParserException {
        String result = CondorQuoteParser.quote("");
        assertNotNull(result);
        assertEquals("", result);
    }

    @Test
    public void testQuoteStringWithSingleQuotes() throws CondorQuoteParserException {
        // single quotes in input should be preserved
        String result = CondorQuoteParser.quote("'test'");
        assertNotNull(result);
    }

    @Test
    public void testQuoteStringWithDoubleQuoteConvertsToSingleQuote()
            throws CondorQuoteParserException {
        // " not enclosed in single quotes => converted to '
        String result = CondorQuoteParser.quote("Karan \"Vahi\"");
        assertNotNull(result);
        assertTrue(result.contains("'Vahi'"));
    }

    @Test
    public void testQuoteWithEncloseAddsOuterQuotes() throws CondorQuoteParserException {
        String result = CondorQuoteParser.quote("hello", true);
        assertNotNull(result);
        assertTrue(result.startsWith("\""));
    }

    @Test
    public void testQuoteWithoutEncloseNoOuterQuotes() throws CondorQuoteParserException {
        String result = CondorQuoteParser.quote("hello", false);
        assertNotNull(result);
        assertFalse(result.startsWith("\""));
    }

    @Test
    public void testQuoteClassIsNotAbstract() {
        assertFalse(java.lang.reflect.Modifier.isAbstract(CondorQuoteParser.class.getModifiers()));
    }

    @Test
    public void testQuoteClassExists() {
        assertNotNull(CondorQuoteParser.class);
    }
}
