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
package edu.isi.pegasus.planner.client;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Modifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import org.xml.sax.SAXParseException;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.helpers.LocatorImpl;

/** Unit tests for the DAXValidator class. */
public class DAXValidatorTest {

    private DAXValidator mValidator;

    @BeforeEach
    public void setUp() throws Exception {
        // DAXValidator only has DAXValidator(boolean verbose) constructor
        mValidator = new DAXValidator(false);
    }

    @Test
    public void testDAXValidatorExtendsDefaultHandler() {
        assertThat(mValidator instanceof DefaultHandler, is(true));
    }

    @Test
    public void testSchemaNamespaceConstant() {
        assertThat(DAXValidator.SCHEMA_NAMESPACE, is("https://pegasus.isi.edu/schema/DAX"));
    }

    @Test
    public void testInitialWarningCountIsZero() throws Exception {
        int warnings = (int) ReflectionTestUtils.getField(mValidator, "m_warnings");
        assertThat(warnings, is(0));
    }

    @Test
    public void testInitialErrorCountIsZero() throws Exception {
        int errors = (int) ReflectionTestUtils.getField(mValidator, "m_errors");
        assertThat(errors, is(0));
    }

    @Test
    public void testInitialFatalCountIsZero() throws Exception {
        int fatals = (int) ReflectionTestUtils.getField(mValidator, "m_fatals");
        assertThat(fatals, is(0));
    }

    @Test
    public void testIsConcreteClass() {
        assertThat(Modifier.isAbstract(DAXValidator.class.getModifiers()), is(false));
    }

    @Test
    public void testSchemaNamespaceIsNotEmpty() {
        assertThat(DAXValidator.SCHEMA_NAMESPACE.isEmpty(), is(false));
    }

    @Test
    public void testVendorParserClassIsNotEmpty() {
        assertThat(DAXValidator.vendorParserClass.isEmpty(), is(false));
    }

    @Test
    public void testDefaultSchemaFileName() throws Exception {
        assertThat(
                (String) ReflectionTestUtils.getField(mValidator, "m_schemafile"),
                is("dax-3.3.xsd"));
    }

    @Test
    public void testVerboseConstructorStoresVerboseFlag() throws Exception {
        DAXValidator verboseValidator = new DAXValidator(true);
        assertThat((Boolean) ReflectionTestUtils.getField(verboseValidator, "m_verbose"), is(true));
    }

    @Test
    public void testWarningIncrementsCounter() throws Exception {
        mValidator.setDocumentLocator(locatorAt(3, 7));

        mValidator.warning(new SAXParseException("warn", null));

        assertThat((Integer) ReflectionTestUtils.getField(mValidator, "m_warnings"), is(1));
    }

    @Test
    public void testErrorAndFatalIncrementCountersAndStatisticsReturnsTrue() throws Exception {
        mValidator.setDocumentLocator(locatorAt(4, 2));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintStream original = System.out;
        System.setOut(new PrintStream(out));
        try {
            mValidator.error(new SAXParseException("error", null));
            mValidator.fatalError(new SAXParseException("fatal", null));

            assertThat(mValidator.statistics(), is(true));
        } finally {
            System.setOut(original);
        }

        String text = out.toString();
        assertThat(text, containsString("1 errors"));
        assertThat(text, containsString("1 fatal errors detected"));
    }

    @Test
    public void testStatisticsReturnsFalseWhenNoErrorsOrFatals() {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintStream original = System.out;
        System.setOut(new PrintStream(out));
        try {
            assertThat(mValidator.statistics(), is(false));
        } finally {
            System.setOut(original);
        }

        assertThat(
                out.toString(),
                containsString("0 warnings, 0 errors, and 0 fatal errors detected."));
    }

    @Test
    public void testVerboseContentHandlerMethodsWriteLocationAwareOutput() throws Exception {
        DAXValidator verboseValidator = new DAXValidator(true);
        verboseValidator.setDocumentLocator(locatorAt(9, 5));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintStream original = System.out;
        System.setOut(new PrintStream(out));
        try {
            verboseValidator.startDocument();
            verboseValidator.characters(" text ".toCharArray(), 0, 6);
            verboseValidator.endDocument();
        } finally {
            System.setOut(original);
        }

        String text = out.toString();
        assertThat(text, containsString("9:5 *** start of document ***"));
        assertThat(text, containsString("9:5 \"text\""));
        assertThat(text, containsString("9:5 *** end of document ***"));
    }

    private LocatorImpl locatorAt(int line, int column) {
        LocatorImpl locator = new LocatorImpl();
        locator.setLineNumber(line);
        locator.setColumnNumber(column);
        return locator;
    }
}
