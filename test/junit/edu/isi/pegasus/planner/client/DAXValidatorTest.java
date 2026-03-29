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

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Modifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.xml.sax.helpers.DefaultHandler;

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
        assertTrue(
                mValidator instanceof DefaultHandler,
                "DAXValidator should extend SAX DefaultHandler");
    }

    @Test
    public void testSchemaNamespaceConstant() {
        assertEquals(
                "https://pegasus.isi.edu/schema/DAX",
                DAXValidator.SCHEMA_NAMESPACE,
                "SCHEMA_NAMESPACE should be the Pegasus DAX schema URI");
    }

    @Test
    public void testInitialWarningCountIsZero() throws Exception {
        java.lang.reflect.Field warnField = DAXValidator.class.getDeclaredField("m_warnings");
        warnField.setAccessible(true);
        int warnings = (int) warnField.get(mValidator);
        assertEquals(0, warnings, "Initial warning count should be 0");
    }

    @Test
    public void testInitialErrorCountIsZero() throws Exception {
        java.lang.reflect.Field errField = DAXValidator.class.getDeclaredField("m_errors");
        errField.setAccessible(true);
        int errors = (int) errField.get(mValidator);
        assertEquals(0, errors, "Initial error count should be 0");
    }

    @Test
    public void testInitialFatalCountIsZero() throws Exception {
        java.lang.reflect.Field fatalField = DAXValidator.class.getDeclaredField("m_fatals");
        fatalField.setAccessible(true);
        int fatals = (int) fatalField.get(mValidator);
        assertEquals(0, fatals, "Initial fatal count should be 0");
    }

    @Test
    public void testIsConcreteClass() {
        assertFalse(
                Modifier.isAbstract(DAXValidator.class.getModifiers()),
                "DAXValidator should be a concrete class");
    }

    @Test
    public void testSchemaNamespaceIsNotEmpty() {
        assertFalse(
                DAXValidator.SCHEMA_NAMESPACE.isEmpty(), "SCHEMA_NAMESPACE should not be empty");
    }

    @Test
    public void testVendorParserClassIsNotEmpty() {
        assertFalse(
                DAXValidator.vendorParserClass.isEmpty(), "vendorParserClass should not be empty");
    }
}
