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
package edu.isi.pegasus.planner.catalog.site.classes;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for the XML4PrintVisitor class. */
public class XML4PrintVisitorTest {
    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testXML4PrintVisitorExtendsAbstractXMLPrintVisitor() {
        assertTrue(AbstractXMLPrintVisitor.class.isAssignableFrom(XML4PrintVisitor.class));
    }

    @Test
    public void testSchemaNamespaceConstant() {
        assertNotNull(XML4PrintVisitor.SCHEMA_NAMESPACE);
        assertTrue(XML4PrintVisitor.SCHEMA_NAMESPACE.contains("pegasus.isi.edu"));
    }

    @Test
    public void testSchemaLocationConstant() {
        assertNotNull(XML4PrintVisitor.SCHEMA_LOCATION);
        assertTrue(XML4PrintVisitor.SCHEMA_LOCATION.endsWith(".xsd"));
    }

    @Test
    public void testSchemaVersionConstant() {
        assertEquals("4.0", XML4PrintVisitor.SCHEMA_VERSION);
    }

    @Test
    public void testClassIsConcreteClass() {
        assertFalse(java.lang.reflect.Modifier.isAbstract(XML4PrintVisitor.class.getModifiers()));
    }

    @Test
    public void testClassIsNotInterface() {
        assertFalse(XML4PrintVisitor.class.isInterface());
    }
}
