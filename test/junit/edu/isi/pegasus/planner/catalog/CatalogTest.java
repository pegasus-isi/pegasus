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
package edu.isi.pegasus.planner.catalog;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for the Catalog interface constants and structure. */
public class CatalogTest {

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testCatalogIsInterface() {
        assertTrue(Catalog.class.isInterface(), "Catalog should be an interface");
    }

    @Test
    public void testDbAllPrefixConstant() {
        assertEquals("pegasus.catalog.*.db", Catalog.DB_ALL_PREFIX);
    }

    @Test
    public void testParserDocumentSizePropertyKeyConstant() {
        assertEquals("parser.document.size", Catalog.PARSER_DOCUMENT_SIZE_PROPERTY_KEY);
    }

    @Test
    public void testCatalogDeclaresMethods() {
        // Verify the three core methods are declared
        boolean hasConnect = false;
        boolean hasClose = false;
        boolean hasIsClosed = false;
        for (java.lang.reflect.Method m : Catalog.class.getDeclaredMethods()) {
            if (m.getName().equals("connect")) hasConnect = true;
            if (m.getName().equals("close")) hasClose = true;
            if (m.getName().equals("isClosed")) hasIsClosed = true;
        }
        assertTrue(hasConnect, "Catalog should declare connect method");
        assertTrue(hasClose, "Catalog should declare close method");
        assertTrue(hasIsClosed, "Catalog should declare isClosed method");
    }

    @Test
    public void testConnectMethodSignature() throws NoSuchMethodException {
        java.lang.reflect.Method connect =
                Catalog.class.getDeclaredMethod("connect", java.util.Properties.class);
        assertEquals(boolean.class, connect.getReturnType());
    }

    @Test
    public void testIsClosedMethodSignature() throws NoSuchMethodException {
        java.lang.reflect.Method isClosed = Catalog.class.getDeclaredMethod("isClosed");
        assertEquals(boolean.class, isClosed.getReturnType());
    }
}
