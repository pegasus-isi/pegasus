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
package edu.isi.pegasus.planner.parser.tokens;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for {@link TransformationCatalogReservedWord}. */
public class TransformationCatalogReservedWordTest {

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testSymbolTableContainsTr() {
        Map<String, TransformationCatalogReservedWord> table =
                TransformationCatalogReservedWord.symbolTable();
        assertTrue(table.containsKey("tr"));
    }

    @Test
    public void testSymbolTableContainsSite() {
        assertTrue(TransformationCatalogReservedWord.symbolTable().containsKey("site"));
    }

    @Test
    public void testTrTokenValue() {
        TransformationCatalogReservedWord w =
                TransformationCatalogReservedWord.symbolTable().get("tr");
        assertEquals(TransformationCatalogReservedWord.TRANSFORMATION, w.getValue());
    }

    @Test
    public void testSiteTokenValue() {
        TransformationCatalogReservedWord w =
                TransformationCatalogReservedWord.symbolTable().get("site");
        assertEquals(TransformationCatalogReservedWord.SITE, w.getValue());
    }

    @Test
    public void testContainerTokenValue() {
        TransformationCatalogReservedWord w =
                TransformationCatalogReservedWord.symbolTable().get("container");
        assertEquals(TransformationCatalogReservedWord.CONTAINER, w.getValue());
    }

    @Test
    public void testSymbolTableContainsMountKeyword() {
        assertTrue(TransformationCatalogReservedWord.symbolTable().containsKey("mount"));
    }

    @Test
    public void testConstantValues() {
        assertEquals(0, TransformationCatalogReservedWord.TRANSFORMATION);
        assertEquals(1, TransformationCatalogReservedWord.SITE);
        assertEquals(2, TransformationCatalogReservedWord.PROFILE);
        assertEquals(3, TransformationCatalogReservedWord.PFN);
    }

    @Test
    public void testIsToken() {
        TransformationCatalogReservedWord w =
                TransformationCatalogReservedWord.symbolTable().get("pfn");
        assertInstanceOf(Token.class, w);
    }
}
