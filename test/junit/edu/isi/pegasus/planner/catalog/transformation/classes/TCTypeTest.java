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
package edu.isi.pegasus.planner.catalog.transformation.classes;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for the TCType enum. */
public class TCTypeTest {

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testEnumHasThreeValues() {
        assertEquals(3, TCType.values().length);
    }

    @Test
    public void testStaticBinaryExists() {
        assertNotNull(TCType.STATIC_BINARY);
    }

    @Test
    public void testInstalledExists() {
        assertNotNull(TCType.INSTALLED);
    }

    @Test
    public void testStageableExists() {
        assertNotNull(TCType.STAGEABLE);
    }

    @Test
    public void testValueOfInstalledReturnsCorrectConstant() {
        assertSame(TCType.INSTALLED, TCType.valueOf("INSTALLED"));
    }

    @Test
    public void testValueOfStageableReturnsCorrectConstant() {
        assertSame(TCType.STAGEABLE, TCType.valueOf("STAGEABLE"));
    }

    @Test
    public void testValueOfStaticBinaryReturnsCorrectConstant() {
        assertSame(TCType.STATIC_BINARY, TCType.valueOf("STATIC_BINARY"));
    }

    @Test
    public void testToStringMatchesName() {
        assertEquals("INSTALLED", TCType.INSTALLED.name());
        assertEquals("STAGEABLE", TCType.STAGEABLE.name());
        assertEquals("STATIC_BINARY", TCType.STATIC_BINARY.name());
    }

    @Test
    public void testOrdinalOrdering() {
        assertTrue(TCType.STATIC_BINARY.ordinal() < TCType.INSTALLED.ordinal());
        assertTrue(TCType.INSTALLED.ordinal() < TCType.STAGEABLE.ordinal());
    }

    @Test
    public void testValueOfInvalidThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> TCType.valueOf("NONEXISTENT"));
    }
}
