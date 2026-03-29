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

/** Tests for the Arch enumerated type class. */
public class ArchTest {

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testPredefinedConstantsHaveCorrectValues() {
        assertEquals("INTEL32", Arch.INTEL32.getValue());
        assertEquals("INTEL64", Arch.INTEL64.getValue());
        assertEquals("AMD64", Arch.AMD64.getValue());
        assertEquals("SPARCV7", Arch.SPARCV7.getValue());
        assertEquals("SPARCV9", Arch.SPARCV9.getValue());
    }

    @Test
    public void testFromValueReturnsKnownConstant() {
        assertSame(Arch.INTEL32, Arch.fromValue("INTEL32"));
        assertSame(Arch.INTEL64, Arch.fromValue("INTEL64"));
        assertSame(Arch.AMD64, Arch.fromValue("AMD64"));
        assertSame(Arch.SPARCV7, Arch.fromValue("SPARCV7"));
        assertSame(Arch.SPARCV9, Arch.fromValue("SPARCV9"));
    }

    @Test
    public void testFromValueIsCaseInsensitive() {
        assertSame(Arch.INTEL32, Arch.fromValue("intel32"));
        assertSame(Arch.AMD64, Arch.fromValue("amd64"));
        assertSame(Arch.INTEL64, Arch.fromValue("InTeL64"));
    }

    @Test
    public void testFromStringDelegatesToFromValue() {
        assertSame(Arch.INTEL32, Arch.fromString("INTEL32"));
        assertSame(Arch.SPARCV9, Arch.fromString("sparcv9"));
    }

    @Test
    public void testFromValueThrowsForUnknownArchitecture() {
        assertThrows(IllegalStateException.class, () -> Arch.fromValue("UNKNOWN_ARCH"));
    }

    @Test
    public void testToStringReturnsValue() {
        assertEquals("INTEL32", Arch.INTEL32.toString());
        assertEquals("AMD64", Arch.AMD64.toString());
    }

    @Test
    public void testEqualsSameInstance() {
        assertTrue(Arch.INTEL32.equals(Arch.INTEL32));
    }

    @Test
    public void testEqualsDifferentInstances() {
        assertFalse(Arch.INTEL32.equals(Arch.INTEL64));
    }

    @Test
    public void testHashCodeConsistentWithToString() {
        assertEquals(Arch.INTEL32.toString().hashCode(), Arch.INTEL32.hashCode());
    }

    @Test
    public void testErrorMessageIsNonNull() {
        assertNotNull(Arch.err);
        assertFalse(Arch.err.isEmpty());
    }
}
