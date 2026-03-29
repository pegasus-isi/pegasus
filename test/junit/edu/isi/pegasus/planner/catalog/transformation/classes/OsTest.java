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

/** Tests for the Os enumerated type class. */
public class OsTest {

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
        assertEquals("LINUX", Os.LINUX.getValue());
        assertEquals("SUNOS", Os.SUNOS.getValue());
        assertEquals("AIX", Os.AIX.getValue());
        assertEquals("WINDOWS", Os.WINDOWS.getValue());
    }

    @Test
    public void testFromValueReturnsKnownConstant() {
        assertSame(Os.LINUX, Os.fromValue("LINUX"));
        assertSame(Os.SUNOS, Os.fromValue("SUNOS"));
        assertSame(Os.AIX, Os.fromValue("AIX"));
        assertSame(Os.WINDOWS, Os.fromValue("WINDOWS"));
    }

    @Test
    public void testFromValueIsCaseInsensitive() {
        assertSame(Os.LINUX, Os.fromValue("linux"));
        assertSame(Os.WINDOWS, Os.fromValue("Windows"));
        assertSame(Os.AIX, Os.fromValue("aix"));
    }

    @Test
    public void testFromStringDelegatesToFromValue() {
        assertSame(Os.LINUX, Os.fromString("LINUX"));
        assertSame(Os.SUNOS, Os.fromString("sunos"));
    }

    @Test
    public void testFromValueThrowsForUnknownOs() {
        assertThrows(IllegalStateException.class, () -> Os.fromValue("UNKNOWN_OS"));
    }

    @Test
    public void testToStringReturnsValue() {
        assertEquals("LINUX", Os.LINUX.toString());
        assertEquals("WINDOWS", Os.WINDOWS.toString());
    }

    @Test
    public void testEqualsSameInstance() {
        assertTrue(Os.LINUX.equals(Os.LINUX));
    }

    @Test
    public void testEqualsDifferentInstances() {
        assertFalse(Os.LINUX.equals(Os.WINDOWS));
    }

    @Test
    public void testHashCodeConsistentWithToString() {
        assertEquals(Os.LINUX.toString().hashCode(), Os.LINUX.hashCode());
    }

    @Test
    public void testErrorMessageIsNonNull() {
        assertNotNull(Os.err);
        assertFalse(Os.err.isEmpty());
    }
}
