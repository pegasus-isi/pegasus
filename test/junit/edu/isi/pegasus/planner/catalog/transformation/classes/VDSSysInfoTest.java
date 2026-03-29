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

/** Tests for the VDSSysInfo class. */
public class VDSSysInfoTest {

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testDefaultConstructorSetsIntel32Linux() {
        VDSSysInfo sysInfo = new VDSSysInfo();
        assertSame(Arch.INTEL32, sysInfo.getArch());
        assertSame(Os.LINUX, sysInfo.getOs());
        assertNull(sysInfo.getOsversion());
        assertNull(sysInfo.getGlibc());
    }

    @Test
    public void testFullConstructorSetsAllFields() {
        VDSSysInfo sysInfo = new VDSSysInfo(Arch.AMD64, Os.LINUX, "7.0", "2.12");
        assertSame(Arch.AMD64, sysInfo.getArch());
        assertSame(Os.LINUX, sysInfo.getOs());
        assertEquals("7.0", sysInfo.getOsversion());
        assertEquals("2.12", sysInfo.getGlibc());
    }

    @Test
    public void testStringConstructorParsesCorrectly() {
        VDSSysInfo sysInfo = new VDSSysInfo("INTEL32::LINUX");
        assertSame(Arch.INTEL32, sysInfo.getArch());
        assertSame(Os.LINUX, sysInfo.getOs());
    }

    @Test
    public void testStringConstructorWithOsVersionAndGlibc() {
        VDSSysInfo sysInfo = new VDSSysInfo("INTEL64::LINUX:7.0:2.17");
        assertSame(Arch.INTEL64, sysInfo.getArch());
        assertSame(Os.LINUX, sysInfo.getOs());
        assertEquals("7.0", sysInfo.getOsversion());
        assertEquals("2.17", sysInfo.getGlibc());
    }

    @Test
    public void testStringConstructorThrowsForInvalidFormat() {
        assertThrows(IllegalStateException.class, () -> new VDSSysInfo("BADFORMAT"));
    }

    @Test
    public void testNullArchFallsBackToIntel32() {
        VDSSysInfo sysInfo = new VDSSysInfo((Arch) null, Os.LINUX, null, null);
        assertSame(Arch.INTEL32, sysInfo.getArch());
    }

    @Test
    public void testNullOsFallsBackToLinux() {
        VDSSysInfo sysInfo = new VDSSysInfo(Arch.AMD64, (Os) null, null, null);
        assertSame(Os.LINUX, sysInfo.getOs());
    }

    @Test
    public void testSetArchUpdatesValue() {
        VDSSysInfo sysInfo = new VDSSysInfo();
        sysInfo.setArch(Arch.AMD64);
        assertSame(Arch.AMD64, sysInfo.getArch());
    }

    @Test
    public void testSetOsUpdatesValue() {
        VDSSysInfo sysInfo = new VDSSysInfo();
        sysInfo.setOs(Os.WINDOWS);
        assertSame(Os.WINDOWS, sysInfo.getOs());
    }

    @Test
    public void testSetGlibcAndOsVersion() {
        VDSSysInfo sysInfo = new VDSSysInfo();
        sysInfo.setGlibc("2.12");
        sysInfo.setOsversion("6");
        assertEquals("2.12", sysInfo.getGlibc());
        assertEquals("6", sysInfo.getOsversion());
    }

    @Test
    public void testToStringContainsArchAndOs() {
        VDSSysInfo sysInfo = new VDSSysInfo(Arch.INTEL32, Os.LINUX, null, null);
        String s = sysInfo.toString();
        assertTrue(s.contains("INTEL32"));
        assertTrue(s.contains("LINUX"));
    }

    @Test
    public void testToStringIncludesOsVersionAndGlibc() {
        VDSSysInfo sysInfo = new VDSSysInfo(Arch.INTEL64, Os.LINUX, "7.0", "2.17");
        String s = sysInfo.toString();
        assertTrue(s.contains("7.0"));
        assertTrue(s.contains("2.17"));
    }

    @Test
    public void testEqualsWithSameArchAndOs() {
        VDSSysInfo a = new VDSSysInfo(Arch.INTEL32, Os.LINUX, null, null);
        VDSSysInfo b = new VDSSysInfo(Arch.INTEL32, Os.LINUX, "7.0", "2.12");
        // equals only considers arch and os
        assertTrue(a.equals(b));
    }

    @Test
    public void testEqualsWithDifferentArch() {
        VDSSysInfo a = new VDSSysInfo(Arch.INTEL32, Os.LINUX, null, null);
        VDSSysInfo b = new VDSSysInfo(Arch.AMD64, Os.LINUX, null, null);
        assertFalse(a.equals(b));
    }

    @Test
    public void testCloneProducesEqualObject() {
        VDSSysInfo original = new VDSSysInfo(Arch.AMD64, Os.LINUX, "6", "2.12");
        VDSSysInfo cloned = (VDSSysInfo) original.clone();
        assertSame(original.getArch(), cloned.getArch());
        assertSame(original.getOs(), cloned.getOs());
        assertEquals(original.getOsversion(), cloned.getOsversion());
        assertEquals(original.getGlibc(), cloned.getGlibc());
    }

    @Test
    public void testStringConstructorWithNullFallsBackToDefaults() {
        VDSSysInfo sysInfo = new VDSSysInfo((String) null);
        assertSame(Arch.INTEL32, sysInfo.getArch());
        assertSame(Os.LINUX, sysInfo.getOs());
    }

    @Test
    public void testStringArgsConstructorParsesStrings() {
        VDSSysInfo sysInfo = new VDSSysInfo("INTEL32", "LINUX", "2.12");
        assertSame(Arch.INTEL32, sysInfo.getArch());
        assertSame(Os.LINUX, sysInfo.getOs());
        assertEquals("2.12", sysInfo.getGlibc());
    }

    @Test
    public void testEmptyOsVersionTreatedAsNull() {
        VDSSysInfo sysInfo = new VDSSysInfo(Arch.INTEL32, Os.LINUX, "", null);
        assertNull(sysInfo.getOsversion());
    }
}
