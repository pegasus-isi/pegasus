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
package edu.isi.pegasus.planner.catalog.classes;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class SysInfoTest {

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testDefaultConstructorSetsDefaults() {
        SysInfo s = new SysInfo();
        assertEquals(SysInfo.DEFAULT_ARCHITECTURE, s.getArchitecture());
        assertEquals(SysInfo.DEFAULT_OS, s.getOS());
        assertEquals("", s.getOSRelease());
        assertEquals("", s.getOSVersion());
        assertEquals("", s.getGlibc());
    }

    @Test
    public void testDefaultArchitectureIsX86_64() {
        assertEquals(SysInfo.Architecture.x86_64, SysInfo.DEFAULT_ARCHITECTURE);
    }

    @Test
    public void testDefaultOSIsLinux() {
        assertEquals(SysInfo.OS.linux, SysInfo.DEFAULT_OS);
    }

    @Test
    public void testDefaultOSReleaseIsNull() {
        assertNull(SysInfo.DEFAULT_OS_RELEASE);
    }

    @Test
    public void testStringConstructorParsesArchAndOS() {
        SysInfo s = new SysInfo("x86_64::linux");
        assertEquals(SysInfo.Architecture.x86_64, s.getArchitecture());
        assertEquals(SysInfo.OS.linux, s.getOS());
    }

    @Test
    public void testStringConstructorParsesArchOSAndVersion() {
        SysInfo s = new SysInfo("x86_64::linux:5.0");
        assertEquals(SysInfo.Architecture.x86_64, s.getArchitecture());
        assertEquals(SysInfo.OS.linux, s.getOS());
        assertEquals("5.0", s.getOSVersion());
    }

    @Test
    public void testStringConstructorParsesArchOSVersionAndGlibc() {
        SysInfo s = new SysInfo("x86_64::linux:5.0:2.17");
        assertEquals("5.0", s.getOSVersion());
        assertEquals("2.17", s.getGlibc());
    }

    @Test
    public void testStringConstructorWithNullUsesDefaults() {
        SysInfo s = new SysInfo((String) null);
        assertEquals(SysInfo.DEFAULT_ARCHITECTURE, s.getArchitecture());
        assertEquals(SysInfo.DEFAULT_OS, s.getOS());
    }

    @Test
    public void testStringConstructorWithInvalidArchThrows() {
        assertThrows(
                IllegalStateException.class,
                () -> new SysInfo("invalid_arch::linux"),
                "Invalid architecture should throw IllegalStateException");
    }

    @Test
    public void testStringConstructorWithInvalidOSThrows() {
        assertThrows(
                IllegalStateException.class,
                () -> new SysInfo("x86_64::unknownOS"),
                "Invalid OS should throw IllegalStateException");
    }

    @Test
    public void testStringConstructorWithMissingDelimiterThrows() {
        assertThrows(
                IllegalStateException.class,
                () -> new SysInfo("x86_64linux"),
                "Missing :: delimiter should throw IllegalStateException");
    }

    @Test
    public void testSetAndGetArchitecture() {
        SysInfo s = new SysInfo();
        s.setArchitecture(SysInfo.Architecture.aarch64);
        assertEquals(SysInfo.Architecture.aarch64, s.getArchitecture());
    }

    @Test
    public void testSetAndGetOS() {
        SysInfo s = new SysInfo();
        s.setOS(SysInfo.OS.macosx);
        assertEquals(SysInfo.OS.macosx, s.getOS());
    }

    @Test
    public void testSetAndGetOSRelease() {
        SysInfo s = new SysInfo();
        s.setOSRelease("rhel");
        assertEquals("rhel", s.getOSRelease());
    }

    @Test
    public void testSetOSReleaseEmptyStringUsesDefault() {
        SysInfo s = new SysInfo();
        s.setOSRelease("rhel");
        s.setOSRelease(""); // reset to default
        assertEquals("", s.getOSRelease());
    }

    @Test
    public void testSetAndGetOSVersion() {
        SysInfo s = new SysInfo();
        s.setOSVersion("7");
        assertEquals("7", s.getOSVersion());
    }

    @Test
    public void testSetAndGetGlibc() {
        SysInfo s = new SysInfo();
        s.setGlibc("2.17");
        assertEquals("2.17", s.getGlibc());
    }

    @Test
    public void testEqualsTwoIdenticalObjects() {
        SysInfo s1 = new SysInfo();
        s1.setArchitecture(SysInfo.Architecture.x86_64);
        s1.setOS(SysInfo.OS.linux);

        SysInfo s2 = new SysInfo();
        s2.setArchitecture(SysInfo.Architecture.x86_64);
        s2.setOS(SysInfo.OS.linux);

        assertEquals(s1, s2);
    }

    @Test
    public void testEqualsDifferentArchitecture() {
        SysInfo s1 = new SysInfo();
        s1.setArchitecture(SysInfo.Architecture.x86);

        SysInfo s2 = new SysInfo();
        s2.setArchitecture(SysInfo.Architecture.x86_64);

        assertNotEquals(s1, s2);
    }

    @Test
    public void testEqualsDifferentOS() {
        SysInfo s1 = new SysInfo();
        s1.setOS(SysInfo.OS.linux);

        SysInfo s2 = new SysInfo();
        s2.setOS(SysInfo.OS.macosx);

        assertNotEquals(s1, s2);
    }

    @Test
    public void testEqualsNonSysInfoObject() {
        SysInfo s = new SysInfo();
        assertFalse(s.equals("not a SysInfo"));
    }

    @Test
    public void testCloneProducesEqualObject() {
        SysInfo original = new SysInfo();
        original.setArchitecture(SysInfo.Architecture.ppc64le);
        original.setOS(SysInfo.OS.linux);
        original.setOSRelease("rhel");
        original.setOSVersion("8");
        original.setGlibc("2.28");

        SysInfo clone = (SysInfo) original.clone();
        assertEquals(original, clone);
    }

    @Test
    public void testToStringContainsArchAndOS() {
        SysInfo s = new SysInfo();
        s.setArchitecture(SysInfo.Architecture.x86_64);
        s.setOS(SysInfo.OS.linux);
        String str = s.toString();
        assertThat(str, containsString("x86_64"));
        assertThat(str, containsString("linux"));
    }

    @Test
    public void testComputeOSFromRhelRelease() {
        SysInfo.OS os = SysInfo.computeOS(SysInfo.OS_RELEASE.rhel);
        assertEquals(SysInfo.OS.linux, os);
    }

    @Test
    public void testComputeOSFromMacosRelease() {
        SysInfo.OS os = SysInfo.computeOS(SysInfo.OS_RELEASE.macos);
        assertEquals(SysInfo.OS.macosx, os);
    }

    @Test
    public void testComputeOSFromSunosRelease() {
        SysInfo.OS os = SysInfo.computeOS(SysInfo.OS_RELEASE.sunos);
        assertEquals(SysInfo.OS.sunos, os);
    }

    @Test
    public void testComputeOSFromUbuntuRelease() {
        SysInfo.OS os = SysInfo.computeOS(SysInfo.OS_RELEASE.ubuntu);
        assertEquals(SysInfo.OS.linux, os);
    }
}
