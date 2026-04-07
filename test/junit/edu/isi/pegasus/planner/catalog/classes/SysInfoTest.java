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

import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class SysInfoTest {

    @Test
    public void testDefaultConstructorSetsDefaults() {
        SysInfo s = new SysInfo();
        assertThat(s.getArchitecture(), is(SysInfo.DEFAULT_ARCHITECTURE));
        assertThat(s.getOS(), is(SysInfo.DEFAULT_OS));
        assertThat(s.getOSRelease(), is(""));
        assertThat(s.getOSVersion(), is(""));
        assertThat(s.getGlibc(), is(""));
    }

    @Test
    public void testDefaultArchitectureIsX86_64() {
        assertThat(SysInfo.DEFAULT_ARCHITECTURE, is(SysInfo.Architecture.x86_64));
    }

    @Test
    public void testDefaultOSIsLinux() {
        assertThat(SysInfo.DEFAULT_OS, is(SysInfo.OS.linux));
    }

    @Test
    public void testDefaultOSReleaseIsNull() {
        assertThat(SysInfo.DEFAULT_OS_RELEASE, is(nullValue()));
    }

    @Test
    public void testStringConstructorParsesArchAndOS() {
        SysInfo s = new SysInfo("x86_64::linux");
        assertThat(s.getArchitecture(), is(SysInfo.Architecture.x86_64));
        assertThat(s.getOS(), is(SysInfo.OS.linux));
    }

    @Test
    public void testStringConstructorParsesArchOSAndVersion() {
        SysInfo s = new SysInfo("x86_64::linux:5.0");
        assertThat(s.getArchitecture(), is(SysInfo.Architecture.x86_64));
        assertThat(s.getOS(), is(SysInfo.OS.linux));
        assertThat(s.getOSVersion(), is("5.0"));
    }

    @Test
    public void testStringConstructorParsesArchOSVersionAndGlibc() {
        SysInfo s = new SysInfo("x86_64::linux:5.0:2.17");
        assertThat(s.getOSVersion(), is("5.0"));
        assertThat(s.getGlibc(), is("2.17"));
    }

    @Test
    public void testStringConstructorWithNullUsesDefaults() {
        SysInfo s = new SysInfo((String) null);
        assertThat(s.getArchitecture(), is(SysInfo.DEFAULT_ARCHITECTURE));
        assertThat(s.getOS(), is(SysInfo.DEFAULT_OS));
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
        assertThat(s.getArchitecture(), is(SysInfo.Architecture.aarch64));
    }

    @Test
    public void testSetAndGetOS() {
        SysInfo s = new SysInfo();
        s.setOS(SysInfo.OS.macosx);
        assertThat(s.getOS(), is(SysInfo.OS.macosx));
    }

    @Test
    public void testSetAndGetOSRelease() {
        SysInfo s = new SysInfo();
        s.setOSRelease("rhel");
        assertThat(s.getOSRelease(), is("rhel"));
    }

    @Test
    public void testSetOSReleaseEmptyStringUsesDefault() {
        SysInfo s = new SysInfo();
        s.setOSRelease("rhel");
        s.setOSRelease(""); // reset to default
        assertThat(s.getOSRelease(), is(""));
    }

    @Test
    public void testSetAndGetOSVersion() {
        SysInfo s = new SysInfo();
        s.setOSVersion("7");
        assertThat(s.getOSVersion(), is("7"));
    }

    @Test
    public void testSetAndGetGlibc() {
        SysInfo s = new SysInfo();
        s.setGlibc("2.17");
        assertThat(s.getGlibc(), is("2.17"));
    }

    @Test
    public void testEqualsTwoIdenticalObjects() {
        SysInfo s1 = new SysInfo();
        s1.setArchitecture(SysInfo.Architecture.x86_64);
        s1.setOS(SysInfo.OS.linux);

        SysInfo s2 = new SysInfo();
        s2.setArchitecture(SysInfo.Architecture.x86_64);
        s2.setOS(SysInfo.OS.linux);

        assertThat(s1, is(s2));
    }

    @Test
    public void testEqualsDifferentArchitecture() {
        SysInfo s1 = new SysInfo();
        s1.setArchitecture(SysInfo.Architecture.x86);

        SysInfo s2 = new SysInfo();
        s2.setArchitecture(SysInfo.Architecture.x86_64);

        assertThat(s1.equals(s2), is(false));
    }

    @Test
    public void testEqualsDifferentOS() {
        SysInfo s1 = new SysInfo();
        s1.setOS(SysInfo.OS.linux);

        SysInfo s2 = new SysInfo();
        s2.setOS(SysInfo.OS.macosx);

        assertThat(s1.equals(s2), is(false));
    }

    @Test
    public void testEqualsNonSysInfoObject() {
        SysInfo s = new SysInfo();
        assertThat(s.equals("not a SysInfo"), is(false));
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
        assertThat(clone, is(original));
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
        assertThat(os, is(SysInfo.OS.linux));
    }

    @Test
    public void testComputeOSFromMacosRelease() {
        SysInfo.OS os = SysInfo.computeOS(SysInfo.OS_RELEASE.macos);
        assertThat(os, is(SysInfo.OS.macosx));
    }

    @Test
    public void testComputeOSFromSunosRelease() {
        SysInfo.OS os = SysInfo.computeOS(SysInfo.OS_RELEASE.sunos);
        assertThat(os, is(SysInfo.OS.sunos));
    }

    @Test
    public void testComputeOSFromUbuntuRelease() {
        SysInfo.OS os = SysInfo.computeOS(SysInfo.OS_RELEASE.ubuntu);
        assertThat(os, is(SysInfo.OS.linux));
    }

    // -----------------------------------------------------------------------
    // computeOS — remaining releases mapped to linux
    // -----------------------------------------------------------------------

    @Test
    public void testComputeOSFromAlpineRelease() {
        assertThat(SysInfo.computeOS(SysInfo.OS_RELEASE.alpine), is(SysInfo.OS.linux));
    }

    @Test
    public void testComputeOSFromDebRelease() {
        assertThat(SysInfo.computeOS(SysInfo.OS_RELEASE.deb), is(SysInfo.OS.linux));
    }

    @Test
    public void testComputeOSFromFcRelease() {
        assertThat(SysInfo.computeOS(SysInfo.OS_RELEASE.fc), is(SysInfo.OS.linux));
    }

    @Test
    public void testComputeOSFromSuseRelease() {
        assertThat(SysInfo.computeOS(SysInfo.OS_RELEASE.suse), is(SysInfo.OS.linux));
    }

    @Test
    public void testComputeOSFromSlesRelease() {
        assertThat(SysInfo.computeOS(SysInfo.OS_RELEASE.sles), is(SysInfo.OS.linux));
    }

    @Test
    public void testComputeOSFromFreebsdRelease() {
        assertThat(SysInfo.computeOS(SysInfo.OS_RELEASE.freebsd), is(SysInfo.OS.linux));
    }

    @Test
    public void testComputeOSFromFedoraReleaseThrows() {
        // fedora is not handled in the switch statement — falls to default which throws
        assertThrows(
                RuntimeException.class,
                () -> SysInfo.computeOS(SysInfo.OS_RELEASE.fedora),
                "fedora release is not mapped in computeOS and should throw");
    }

    // -----------------------------------------------------------------------
    // Enum value counts
    // -----------------------------------------------------------------------

    @Test
    public void testOSEnumHasFiveValues() {
        assertThat(SysInfo.OS.values().length, is(5));
    }

    @Test
    public void testArchitectureEnumHasTenValues() {
        assertThat(SysInfo.Architecture.values().length, is(10));
    }

    @Test
    public void testOSReleaseEnumHasElevenValues() {
        assertThat(SysInfo.OS_RELEASE.values().length, is(11));
    }

    // -----------------------------------------------------------------------
    // String constructor — additional arch/OS combinations
    // -----------------------------------------------------------------------

    @Test
    public void testStringConstructorWithMacosx() {
        SysInfo s = new SysInfo("x86_64::macosx");
        assertThat(s.getOS(), is(SysInfo.OS.macosx));
    }

    @Test
    public void testStringConstructorWithPpc64le() {
        SysInfo s = new SysInfo("ppc64le::linux");
        assertThat(s.getArchitecture(), is(SysInfo.Architecture.ppc64le));
    }

    @Test
    public void testStringConstructorWithAarch64() {
        SysInfo s = new SysInfo("aarch64::linux");
        assertThat(s.getArchitecture(), is(SysInfo.Architecture.aarch64));
    }

    @Test
    public void testStringConstructorWithAmd64() {
        SysInfo s = new SysInfo("amd64::linux");
        assertThat(s.getArchitecture(), is(SysInfo.Architecture.amd64));
    }

    @Test
    public void testStringConstructorWithWindows() {
        SysInfo s = new SysInfo("x86_64::windows");
        assertThat(s.getOS(), is(SysInfo.OS.windows));
    }

    // -----------------------------------------------------------------------
    // equals — field-by-field differentiation
    // -----------------------------------------------------------------------

    @Test
    public void testEqualsDifferentOSRelease() {
        SysInfo s1 = new SysInfo();
        s1.setOSRelease("rhel");

        SysInfo s2 = new SysInfo();
        s2.setOSRelease("ubuntu");

        assertNotEquals(s1, s2);
    }

    @Test
    public void testEqualsDifferentOSVersion() {
        SysInfo s1 = new SysInfo();
        s1.setOSVersion("7");

        SysInfo s2 = new SysInfo();
        s2.setOSVersion("8");

        assertNotEquals(s1, s2);
    }

    @Test
    public void testEqualsDifferentGlibc() {
        SysInfo s1 = new SysInfo();
        s1.setGlibc("2.17");

        SysInfo s2 = new SysInfo();
        s2.setGlibc("2.28");

        assertNotEquals(s1, s2);
    }

    @Test
    public void testEqualsWithAllFieldsSet() {
        SysInfo s1 = new SysInfo();
        s1.setArchitecture(SysInfo.Architecture.x86_64);
        s1.setOS(SysInfo.OS.linux);
        s1.setOSRelease("rhel");
        s1.setOSVersion("8");
        s1.setGlibc("2.28");

        SysInfo s2 = new SysInfo();
        s2.setArchitecture(SysInfo.Architecture.x86_64);
        s2.setOS(SysInfo.OS.linux);
        s2.setOSRelease("rhel");
        s2.setOSVersion("8");
        s2.setGlibc("2.28");

        assertThat(s1, is(s2));
    }

    // -----------------------------------------------------------------------
    // clone — independence
    // -----------------------------------------------------------------------

    @Test
    public void testCloneIsIndependentFromOriginal() {
        SysInfo original = new SysInfo();
        original.setArchitecture(SysInfo.Architecture.x86_64);
        original.setOS(SysInfo.OS.linux);

        SysInfo clone = (SysInfo) original.clone();
        clone.setOS(SysInfo.OS.macosx);
        clone.setOSVersion("13");

        // original must not be affected
        assertThat(original.getOS(), is(SysInfo.OS.linux));
        assertThat(original.getOSVersion(), is(""));
    }

    // -----------------------------------------------------------------------
    // toString — optional fields included when set
    // -----------------------------------------------------------------------

    @Test
    public void testToStringIncludesOSReleaseWhenSet() {
        SysInfo s = new SysInfo();
        s.setOSRelease("rhel");
        assertThat(s.toString(), containsString("osrelease=rhel"));
    }

    @Test
    public void testToStringIncludesOSVersionWhenSet() {
        SysInfo s = new SysInfo();
        s.setOSVersion("8");
        assertThat(s.toString(), containsString("osversion=8"));
    }

    @Test
    public void testToStringOmitsOSReleaseWhenEmpty() {
        SysInfo s = new SysInfo();
        assertThat(s.toString(), not(containsString("osrelease")));
    }

    @Test
    public void testToStringOmitsOSVersionWhenEmpty() {
        SysInfo s = new SysInfo();
        assertThat(s.toString(), not(containsString("osversion")));
    }

    @Test
    public void testToStringBracesFormat() {
        SysInfo s = new SysInfo();
        String str = s.toString();
        assertThat(str, startsWith("{"));
        assertThat(str, endsWith("}"));
    }

    // -----------------------------------------------------------------------
    // setOSRelease — invalid value
    // -----------------------------------------------------------------------

    @Test
    public void testSetOSReleaseWithInvalidValueThrows() {
        SysInfo s = new SysInfo();
        assertThrows(IllegalArgumentException.class, () -> s.setOSRelease("invalidRelease"));
    }

    @Test
    public void testStringConstructorTrimsArchitectureAndOSTokens() {
        SysInfo s = new SysInfo(" x86_64 :: linux :8:2.17");

        assertThat(s.getArchitecture(), is(SysInfo.Architecture.x86_64));
        assertThat(s.getOS(), is(SysInfo.OS.linux));
        assertThat(s.getOSVersion(), is("8"));
        assertThat(s.getGlibc(), is("2.17"));
    }

    @Test
    public void testStringConstructorWithEmptyStringThrows() {
        assertThrows(IllegalStateException.class, () -> new SysInfo(""));
    }

    @Test
    public void testCloneReturnsDistinctInstance() {
        SysInfo original = new SysInfo();

        SysInfo clone = (SysInfo) original.clone();

        assertNotSame(original, clone);
    }
}
