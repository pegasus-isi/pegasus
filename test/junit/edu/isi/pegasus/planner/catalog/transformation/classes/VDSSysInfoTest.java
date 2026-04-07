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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** Tests for the VDSSysInfo class. */
public class VDSSysInfoTest {

    @Test
    public void testDefaultConstructorSetsIntel32Linux() {
        VDSSysInfo sysInfo = new VDSSysInfo();
        assertThat(sysInfo.getArch(), is(Arch.INTEL32));
        assertThat(sysInfo.getOs(), is(Os.LINUX));
        assertThat(sysInfo.getOsversion(), is(nullValue()));
        assertThat(sysInfo.getGlibc(), is(nullValue()));
    }

    @Test
    public void testFullConstructorSetsAllFields() {
        VDSSysInfo sysInfo = new VDSSysInfo(Arch.AMD64, Os.LINUX, "7.0", "2.12");
        assertThat(sysInfo.getArch(), is(Arch.AMD64));
        assertThat(sysInfo.getOs(), is(Os.LINUX));
        assertThat(sysInfo.getOsversion(), is("7.0"));
        assertThat(sysInfo.getGlibc(), is("2.12"));
    }

    @Test
    public void testStringConstructorParsesCorrectly() {
        VDSSysInfo sysInfo = new VDSSysInfo("INTEL32::LINUX");
        assertThat(sysInfo.getArch(), is(Arch.INTEL32));
        assertThat(sysInfo.getOs(), is(Os.LINUX));
    }

    @Test
    public void testStringConstructorWithOsVersionAndGlibc() {
        VDSSysInfo sysInfo = new VDSSysInfo("INTEL64::LINUX:7.0:2.17");
        assertThat(sysInfo.getArch(), is(Arch.INTEL64));
        assertThat(sysInfo.getOs(), is(Os.LINUX));
        assertThat(sysInfo.getOsversion(), is("7.0"));
        assertThat(sysInfo.getGlibc(), is("2.17"));
    }

    @Test
    public void testStringConstructorThrowsForInvalidFormat() {
        assertThrows(IllegalStateException.class, () -> new VDSSysInfo("BADFORMAT"));
    }

    @Test
    public void testNullArchFallsBackToIntel32() {
        VDSSysInfo sysInfo = new VDSSysInfo((Arch) null, Os.LINUX, null, null);
        assertThat(sysInfo.getArch(), is(Arch.INTEL32));
    }

    @Test
    public void testNullOsFallsBackToLinux() {
        VDSSysInfo sysInfo = new VDSSysInfo(Arch.AMD64, (Os) null, null, null);
        assertThat(sysInfo.getOs(), is(Os.LINUX));
    }

    @Test
    public void testSetArchUpdatesValue() {
        VDSSysInfo sysInfo = new VDSSysInfo();
        sysInfo.setArch(Arch.AMD64);
        assertThat(sysInfo.getArch(), is(Arch.AMD64));
    }

    @Test
    public void testSetOsUpdatesValue() {
        VDSSysInfo sysInfo = new VDSSysInfo();
        sysInfo.setOs(Os.WINDOWS);
        assertThat(sysInfo.getOs(), is(Os.WINDOWS));
    }

    @Test
    public void testSetArchWithNullFallsBackToIntel32() {
        VDSSysInfo sysInfo = new VDSSysInfo(Arch.AMD64, Os.LINUX, null, null);
        sysInfo.setArch(null);
        assertThat(sysInfo.getArch(), is(Arch.INTEL32));
    }

    @Test
    public void testSetOsWithNullFallsBackToLinux() {
        VDSSysInfo sysInfo = new VDSSysInfo(Arch.AMD64, Os.WINDOWS, null, null);
        sysInfo.setOs(null);
        assertThat(sysInfo.getOs(), is(Os.LINUX));
    }

    @Test
    public void testSetGlibcAndOsVersion() {
        VDSSysInfo sysInfo = new VDSSysInfo();
        sysInfo.setGlibc("2.12");
        sysInfo.setOsversion("6");
        assertThat(sysInfo.getGlibc(), is("2.12"));
        assertThat(sysInfo.getOsversion(), is("6"));
    }

    @Test
    public void testToStringContainsArchAndOs() {
        VDSSysInfo sysInfo = new VDSSysInfo(Arch.INTEL32, Os.LINUX, null, null);
        String s = sysInfo.toString();
        assertThat(s, containsString("INTEL32"));
        assertThat(s, containsString("LINUX"));
    }

    @Test
    public void testToStringIncludesOsVersionAndGlibc() {
        VDSSysInfo sysInfo = new VDSSysInfo(Arch.INTEL64, Os.LINUX, "7.0", "2.17");
        String s = sysInfo.toString();
        assertThat(s, containsString("7.0"));
        assertThat(s, containsString("2.17"));
    }

    @Test
    public void testEqualsWithSameArchAndOs() {
        VDSSysInfo a = new VDSSysInfo(Arch.INTEL32, Os.LINUX, null, null);
        VDSSysInfo b = new VDSSysInfo(Arch.INTEL32, Os.LINUX, "7.0", "2.12");
        // equals only considers arch and os
        assertThat(a.equals(b), is(true));
    }

    @Test
    public void testEqualsWithDifferentArch() {
        VDSSysInfo a = new VDSSysInfo(Arch.INTEL32, Os.LINUX, null, null);
        VDSSysInfo b = new VDSSysInfo(Arch.AMD64, Os.LINUX, null, null);
        assertThat(a.equals(b), is(false));
    }

    @Test
    public void testEqualsWithDifferentOs() {
        VDSSysInfo a = new VDSSysInfo(Arch.INTEL32, Os.LINUX, null, null);
        VDSSysInfo b = new VDSSysInfo(Arch.INTEL32, Os.WINDOWS, null, null);
        assertThat(a.equals(b), is(false));
    }

    @Test
    public void testEqualsReturnsFalseForNull() {
        assertThat(new VDSSysInfo().equals(null), is(false));
    }

    @Test
    public void testEqualsReturnsFalseForDifferentType() {
        assertThat(new VDSSysInfo().equals("INTEL32::LINUX"), is(false));
    }

    @Test
    public void testCloneProducesEqualObject() {
        VDSSysInfo original = new VDSSysInfo(Arch.AMD64, Os.LINUX, "6", "2.12");
        VDSSysInfo cloned = (VDSSysInfo) original.clone();
        assertThat(cloned.getArch(), is(original.getArch()));
        assertThat(cloned.getOs(), is(original.getOs()));
        assertThat(cloned.getOsversion(), is(original.getOsversion()));
        assertThat(cloned.getGlibc(), is(original.getGlibc()));
    }

    @Test
    public void testStringConstructorWithNullFallsBackToDefaults() {
        VDSSysInfo sysInfo = new VDSSysInfo((String) null);
        assertThat(sysInfo.getArch(), is(Arch.INTEL32));
        assertThat(sysInfo.getOs(), is(Os.LINUX));
    }

    @Test
    public void testStringArgsConstructorParsesStrings() {
        VDSSysInfo sysInfo = new VDSSysInfo("INTEL32", "LINUX", "2.12");
        assertThat(sysInfo.getArch(), is(Arch.INTEL32));
        assertThat(sysInfo.getOs(), is(Os.LINUX));
        assertThat(sysInfo.getGlibc(), is("2.12"));
    }

    @Test
    public void testEmptyOsVersionTreatedAsNull() {
        VDSSysInfo sysInfo = new VDSSysInfo(Arch.INTEL32, Os.LINUX, "", null);
        assertThat(sysInfo.getOsversion(), is(nullValue()));
    }

    @Test
    public void testEmptyGlibcTreatedAsNull() {
        VDSSysInfo sysInfo = new VDSSysInfo(Arch.INTEL32, Os.LINUX, "7.0", "");
        assertThat(sysInfo.getGlibc(), is(nullValue()));
    }

    @Test
    public void testStringArgsConstructorWithNullArchAndOsFallsBackToDefaults() {
        VDSSysInfo sysInfo = new VDSSysInfo((String) null, null, null, null);
        assertThat(sysInfo.getArch(), is(Arch.INTEL32));
        assertThat(sysInfo.getOs(), is(Os.LINUX));
        assertThat(sysInfo.getOsversion(), is(nullValue()));
        assertThat(sysInfo.getGlibc(), is(nullValue()));
    }

    @Test
    public void testToStringOmitsTrailingFieldsWhenUnset() {
        VDSSysInfo sysInfo = new VDSSysInfo(Arch.AMD64, Os.WINDOWS, null, null);
        assertThat(sysInfo.toString(), is("AMD64::WINDOWS"));
    }

    @Test
    public void testStringConstructorWithOsVersionOnly() {
        VDSSysInfo sysInfo = new VDSSysInfo("AMD64::WINDOWS:11");
        assertThat(sysInfo.getArch(), is(Arch.AMD64));
        assertThat(sysInfo.getOs(), is(Os.WINDOWS));
        assertThat(sysInfo.getOsversion(), is("11"));
        assertThat(sysInfo.getGlibc(), is(nullValue()));
    }
}
