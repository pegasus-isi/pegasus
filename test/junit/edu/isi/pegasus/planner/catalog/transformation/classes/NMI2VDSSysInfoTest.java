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
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.catalog.classes.SysInfo;
import org.junit.jupiter.api.Test;

/** Tests for the NMI2VDSSysInfo adapter class. */
public class NMI2VDSSysInfoTest {

    @Test
    public void testNMIArchToVDSArchMapIsNotNull() {
        assertThat(NMI2VDSSysInfo.NMIArchToVDSArchMap(), is(notNullValue()));
    }

    @Test
    public void testNMIOSToVDSOSMapIsNotNull() {
        assertThat(NMI2VDSSysInfo.NMIOSToVDSOSMap(), is(notNullValue()));
    }

    @Test
    public void testX86MapsToIntel32() {
        Arch result = NMI2VDSSysInfo.nmiArchToVDSArch(SysInfo.Architecture.x86);
        assertThat(result, is(Arch.INTEL32));
    }

    @Test
    public void testX86_64MapsToIntel64() {
        Arch result = NMI2VDSSysInfo.nmiArchToVDSArch(SysInfo.Architecture.x86_64);
        assertThat(result, is(Arch.INTEL64));
    }

    @Test
    public void testAmd64MapsToAmd64() {
        Arch result = NMI2VDSSysInfo.nmiArchToVDSArch(SysInfo.Architecture.amd64);
        assertThat(result, is(Arch.AMD64));
    }

    @Test
    public void testSparcV7MapsToSparcV7() {
        Arch result = NMI2VDSSysInfo.nmiArchToVDSArch(SysInfo.Architecture.sparcv7);
        assertThat(result, is(Arch.SPARCV7));
    }

    @Test
    public void testSparcV9MapsToSparcV9() {
        Arch result = NMI2VDSSysInfo.nmiArchToVDSArch(SysInfo.Architecture.sparcv9);
        assertThat(result, is(Arch.SPARCV9));
    }

    @Test
    public void testLinuxMapsToLinux() {
        Os result = NMI2VDSSysInfo.nmiOSToVDSOS(SysInfo.OS.linux);
        assertThat(result, is(Os.LINUX));
    }

    @Test
    public void testWindowsMapsToWindows() {
        Os result = NMI2VDSSysInfo.nmiOSToVDSOS(SysInfo.OS.windows);
        assertThat(result, is(Os.WINDOWS));
    }

    @Test
    public void testAixMapsToAix() {
        Os result = NMI2VDSSysInfo.nmiOSToVDSOS(SysInfo.OS.aix);
        assertThat(result, is(Os.AIX));
    }

    @Test
    public void testSunosMapsToSunos() {
        Os result = NMI2VDSSysInfo.nmiOSToVDSOS(SysInfo.OS.sunos);
        assertThat(result, is(Os.SUNOS));
    }

    @Test
    public void testNmiArchToVDSArchByStringX86() {
        Arch result = NMI2VDSSysInfo.nmiArchToVDSArch("x86");
        assertThat(result, is(Arch.INTEL32));
    }

    @Test
    public void testNmiOSToVDSOSByStringLinux() {
        Os result = NMI2VDSSysInfo.nmiOSToVDSOS("linux");
        assertThat(result, is(Os.LINUX));
    }

    @Test
    public void testNmiToVDSSysInfoFromSysInfoObject() {
        SysInfo sysInfo = new SysInfo();
        sysInfo.setArchitecture(SysInfo.Architecture.x86_64);
        sysInfo.setOS(SysInfo.OS.linux);
        sysInfo.setGlibc("2.17");

        VDSSysInfo result = NMI2VDSSysInfo.nmiToVDSSysInfo(sysInfo);

        assertThat(result, is(notNullValue()));
        assertThat(result.getArch(), is(Arch.INTEL64));
        assertThat(result.getOs(), is(Os.LINUX));
        assertThat(result.getGlibc(), is("2.17"));
    }

    @Test
    public void testNmiToVDSSysInfoWithReleaseAndVersion() {
        SysInfo sysInfo = new SysInfo();
        sysInfo.setArchitecture(SysInfo.Architecture.x86);
        sysInfo.setOS(SysInfo.OS.linux);
        sysInfo.setOSRelease("rhel");
        sysInfo.setOSVersion("7");

        VDSSysInfo result = NMI2VDSSysInfo.nmiToVDSSysInfo(sysInfo);

        assertThat(result, is(notNullValue()));
        // OS version combines release and version with separator
        String osversion = result.getOsversion();
        assertThat(osversion, containsString("rhel"));
        assertThat(osversion, containsString("7"));
    }

    @Test
    public void testIa64MapsToIntel64() {
        Arch result = NMI2VDSSysInfo.nmiArchToVDSArch(SysInfo.Architecture.ia64);
        assertThat(result, is(Arch.INTEL64));
    }

    @Test
    public void testNmiArchToVDSArchByStringForIa64() {
        Arch result = NMI2VDSSysInfo.nmiArchToVDSArch("ia64");
        assertThat(result, is(Arch.INTEL64));
    }

    @Test
    public void testNmiToVDSSysInfoWithReleaseOnlyDoesNotAppendSeparator() {
        SysInfo sysInfo = new SysInfo();
        sysInfo.setArchitecture(SysInfo.Architecture.x86);
        sysInfo.setOS(SysInfo.OS.linux);
        sysInfo.setOSRelease("rhel");
        sysInfo.setOSVersion("");

        VDSSysInfo result = NMI2VDSSysInfo.nmiToVDSSysInfo(sysInfo);

        assertThat(result.getOsversion(), is("rhel"));
    }

    @Test
    public void testNmiToVDSSysInfoWithEmptyReleaseAndVersionProducesEmptyOsversion() {
        SysInfo sysInfo = new SysInfo();
        sysInfo.setArchitecture(SysInfo.Architecture.x86);
        sysInfo.setOS(SysInfo.OS.linux);
        sysInfo.setOSRelease("");
        sysInfo.setOSVersion("7");

        VDSSysInfo result = NMI2VDSSysInfo.nmiToVDSSysInfo(sysInfo);

        assertThat(result.getOsversion(), is(""));
    }

    @Test
    public void testNmiToVDSSysInfoFromComponentsPreservesGlibc() {
        VDSSysInfo result =
                NMI2VDSSysInfo.nmiToVDSSysInfo(
                        SysInfo.Architecture.amd64, SysInfo.OS.windows, "2.28");

        assertThat(result.getArch(), is(Arch.AMD64));
        assertThat(result.getOs(), is(Os.WINDOWS));
        assertThat(result.getGlibc(), is("2.28"));
        assertThat(result.getOsversion(), is(nullValue()));
    }

    @Test
    public void testStringBasedConversionRejectsUnknownArchitecture() {
        assertThrows(
                IllegalArgumentException.class, () -> NMI2VDSSysInfo.nmiArchToVDSArch("bogus"));
    }

    @Test
    public void testStringBasedConversionRejectsUnknownOperatingSystem() {
        assertThrows(IllegalArgumentException.class, () -> NMI2VDSSysInfo.nmiOSToVDSOS("bogus"));
    }

    @Test
    public void testNmiToVDSSysInfoFromComponentsX86Linux() {
        VDSSysInfo result =
                NMI2VDSSysInfo.nmiToVDSSysInfo(SysInfo.Architecture.x86, SysInfo.OS.linux, null);
        assertThat(result, is(notNullValue()));
        assertThat(result.getArch(), is(Arch.INTEL32));
        assertThat(result.getOs(), is(Os.LINUX));
    }

    @Test
    public void testOsCombineSeparatorIsUnderscore() {
        assertThat(NMI2VDSSysInfo.OS_COMBINE_SEPARATOR, is("_"));
    }

    @Test
    public void testArchMapIsSingleton() {
        assertThat(
                NMI2VDSSysInfo.NMIArchToVDSArchMap(),
                is(sameInstance(NMI2VDSSysInfo.NMIArchToVDSArchMap())));
    }

    @Test
    public void testOSMapIsSingleton() {
        assertThat(
                NMI2VDSSysInfo.NMIOSToVDSOSMap(),
                is(sameInstance(NMI2VDSSysInfo.NMIOSToVDSOSMap())));
    }
}
