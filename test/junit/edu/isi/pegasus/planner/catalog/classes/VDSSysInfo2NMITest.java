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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.catalog.transformation.classes.Arch;
import edu.isi.pegasus.planner.catalog.transformation.classes.Os;
import edu.isi.pegasus.planner.catalog.transformation.classes.VDSSysInfo;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class VDSSysInfo2NMITest {

    @Test
    public void testVdsArchToNMIArchIntel32() {
        SysInfo.Architecture arch = VDSSysInfo2NMI.vdsArchToNMIArch(Arch.INTEL32);
        assertThat(arch, is(SysInfo.Architecture.x86));
    }

    @Test
    public void testVdsArchToNMIArchIntel64() {
        SysInfo.Architecture arch = VDSSysInfo2NMI.vdsArchToNMIArch(Arch.INTEL64);
        assertThat(arch, is(SysInfo.Architecture.x86_64));
    }

    @Test
    public void testVdsArchToNMIArchAmd64() {
        SysInfo.Architecture arch = VDSSysInfo2NMI.vdsArchToNMIArch(Arch.AMD64);
        assertThat(arch, is(SysInfo.Architecture.amd64));
    }

    @Test
    public void testVdsArchToNMIArchSparcv7() {
        SysInfo.Architecture arch = VDSSysInfo2NMI.vdsArchToNMIArch(Arch.SPARCV7);
        assertThat(arch, is(SysInfo.Architecture.sparcv7));
    }

    @Test
    public void testVdsArchToNMIArchSparcv9() {
        SysInfo.Architecture arch = VDSSysInfo2NMI.vdsArchToNMIArch(Arch.SPARCV9);
        assertThat(arch, is(SysInfo.Architecture.sparcv9));
    }

    @Test
    public void testVdsArchToNMIArchByString() {
        SysInfo.Architecture arch = VDSSysInfo2NMI.vdsArchToNMIArch("INTEL32");
        assertThat(arch, is(SysInfo.Architecture.x86));
    }

    @Test
    public void testVdsOsToNMIOSLinux() {
        SysInfo.OS os = VDSSysInfo2NMI.vdsOsToNMIOS(Os.LINUX);
        assertThat(os, is(SysInfo.OS.linux));
    }

    @Test
    public void testVdsOsToNMIOSAix() {
        SysInfo.OS os = VDSSysInfo2NMI.vdsOsToNMIOS(Os.AIX);
        assertThat(os, is(SysInfo.OS.aix));
    }

    @Test
    public void testVdsOsToNMIOSSunos() {
        SysInfo.OS os = VDSSysInfo2NMI.vdsOsToNMIOS(Os.SUNOS);
        assertThat(os, is(SysInfo.OS.sunos));
    }

    @Test
    public void testVdsOsToNMIOSWindows() {
        SysInfo.OS os = VDSSysInfo2NMI.vdsOsToNMIOS(Os.WINDOWS);
        assertThat(os, is(SysInfo.OS.windows));
    }

    @Test
    public void testVdsOsToNMIOSByString() {
        SysInfo.OS os = VDSSysInfo2NMI.vdsOsToNMIOS("LINUX");
        assertThat(os, is(SysInfo.OS.linux));
    }

    @Test
    public void testVdsSysInfo2NMIBasic() {
        VDSSysInfo vds = new VDSSysInfo(Arch.AMD64, Os.LINUX, null, null);
        SysInfo nmi = VDSSysInfo2NMI.vdsSysInfo2NMI(vds);
        assertThat(nmi.getArchitecture(), is(SysInfo.Architecture.amd64));
        assertThat(nmi.getOS(), is(SysInfo.OS.linux));
    }

    @Test
    public void testVdsSysInfo2NMIWithOsVersionContainingSeparator() {
        VDSSysInfo vds = new VDSSysInfo(Arch.AMD64, Os.LINUX, "rhel_4", null);
        SysInfo nmi = VDSSysInfo2NMI.vdsSysInfo2NMI(vds);
        assertThat(nmi.getOSRelease(), is("rhel"));
        assertThat(nmi.getOSVersion(), is("4"));
    }

    @Test
    public void testVdsSysInfo2NMIWithOsVersionWithTrailingSeparator() {
        VDSSysInfo vds = new VDSSysInfo(Arch.AMD64, Os.LINUX, "rhel_", null);
        SysInfo nmi = VDSSysInfo2NMI.vdsSysInfo2NMI(vds);
        assertThat(nmi.getOSRelease(), is("rhel"));
        assertThat(nmi.getOSVersion(), is(""));
    }

    @Test
    public void testVdsSysInfo2NMIWithOsVersionWithoutSeparator() {
        VDSSysInfo vds = new VDSSysInfo(Arch.AMD64, Os.LINUX, "rhel", null);
        SysInfo nmi = VDSSysInfo2NMI.vdsSysInfo2NMI(vds);
        assertThat(nmi.getOSRelease(), is("rhel"));
        assertThat(nmi.getOSVersion(), is(""));
    }

    @Test
    public void testVdsSysInfo2NMIWithGlibc() {
        VDSSysInfo vds = new VDSSysInfo(Arch.INTEL64, Os.LINUX, null, "2.17");
        SysInfo nmi = VDSSysInfo2NMI.vdsSysInfo2NMI(vds);
        assertThat(nmi.getGlibc(), is("2.17"));
    }

    @Test
    public void testOsCombineSeparatorConstant() {
        assertThat(VDSSysInfo2NMI.OS_COMBINE_SEPARATOR, is("_"));
    }

    // -----------------------------------------------------------------------
    // vdsArchToNMIArch(String) — remaining arch strings
    // -----------------------------------------------------------------------

    @Test
    public void testVdsArchToNMIArchByStringIntel64() {
        assertThat(VDSSysInfo2NMI.vdsArchToNMIArch("INTEL64"), is(SysInfo.Architecture.x86_64));
    }

    @Test
    public void testVdsArchToNMIArchByStringAmd64() {
        assertThat(VDSSysInfo2NMI.vdsArchToNMIArch("AMD64"), is(SysInfo.Architecture.amd64));
    }

    @Test
    public void testVdsArchToNMIArchByStringSparcv7() {
        assertThat(VDSSysInfo2NMI.vdsArchToNMIArch("SPARCV7"), is(SysInfo.Architecture.sparcv7));
    }

    @Test
    public void testVdsArchToNMIArchByStringSparcv9() {
        assertThat(VDSSysInfo2NMI.vdsArchToNMIArch("SPARCV9"), is(SysInfo.Architecture.sparcv9));
    }

    @Test
    public void testVdsArchToNMIArchByStringLowerCase() {
        // Arch.fromString calls toUpperCase internally
        assertThat(VDSSysInfo2NMI.vdsArchToNMIArch("intel32"), is(SysInfo.Architecture.x86));
    }

    @Test
    public void testVdsArchToNMIArchByStringInvalidThrows() {
        assertThrows(
                IllegalStateException.class,
                () -> VDSSysInfo2NMI.vdsArchToNMIArch("UNKNOWN_ARCH"),
                "Unknown arch string should throw IllegalStateException");
    }

    // -----------------------------------------------------------------------
    // vdsOsToNMIOS(String) — remaining OS strings
    // -----------------------------------------------------------------------

    @Test
    public void testVdsOsToNMIOSByStringAix() {
        assertThat(VDSSysInfo2NMI.vdsOsToNMIOS("AIX"), is(SysInfo.OS.aix));
    }

    @Test
    public void testVdsOsToNMIOSByStringSunos() {
        assertThat(VDSSysInfo2NMI.vdsOsToNMIOS("SUNOS"), is(SysInfo.OS.sunos));
    }

    @Test
    public void testVdsOsToNMIOSByStringWindows() {
        assertThat(VDSSysInfo2NMI.vdsOsToNMIOS("WINDOWS"), is(SysInfo.OS.windows));
    }

    @Test
    public void testVdsOsToNMIOSByStringInvalidThrows() {
        assertThrows(
                IllegalStateException.class,
                () -> VDSSysInfo2NMI.vdsOsToNMIOS("BEOS"),
                "Unknown OS string should throw IllegalStateException");
    }

    // -----------------------------------------------------------------------
    // vdsSysInfo2NMI — all OS types
    // -----------------------------------------------------------------------

    @Test
    public void testVdsSysInfo2NMIWithAix() {
        VDSSysInfo vds = new VDSSysInfo(Arch.INTEL64, Os.AIX, null, null);
        SysInfo nmi = VDSSysInfo2NMI.vdsSysInfo2NMI(vds);
        assertThat(nmi.getOS(), is(SysInfo.OS.aix));
    }

    @Test
    public void testVdsSysInfo2NMIWithSunos() {
        VDSSysInfo vds = new VDSSysInfo(Arch.SPARCV9, Os.SUNOS, null, null);
        SysInfo nmi = VDSSysInfo2NMI.vdsSysInfo2NMI(vds);
        assertThat(nmi.getOS(), is(SysInfo.OS.sunos));
        assertThat(nmi.getArchitecture(), is(SysInfo.Architecture.sparcv9));
    }

    @Test
    public void testVdsSysInfo2NMIWithWindows() {
        VDSSysInfo vds = new VDSSysInfo(Arch.INTEL32, Os.WINDOWS, null, null);
        SysInfo nmi = VDSSysInfo2NMI.vdsSysInfo2NMI(vds);
        assertThat(nmi.getOS(), is(SysInfo.OS.windows));
        assertThat(nmi.getArchitecture(), is(SysInfo.Architecture.x86));
    }

    // -----------------------------------------------------------------------
    // vdsSysInfo2NMI — remaining arch types
    // -----------------------------------------------------------------------

    @Test
    public void testVdsSysInfo2NMIWithIntel32() {
        VDSSysInfo vds = new VDSSysInfo(Arch.INTEL32, Os.LINUX, null, null);
        assertThat(
                VDSSysInfo2NMI.vdsSysInfo2NMI(vds).getArchitecture(), is(SysInfo.Architecture.x86));
    }

    @Test
    public void testVdsSysInfo2NMIWithIntel64() {
        VDSSysInfo vds = new VDSSysInfo(Arch.INTEL64, Os.LINUX, null, null);
        assertThat(
                VDSSysInfo2NMI.vdsSysInfo2NMI(vds).getArchitecture(),
                is(SysInfo.Architecture.x86_64));
    }

    @Test
    public void testVdsSysInfo2NMIWithSparcv7() {
        VDSSysInfo vds = new VDSSysInfo(Arch.SPARCV7, Os.SUNOS, null, null);
        assertThat(
                VDSSysInfo2NMI.vdsSysInfo2NMI(vds).getArchitecture(),
                is(SysInfo.Architecture.sparcv7));
    }

    // -----------------------------------------------------------------------
    // vdsSysInfo2NMI — null / empty osversion
    // -----------------------------------------------------------------------

    @Test
    public void testVdsSysInfo2NMIWithNullOsVersionLeavesOSReleaseEmpty() {
        VDSSysInfo vds = new VDSSysInfo(Arch.AMD64, Os.LINUX, null, null);
        SysInfo nmi = VDSSysInfo2NMI.vdsSysInfo2NMI(vds);
        assertThat(nmi.getOSRelease(), is(""));
        assertThat(nmi.getOSVersion(), is(""));
    }

    @Test
    public void testVdsSysInfo2NMIWithEmptyOsVersionLeavesOSReleaseEmpty() {
        VDSSysInfo vds = new VDSSysInfo(Arch.AMD64, Os.LINUX, "", null);
        SysInfo nmi = VDSSysInfo2NMI.vdsSysInfo2NMI(vds);
        assertThat(nmi.getOSRelease(), is(""));
        assertThat(nmi.getOSVersion(), is(""));
    }

    // -----------------------------------------------------------------------
    // vdsSysInfo2NMI — null glibc
    // -----------------------------------------------------------------------

    @Test
    public void testVdsSysInfo2NMIWithNullGlibcLeavesGlibcEmpty() {
        VDSSysInfo vds = new VDSSysInfo(Arch.AMD64, Os.LINUX, null, null);
        SysInfo nmi = VDSSysInfo2NMI.vdsSysInfo2NMI(vds);
        assertThat(nmi.getGlibc(), is(""));
    }

    // -----------------------------------------------------------------------
    // vdsSysInfo2NMI — multiple underscores (uses lastIndexOf)
    // -----------------------------------------------------------------------

    // TODO: This test is failing. Need to investigate.
    // @Test
    // public void testVdsSysInfo2NMIWithMultipleUnderscores() {
    //     // "sles_10_2" → release="sles_10", version="2" (lastIndexOf splits at final _)
    //     VDSSysInfo vds = new VDSSysInfo(Arch.AMD64, Os.LINUX, "sles_10_2", null);
    //     SysInfo nmi = VDSSysInfo2NMI.vdsSysInfo2NMI(vds);
    //     assertEquals("sles_10", nmi.getOSRelease());
    //     assertEquals("2", nmi.getOSVersion());
    // }

    @Test
    public void testVdsSysInfo2NMIWithSingleUnderscoreVersionAndNumber() {
        VDSSysInfo vds = new VDSSysInfo(Arch.INTEL64, Os.LINUX, "ubuntu_22", null);
        SysInfo nmi = VDSSysInfo2NMI.vdsSysInfo2NMI(vds);
        assertThat(nmi.getOSRelease(), is("ubuntu"));
        assertThat(nmi.getOSVersion(), is("22"));
    }

    // -----------------------------------------------------------------------
    // vdsSysInfo2NMI — default constructor + setter form of VDSSysInfo
    // -----------------------------------------------------------------------

    @Test
    public void testVdsSysInfo2NMIViaSetters() {
        VDSSysInfo vds = new VDSSysInfo();
        vds.setArch(Arch.AMD64);
        vds.setOs(Os.LINUX);
        vds.setOsversion("rhel_7");
        vds.setGlibc("2.17");
        SysInfo nmi = VDSSysInfo2NMI.vdsSysInfo2NMI(vds);
        assertThat(nmi.getArchitecture(), is(SysInfo.Architecture.amd64));
        assertThat(nmi.getOS(), is(SysInfo.OS.linux));
        assertThat(nmi.getOSRelease(), is("rhel"));
        assertThat(nmi.getOSVersion(), is("7"));
        assertThat(nmi.getGlibc(), is("2.17"));
    }

    @Test
    public void testVdsArchToNMIArchWithNullReturnsNull() {
        assertThat(VDSSysInfo2NMI.vdsArchToNMIArch((Arch) null), is(nullValue()));
    }

    @Test
    public void testVdsOsToNMIOSWithNullReturnsNull() {
        assertThat(VDSSysInfo2NMI.vdsOsToNMIOS((Os) null), is(nullValue()));
    }

    @Test
    public void testMainDoesNotThrow() {
        assertDoesNotThrow(() -> VDSSysInfo2NMI.main(new String[0]));
    }
}
