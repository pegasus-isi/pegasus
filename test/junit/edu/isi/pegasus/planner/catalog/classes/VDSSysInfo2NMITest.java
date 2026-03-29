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

import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.catalog.transformation.classes.Arch;
import edu.isi.pegasus.planner.catalog.transformation.classes.Os;
import edu.isi.pegasus.planner.catalog.transformation.classes.VDSSysInfo;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class VDSSysInfo2NMITest {

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testVdsArchToNMIArchIntel32() {
        SysInfo.Architecture arch = VDSSysInfo2NMI.vdsArchToNMIArch(Arch.INTEL32);
        assertEquals(SysInfo.Architecture.x86, arch);
    }

    @Test
    public void testVdsArchToNMIArchIntel64() {
        SysInfo.Architecture arch = VDSSysInfo2NMI.vdsArchToNMIArch(Arch.INTEL64);
        assertEquals(SysInfo.Architecture.x86_64, arch);
    }

    @Test
    public void testVdsArchToNMIArchAmd64() {
        SysInfo.Architecture arch = VDSSysInfo2NMI.vdsArchToNMIArch(Arch.AMD64);
        assertEquals(SysInfo.Architecture.amd64, arch);
    }

    @Test
    public void testVdsArchToNMIArchSparcv7() {
        SysInfo.Architecture arch = VDSSysInfo2NMI.vdsArchToNMIArch(Arch.SPARCV7);
        assertEquals(SysInfo.Architecture.sparcv7, arch);
    }

    @Test
    public void testVdsArchToNMIArchSparcv9() {
        SysInfo.Architecture arch = VDSSysInfo2NMI.vdsArchToNMIArch(Arch.SPARCV9);
        assertEquals(SysInfo.Architecture.sparcv9, arch);
    }

    @Test
    public void testVdsArchToNMIArchByString() {
        SysInfo.Architecture arch = VDSSysInfo2NMI.vdsArchToNMIArch("INTEL32");
        assertEquals(SysInfo.Architecture.x86, arch);
    }

    @Test
    public void testVdsOsToNMIOSLinux() {
        SysInfo.OS os = VDSSysInfo2NMI.vdsOsToNMIOS(Os.LINUX);
        assertEquals(SysInfo.OS.linux, os);
    }

    @Test
    public void testVdsOsToNMIOSAix() {
        SysInfo.OS os = VDSSysInfo2NMI.vdsOsToNMIOS(Os.AIX);
        assertEquals(SysInfo.OS.aix, os);
    }

    @Test
    public void testVdsOsToNMIOSSunos() {
        SysInfo.OS os = VDSSysInfo2NMI.vdsOsToNMIOS(Os.SUNOS);
        assertEquals(SysInfo.OS.sunos, os);
    }

    @Test
    public void testVdsOsToNMIOSWindows() {
        SysInfo.OS os = VDSSysInfo2NMI.vdsOsToNMIOS(Os.WINDOWS);
        assertEquals(SysInfo.OS.windows, os);
    }

    @Test
    public void testVdsOsToNMIOSByString() {
        SysInfo.OS os = VDSSysInfo2NMI.vdsOsToNMIOS("LINUX");
        assertEquals(SysInfo.OS.linux, os);
    }

    @Test
    public void testVdsSysInfo2NMIBasic() {
        VDSSysInfo vds = new VDSSysInfo(Arch.AMD64, Os.LINUX, null, null);
        SysInfo nmi = VDSSysInfo2NMI.vdsSysInfo2NMI(vds);
        assertEquals(SysInfo.Architecture.amd64, nmi.getArchitecture());
        assertEquals(SysInfo.OS.linux, nmi.getOS());
    }

    @Test
    public void testVdsSysInfo2NMIWithOsVersionContainingSeparator() {
        VDSSysInfo vds = new VDSSysInfo(Arch.AMD64, Os.LINUX, "rhel_4", null);
        SysInfo nmi = VDSSysInfo2NMI.vdsSysInfo2NMI(vds);
        assertEquals("rhel", nmi.getOSRelease());
        assertEquals("4", nmi.getOSVersion());
    }

    @Test
    public void testVdsSysInfo2NMIWithOsVersionWithTrailingSeparator() {
        VDSSysInfo vds = new VDSSysInfo(Arch.AMD64, Os.LINUX, "rhel_", null);
        SysInfo nmi = VDSSysInfo2NMI.vdsSysInfo2NMI(vds);
        assertEquals("rhel", nmi.getOSRelease());
        assertEquals("", nmi.getOSVersion());
    }

    @Test
    public void testVdsSysInfo2NMIWithOsVersionWithoutSeparator() {
        VDSSysInfo vds = new VDSSysInfo(Arch.AMD64, Os.LINUX, "rhel", null);
        SysInfo nmi = VDSSysInfo2NMI.vdsSysInfo2NMI(vds);
        assertEquals("rhel", nmi.getOSRelease());
        assertEquals("", nmi.getOSVersion());
    }

    @Test
    public void testVdsSysInfo2NMIWithGlibc() {
        VDSSysInfo vds = new VDSSysInfo(Arch.INTEL64, Os.LINUX, null, "2.17");
        SysInfo nmi = VDSSysInfo2NMI.vdsSysInfo2NMI(vds);
        assertEquals("2.17", nmi.getGlibc());
    }

    @Test
    public void testOsCombineSeparatorConstant() {
        assertEquals("_", VDSSysInfo2NMI.OS_COMBINE_SEPARATOR);
    }
}
