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

import edu.isi.pegasus.planner.catalog.classes.SysInfo;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for the NMI2VDSSysInfo adapter class. */
public class NMI2VDSSysInfoTest {

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testNMIArchToVDSArchMapIsNotNull() {
        assertNotNull(NMI2VDSSysInfo.NMIArchToVDSArchMap());
    }

    @Test
    public void testNMIOSToVDSOSMapIsNotNull() {
        assertNotNull(NMI2VDSSysInfo.NMIOSToVDSOSMap());
    }

    @Test
    public void testX86MapsToIntel32() {
        Arch result = NMI2VDSSysInfo.nmiArchToVDSArch(SysInfo.Architecture.x86);
        assertSame(Arch.INTEL32, result);
    }

    @Test
    public void testX86_64MapsToIntel64() {
        Arch result = NMI2VDSSysInfo.nmiArchToVDSArch(SysInfo.Architecture.x86_64);
        assertSame(Arch.INTEL64, result);
    }

    @Test
    public void testAmd64MapsToAmd64() {
        Arch result = NMI2VDSSysInfo.nmiArchToVDSArch(SysInfo.Architecture.amd64);
        assertSame(Arch.AMD64, result);
    }

    @Test
    public void testSparcV7MapsToSparcV7() {
        Arch result = NMI2VDSSysInfo.nmiArchToVDSArch(SysInfo.Architecture.sparcv7);
        assertSame(Arch.SPARCV7, result);
    }

    @Test
    public void testSparcV9MapsToSparcV9() {
        Arch result = NMI2VDSSysInfo.nmiArchToVDSArch(SysInfo.Architecture.sparcv9);
        assertSame(Arch.SPARCV9, result);
    }

    @Test
    public void testLinuxMapsToLinux() {
        Os result = NMI2VDSSysInfo.nmiOSToVDSOS(SysInfo.OS.linux);
        assertSame(Os.LINUX, result);
    }

    @Test
    public void testWindowsMapsToWindows() {
        Os result = NMI2VDSSysInfo.nmiOSToVDSOS(SysInfo.OS.windows);
        assertSame(Os.WINDOWS, result);
    }

    @Test
    public void testAixMapsToAix() {
        Os result = NMI2VDSSysInfo.nmiOSToVDSOS(SysInfo.OS.aix);
        assertSame(Os.AIX, result);
    }

    @Test
    public void testSunosMapsToSunos() {
        Os result = NMI2VDSSysInfo.nmiOSToVDSOS(SysInfo.OS.sunos);
        assertSame(Os.SUNOS, result);
    }

    @Test
    public void testNmiArchToVDSArchByStringX86() {
        Arch result = NMI2VDSSysInfo.nmiArchToVDSArch("x86");
        assertSame(Arch.INTEL32, result);
    }

    @Test
    public void testNmiOSToVDSOSByStringLinux() {
        Os result = NMI2VDSSysInfo.nmiOSToVDSOS("linux");
        assertSame(Os.LINUX, result);
    }

    @Test
    public void testNmiToVDSSysInfoFromSysInfoObject() {
        SysInfo sysInfo = new SysInfo();
        sysInfo.setArchitecture(SysInfo.Architecture.x86_64);
        sysInfo.setOS(SysInfo.OS.linux);
        sysInfo.setGlibc("2.17");

        VDSSysInfo result = NMI2VDSSysInfo.nmiToVDSSysInfo(sysInfo);

        assertNotNull(result);
        assertSame(Arch.INTEL64, result.getArch());
        assertSame(Os.LINUX, result.getOs());
        assertEquals("2.17", result.getGlibc());
    }

    @Test
    public void testNmiToVDSSysInfoWithReleaseAndVersion() {
        SysInfo sysInfo = new SysInfo();
        sysInfo.setArchitecture(SysInfo.Architecture.x86);
        sysInfo.setOS(SysInfo.OS.linux);
        sysInfo.setOSRelease("rhel");
        sysInfo.setOSVersion("7");

        VDSSysInfo result = NMI2VDSSysInfo.nmiToVDSSysInfo(sysInfo);

        assertNotNull(result);
        // OS version combines release and version with separator
        String osversion = result.getOsversion();
        assertTrue(
                osversion.contains("rhel"),
                "Expected osversion to contain release, got: " + osversion);
        assertTrue(
                osversion.contains("7"),
                "Expected osversion to contain version, got: " + osversion);
    }

    @Test
    public void testNmiToVDSSysInfoFromComponentsX86Linux() {
        VDSSysInfo result =
                NMI2VDSSysInfo.nmiToVDSSysInfo(SysInfo.Architecture.x86, SysInfo.OS.linux, null);
        assertNotNull(result);
        assertSame(Arch.INTEL32, result.getArch());
        assertSame(Os.LINUX, result.getOs());
    }

    @Test
    public void testOsCombineSeparatorIsUnderscore() {
        assertEquals("_", NMI2VDSSysInfo.OS_COMBINE_SEPARATOR);
    }

    @Test
    public void testArchMapIsSingleton() {
        assertSame(NMI2VDSSysInfo.NMIArchToVDSArchMap(), NMI2VDSSysInfo.NMIArchToVDSArchMap());
    }

    @Test
    public void testOSMapIsSingleton() {
        assertSame(NMI2VDSSysInfo.NMIOSToVDSOSMap(), NMI2VDSSysInfo.NMIOSToVDSOSMap());
    }
}
