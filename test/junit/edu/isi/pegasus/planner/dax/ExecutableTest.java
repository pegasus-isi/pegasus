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
package edu.isi.pegasus.planner.dax;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for the Executable class. */
public class ExecutableTest {

    private Executable mExec;

    @BeforeEach
    public void setUp() {
        mExec = new Executable("pegasus", "preprocess", "1.0");
    }

    @Test
    public void testInstantiation() {
        assertNotNull(mExec, "Executable should be instantiatable");
    }

    @Test
    public void testGetName() {
        assertEquals("preprocess", mExec.getName(), "Name should match constructor argument");
    }

    @Test
    public void testGetNamespace() {
        assertEquals(
                "pegasus", mExec.getNamespace(), "Namespace should match constructor argument");
    }

    @Test
    public void testGetVersion() {
        assertEquals("1.0", mExec.getVersion(), "Version should match constructor argument");
    }

    @Test
    public void testDefaultInstalledIsTrue() {
        assertTrue(mExec.getInstalled(), "Default installed flag should be true");
    }

    @Test
    public void testSetInstalledFalse() {
        mExec.setInstalled(false);
        assertFalse(
                mExec.getInstalled(), "Installed flag should be false after setInstalled(false)");
    }

    @Test
    public void testUnsetInstalled() {
        mExec.unsetInstalled();
        assertFalse(mExec.getInstalled(), "Installed should be false after unsetInstalled");
    }

    @Test
    public void testSetArchitecture() {
        mExec.setArchitecture(Executable.ARCH.X86_64);
        assertEquals(Executable.ARCH.X86_64, mExec.getArchitecture(), "Architecture should be set");
    }

    @Test
    public void testSetOS() {
        mExec.setOS(Executable.OS.LINUX);
        assertEquals(Executable.OS.LINUX, mExec.getOS(), "OS should be set");
    }

    @Test
    public void testSimpleNameConstructor() {
        Executable e = new Executable("my-tool");
        assertEquals("my-tool", e.getName(), "Simple name constructor should set name");
    }

    @Test
    public void testExtendsCatalogType() {
        assertInstanceOf(CatalogType.class, mExec, "Executable should extend CatalogType");
    }

    @Test
    public void testAddPFN() {
        mExec.addPhysicalFile("/usr/bin/preprocess", "local");
        assertFalse(mExec.getPhysicalFiles().isEmpty(), "PFNs should not be empty");
    }

    @Test
    public void testCopyConstructor() {
        mExec.setArchitecture(Executable.ARCH.X86);
        mExec.setOS(Executable.OS.LINUX);
        Executable copy = new Executable(mExec);
        assertEquals(mExec.getName(), copy.getName(), "Copy should have same name");
        assertEquals(mExec.getNamespace(), copy.getNamespace(), "Copy should have same namespace");
        assertEquals(mExec.getArchitecture(), copy.getArchitecture(), "Copy should have same arch");
    }
}
