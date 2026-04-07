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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.util.XMLWriter;
import java.io.StringWriter;
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
        assertThat(mExec, notNullValue());
    }

    @Test
    public void testGetName() {
        assertThat(mExec.getName(), is("preprocess"));
    }

    @Test
    public void testGetNamespace() {
        assertThat(mExec.getNamespace(), is("pegasus"));
    }

    @Test
    public void testGetVersion() {
        assertThat(mExec.getVersion(), is("1.0"));
    }

    @Test
    public void testDefaultInstalledIsTrue() {
        assertThat(mExec.getInstalled(), is(true));
    }

    @Test
    public void testSetInstalledFalse() {
        mExec.setInstalled(false);
        assertThat(mExec.getInstalled(), is(false));
    }

    @Test
    public void testUnsetInstalled() {
        mExec.unsetInstalled();
        assertThat(mExec.getInstalled(), is(false));
    }

    @Test
    public void testSetArchitecture() {
        mExec.setArchitecture(Executable.ARCH.X86_64);
        assertThat(mExec.getArchitecture(), is(Executable.ARCH.X86_64));
    }

    @Test
    public void testSetOS() {
        mExec.setOS(Executable.OS.LINUX);
        assertThat(mExec.getOS(), is(Executable.OS.LINUX));
    }

    @Test
    public void testSimpleNameConstructor() {
        Executable e = new Executable("my-tool");
        assertThat(e.getName(), is("my-tool"));
    }

    @Test
    public void testExtendsCatalogType() {
        assertThat(mExec, instanceOf(CatalogType.class));
    }

    @Test
    public void testAddPFN() {
        mExec.addPhysicalFile("/usr/bin/preprocess", "local");
        assertThat(mExec.getPhysicalFiles().isEmpty(), is(false));
    }

    @Test
    public void testCopyConstructor() {
        mExec.setArchitecture(Executable.ARCH.X86);
        mExec.setOS(Executable.OS.LINUX);
        Executable copy = new Executable(mExec);
        assertThat(copy.getName(), is(mExec.getName()));
        assertThat(copy.getNamespace(), is(mExec.getNamespace()));
        assertThat(copy.getArchitecture(), is(mExec.getArchitecture()));
    }

    @Test
    public void testSimpleNameConstructorDefaultsNamespaceAndVersionToEmptyString() {
        Executable e = new Executable("my-tool");

        assertThat(e.getNamespace(), is(""));
        assertThat(e.getVersion(), is(""));
    }

    @Test
    public void testDefaultOptionalStringFieldsReturnEmptyStrings() {
        assertThat(mExec.getOsRelease(), is(""));
        assertThat(mExec.getOsVersion(), is(""));
        assertThat(mExec.getGlibc(), is(""));
    }

    @Test
    public void testSetInstalledMarksExecutableInstalledAgain() {
        mExec.unsetInstalled();

        mExec.setInstalled();

        assertThat(mExec.getInstalled(), is(true));
    }

    @Test
    public void testNotificationAliasAndInvokeCloneBehavior() {
        Invoke invoke = new Invoke(Invoke.WHEN.start, "/bin/date");

        mExec.addNotification(invoke);

        assertThat(mExec.getNotification().size(), is(1));
        assertThat(mExec.getInvoke(), sameInstance(mExec.getNotification()));
        assertNotSame(invoke, mExec.getInvoke().get(0));
    }

    @Test
    public void testAddRequirementUsesSetSemantics() {
        Executable requirement = new Executable("pegasus", "helper", "1.0");

        mExec.addRequirement(requirement).addRequirement(requirement);

        assertThat(mExec.getRequirements().size(), is(1));
        assertThat(mExec.getRequirements().contains(requirement), is(true));
    }

    @Test
    public void testIsExecutableAlwaysReturnsTrue() {
        assertThat(mExec.isExecutable(), is(true));
    }

    @Test
    public void testToXMLIncludesInstalledFalseAndSysInfoAttributesWhenPresent() {
        mExec.unsetInstalled();
        mExec.setArchitecture(Executable.ARCH.X86_64);
        mExec.setOS(Executable.OS.LINUX);
        mExec.setOSRelease("rhel");
        mExec.setOSVersion("8");
        mExec.setGlibc("2.28");
        mExec.addPhysicalFile("/bin/preprocess", "local");

        StringWriter sw = new StringWriter();
        XMLWriter writer = new XMLWriter(sw);
        mExec.toXML(writer, 0);
        String result = sw.toString();

        assertThat(
                result,
                allOf(
                        containsString("<executable"),
                        containsString("installed=\"false\""),
                        containsString("arch=\"x86_64\""),
                        containsString("os=\"linux\""),
                        containsString("osrelease=\"rhel\""),
                        containsString("osversion=\"8\""),
                        containsString("glibc=\"2.28\"")));
    }
}
