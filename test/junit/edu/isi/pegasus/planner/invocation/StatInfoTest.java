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
package edu.isi.pegasus.planner.invocation;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Date;
import org.junit.jupiter.api.Test;

/** Tests for StatInfo invocation class. */
public class StatInfoTest {

    @Test
    public void testExtendsInvocation() {
        assertThat(Invocation.class.isAssignableFrom(StatInfo.class), is(true));
    }

    @Test
    public void testDefaultConstructorUidMinusOne() {
        StatInfo si = new StatInfo();
        assertThat(si.getUID(), is(-1));
    }

    @Test
    public void testDefaultConstructorGidMinusOne() {
        StatInfo si = new StatInfo();
        assertThat(si.getGID(), is(-1));
    }

    @Test
    public void testSetAndGetSize() {
        StatInfo si = new StatInfo();
        si.setSize(4096L);
        assertThat(si.getSize(), is(4096L));
    }

    @Test
    public void testSetAndGetMode() {
        StatInfo si = new StatInfo();
        si.setMode(0644);
        assertThat(si.getMode(), is(0644));
    }

    @Test
    public void testSetAndGetUser() {
        StatInfo si = new StatInfo();
        si.setUser("testuser");
        assertThat(si.getUser(), is("testuser"));
    }

    @Test
    public void testSetAndGetGroup() {
        StatInfo si = new StatInfo();
        si.setGroup("testgroup");
        assertThat(si.getGroup(), is("testgroup"));
    }

    @Test
    public void testSetAndGetINode() {
        StatInfo si = new StatInfo();
        si.setINode(12345L);
        assertThat(si.getINode(), is(12345L));
    }

    @Test
    public void testToXMLContainsStatinfo() throws Exception {
        StatInfo si = new StatInfo();
        si.setSize(1024L);
        StringWriter sw = new StringWriter();
        si.toXML(sw, "", null);
        assertThat(sw.toString(), containsString("statinfo"));
        assertThat(sw.toString(), containsString("size=\"1024\""));
    }

    @Test
    public void testSetAndGetLinkBlockAndOwnerFields() {
        StatInfo si = new StatInfo();
        si.setLinkCount(3L);
        si.setBlockSize(4096L);
        si.setBlocks(8L);
        si.setUID(1000);
        si.setGID(100);

        assertThat(si.getLinkCount(), is(3L));
        assertThat(si.getBlockSize(), is(4096L));
        assertThat(si.getBlocks(), is(8L));
        assertThat(si.getUID(), is(1000));
        assertThat(si.getGID(), is(100));
    }

    @Test
    public void testSetAndGetTimestamps() {
        StatInfo si = new StatInfo();
        Date access = new Date(1000L);
        Date creation = new Date(2000L);
        Date modification = new Date(3000L);

        si.setAccessTime(access);
        si.setCreationTime(creation);
        si.setModificationTime(modification);

        assertThat(si.getAccessTime(), is(sameInstance(access)));
        assertThat(si.getCreationTime(), is(sameInstance(creation)));
        assertThat(si.getModificationTime(), is(sameInstance(modification)));
    }

    @Test
    public void testToStringWriterThrowsIOException() {
        StatInfo si = new StatInfo();

        IOException exception =
                assertThrows(IOException.class, () -> si.toString(new StringWriter()));

        assertThat(
                exception.getMessage(),
                is("method not implemented, please contact vds-support@griphyn.org"));
    }

    @Test
    public void testToXMLUsesNamespaceAndIncludesOptionalUserAndGroup() throws Exception {
        StatInfo si = new StatInfo();
        si.setMode(0644);
        si.setSize(1024L);
        si.setINode(12345L);
        si.setLinkCount(2L);
        si.setBlockSize(4096L);
        si.setBlocks(8L);
        si.setModificationTime(new Date(3000L));
        si.setAccessTime(new Date(2000L));
        si.setCreationTime(new Date(1000L));
        si.setUID(1000);
        si.setUser("alice");
        si.setGID(100);
        si.setGroup("users");

        StringWriter sw = new StringWriter();
        si.toXML(sw, "  ", "inv");

        String xml = sw.toString();
        assertThat(xml.startsWith("  <inv:statinfo"), is(true));
        assertThat(xml, containsString("mode=\"0644\""));
        assertThat(xml, containsString("size=\"1024\""));
        assertThat(xml, containsString("inode=\"12345\""));
        assertThat(xml, containsString("nlink=\"2\""));
        assertThat(xml, containsString("blksize=\"4096\""));
        assertThat(xml, containsString("blocks=\"8\""));
        assertThat(xml, containsString("uid=\"1000\""));
        assertThat(xml, containsString("user=\"alice\""));
        assertThat(xml, containsString("gid=\"100\""));
        assertThat(xml, containsString("group=\"users\""));
        assertThat(xml.endsWith("/>" + System.lineSeparator()), is(true));
    }

    @Test
    public void testToXMLOmitsEmptyUserAndGroupAttributes() throws Exception {
        StatInfo si = new StatInfo();
        si.setUser("");
        si.setGroup("");

        StringWriter sw = new StringWriter();
        si.toXML(sw, "", null);

        String xml = sw.toString();
        assertThat(xml.contains(" user="), is(false));
        assertThat(xml.contains(" group="), is(false));
    }
}
