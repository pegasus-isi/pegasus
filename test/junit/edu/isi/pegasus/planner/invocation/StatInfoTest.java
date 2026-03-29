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

import static org.junit.jupiter.api.Assertions.*;

import java.io.StringWriter;
import org.junit.jupiter.api.Test;

/** Tests for StatInfo invocation class. */
public class StatInfoTest {

    @Test
    public void testExtendsInvocation() {
        assertTrue(Invocation.class.isAssignableFrom(StatInfo.class));
    }

    @Test
    public void testDefaultConstructorUidMinusOne() {
        StatInfo si = new StatInfo();
        assertEquals(-1, si.getUID());
    }

    @Test
    public void testDefaultConstructorGidMinusOne() {
        StatInfo si = new StatInfo();
        assertEquals(-1, si.getGID());
    }

    @Test
    public void testSetAndGetSize() {
        StatInfo si = new StatInfo();
        si.setSize(4096L);
        assertEquals(4096L, si.getSize());
    }

    @Test
    public void testSetAndGetMode() {
        StatInfo si = new StatInfo();
        si.setMode(0644);
        assertEquals(0644, si.getMode());
    }

    @Test
    public void testSetAndGetUser() {
        StatInfo si = new StatInfo();
        si.setUser("testuser");
        assertEquals("testuser", si.getUser());
    }

    @Test
    public void testSetAndGetGroup() {
        StatInfo si = new StatInfo();
        si.setGroup("testgroup");
        assertEquals("testgroup", si.getGroup());
    }

    @Test
    public void testSetAndGetINode() {
        StatInfo si = new StatInfo();
        si.setINode(12345L);
        assertEquals(12345L, si.getINode());
    }

    @Test
    public void testToXMLContainsStatinfo() throws Exception {
        StatInfo si = new StatInfo();
        si.setSize(1024L);
        StringWriter sw = new StringWriter();
        si.toXML(sw, "", null);
        assertTrue(sw.toString().contains("statinfo"));
        assertTrue(sw.toString().contains("size=\"1024\""));
    }
}
