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

import java.util.Iterator;
import org.junit.jupiter.api.Test;

/** Tests for MachineSpecific invocation class. */
public class MachineSpecificTest {

    @Test
    public void testExtendsInvocation() {
        assertTrue(Invocation.class.isAssignableFrom(MachineSpecific.class));
    }

    @Test
    public void testConstructorWithTag() {
        MachineSpecific ms = new MachineSpecific("linux");
        assertEquals("linux", ms.getTag());
    }

    @Test
    public void testSetAndGetTag() {
        MachineSpecific ms = new MachineSpecific("darwin");
        ms.setTag("sunos");
        assertEquals("sunos", ms.getTag());
    }

    @Test
    public void testAddMachineInfo() {
        MachineSpecific ms = new MachineSpecific("linux");
        RAM ram = new RAM();
        ram.addAttribute("total", "16384");
        ms.addMachineInfo(ram);
        // iterator should have at least one entry
        Iterator<MachineInfo> it = ms.getMachineInfoIterator();
        assertTrue(it.hasNext(), "MachineSpecific should have contents after addMachineInfo");
    }

    @Test
    public void testEmptyContentsInitially() {
        MachineSpecific ms = new MachineSpecific("basic");
        Iterator<MachineInfo> it = ms.getMachineInfoIterator();
        assertFalse(it.hasNext(), "MachineSpecific should be empty initially");
    }

    @Test
    public void testAddMultipleMachineInfoElements() {
        MachineSpecific ms = new MachineSpecific("linux");
        ms.addMachineInfo(new RAM());
        ms.addMachineInfo(new Load());
        ms.addMachineInfo(new Swap());
        // count by draining the iterator
        int count = 0;
        for (Iterator<MachineInfo> it = ms.getMachineInfoIterator(); it.hasNext(); it.next()) {
            count++;
        }
        assertEquals(3, count);
    }

    @Test
    public void testGetElementNameMatchesTag() {
        MachineSpecific ms = new MachineSpecific("linux");
        assertEquals("linux", ms.getElementName());
    }
}
