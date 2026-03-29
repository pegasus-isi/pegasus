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

import org.junit.jupiter.api.Test;

/** Tests for Uname invocation class. */
public class UnameTest {

    @Test
    public void testExtendsMachineInfo() {
        assertTrue(MachineInfo.class.isAssignableFrom(Uname.class));
    }

    @Test
    public void testImplementsHasText() {
        assertTrue(HasText.class.isAssignableFrom(Uname.class));
    }

    @Test
    public void testElementName() {
        assertEquals("uname", Uname.ELEMENT_NAME);
    }

    @Test
    public void testDefaultConstructorNullValue() {
        Uname u = new Uname();
        assertNull(u.getValue());
    }

    @Test
    public void testConstructorWithValue() {
        Uname u = new Uname("Linux host 5.4");
        assertEquals("Linux host 5.4", u.getValue());
    }

    @Test
    public void testConstructorNullThrows() {
        assertThrows(NullPointerException.class, () -> new Uname(null));
    }

    @Test
    public void testSetAndGetValue() {
        Uname u = new Uname();
        u.setValue("Darwin kernel");
        assertEquals("Darwin kernel", u.getValue());
    }

    @Test
    public void testAppendValue() {
        Uname u = new Uname("Linux ");
        u.appendValue("x86_64");
        assertEquals("Linux x86_64", u.getValue());
    }

    @Test
    public void testToArchitectureNotNull() {
        Uname u = new Uname();
        u.addAttribute(Uname.SYSTEM_ATTRIBUTE_KEY, "Linux");
        u.addAttribute(Uname.MACHINE_ATTRIBUTE_KEY, "x86_64");
        u.addAttribute(Uname.NODENAME_ATTRIBUTE_KEY, "host1");
        u.addAttribute(Uname.RELEASE_ATTRIBUTE_KEY, "5.4.0");
        Architecture arch = u.toArchitecture();
        assertNotNull(arch);
        assertEquals("Linux", arch.getSystemName());
    }

    @Test
    public void testUnameToArchitectureStaticMethod() {
        Uname u = new Uname();
        u.addAttribute(Uname.SYSTEM_ATTRIBUTE_KEY, "Darwin");
        u.addAttribute(Uname.NODENAME_ATTRIBUTE_KEY, "myhost");
        Architecture arch = Uname.unameToArchitecture(u);
        assertEquals("Darwin", arch.getSystemName());
    }
}
