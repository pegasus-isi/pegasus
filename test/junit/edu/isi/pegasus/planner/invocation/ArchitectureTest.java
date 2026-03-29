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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for the Architecture invocation class. */
public class ArchitectureTest {

    private Architecture mArchitecture;

    @BeforeEach
    public void setUp() {
        mArchitecture = new Architecture();
    }

    @AfterEach
    public void tearDown() {
        mArchitecture = null;
    }

    @Test
    public void testImplementsHasText() {
        assertTrue(HasText.class.isAssignableFrom(Architecture.class));
    }

    @Test
    public void testExtendsInvocation() {
        assertTrue(Invocation.class.isAssignableFrom(Architecture.class));
    }

    @Test
    public void testDefaultConstructorInitializesNullValue() {
        assertNull(mArchitecture.getValue());
    }

    @Test
    public void testSetAndGetSystemName() {
        mArchitecture.setSystemName("linux");
        assertEquals("linux", mArchitecture.getSystemName());
    }

    @Test
    public void testSetAndGetMachine() {
        mArchitecture.setMachine("x86_64");
        assertEquals("x86_64", mArchitecture.getMachine());
    }

    @Test
    public void testSetAndGetRelease() {
        mArchitecture.setRelease("5.4.0");
        assertEquals("5.4.0", mArchitecture.getRelease());
    }

    @Test
    public void testSetNodeNameNormalizesWithDomain() {
        mArchitecture.setNodeName("myhost.example.com");
        assertEquals("myhost", mArchitecture.getNodeName());
        assertEquals("example.com", mArchitecture.getDomainName());
    }

    @Test
    public void testSetNodeNameWithoutDomain() {
        mArchitecture.setNodeName("myhost");
        assertEquals("myhost", mArchitecture.getNodeName());
        assertNull(mArchitecture.getDomainName());
    }

    @Test
    public void testAppendValueBuildsString() {
        mArchitecture.appendValue("hello");
        mArchitecture.appendValue(" world");
        assertEquals("hello world", mArchitecture.getValue());
    }

    @Test
    public void testToXMLContainsSysname() throws Exception {
        mArchitecture.setSystemName("linux");
        mArchitecture.setMachine("x86_64");
        mArchitecture.setRelease("5.4");
        mArchitecture.setNodeName("host1");
        StringWriter sw = new StringWriter();
        mArchitecture.toXML(sw, "", null);
        String xml = sw.toString();
        assertTrue(xml.contains("sysname=\"linux\""));
        assertTrue(xml.contains("<uname"));
    }
}
