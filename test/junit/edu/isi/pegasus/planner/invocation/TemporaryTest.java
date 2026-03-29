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

/** Tests for Temporary invocation class. */
public class TemporaryTest {

    @Test
    public void testExtendsFile() {
        assertTrue(File.class.isAssignableFrom(Temporary.class));
    }

    @Test
    public void testImplementsHasDescriptor() {
        assertTrue(HasDescriptor.class.isAssignableFrom(Temporary.class));
    }

    @Test
    public void testImplementsHasFilename() {
        assertTrue(HasFilename.class.isAssignableFrom(Temporary.class));
    }

    @Test
    public void testDefaultConstructorNullFilename() {
        Temporary t = new Temporary();
        assertNull(t.getFilename());
    }

    @Test
    public void testDefaultConstructorDescriptorMinusOne() {
        Temporary t = new Temporary();
        assertEquals(-1, t.getDescriptor());
    }

    @Test
    public void testConstructorWithFilenameAndDescriptor() {
        Temporary t = new Temporary("/tmp/work.tmp", 3);
        assertEquals("/tmp/work.tmp", t.getFilename());
        assertEquals(3, t.getDescriptor());
    }

    @Test
    public void testSetAndGetFilename() {
        Temporary t = new Temporary();
        t.setFilename("/var/tmp/data.tmp");
        assertEquals("/var/tmp/data.tmp", t.getFilename());
    }

    @Test
    public void testSetAndGetDescriptor() {
        Temporary t = new Temporary();
        t.setDescriptor(5);
        assertEquals(5, t.getDescriptor());
    }

    @Test
    public void testToXMLContainsTemporaryTag() throws Exception {
        Temporary t = new Temporary("/tmp/out.tmp", 2);
        StringWriter sw = new StringWriter();
        t.toXML(sw, "", null);
        String xml = sw.toString();
        assertTrue(xml.contains("<temporary"));
        assertTrue(xml.contains("name=\"/tmp/out.tmp\""));
        assertTrue(xml.contains("descriptor=\"2\""));
    }
}
