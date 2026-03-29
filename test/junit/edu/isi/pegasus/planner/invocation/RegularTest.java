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

/** Tests for Regular invocation class. */
public class RegularTest {

    @Test
    public void testExtendsFile() {
        assertTrue(File.class.isAssignableFrom(Regular.class));
    }

    @Test
    public void testImplementsHasFilename() {
        assertTrue(HasFilename.class.isAssignableFrom(Regular.class));
    }

    @Test
    public void testDefaultConstructorNullFilename() {
        Regular r = new Regular();
        assertNull(r.getFilename());
    }

    @Test
    public void testConstructorWithFilename() {
        Regular r = new Regular("/tmp/output.txt");
        assertEquals("/tmp/output.txt", r.getFilename());
    }

    @Test
    public void testSetAndGetFilename() {
        Regular r = new Regular();
        r.setFilename("/data/file.dat");
        assertEquals("/data/file.dat", r.getFilename());
    }

    @Test
    public void testToXMLContainsNameAttribute() throws Exception {
        Regular r = new Regular("/tmp/myfile.txt");
        StringWriter sw = new StringWriter();
        r.toXML(sw, "", null);
        String xml = sw.toString();
        assertTrue(xml.contains("name=\"/tmp/myfile.txt\""));
        assertTrue(xml.contains("<file"));
    }

    @Test
    public void testToXMLSelfClosingWhenNoContent() throws Exception {
        Regular r = new Regular("/tmp/empty.txt");
        StringWriter sw = new StringWriter();
        r.toXML(sw, "", null);
        assertTrue(sw.toString().contains("/>"));
    }
}
