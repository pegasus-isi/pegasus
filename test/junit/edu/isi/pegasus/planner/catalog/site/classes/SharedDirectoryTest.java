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
package edu.isi.pegasus.planner.catalog.site.classes;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.io.StringWriter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class SharedDirectoryTest {

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testDefaultConstructorIsEmpty() {
        SharedDirectory dir = new SharedDirectory();
        assertTrue(dir.isEmpty());
    }

    @Test
    public void testSetAndGetInternalMountPoint() {
        SharedDirectory dir = new SharedDirectory();
        InternalMountPoint mp = new InternalMountPoint("/lustre");
        dir.setInternalMountPoint(mp);
        assertEquals("/lustre", dir.getInternalMountPoint().getMountPoint());
    }

    @Test
    public void testAddFileServerMakesNotEmpty() {
        SharedDirectory dir = new SharedDirectory();
        FileServer fs = new FileServer("gsiftp", "gsiftp://site.edu", "/lustre");
        fs.setSupportedOperation(FileServer.OPERATION.all);
        dir.addFileServer(fs);
        assertFalse(dir.isEmpty());
    }

    @Test
    public void testToXMLIsEmptyWhenNoContent() throws IOException {
        SharedDirectory dir = new SharedDirectory();
        StringWriter sw = new StringWriter();
        dir.toXML(sw, "");
        assertEquals("", sw.toString());
    }

    @Test
    public void testToXMLContainsSharedElementWhenNotEmpty() throws IOException {
        SharedDirectory dir = new SharedDirectory();
        InternalMountPoint mp = new InternalMountPoint("/shared/scratch");
        dir.setInternalMountPoint(mp);
        FileServer fs = new FileServer("gsiftp", "gsiftp://site.edu", "/shared/scratch");
        fs.setSupportedOperation(FileServer.OPERATION.all);
        dir.addFileServer(fs);
        StringWriter sw = new StringWriter();
        dir.toXML(sw, "");
        String xml = sw.toString();
        assertThat(xml, containsString("<shared>"));
        assertThat(xml, containsString("</shared>"));
    }

    @Test
    public void testHasFileServerForPUTWithPutOperation() {
        SharedDirectory dir = new SharedDirectory();
        FileServer fs = new FileServer("gsiftp", "gsiftp://site.edu", "/data");
        fs.setSupportedOperation(FileServer.OPERATION.put);
        dir.addFileServer(fs);
        assertTrue(dir.hasFileServerForPUTOperations());
    }

    @Test
    public void testCloneProducesDistinctInstance() {
        SharedDirectory dir = new SharedDirectory();
        InternalMountPoint mp = new InternalMountPoint("/shared");
        dir.setInternalMountPoint(mp);
        SharedDirectory cloned = (SharedDirectory) dir.clone();
        assertNotSame(dir, cloned);
        assertEquals(
                dir.getInternalMountPoint().getMountPoint(),
                cloned.getInternalMountPoint().getMountPoint());
    }
}
