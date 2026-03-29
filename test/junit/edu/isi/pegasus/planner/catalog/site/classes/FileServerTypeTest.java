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

import static org.junit.jupiter.api.Assertions.*;

import java.util.Collection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for FileServerType via FileServer (concrete subclass) and for the OPERATION enum.
 *
 * @author Rajiv Mayani
 */
public class FileServerTypeTest {

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testDefaultConstructorDefaults() {
        FileServer fs = new FileServer();
        assertEquals("", fs.getProtocol());
        assertEquals("", fs.getURLPrefix());
        assertEquals("", fs.getMountPoint());
        assertEquals(FileServerType.OPERATION.all, fs.getSupportedOperation());
    }

    @Test
    public void testOverloadedConstructorSetsFields() {
        FileServer fs = new FileServer("gsiftp", "gsiftp://site.edu", "/data");
        assertEquals("gsiftp", fs.getProtocol());
        assertEquals("gsiftp://site.edu", fs.getURLPrefix());
        assertEquals("/data", fs.getMountPoint());
    }

    @Test
    public void testSetAndGetProtocol() {
        FileServer fs = new FileServer();
        fs.setProtocol("scp");
        assertEquals("scp", fs.getProtocol());
    }

    @Test
    public void testSetAndGetURLPrefix() {
        FileServer fs = new FileServer();
        fs.setURLPrefix("http://storage.example.org");
        assertEquals("http://storage.example.org", fs.getURLPrefix());
    }

    @Test
    public void testSetAndGetMountPoint() {
        FileServer fs = new FileServer();
        fs.setMountPoint("/lustre/scratch");
        assertEquals("/lustre/scratch", fs.getMountPoint());
    }

    @Test
    public void testSetSupportedOperationByEnum() {
        FileServer fs = new FileServer();
        fs.setSupportedOperation(FileServerType.OPERATION.put);
        assertEquals(FileServerType.OPERATION.put, fs.getSupportedOperation());
    }

    @Test
    public void testSetSupportedOperationByString() {
        FileServer fs = new FileServer();
        fs.setSupportedOperation("get");
        assertEquals(FileServerType.OPERATION.get, fs.getSupportedOperation());
    }

    @Test
    public void testOperationEnumValues() {
        FileServerType.OPERATION[] ops = FileServerType.OPERATION.values();
        assertEquals(3, ops.length);
    }

    @Test
    public void testOperationsForGETContainsGetAndAll() {
        Collection<FileServerType.OPERATION> ops = FileServerType.OPERATION.operationsForGET();
        assertTrue(ops.contains(FileServerType.OPERATION.get));
        assertTrue(ops.contains(FileServerType.OPERATION.all));
    }

    @Test
    public void testOperationsForPUTContainsPutAndAll() {
        Collection<FileServerType.OPERATION> ops = FileServerType.OPERATION.operationsForPUT();
        assertTrue(ops.contains(FileServerType.OPERATION.put));
        assertTrue(ops.contains(FileServerType.OPERATION.all));
    }

    @Test
    public void testCloneProducesEqualButDistinctInstance() {
        FileServer fs = new FileServer("gsiftp", "gsiftp://site.edu", "/data");
        fs.setSupportedOperation(FileServerType.OPERATION.put);
        FileServer cloned = (FileServer) fs.clone();
        assertNotSame(fs, cloned);
        assertEquals(fs.getProtocol(), cloned.getProtocol());
        assertEquals(fs.getURLPrefix(), cloned.getURLPrefix());
        assertEquals(fs.getMountPoint(), cloned.getMountPoint());
        assertEquals(fs.getSupportedOperation(), cloned.getSupportedOperation());
    }
}
