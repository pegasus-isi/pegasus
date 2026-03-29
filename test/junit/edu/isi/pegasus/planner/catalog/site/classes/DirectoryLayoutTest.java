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

import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for DirectoryLayout via LocalDirectory (concrete subclass).
 *
 * @author Rajiv Mayani
 */
public class DirectoryLayoutTest {

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
        LocalDirectory dir = new LocalDirectory();
        assertTrue(dir.isEmpty());
    }

    @Test
    public void testSetAndGetInternalMountPoint() {
        LocalDirectory dir = new LocalDirectory();
        InternalMountPoint mp = new InternalMountPoint("/scratch");
        dir.setInternalMountPoint(mp);
        assertEquals("/scratch", dir.getInternalMountPoint().getMountPoint());
    }

    @Test
    public void testAddFileServerMakesNotEmpty() {
        LocalDirectory dir = new LocalDirectory();
        FileServer fs = new FileServer("gsiftp", "gsiftp://site.edu", "/data");
        fs.setSupportedOperation(FileServer.OPERATION.all);
        dir.addFileServer(fs);
        assertFalse(dir.isEmpty());
    }

    @Test
    public void testGetFileServersReturnsAddedServer() {
        LocalDirectory dir = new LocalDirectory();
        FileServer fs = new FileServer("file", "file://", "/tmp");
        fs.setSupportedOperation(FileServer.OPERATION.get);
        dir.addFileServer(fs);
        List<FileServer> servers = dir.getFileServers(FileServer.OPERATION.get);
        assertNotNull(servers);
        assertEquals(1, servers.size());
        assertEquals("/tmp", servers.get(0).getMountPoint());
    }

    @Test
    public void testResetFileServersResultsInEmpty() {
        LocalDirectory dir = new LocalDirectory();
        FileServer fs = new FileServer("gsiftp", "gsiftp://site.edu", "/data");
        fs.setSupportedOperation(FileServer.OPERATION.all);
        dir.addFileServer(fs);
        dir.resetFileServers();
        assertTrue(dir.isEmpty());
    }

    @Test
    public void testHasFileServerForGETOperationsWithAllServer() {
        LocalDirectory dir = new LocalDirectory();
        FileServer fs = new FileServer("file", "file://", "/tmp");
        fs.setSupportedOperation(FileServer.OPERATION.all);
        dir.addFileServer(fs);
        assertTrue(dir.hasFileServerForGETOperations());
    }

    @Test
    public void testHasFileServerForPUTOperationsWithPutServer() {
        LocalDirectory dir = new LocalDirectory();
        FileServer fs = new FileServer("gsiftp", "gsiftp://site.edu", "/data");
        fs.setSupportedOperation(FileServer.OPERATION.put);
        dir.addFileServer(fs);
        assertTrue(dir.hasFileServerForPUTOperations());
    }

    @Test
    public void testCloneProducesEquivalentDirectory() {
        LocalDirectory dir = new LocalDirectory();
        InternalMountPoint mp = new InternalMountPoint("/work");
        dir.setInternalMountPoint(mp);
        FileServer fs = new FileServer("gsiftp", "gsiftp://site.edu", "/work");
        fs.setSupportedOperation(FileServer.OPERATION.all);
        dir.addFileServer(fs);

        LocalDirectory cloned = (LocalDirectory) dir.clone();
        assertNotSame(dir, cloned);
        assertEquals(
                dir.getInternalMountPoint().getMountPoint(),
                cloned.getInternalMountPoint().getMountPoint());
        assertFalse(cloned.isEmpty());
    }
}
