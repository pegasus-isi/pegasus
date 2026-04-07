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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Tests for DirectoryLayout via LocalDirectory (concrete subclass).
 *
 * @author Rajiv Mayani
 */
public class DirectoryLayoutTest {

    @Test
    public void testDefaultConstructorIsEmpty() {
        LocalDirectory dir = new LocalDirectory();
        assertThat(dir.isEmpty(), is(true));
    }

    @Test
    public void testSetAndGetInternalMountPoint() {
        LocalDirectory dir = new LocalDirectory();
        InternalMountPoint mp = new InternalMountPoint("/scratch");
        dir.setInternalMountPoint(mp);
        assertThat(dir.getInternalMountPoint().getMountPoint(), is("/scratch"));
    }

    @Test
    public void testAddFileServerMakesNotEmpty() {
        LocalDirectory dir = new LocalDirectory();
        FileServer fs = new FileServer("gsiftp", "gsiftp://site.edu", "/data");
        fs.setSupportedOperation(FileServer.OPERATION.all);
        dir.addFileServer(fs);
        assertThat(dir.isEmpty(), is(false));
    }

    @Test
    public void testGetFileServersReturnsAddedServer() {
        LocalDirectory dir = new LocalDirectory();
        FileServer fs = new FileServer("file", "file://", "/tmp");
        fs.setSupportedOperation(FileServer.OPERATION.get);
        dir.addFileServer(fs);
        List<FileServer> servers = dir.getFileServers(FileServer.OPERATION.get);
        assertThat(servers, is(notNullValue()));
        assertThat(servers.size(), is(1));
        assertThat(servers.get(0).getMountPoint(), is("/tmp"));
    }

    @Test
    public void testResetFileServersResultsInEmpty() {
        LocalDirectory dir = new LocalDirectory();
        FileServer fs = new FileServer("gsiftp", "gsiftp://site.edu", "/data");
        fs.setSupportedOperation(FileServer.OPERATION.all);
        dir.addFileServer(fs);
        dir.resetFileServers();
        assertThat(dir.isEmpty(), is(true));
    }

    @Test
    public void testHasFileServerForGETOperationsWithAllServer() {
        LocalDirectory dir = new LocalDirectory();
        FileServer fs = new FileServer("file", "file://", "/tmp");
        fs.setSupportedOperation(FileServer.OPERATION.all);
        dir.addFileServer(fs);
        assertThat(dir.hasFileServerForGETOperations(), is(true));
    }

    @Test
    public void testHasFileServerForPUTOperationsWithPutServer() {
        LocalDirectory dir = new LocalDirectory();
        FileServer fs = new FileServer("gsiftp", "gsiftp://site.edu", "/data");
        fs.setSupportedOperation(FileServer.OPERATION.put);
        dir.addFileServer(fs);
        assertThat(dir.hasFileServerForPUTOperations(), is(true));
    }

    @Test
    public void testHasFileServerForGETOperationsReturnsFalseWhenNoServersExist() {
        LocalDirectory dir = new LocalDirectory();

        assertThat(dir.hasFileServerForGETOperations(), is(false));
    }

    @Test
    public void testSetFileServersReplacesExistingServers() {
        LocalDirectory dir = new LocalDirectory();
        FileServer original = new FileServer("file", "file://", "/tmp");
        original.setSupportedOperation(FileServer.OPERATION.get);
        dir.addFileServer(original);

        FileServer replacement = new FileServer("gsiftp", "gsiftp://site.edu", "/data");
        replacement.setSupportedOperation(FileServer.OPERATION.put);

        List<FileServer> servers = new ArrayList<FileServer>();
        servers.add(replacement);
        dir.setFileServers(servers);

        assertThat(dir.getFileServers(FileServer.OPERATION.get).isEmpty(), is(true));
        assertThat(dir.getFileServers(FileServer.OPERATION.put).size(), is(1));
        assertThat(dir.getFileServers(FileServer.OPERATION.put).get(0), is(replacement));
    }

    @Test
    public void testSelectFileServerFallsBackToAllOperationServer() {
        LocalDirectory dir = new LocalDirectory();
        FileServer fs = new FileServer("file", "file://", "/shared");
        fs.setSupportedOperation(FileServer.OPERATION.all);
        dir.addFileServer(fs);

        FileServer selected = dir.selectFileServer(FileServer.OPERATION.get);

        assertThat(selected, is(fs));
    }

    @Test
    public void testSelectFileServerReturnsNullWhenNoServersExist() {
        LocalDirectory dir = new LocalDirectory();

        assertThat(dir.selectFileServer(FileServer.OPERATION.get), is(nullValue()));
    }

    @Test
    public void testGetFileServersIteratorReturnsAddedServersInOrder() {
        LocalDirectory dir = new LocalDirectory();
        FileServer first = new FileServer("file", "file://", "/tmp/a");
        first.setSupportedOperation(FileServer.OPERATION.get);
        FileServer second = new FileServer("file", "file://", "/tmp/b");
        second.setSupportedOperation(FileServer.OPERATION.get);
        dir.addFileServer(first);
        dir.addFileServer(second);

        Iterator<FileServer> iterator = dir.getFileServersIterator(FileServer.OPERATION.get);

        assertThat(iterator.hasNext(), is(true));
        assertThat(iterator.next(), is(first));
        assertThat(iterator.hasNext(), is(true));
        assertThat(iterator.next(), is(second));
        assertThat(iterator.hasNext(), is(false));
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
        assertThat(cloned, is(notNullValue()));
        assertThat(cloned == dir, is(false));
        assertThat(
                cloned.getInternalMountPoint().getMountPoint(),
                is(dir.getInternalMountPoint().getMountPoint()));
        assertThat(cloned.isEmpty(), is(false));
    }

    @Test
    public void testCloneDeepCopiesInternalMountPointAndFileServers() {
        LocalDirectory dir = new LocalDirectory();
        InternalMountPoint mp = new InternalMountPoint("/work");
        dir.setInternalMountPoint(mp);
        FileServer fs = new FileServer("gsiftp", "gsiftp://site.edu", "/work");
        fs.setSupportedOperation(FileServer.OPERATION.all);
        dir.addFileServer(fs);

        LocalDirectory cloned = (LocalDirectory) dir.clone();

        assertThat(cloned.getInternalMountPoint() == dir.getInternalMountPoint(), is(false));
        assertThat(
                cloned.getFileServers(FileServer.OPERATION.all).get(0)
                        == dir.getFileServers(FileServer.OPERATION.all).get(0),
                is(false));

        dir.getInternalMountPoint().setMountPoint("/changed");
        dir.getFileServers(FileServer.OPERATION.all).get(0).setMountPoint("/changed");

        assertThat(cloned.getInternalMountPoint().getMountPoint(), is("/work"));
        assertThat(
                cloned.getFileServers(FileServer.OPERATION.all).get(0).getMountPoint(),
                is("/work"));
    }
}
