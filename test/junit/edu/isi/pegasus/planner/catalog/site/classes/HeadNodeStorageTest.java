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
public class HeadNodeStorageTest {

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testDefaultConstructorCreatesNonNullDirectories() {
        HeadNodeStorage storage = new HeadNodeStorage();
        assertNotNull(storage.getLocalDirectory());
        assertNotNull(storage.getSharedDirectory());
    }

    @Test
    public void testOverloadedConstructorWithLocalAndShared() {
        LocalDirectory local = new LocalDirectory();
        SharedDirectory shared = new SharedDirectory();
        HeadNodeStorage storage = new HeadNodeStorage(local, shared);
        assertSame(local, storage.getLocalDirectory());
        assertSame(shared, storage.getSharedDirectory());
    }

    @Test
    public void testSetLocalDirectory() {
        HeadNodeStorage storage = new HeadNodeStorage();
        LocalDirectory local = new LocalDirectory();
        InternalMountPoint mp = new InternalMountPoint("/storage/local");
        local.setInternalMountPoint(mp);
        storage.setLocalDirectory(local);
        assertEquals(
                "/storage/local",
                storage.getLocalDirectory().getInternalMountPoint().getMountPoint());
    }

    @Test
    public void testSetSharedDirectory() {
        HeadNodeStorage storage = new HeadNodeStorage();
        SharedDirectory shared = new SharedDirectory();
        InternalMountPoint mp = new InternalMountPoint("/storage/shared");
        shared.setInternalMountPoint(mp);
        storage.setSharedDirectory(shared);
        assertEquals(
                "/storage/shared",
                storage.getSharedDirectory().getInternalMountPoint().getMountPoint());
    }

    @Test
    public void testToXMLContainsStorageElement() throws IOException {
        HeadNodeStorage storage = new HeadNodeStorage();
        StringWriter sw = new StringWriter();
        storage.toXML(sw, "");
        String xml = sw.toString();
        assertThat(xml, containsString("<storage>"));
        assertThat(xml, containsString("</storage>"));
    }

    @Test
    public void testCloneProducesDistinctInstance() {
        HeadNodeStorage storage = new HeadNodeStorage();
        HeadNodeStorage cloned = (HeadNodeStorage) storage.clone();
        assertNotSame(storage, cloned);
        assertNotSame(storage.getLocalDirectory(), cloned.getLocalDirectory());
        assertNotSame(storage.getSharedDirectory(), cloned.getSharedDirectory());
    }
}
