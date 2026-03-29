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
public class HeadNodeScratchTest {

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testDefaultConstructorCreatesNonNullLocalAndSharedDirectories() {
        HeadNodeScratch scratch = new HeadNodeScratch();
        assertNotNull(scratch.getLocalDirectory());
        assertNotNull(scratch.getSharedDirectory());
    }

    @Test
    public void testOverloadedConstructorWithLocalAndShared() {
        LocalDirectory local = new LocalDirectory();
        SharedDirectory shared = new SharedDirectory();
        HeadNodeScratch scratch = new HeadNodeScratch(local, shared);
        assertSame(local, scratch.getLocalDirectory());
        assertSame(shared, scratch.getSharedDirectory());
    }

    @Test
    public void testSetLocalDirectory() {
        HeadNodeScratch scratch = new HeadNodeScratch();
        LocalDirectory local = new LocalDirectory();
        InternalMountPoint mp = new InternalMountPoint("/scratch/local");
        local.setInternalMountPoint(mp);
        scratch.setLocalDirectory(local);
        assertEquals(
                "/scratch/local",
                scratch.getLocalDirectory().getInternalMountPoint().getMountPoint());
    }

    @Test
    public void testSetSharedDirectory() {
        HeadNodeScratch scratch = new HeadNodeScratch();
        SharedDirectory shared = new SharedDirectory();
        InternalMountPoint mp = new InternalMountPoint("/scratch/shared");
        shared.setInternalMountPoint(mp);
        scratch.setSharedDirectory(shared);
        assertEquals(
                "/scratch/shared",
                scratch.getSharedDirectory().getInternalMountPoint().getMountPoint());
    }

    @Test
    public void testToXMLContainsScratchElement() throws IOException {
        HeadNodeScratch scratch = new HeadNodeScratch();
        StringWriter sw = new StringWriter();
        scratch.toXML(sw, "");
        String xml = sw.toString();
        assertThat(xml, containsString("<scratch>"));
        assertThat(xml, containsString("</scratch>"));
    }

    @Test
    public void testCloneProducesDistinctInstance() {
        HeadNodeScratch scratch = new HeadNodeScratch();
        HeadNodeScratch cloned = (HeadNodeScratch) scratch.clone();
        assertNotSame(scratch, cloned);
        assertNotSame(scratch.getLocalDirectory(), cloned.getLocalDirectory());
        assertNotSame(scratch.getSharedDirectory(), cloned.getSharedDirectory());
    }
}
