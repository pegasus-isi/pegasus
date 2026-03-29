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
public class WorkerNodeFSTest {

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testDefaultConstructorCreatesNonNullScratchAndStorage() {
        WorkerNodeFS wfs = new WorkerNodeFS();
        assertNotNull(wfs.getScratch());
        assertNotNull(wfs.getStorage());
    }

    @Test
    public void testOverloadedConstructorSetsScratchAndStorage() {
        WorkerNodeScratch scratch = new WorkerNodeScratch();
        WorkerNodeStorage storage = new WorkerNodeStorage();
        WorkerNodeFS wfs = new WorkerNodeFS(scratch, storage);
        assertSame(scratch, wfs.getScratch());
        assertSame(storage, wfs.getStorage());
    }

    @Test
    public void testSetAndGetScratch() {
        WorkerNodeFS wfs = new WorkerNodeFS();
        WorkerNodeScratch scratch = new WorkerNodeScratch();
        wfs.setScratch(scratch);
        assertSame(scratch, wfs.getScratch());
    }

    @Test
    public void testSetAndGetStorage() {
        WorkerNodeFS wfs = new WorkerNodeFS();
        WorkerNodeStorage storage = new WorkerNodeStorage();
        wfs.setStorage(storage);
        assertSame(storage, wfs.getStorage());
    }

    @Test
    public void testDefaultConstructorCreatesNonNullProfiles() {
        WorkerNodeFS wfs = new WorkerNodeFS();
        assertNotNull(wfs.getProfiles());
    }

    @Test
    public void testToXMLContainsWorkerFsElement() throws IOException {
        WorkerNodeFS wfs = new WorkerNodeFS();
        StringWriter sw = new StringWriter();
        wfs.toXML(sw, "");
        String xml = sw.toString();
        assertThat(xml, containsString("<worker-fs>"));
        assertThat(xml, containsString("</worker-fs>"));
    }

    @Test
    public void testCloneProducesDistinctInstance() {
        WorkerNodeFS wfs = new WorkerNodeFS();
        WorkerNodeFS cloned = (WorkerNodeFS) wfs.clone();
        assertNotSame(wfs, cloned);
        assertNotSame(wfs.getScratch(), cloned.getScratch());
        assertNotSame(wfs.getStorage(), cloned.getStorage());
    }
}
