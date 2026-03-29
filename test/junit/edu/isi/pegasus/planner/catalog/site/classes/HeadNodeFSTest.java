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
public class HeadNodeFSTest {

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
        HeadNodeFS hfs = new HeadNodeFS();
        assertNotNull(hfs.getScratch());
        assertNotNull(hfs.getStorage());
    }

    @Test
    public void testOverloadedConstructorSetsScratchAndStorage() {
        HeadNodeScratch scratch = new HeadNodeScratch();
        HeadNodeStorage storage = new HeadNodeStorage();
        HeadNodeFS hfs = new HeadNodeFS(scratch, storage);
        assertSame(scratch, hfs.getScratch());
        assertSame(storage, hfs.getStorage());
    }

    @Test
    public void testSetAndGetScratch() {
        HeadNodeFS hfs = new HeadNodeFS();
        HeadNodeScratch scratch = new HeadNodeScratch();
        hfs.setScratch(scratch);
        assertSame(scratch, hfs.getScratch());
    }

    @Test
    public void testSetAndGetStorage() {
        HeadNodeFS hfs = new HeadNodeFS();
        HeadNodeStorage storage = new HeadNodeStorage();
        hfs.setStorage(storage);
        assertSame(storage, hfs.getStorage());
    }

    @Test
    public void testDefaultConstructorCreatesNonNullProfiles() {
        HeadNodeFS hfs = new HeadNodeFS();
        assertNotNull(hfs.getProfiles());
    }

    @Test
    public void testToXMLContainsHeadFsElement() throws IOException {
        HeadNodeFS hfs = new HeadNodeFS();
        StringWriter sw = new StringWriter();
        hfs.toXML(sw, "");
        String xml = sw.toString();
        assertThat(xml, containsString("<head-fs>"));
        assertThat(xml, containsString("</head-fs>"));
    }

    @Test
    public void testCloneProducesDistinctInstance() {
        HeadNodeFS hfs = new HeadNodeFS();
        HeadNodeFS cloned = (HeadNodeFS) hfs.clone();
        assertNotSame(hfs, cloned);
        assertNotSame(hfs.getScratch(), cloned.getScratch());
        assertNotSame(hfs.getStorage(), cloned.getStorage());
    }
}
