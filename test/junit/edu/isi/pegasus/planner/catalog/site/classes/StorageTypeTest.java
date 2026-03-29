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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for StorageType via HeadNodeScratch (concrete subclass).
 *
 * @author Rajiv Mayani
 */
public class StorageTypeTest {

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testDefaultConstructorCreatesNonNullLocalDirectory() {
        HeadNodeScratch scratch = new HeadNodeScratch();
        assertNotNull(scratch.getLocalDirectory());
    }

    @Test
    public void testDefaultConstructorCreatesNonNullSharedDirectory() {
        HeadNodeScratch scratch = new HeadNodeScratch();
        assertNotNull(scratch.getSharedDirectory());
    }

    @Test
    public void testSetAndGetLocalDirectory() {
        HeadNodeScratch scratch = new HeadNodeScratch();
        LocalDirectory local = new LocalDirectory();
        scratch.setLocalDirectory(local);
        assertSame(local, scratch.getLocalDirectory());
    }

    @Test
    public void testSetAndGetSharedDirectory() {
        HeadNodeScratch scratch = new HeadNodeScratch();
        SharedDirectory shared = new SharedDirectory();
        scratch.setSharedDirectory(shared);
        assertSame(shared, scratch.getSharedDirectory());
    }

    @Test
    public void testOverloadedConstructorSetsLocalAndShared() {
        LocalDirectory local = new LocalDirectory();
        SharedDirectory shared = new SharedDirectory();
        HeadNodeScratch scratch = new HeadNodeScratch(local, shared);
        assertSame(local, scratch.getLocalDirectory());
        assertSame(shared, scratch.getSharedDirectory());
    }

    @Test
    public void testCloneProducesDistinctDirectories() {
        HeadNodeScratch scratch = new HeadNodeScratch();
        HeadNodeScratch cloned = (HeadNodeScratch) scratch.clone();
        assertNotSame(scratch, cloned);
        assertNotSame(scratch.getLocalDirectory(), cloned.getLocalDirectory());
        assertNotSame(scratch.getSharedDirectory(), cloned.getSharedDirectory());
    }
}
