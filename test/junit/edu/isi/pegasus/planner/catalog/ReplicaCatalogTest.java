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
package edu.isi.pegasus.planner.catalog;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for the ReplicaCatalog interface constants and structure. */
public class ReplicaCatalogTest {

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testReplicaCatalogIsInterface() {
        assertTrue(ReplicaCatalog.class.isInterface(), "ReplicaCatalog should be an interface");
    }

    @Test
    public void testReplicaCatalogExtendsCatalog() {
        assertTrue(
                Catalog.class.isAssignableFrom(ReplicaCatalog.class),
                "ReplicaCatalog should extend Catalog");
    }

    @Test
    public void testCPrefixConstant() {
        assertEquals("pegasus.catalog.replica", ReplicaCatalog.c_prefix);
    }

    @Test
    public void testDbPrefixConstant() {
        assertEquals("pegasus.catalog.replica.db", ReplicaCatalog.DB_PREFIX);
    }

    @Test
    public void testProxyKeyConstant() {
        assertEquals("proxy", ReplicaCatalog.PROXY_KEY);
    }

    @Test
    public void testFileKeyConstant() {
        assertEquals("file", ReplicaCatalog.FILE_KEY);
    }

    @Test
    public void testBatchKeyConstant() {
        assertEquals("chunk.size", ReplicaCatalog.BATCH_KEY);
    }

    @Test
    public void testVariableExpansionKeyConstant() {
        assertEquals("expand", ReplicaCatalog.VARIABLE_EXPANSION_KEY);
    }

    @Test
    public void testReadOnlyKeyConstant() {
        assertEquals("read.only", ReplicaCatalog.READ_ONLY_KEY);
    }

    @Test
    public void testPrefixKeyConstant() {
        assertEquals("prefix", ReplicaCatalog.PREFIX_KEY);
    }
}
