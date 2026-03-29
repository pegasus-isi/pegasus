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
package edu.isi.pegasus.planner.catalog.classes;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for the CatalogEntry marker interface and its implementations. */
public class CatalogEntryTest {

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testCatalogEntryIsInterface() {
        assertTrue(CatalogEntry.class.isInterface(), "CatalogEntry should be an interface");
    }

    @Test
    public void testReplicaCatalogEntryImplementsCatalogEntry() {
        ReplicaCatalogEntry rce = new ReplicaCatalogEntry("pfn://test/file");
        assertThat(rce, instanceOf(CatalogEntry.class));
    }

    @Test
    public void testCatalogEntryImplementationInstantiation() {
        // ReplicaCatalogEntry is a concrete implementation of CatalogEntry
        CatalogEntry entry = new ReplicaCatalogEntry("pfn://test/file", "local");
        assertNotNull(entry, "CatalogEntry implementation should be instantiable");
    }

    @Test
    public void testCatalogEntryIsAssignableFromReplicaCatalogEntry() {
        assertTrue(
                CatalogEntry.class.isAssignableFrom(ReplicaCatalogEntry.class),
                "ReplicaCatalogEntry should be assignable to CatalogEntry");
    }

    @Test
    public void testCatalogEntryHasNoMethods() {
        // CatalogEntry is a pure marker interface with no declared methods
        assertEquals(
                0,
                CatalogEntry.class.getDeclaredMethods().length,
                "CatalogEntry marker interface should have no methods");
    }

    @Test
    public void testCatalogEntryPackage() {
        assertEquals(
                "edu.isi.pegasus.planner.catalog.classes",
                CatalogEntry.class.getPackage().getName(),
                "CatalogEntry should be in catalog.classes package");
    }
}
