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
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Tests for the CatalogEntry marker interface and its implementations. */
public class CatalogEntryTest {

    // -----------------------------------------------------------------------
    // ReplicaCatalogEntry — first known implementor
    // -----------------------------------------------------------------------

    @Test
    public void testReplicaCatalogEntryImplementsCatalogEntry() {
        assertThat(new ReplicaCatalogEntry("pfn://test/file"), instanceOf(CatalogEntry.class));
    }

    @Test
    public void testCatalogEntryIsAssignableFromReplicaCatalogEntry() {
        assertThat(CatalogEntry.class.isAssignableFrom(ReplicaCatalogEntry.class), is(true));
    }

    @Test
    public void testCatalogEntryReferenceHoldsReplicaCatalogEntry() {
        CatalogEntry entry = new ReplicaCatalogEntry("pfn://test/file", "local");
        assertThat(entry, is(notNullValue()));
    }

    // -----------------------------------------------------------------------
    // TransformationCatalogEntry — second known implementor
    // -----------------------------------------------------------------------

    @Test
    public void testTransformationCatalogEntryImplementsCatalogEntry() {
        assertThat(new TransformationCatalogEntry(), instanceOf(CatalogEntry.class));
    }

    @Test
    public void testCatalogEntryIsAssignableFromTransformationCatalogEntry() {
        assertThat(CatalogEntry.class.isAssignableFrom(TransformationCatalogEntry.class), is(true));
    }

    @Test
    public void testCatalogEntryReferenceHoldsTransformationCatalogEntry() {
        CatalogEntry entry = new TransformationCatalogEntry("ns", "name", "1.0");
        assertThat(entry, is(notNullValue()));
    }

    // -----------------------------------------------------------------------
    // Anonymous implementation
    // -----------------------------------------------------------------------

    @Test
    public void testAnonymousImplementationSatisfiesInterface() {
        CatalogEntry anon = new CatalogEntry() {};
        assertThat(anon, instanceOf(CatalogEntry.class));
    }

    @Test
    public void testAnonymousImplementationIsNotNull() {
        assertThat(new CatalogEntry() {}, is(notNullValue()));
    }

    // -----------------------------------------------------------------------
    // Polymorphic use
    // -----------------------------------------------------------------------

    @Test
    public void testListOfCatalogEntryHoldsBothImplementors() {
        List<CatalogEntry> entries = new ArrayList<>();
        entries.add(new ReplicaCatalogEntry("pfn://a", "site1"));
        entries.add(new TransformationCatalogEntry("ns", "job", "1.0"));
        entries.add(new CatalogEntry() {});

        assertThat(entries.size(), is(3));
        assertThat(entries.get(0), instanceOf(ReplicaCatalogEntry.class));
        assertThat(entries.get(1), instanceOf(TransformationCatalogEntry.class));
    }

    @Test
    public void testTwoImplementorsBothSatisfyIsAssignableFrom() {
        assertThat(CatalogEntry.class.isAssignableFrom(ReplicaCatalogEntry.class), is(true));
        assertThat(CatalogEntry.class.isAssignableFrom(TransformationCatalogEntry.class), is(true));
    }

    @Test
    public void testCatalogEntryIsInterface() {
        assertThat(CatalogEntry.class.isInterface(), is(true));
    }

    @Test
    public void testCatalogEntryDeclaresNoMethods() {
        assertThat(CatalogEntry.class.getDeclaredMethods().length, is(0));
    }

    @Test
    public void testCatalogEntryDeclaresNoFields() {
        assertThat(CatalogEntry.class.getDeclaredFields().length, is(0));
    }
}
