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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry;
import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for the ReplicaCatalog interface constants and structure. */
public class ReplicaCatalogTest {

    // -----------------------------------------------------------------------
    // Minimal in-memory stub for behavioral contract tests
    // -----------------------------------------------------------------------
    private static class StubReplicaCatalog implements ReplicaCatalog {

        private boolean mClosed = true;
        private boolean mReadOnly = false;
        private final Map<String, Collection<ReplicaCatalogEntry>> mStore = new HashMap<>();

        @Override
        public boolean connect(Properties props) {
            mClosed = false;
            return true;
        }

        @Override
        public boolean isClosed() {
            return mClosed;
        }

        @Override
        public void close() {
            mClosed = true;
        }

        @Override
        public String lookup(String lfn, String handle) {
            Collection<ReplicaCatalogEntry> entries = mStore.get(lfn);
            if (entries == null) return null;
            for (ReplicaCatalogEntry e : entries) {
                if (handle.equals(e.getResourceHandle())) return e.getPFN();
            }
            return null;
        }

        @Override
        @SuppressWarnings("rawtypes")
        public Collection lookup(String lfn) {
            return mStore.getOrDefault(lfn, Collections.emptyList());
        }

        @Override
        @SuppressWarnings({"rawtypes", "unchecked"})
        public Set lookupNoAttributes(String lfn) {
            Set<String> pfns = new HashSet<>();
            for (ReplicaCatalogEntry e : (Collection<ReplicaCatalogEntry>) lookup(lfn)) {
                pfns.add(e.getPFN());
            }
            return pfns;
        }

        @Override
        @SuppressWarnings({"rawtypes", "unchecked"})
        public Map lookup(Set lfns) {
            Map<String, Collection<ReplicaCatalogEntry>> result = new HashMap<>();
            for (Object lfn : lfns) {
                result.put((String) lfn, (Collection<ReplicaCatalogEntry>) lookup((String) lfn));
            }
            return result;
        }

        @Override
        @SuppressWarnings({"rawtypes", "unchecked"})
        public Map lookupNoAttributes(Set lfns) {
            Map<String, Set<String>> result = new HashMap<>();
            for (Object lfn : lfns) {
                result.put((String) lfn, lookupNoAttributes((String) lfn));
            }
            return result;
        }

        @Override
        @SuppressWarnings({"rawtypes", "unchecked"})
        public Map lookup(Set lfns, String handle) {
            Map<String, Collection<ReplicaCatalogEntry>> result = new HashMap<>();
            for (Object lfn : lfns) {
                List<ReplicaCatalogEntry> filtered = new ArrayList<>();
                for (ReplicaCatalogEntry e :
                        (Collection<ReplicaCatalogEntry>) lookup((String) lfn)) {
                    if (handle.equals(e.getResourceHandle())) filtered.add(e);
                }
                result.put((String) lfn, filtered);
            }
            return result;
        }

        @Override
        @SuppressWarnings({"rawtypes", "unchecked"})
        public Map lookupNoAttributes(Set lfns, String handle) {
            Map<String, Set<String>> result = new HashMap<>();
            for (Object lfn : lfns) {
                Set<String> pfns = new HashSet<>();
                for (ReplicaCatalogEntry e :
                        (Collection<ReplicaCatalogEntry>) lookup((String) lfn)) {
                    if (handle.equals(e.getResourceHandle())) pfns.add(e.getPFN());
                }
                result.put((String) lfn, pfns);
            }
            return result;
        }

        @Override
        @SuppressWarnings("rawtypes")
        public Map lookup(Map constraints) {
            return new HashMap<>(mStore);
        }

        @Override
        @SuppressWarnings("rawtypes")
        public Set list() {
            return new HashSet<>(mStore.keySet());
        }

        @Override
        @SuppressWarnings("rawtypes")
        public Set list(String constraint) {
            Set<String> result = new HashSet<>();
            for (String lfn : mStore.keySet()) {
                if (lfn.contains(constraint)) result.add(lfn);
            }
            return result;
        }

        @Override
        public int insert(String lfn, ReplicaCatalogEntry tuple) {
            mStore.computeIfAbsent(lfn, k -> new ArrayList<>()).add(tuple);
            return 1;
        }

        @Override
        public int insert(String lfn, String pfn, String handle) {
            return insert(lfn, new ReplicaCatalogEntry(pfn, handle));
        }

        @Override
        @SuppressWarnings({"rawtypes", "unchecked"})
        public int insert(Map x) {
            int count = 0;
            for (Map.Entry<String, Collection<ReplicaCatalogEntry>> entry :
                    ((Map<String, Collection<ReplicaCatalogEntry>>) x).entrySet()) {
                for (ReplicaCatalogEntry rce : entry.getValue()) {
                    count += insert(entry.getKey(), rce);
                }
            }
            return count;
        }

        @Override
        public int delete(Map<String, Collection<ReplicaCatalogEntry>> x, boolean matchAttributes) {
            int count = 0;
            for (Map.Entry<String, Collection<ReplicaCatalogEntry>> entry : x.entrySet()) {
                String lfn = entry.getKey();
                Collection<ReplicaCatalogEntry> toRemove = entry.getValue();
                Collection<ReplicaCatalogEntry> existing = mStore.get(lfn);
                if (existing != null) {
                    int before = existing.size();
                    existing.removeIf(
                            e -> toRemove.stream().anyMatch(r -> r.getPFN().equals(e.getPFN())));
                    count += before - existing.size();
                    if (existing.isEmpty()) mStore.remove(lfn);
                }
            }
            return count;
        }

        @Override
        public int delete(String lfn, String pfn) {
            Collection<ReplicaCatalogEntry> existing = mStore.get(lfn);
            if (existing == null) return 0;
            int before = existing.size();
            existing.removeIf(e -> pfn.equals(e.getPFN()));
            int removed = before - existing.size();
            if (existing.isEmpty()) mStore.remove(lfn);
            return removed;
        }

        @Override
        public int delete(String lfn, ReplicaCatalogEntry tuple) {
            return delete(lfn, tuple.getPFN());
        }

        @Override
        public int delete(String lfn, String name, Object value) {
            Collection<ReplicaCatalogEntry> existing = mStore.get(lfn);
            if (existing == null) return 0;
            int before = existing.size();
            existing.removeIf(e -> value.equals(e.getAttribute(name)));
            int removed = before - existing.size();
            if (existing.isEmpty()) mStore.remove(lfn);
            return removed;
        }

        @Override
        public int deleteByResource(String lfn, String handle) {
            return delete(lfn, ReplicaCatalogEntry.RESOURCE_HANDLE, handle);
        }

        @Override
        @SuppressWarnings("rawtypes")
        public int remove(String lfn) {
            Collection<ReplicaCatalogEntry> removed = mStore.remove(lfn);
            return removed == null ? 0 : removed.size();
        }

        @Override
        @SuppressWarnings("rawtypes")
        public int remove(Set lfns) {
            int count = 0;
            for (Object lfn : lfns) count += remove((String) lfn);
            return count;
        }

        @Override
        public int removeByAttribute(String name, Object value) {
            int count = 0;
            for (Iterator<Map.Entry<String, Collection<ReplicaCatalogEntry>>> it =
                            mStore.entrySet().iterator();
                    it.hasNext(); ) {
                Map.Entry<String, Collection<ReplicaCatalogEntry>> entry = it.next();
                int before = entry.getValue().size();
                entry.getValue().removeIf(e -> value.equals(e.getAttribute(name)));
                count += before - entry.getValue().size();
                if (entry.getValue().isEmpty()) it.remove();
            }
            return count;
        }

        @Override
        public int removeByAttribute(String handle) {
            return removeByAttribute(ReplicaCatalogEntry.RESOURCE_HANDLE, handle);
        }

        @Override
        public int clear() {
            int count = mStore.values().stream().mapToInt(Collection::size).sum();
            mStore.clear();
            return count;
        }

        @Override
        public File getFileSource() {
            return null;
        }

        @Override
        public void setReadOnly(boolean readonly) {
            mReadOnly = readonly;
        }

        boolean isReadOnly() {
            return mReadOnly;
        }
    }

    // -----------------------------------------------------------------------
    // Test fixture
    // -----------------------------------------------------------------------
    private StubReplicaCatalog catalog;

    @BeforeEach
    public void setUp() {
        catalog = new StubReplicaCatalog();
        catalog.connect(new Properties());
    }

    // -----------------------------------------------------------------------
    // Constant values
    // -----------------------------------------------------------------------

    @Test
    public void testCPrefixConstant() {
        assertThat(ReplicaCatalog.c_prefix, is("pegasus.catalog.replica"));
    }

    @Test
    public void testDbPrefixConstant() {
        assertThat(ReplicaCatalog.DB_PREFIX, is("pegasus.catalog.replica.db"));
    }

    @Test
    public void testProxyKeyConstant() {
        assertThat(ReplicaCatalog.PROXY_KEY, is("proxy"));
    }

    @Test
    public void testFileKeyConstant() {
        assertThat(ReplicaCatalog.FILE_KEY, is("file"));
    }

    @Test
    public void testBatchKeyConstant() {
        assertThat(ReplicaCatalog.BATCH_KEY, is("chunk.size"));
    }

    @Test
    public void testVariableExpansionKeyConstant() {
        assertThat(ReplicaCatalog.VARIABLE_EXPANSION_KEY, is("expand"));
    }

    @Test
    public void testReadOnlyKeyConstant() {
        assertThat(ReplicaCatalog.READ_ONLY_KEY, is("read.only"));
    }

    @Test
    public void testPrefixKeyConstant() {
        assertThat(ReplicaCatalog.PREFIX_KEY, is("prefix"));
    }

    // -----------------------------------------------------------------------
    // Constant modifiers — interface fields are implicitly public static final,
    // but we verify the compiler enforced it
    // -----------------------------------------------------------------------

    @Test
    public void testAllConstantsArePublicStaticFinal() throws NoSuchFieldException {
        String[] names = {
            "c_prefix",
            "DB_PREFIX",
            "PROXY_KEY",
            "FILE_KEY",
            "BATCH_KEY",
            "VARIABLE_EXPANSION_KEY",
            "READ_ONLY_KEY",
            "PREFIX_KEY"
        };
        for (String name : names) {
            Field f = ReplicaCatalog.class.getDeclaredField(name);
            int mods = f.getModifiers();
            assertThat(Modifier.isPublic(mods), is(true));
            assertThat(Modifier.isStatic(mods), is(true));
            assertThat(Modifier.isFinal(mods), is(true));
        }
    }

    // -----------------------------------------------------------------------
    // Method signatures
    // -----------------------------------------------------------------------

    @Test
    public void testLookupLfnHandleReturnType() throws NoSuchMethodException {
        Method m = ReplicaCatalog.class.getDeclaredMethod("lookup", String.class, String.class);
        assertEquals(String.class, m.getReturnType());
    }

    @Test
    public void testLookupLfnReturnType() throws NoSuchMethodException {
        Method m = ReplicaCatalog.class.getDeclaredMethod("lookup", String.class);
        assertEquals(Collection.class, m.getReturnType());
    }

    @Test
    public void testLookupNoAttributesLfnReturnType() throws NoSuchMethodException {
        Method m = ReplicaCatalog.class.getDeclaredMethod("lookupNoAttributes", String.class);
        assertEquals(Set.class, m.getReturnType());
    }

    @Test
    public void testListReturnType() throws NoSuchMethodException {
        Method m = ReplicaCatalog.class.getDeclaredMethod("list");
        assertEquals(Set.class, m.getReturnType());
    }

    @Test
    public void testInsertEntryReturnType() throws NoSuchMethodException {
        Method m =
                ReplicaCatalog.class.getDeclaredMethod(
                        "insert", String.class, ReplicaCatalogEntry.class);
        assertEquals(int.class, m.getReturnType());
    }

    @Test
    public void testInsertPfnHandleReturnType() throws NoSuchMethodException {
        Method m =
                ReplicaCatalog.class.getDeclaredMethod(
                        "insert", String.class, String.class, String.class);
        assertEquals(int.class, m.getReturnType());
    }

    @Test
    public void testDeleteLfnPfnReturnType() throws NoSuchMethodException {
        Method m = ReplicaCatalog.class.getDeclaredMethod("delete", String.class, String.class);
        assertEquals(int.class, m.getReturnType());
    }

    @Test
    public void testRemoveLfnReturnType() throws NoSuchMethodException {
        Method m = ReplicaCatalog.class.getDeclaredMethod("remove", String.class);
        assertEquals(int.class, m.getReturnType());
    }

    @Test
    public void testClearReturnType() throws NoSuchMethodException {
        Method m = ReplicaCatalog.class.getDeclaredMethod("clear");
        assertEquals(int.class, m.getReturnType());
    }

    @Test
    public void testGetFileSourceReturnType() throws NoSuchMethodException {
        Method m = ReplicaCatalog.class.getDeclaredMethod("getFileSource");
        assertEquals(File.class, m.getReturnType());
    }

    @Test
    public void testSetReadOnlyReturnType() throws NoSuchMethodException {
        Method m = ReplicaCatalog.class.getDeclaredMethod("setReadOnly", boolean.class);
        assertEquals(void.class, m.getReturnType());
    }

    @Test
    @SuppressWarnings("rawtypes")
    public void testLookupSetReturnType() throws NoSuchMethodException {
        Method m = ReplicaCatalog.class.getDeclaredMethod("lookup", Set.class);
        assertEquals(Map.class, m.getReturnType());
    }

    @Test
    @SuppressWarnings("rawtypes")
    public void testLookupSetWithHandleReturnType() throws NoSuchMethodException {
        Method m = ReplicaCatalog.class.getDeclaredMethod("lookup", Set.class, String.class);
        assertEquals(Map.class, m.getReturnType());
    }

    @Test
    @SuppressWarnings("rawtypes")
    public void testLookupNoAttributesSetReturnType() throws NoSuchMethodException {
        Method m = ReplicaCatalog.class.getDeclaredMethod("lookupNoAttributes", Set.class);
        assertEquals(Map.class, m.getReturnType());
    }

    @Test
    @SuppressWarnings("rawtypes")
    public void testLookupNoAttributesSetWithHandleReturnType() throws NoSuchMethodException {
        Method m =
                ReplicaCatalog.class.getDeclaredMethod(
                        "lookupNoAttributes", Set.class, String.class);
        assertEquals(Map.class, m.getReturnType());
    }

    @Test
    @SuppressWarnings("rawtypes")
    public void testLookupConstraintsReturnType() throws NoSuchMethodException {
        Method m = ReplicaCatalog.class.getDeclaredMethod("lookup", Map.class);
        assertEquals(Map.class, m.getReturnType());
    }

    @Test
    @SuppressWarnings("rawtypes")
    public void testListConstraintReturnType() throws NoSuchMethodException {
        Method m = ReplicaCatalog.class.getDeclaredMethod("list", String.class);
        assertEquals(Set.class, m.getReturnType());
    }

    @Test
    @SuppressWarnings("rawtypes")
    public void testInsertMapReturnType() throws NoSuchMethodException {
        Method m = ReplicaCatalog.class.getDeclaredMethod("insert", Map.class);
        assertEquals(int.class, m.getReturnType());
    }

    @Test
    public void testDeleteMapReturnType() throws NoSuchMethodException {
        Method m = ReplicaCatalog.class.getDeclaredMethod("delete", Map.class, boolean.class);
        assertEquals(int.class, m.getReturnType());
    }

    @Test
    public void testDeleteLfnEntryReturnType() throws NoSuchMethodException {
        Method m =
                ReplicaCatalog.class.getDeclaredMethod(
                        "delete", String.class, ReplicaCatalogEntry.class);
        assertEquals(int.class, m.getReturnType());
    }

    @Test
    public void testDeleteLfnNameValueReturnType() throws NoSuchMethodException {
        Method m =
                ReplicaCatalog.class.getDeclaredMethod(
                        "delete", String.class, String.class, Object.class);
        assertEquals(int.class, m.getReturnType());
    }

    @Test
    public void testDeleteByResourceReturnType() throws NoSuchMethodException {
        Method m =
                ReplicaCatalog.class.getDeclaredMethod(
                        "deleteByResource", String.class, String.class);
        assertEquals(int.class, m.getReturnType());
    }

    @Test
    @SuppressWarnings("rawtypes")
    public void testRemoveSetReturnType() throws NoSuchMethodException {
        Method m = ReplicaCatalog.class.getDeclaredMethod("remove", Set.class);
        assertEquals(int.class, m.getReturnType());
    }

    @Test
    public void testRemoveByAttributeNameValueReturnType() throws NoSuchMethodException {
        Method m =
                ReplicaCatalog.class.getDeclaredMethod(
                        "removeByAttribute", String.class, Object.class);
        assertEquals(int.class, m.getReturnType());
    }

    @Test
    public void testRemoveByAttributeHandleReturnType() throws NoSuchMethodException {
        Method m = ReplicaCatalog.class.getDeclaredMethod("removeByAttribute", String.class);
        assertEquals(int.class, m.getReturnType());
    }

    // -----------------------------------------------------------------------
    // Catalog lifecycle (inherited from Catalog)
    // -----------------------------------------------------------------------

    @Test
    public void testIsClosedBeforeConnect() {
        StubReplicaCatalog fresh = new StubReplicaCatalog();
        assertTrue(fresh.isClosed());
    }

    @Test
    public void testConnectOpensTheCatalog() {
        StubReplicaCatalog fresh = new StubReplicaCatalog();
        assertTrue(fresh.connect(new Properties()));
        assertFalse(fresh.isClosed());
    }

    @Test
    public void testCloseTransitionsToClosed() {
        catalog.close();
        assertTrue(catalog.isClosed());
    }

    // -----------------------------------------------------------------------
    // insert / lookup
    // -----------------------------------------------------------------------

    @Test
    public void testInsertPfnHandle_lookupByHandle_returnsPfn() {
        catalog.insert("f.txt", "file:///data/f.txt", "condorpool");
        assertEquals("file:///data/f.txt", catalog.lookup("f.txt", "condorpool"));
    }

    @Test
    public void testInsertPfnHandle_lookupWrongHandle_returnsNull() {
        catalog.insert("f.txt", "file:///data/f.txt", "condorpool");
        assertNull(catalog.lookup("f.txt", "othersite"));
    }

    @Test
    public void testLookupUnknownLfn_returnsNull() {
        assertNull(catalog.lookup("nonexistent.txt", "condorpool"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testInsertEntry_lookupByLfn_returnsEntry() {
        ReplicaCatalogEntry entry = new ReplicaCatalogEntry("gsiftp://host/f.txt", "grid");
        catalog.insert("f.txt", entry);
        Collection<ReplicaCatalogEntry> results =
                (Collection<ReplicaCatalogEntry>) catalog.lookup("f.txt");
        assertThat(results, hasSize(1));
        assertThat(results.iterator().next().getPFN(), is("gsiftp://host/f.txt"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testLookupUnknownLfn_returnsEmptyCollection() {
        Collection<ReplicaCatalogEntry> results =
                (Collection<ReplicaCatalogEntry>) catalog.lookup("missing.txt");
        assertTrue(results.isEmpty());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testLookupNoAttributes_returnsPfnSet() {
        catalog.insert("f.txt", "file:///a.txt", "site1");
        catalog.insert("f.txt", "file:///b.txt", "site2");
        Set<String> pfns = (Set<String>) catalog.lookupNoAttributes("f.txt");
        assertThat(pfns, hasSize(2));
        assertThat(pfns, hasItem("file:///a.txt"));
        assertThat(pfns, hasItem("file:///b.txt"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testLookupSet_returnsMapKeyedByLfn() {
        catalog.insert("a.txt", "file:///a.txt", "site1");
        catalog.insert("b.txt", "file:///b.txt", "site1");
        Set<String> lfns = new HashSet<>(Arrays.asList("a.txt", "b.txt"));
        Map<String, Collection<ReplicaCatalogEntry>> result =
                (Map<String, Collection<ReplicaCatalogEntry>>) catalog.lookup(lfns);
        assertThat(result, hasKey("a.txt"));
        assertThat(result, hasKey("b.txt"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testLookupSetWithHandle_filtersToHandle() {
        catalog.insert("f.txt", "file:///site1.txt", "site1");
        catalog.insert("f.txt", "file:///site2.txt", "site2");
        Set<String> lfns = Collections.singleton("f.txt");
        Map<String, Collection<ReplicaCatalogEntry>> result =
                (Map<String, Collection<ReplicaCatalogEntry>>) catalog.lookup(lfns, "site1");
        Collection<ReplicaCatalogEntry> entries = result.get("f.txt");
        assertEquals(1, entries.size());
        assertEquals("file:///site1.txt", entries.iterator().next().getPFN());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testLookupNoAttributesSet_returnsPfnSets() {
        catalog.insert("a.txt", "file:///a.txt", "site1");
        Set<String> lfns = Collections.singleton("a.txt");
        Map<String, Set<String>> result =
                (Map<String, Set<String>>) catalog.lookupNoAttributes(lfns);
        assertThat(result.get("a.txt"), hasItem("file:///a.txt"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testLookupNoAttributesSetWithHandle_filtersToHandle() {
        catalog.insert("f.txt", "file:///site1.txt", "site1");
        catalog.insert("f.txt", "file:///site2.txt", "site2");
        Set<String> lfns = Collections.singleton("f.txt");
        Map<String, Set<String>> result =
                (Map<String, Set<String>>) catalog.lookupNoAttributes(lfns, "site1");
        Set<String> pfns = result.get("f.txt");
        assertThat(pfns, hasSize(1));
        assertThat(pfns, hasItem("file:///site1.txt"));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void testBulkInsert_insertsAllEntries() {
        Map<String, Collection<ReplicaCatalogEntry>> bulk = new HashMap<>();
        bulk.put(
                "x.txt",
                Arrays.asList(
                        new ReplicaCatalogEntry("file:///x1.txt", "s1"),
                        new ReplicaCatalogEntry("file:///x2.txt", "s2")));
        int count = catalog.insert((Map) bulk);
        assertEquals(2, count);
        assertEquals(2, ((Collection<?>) catalog.lookup("x.txt")).size());
    }

    @Test
    public void testInsertEntry_returnsOne() {
        int n = catalog.insert("f.txt", new ReplicaCatalogEntry("file:///f.txt", "site1"));
        assertEquals(1, n);
    }

    @Test
    public void testInsertPfnHandle_returnsOne() {
        assertEquals(1, catalog.insert("f.txt", "file:///f.txt", "site1"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testLookupNoAttributes_unknownLfn_returnsEmptySet() {
        assertTrue(((Set<String>) catalog.lookupNoAttributes("ghost.txt")).isEmpty());
    }

    // -----------------------------------------------------------------------
    // list
    // -----------------------------------------------------------------------

    @Test
    @SuppressWarnings("unchecked")
    public void testList_returnsAllLfns() {
        catalog.insert("a.txt", "file:///a.txt", "site1");
        catalog.insert("b.txt", "file:///b.txt", "site1");
        Set<String> lfns = (Set<String>) catalog.list();
        assertThat(lfns, hasItem("a.txt"));
        assertThat(lfns, hasItem("b.txt"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testListWithConstraint_returnsMatchingLfns() {
        catalog.insert("input.txt", "file:///input.txt", "site1");
        catalog.insert("output.txt", "file:///output.txt", "site1");
        catalog.insert("data.csv", "file:///data.csv", "site1");
        Set<String> lfns = (Set<String>) catalog.list("put");
        assertThat(lfns, hasItem("input.txt"));
        assertThat(lfns, hasItem("output.txt"));
        assertThat(lfns.contains("data.csv"), is(false));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testList_emptyWhenNothingInserted() {
        assertTrue(((Set<String>) catalog.list()).isEmpty());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testListWithConstraint_noMatches_returnsEmptySet() {
        catalog.insert("a.txt", "file:///a.txt", "site1");
        assertTrue(((Set<String>) catalog.list("zzz")).isEmpty());
    }

    // -----------------------------------------------------------------------
    // delete
    // -----------------------------------------------------------------------

    @Test
    public void testDeleteLfnPfn_removesMapping() {
        catalog.insert("f.txt", "file:///f.txt", "site1");
        int removed = catalog.delete("f.txt", "file:///f.txt");
        assertEquals(1, removed);
        assertNull(catalog.lookup("f.txt", "site1"));
    }

    @Test
    public void testDeleteLfnPfn_unknownPfn_returnsZero() {
        catalog.insert("f.txt", "file:///f.txt", "site1");
        assertEquals(0, catalog.delete("f.txt", "file:///other.txt"));
    }

    @Test
    public void testDeleteLfnEntry_removesEntry() {
        ReplicaCatalogEntry entry = new ReplicaCatalogEntry("file:///f.txt", "site1");
        catalog.insert("f.txt", entry);
        int removed = catalog.delete("f.txt", entry);
        assertEquals(1, removed);
        assertTrue(((Collection<?>) catalog.lookup("f.txt")).isEmpty());
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void testBulkDelete_removesMatchingEntries() {
        catalog.insert("a.txt", "file:///a.txt", "s1");
        catalog.insert("b.txt", "file:///b.txt", "s1");
        Map<String, Collection<ReplicaCatalogEntry>> toDelete = new HashMap<>();
        toDelete.put("a.txt", Collections.singletonList(new ReplicaCatalogEntry("file:///a.txt")));
        int removed = catalog.delete(toDelete, false);
        assertEquals(1, removed);
        assertNull(catalog.lookup("a.txt", "s1"));
        assertNotNull(catalog.lookup("b.txt", "s1"));
    }

    @Test
    public void testDeleteByResource_removesEntriesForHandle() {
        catalog.insert("f.txt", "file:///site1.txt", "site1");
        catalog.insert("f.txt", "file:///site2.txt", "site2");
        int removed = catalog.deleteByResource("f.txt", "site1");
        assertEquals(1, removed);
        assertNull(catalog.lookup("f.txt", "site1"));
        assertEquals("file:///site2.txt", catalog.lookup("f.txt", "site2"));
    }

    @Test
    public void testDeleteByResource_unknownLfn_returnsZero() {
        assertEquals(0, catalog.deleteByResource("ghost.txt", "site1"));
    }

    @Test
    public void testDeleteLfnNameValue_removesMatchingEntries() {
        ReplicaCatalogEntry entry = new ReplicaCatalogEntry("file:///f.txt", "site1");
        entry.addAttribute("checksum", "abc123");
        catalog.insert("f.txt", entry);
        int removed = catalog.delete("f.txt", "checksum", "abc123");
        assertEquals(1, removed);
        assertTrue(((Collection<?>) catalog.lookup("f.txt")).isEmpty());
    }

    @Test
    public void testDeleteLfnNameValue_noMatch_returnsZero() {
        catalog.insert("f.txt", "file:///f.txt", "site1");
        assertEquals(0, catalog.delete("f.txt", "checksum", "nonexistent"));
    }

    @Test
    public void testDeleteLfnNameValue_unknownLfn_returnsZero() {
        assertEquals(0, catalog.delete("ghost.txt", "checksum", "abc123"));
    }

    // -----------------------------------------------------------------------
    // remove
    // -----------------------------------------------------------------------

    @Test
    @SuppressWarnings("unchecked")
    public void testRemoveLfn_removesAllMappings() {
        catalog.insert("f.txt", "file:///a.txt", "site1");
        catalog.insert("f.txt", "file:///b.txt", "site2");
        int removed = catalog.remove("f.txt");
        assertEquals(2, removed);
        assertTrue(((Collection<?>) catalog.lookup("f.txt")).isEmpty());
    }

    @Test
    public void testRemoveLfn_unknownLfn_returnsZero() {
        assertEquals(0, catalog.remove("ghost.txt"));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void testRemoveSet_removesAllNamedLfns() {
        catalog.insert("a.txt", "file:///a.txt", "site1");
        catalog.insert("b.txt", "file:///b.txt", "site1");
        catalog.insert("c.txt", "file:///c.txt", "site1");
        Set<String> toRemove = new HashSet<>(Arrays.asList("a.txt", "b.txt"));
        int removed = catalog.remove((Set) toRemove);
        assertEquals(2, removed);
        assertThat(((Set<String>) catalog.list()).contains("a.txt"), is(false));
        assertThat(((Set<String>) catalog.list()).contains("b.txt"), is(false));
        assertThat((Set<String>) catalog.list(), hasItem("c.txt"));
    }

    @Test
    public void testRemoveByAttributeNameValue_removesMatchingEntries() {
        ReplicaCatalogEntry entry = new ReplicaCatalogEntry("file:///f.txt", "site1");
        entry.addAttribute("checksum", "abc123");
        catalog.insert("f.txt", entry);
        int removed = catalog.removeByAttribute("checksum", "abc123");
        assertEquals(1, removed);
        assertTrue(((Collection<?>) catalog.lookup("f.txt")).isEmpty());
    }

    @Test
    public void testRemoveByAttributeHandle_removesAllEntriesForSite() {
        catalog.insert("a.txt", "file:///a.txt", "site1");
        catalog.insert("b.txt", "file:///b.txt", "site1");
        catalog.insert("c.txt", "file:///c.txt", "site2");
        int removed = catalog.removeByAttribute("site1");
        assertEquals(2, removed);
        assertNull(catalog.lookup("a.txt", "site1"));
        assertNull(catalog.lookup("b.txt", "site1"));
        assertEquals("file:///c.txt", catalog.lookup("c.txt", "site2"));
    }

    // -----------------------------------------------------------------------
    // clear
    // -----------------------------------------------------------------------

    @Test
    @SuppressWarnings("unchecked")
    public void testClear_removesAllEntriesAndReturnsCount() {
        catalog.insert("a.txt", "file:///a.txt", "site1");
        catalog.insert("b.txt", "file:///b.txt", "site1");
        int removed = catalog.clear();
        assertEquals(2, removed);
        assertTrue(((Set<String>) catalog.list()).isEmpty());
    }

    @Test
    public void testClear_emptyStoreReturnsZero() {
        assertEquals(0, catalog.clear());
    }

    // -----------------------------------------------------------------------
    // getFileSource / setReadOnly
    // -----------------------------------------------------------------------

    @Test
    public void testGetFileSource_returnsNull() {
        assertNull(catalog.getFileSource());
    }

    @Test
    public void testSetReadOnly_trueMarksReadOnly() {
        catalog.setReadOnly(true);
        assertTrue(catalog.isReadOnly());
    }

    @Test
    public void testSetReadOnly_falseUnmarksReadOnly() {
        catalog.setReadOnly(true);
        catalog.setReadOnly(false);
        assertFalse(catalog.isReadOnly());
    }

    @Test
    public void testReplicaCatalogExtendsCatalog() {
        assertTrue(Catalog.class.isAssignableFrom(ReplicaCatalog.class));
    }

    @Test
    public void testAllDeclaredMethodsArePublicAndAbstract() {
        for (Method method : ReplicaCatalog.class.getDeclaredMethods()) {
            int mods = method.getModifiers();
            assertTrue(Modifier.isPublic(mods), method.getName() + " should be public");
            assertTrue(Modifier.isAbstract(mods), method.getName() + " should be abstract");
        }
    }

    @Test
    public void testReplicaCatalogDeclaresExpectedNumberOfMethods() {
        assertEquals(25, ReplicaCatalog.class.getDeclaredMethods().length);
    }
}
