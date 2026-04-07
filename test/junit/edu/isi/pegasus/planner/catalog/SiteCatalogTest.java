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
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.catalog.site.SiteCatalogException;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.classes.PegasusBag;
import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Structural and behavioral tests for the SiteCatalog interface.
 *
 * @author Rajiv Mayani
 */
public class SiteCatalogTest {

    // -----------------------------------------------------------------------
    // Minimal in-memory stub for behavioral contract tests
    // -----------------------------------------------------------------------
    private static class StubSiteCatalog implements SiteCatalog {

        private boolean mClosed = true;
        private final Map<String, SiteCatalogEntry> mStore = new HashMap<>();

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
        public void initialize(PegasusBag bag) {
            // no-op in stub
        }

        @Override
        public int load(List<String> sites) throws SiteCatalogException {
            if (sites.contains("*")) {
                return mStore.size();
            }
            int count = 0;
            for (String handle : sites) {
                if (mStore.containsKey(handle)) count++;
            }
            return count;
        }

        @Override
        public int insert(SiteCatalogEntry entry) throws SiteCatalogException {
            mStore.put(entry.getSiteHandle(), entry);
            return 1;
        }

        @Override
        public Set<String> list() throws SiteCatalogException {
            return new HashSet<>(mStore.keySet());
        }

        @Override
        public SiteCatalogEntry lookup(String handle) throws SiteCatalogException {
            return mStore.get(handle);
        }

        @Override
        public int remove(String handle) throws SiteCatalogException {
            return mStore.remove(handle) != null ? 1 : 0;
        }

        @Override
        public File getFileSource() {
            return null;
        }
    }

    // -----------------------------------------------------------------------
    // Test fixture
    // -----------------------------------------------------------------------
    private StubSiteCatalog catalog;

    @BeforeEach
    public void setUp() throws Exception {
        catalog = new StubSiteCatalog();
        catalog.connect(new Properties());
    }

    // -----------------------------------------------------------------------
    // Constant values
    // -----------------------------------------------------------------------

    @Test
    public void testVersionConstantValue() {
        assertThat(SiteCatalog.VERSION, is("1.1"));
    }

    @Test
    public void testVersionConstantNotNull() {
        assertThat(SiteCatalog.VERSION, is(not(emptyString())));
    }

    @Test
    public void testPropertyPrefixConstant() {
        assertThat(SiteCatalog.c_prefix, is("pegasus.catalog.site"));
    }

    @Test
    public void testVariableExpansionKeyConstant() {
        assertThat(SiteCatalog.VARIABLE_EXPANSION_KEY, is("expand"));
    }

    @Test
    public void testFileKeyConstant() {
        assertThat(SiteCatalog.FILE_KEY, is("file"));
    }

    // -----------------------------------------------------------------------
    // Constant modifiers
    // -----------------------------------------------------------------------

    @Test
    public void testAllConstantsArePublicStaticFinal() throws NoSuchFieldException {
        String[] names = {"VERSION", "c_prefix", "VARIABLE_EXPANSION_KEY", "FILE_KEY"};
        for (String name : names) {
            Field f = SiteCatalog.class.getDeclaredField(name);
            int mods = f.getModifiers();
            assertThat(Modifier.isPublic(mods), is(true));
            assertThat(Modifier.isStatic(mods), is(true));
            assertThat(Modifier.isFinal(mods), is(true));
        }
    }

    // -----------------------------------------------------------------------
    // Method signatures — return types
    // -----------------------------------------------------------------------

    @Test
    public void testInitializeMethodSignature() throws NoSuchMethodException {
        Method m = SiteCatalog.class.getDeclaredMethod("initialize", PegasusBag.class);
        assertThat(m.getReturnType(), is(void.class));
        assertThat(m.getParameterCount(), is(1));
    }

    @Test
    public void testLoadMethodSignature() throws NoSuchMethodException {
        Method m = SiteCatalog.class.getDeclaredMethod("load", List.class);
        assertThat(m.getReturnType(), is(int.class));
        assertThat(m.getParameterCount(), is(1));
    }

    @Test
    public void testInsertMethodSignature() throws NoSuchMethodException {
        Method m = SiteCatalog.class.getDeclaredMethod("insert", SiteCatalogEntry.class);
        assertThat(m.getReturnType(), is(int.class));
        assertThat(m.getParameterCount(), is(1));
    }

    @Test
    public void testListMethodSignature() throws NoSuchMethodException {
        Method m = SiteCatalog.class.getDeclaredMethod("list");
        assertThat(m.getReturnType(), is(Set.class));
        assertThat(m.getParameterCount(), is(0));
    }

    @Test
    public void testLookupMethodSignature() throws NoSuchMethodException {
        Method m = SiteCatalog.class.getDeclaredMethod("lookup", String.class);
        assertThat(m.getReturnType(), is(SiteCatalogEntry.class));
        assertThat(m.getParameterCount(), is(1));
    }

    @Test
    public void testRemoveMethodSignature() throws NoSuchMethodException {
        Method m = SiteCatalog.class.getDeclaredMethod("remove", String.class);
        assertThat(m.getReturnType(), is(int.class));
        assertThat(m.getParameterCount(), is(1));
    }

    @Test
    public void testGetFileSourceMethodSignature() throws NoSuchMethodException {
        Method m = SiteCatalog.class.getDeclaredMethod("getFileSource");
        assertThat(m.getReturnType(), is(File.class));
        assertThat(m.getParameterCount(), is(0));
    }

    // -----------------------------------------------------------------------
    // Checked exception declarations
    // -----------------------------------------------------------------------

    @Test
    public void testLoadDeclaresSiteCatalogException() throws NoSuchMethodException {
        Method m = SiteCatalog.class.getDeclaredMethod("load", List.class);
        List<Class<?>> exceptions = Arrays.asList(m.getExceptionTypes());
        assertThat(exceptions, hasItem(SiteCatalogException.class));
    }

    @Test
    public void testInsertDeclaresSiteCatalogException() throws NoSuchMethodException {
        Method m = SiteCatalog.class.getDeclaredMethod("insert", SiteCatalogEntry.class);
        List<Class<?>> exceptions = Arrays.asList(m.getExceptionTypes());
        assertThat(exceptions, hasItem(SiteCatalogException.class));
    }

    @Test
    public void testListDeclaresSiteCatalogException() throws NoSuchMethodException {
        Method m = SiteCatalog.class.getDeclaredMethod("list");
        List<Class<?>> exceptions = Arrays.asList(m.getExceptionTypes());
        assertThat(exceptions, hasItem(SiteCatalogException.class));
    }

    @Test
    public void testLookupDeclaresSiteCatalogException() throws NoSuchMethodException {
        Method m = SiteCatalog.class.getDeclaredMethod("lookup", String.class);
        List<Class<?>> exceptions = Arrays.asList(m.getExceptionTypes());
        assertThat(exceptions, hasItem(SiteCatalogException.class));
    }

    @Test
    public void testRemoveDeclaresSiteCatalogException() throws NoSuchMethodException {
        Method m = SiteCatalog.class.getDeclaredMethod("remove", String.class);
        List<Class<?>> exceptions = Arrays.asList(m.getExceptionTypes());
        assertThat(exceptions, hasItem(SiteCatalogException.class));
    }

    // -----------------------------------------------------------------------
    // Catalog lifecycle
    // -----------------------------------------------------------------------

    @Test
    public void testIsClosedBeforeConnect() {
        StubSiteCatalog fresh = new StubSiteCatalog();
        assertThat(fresh.isClosed(), is(true));
    }

    @Test
    public void testConnectOpensTheCatalog() {
        StubSiteCatalog fresh = new StubSiteCatalog();
        assertThat(fresh.connect(new Properties()), is(true));
        assertThat(fresh.isClosed(), is(false));
    }

    @Test
    public void testCloseTransitionsToClosed() {
        catalog.close();
        assertThat(catalog.isClosed(), is(true));
    }

    @Test
    public void testConnectCloseRoundTrip() {
        catalog.close();
        assertThat(catalog.isClosed(), is(true));
        catalog.connect(new Properties());
        assertThat(catalog.isClosed(), is(false));
    }

    // -----------------------------------------------------------------------
    // insert / lookup
    // -----------------------------------------------------------------------

    @Test
    public void testInsert_returnsOne() throws SiteCatalogException {
        assertThat(catalog.insert(new SiteCatalogEntry("condorpool")), is(1));
    }

    @Test
    public void testInsert_lookup_returnsEntry() throws SiteCatalogException {
        SiteCatalogEntry entry = new SiteCatalogEntry("condorpool");
        catalog.insert(entry);
        SiteCatalogEntry result = catalog.lookup("condorpool");
        assertThat(result, is(notNullValue()));
        assertThat(result.getSiteHandle(), is("condorpool"));
    }

    @Test
    public void testLookup_unknownHandle_returnsNull() throws SiteCatalogException {
        assertThat(catalog.lookup("nonexistent"), is(nullValue()));
    }

    @Test
    public void testInsert_overwritesExistingEntry() throws SiteCatalogException {
        catalog.insert(new SiteCatalogEntry("site1"));
        SiteCatalogEntry updated = new SiteCatalogEntry("site1");
        catalog.insert(updated);
        assertThat(catalog.lookup("site1"), is(sameInstance(updated)));
    }

    // -----------------------------------------------------------------------
    // list
    // -----------------------------------------------------------------------

    @Test
    public void testList_emptyWhenNothingInserted() throws SiteCatalogException {
        assertThat(catalog.list(), is(empty()));
    }

    @Test
    public void testList_returnsHandleAfterInsert() throws SiteCatalogException {
        catalog.insert(new SiteCatalogEntry("condorpool"));
        assertThat(catalog.list(), hasItem("condorpool"));
    }

    @Test
    public void testList_returnsAllInsertedHandles() throws SiteCatalogException {
        catalog.insert(new SiteCatalogEntry("site1"));
        catalog.insert(new SiteCatalogEntry("site2"));
        catalog.insert(new SiteCatalogEntry("site3"));
        Set<String> handles = catalog.list();
        assertThat(handles, hasSize(3));
        assertThat(handles, containsInAnyOrder("site1", "site2", "site3"));
    }

    // -----------------------------------------------------------------------
    // load
    // -----------------------------------------------------------------------

    @Test
    public void testLoad_wildcard_loadsAllSites() throws SiteCatalogException {
        catalog.insert(new SiteCatalogEntry("site1"));
        catalog.insert(new SiteCatalogEntry("site2"));
        assertThat(catalog.load(Collections.singletonList("*")), is(2));
    }

    @Test
    public void testLoad_specificHandles_countsMatchingOnes() throws SiteCatalogException {
        catalog.insert(new SiteCatalogEntry("site1"));
        catalog.insert(new SiteCatalogEntry("site2"));
        assertThat(catalog.load(Collections.singletonList("site1")), is(1));
    }

    @Test
    public void testLoad_unknownHandle_returnsZero() throws SiteCatalogException {
        assertThat(catalog.load(Collections.singletonList("ghost")), is(0));
    }

    @Test
    public void testLoad_emptyList_returnsZero() throws SiteCatalogException {
        catalog.insert(new SiteCatalogEntry("site1"));
        assertThat(catalog.load(Collections.emptyList()), is(0));
    }

    // -----------------------------------------------------------------------
    // remove
    // -----------------------------------------------------------------------

    @Test
    public void testRemove_afterInsert_entryGone() throws SiteCatalogException {
        catalog.insert(new SiteCatalogEntry("site1"));
        catalog.remove("site1");
        assertThat(catalog.lookup("site1"), is(nullValue()));
    }

    @Test
    public void testRemove_returnsOne() throws SiteCatalogException {
        catalog.insert(new SiteCatalogEntry("site1"));
        assertThat(catalog.remove("site1"), is(1));
    }

    @Test
    public void testRemove_unknownHandle_returnsZero() throws SiteCatalogException {
        assertThat(catalog.remove("ghost"), is(0));
    }

    @Test
    public void testRemove_doesNotAffectOtherEntries() throws SiteCatalogException {
        catalog.insert(new SiteCatalogEntry("site1"));
        catalog.insert(new SiteCatalogEntry("site2"));
        catalog.remove("site1");
        assertThat(catalog.lookup("site2"), is(notNullValue()));
        assertThat(catalog.list(), hasSize(1));
    }

    // -----------------------------------------------------------------------
    // getFileSource / initialize
    // -----------------------------------------------------------------------

    @Test
    public void testGetFileSource_returnsNull() {
        assertThat(catalog.getFileSource(), is(nullValue()));
    }

    @Test
    public void testInitialize_acceptsNullBag() {
        // initialize is a no-op in the stub; must not throw
        assertDoesNotThrow(() -> catalog.initialize(null));
    }

    @Test
    public void testInitialize_acceptsPegasusBag() {
        assertDoesNotThrow(() -> catalog.initialize(new PegasusBag()));
    }

    @Test
    public void testSiteCatalogExtendsCatalog() {
        assertThat(Catalog.class.isAssignableFrom(SiteCatalog.class), is(true));
    }

    @Test
    public void testAllDeclaredMethodsArePublicAndAbstract() {
        for (Method method : SiteCatalog.class.getDeclaredMethods()) {
            int mods = method.getModifiers();
            assertThat(Modifier.isPublic(mods), is(true));
            assertThat(Modifier.isAbstract(mods), is(true));
        }
    }

    @Test
    public void testSiteCatalogDeclaresExpectedNumberOfMethods() {
        assertThat(SiteCatalog.class.getDeclaredMethods().length, is(7));
    }
}
