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
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.catalog.work.WorkCatalogException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Structural and behavioral tests for the WorkCatalog interface.
 *
 * @author Rajiv Mayani
 */
public class WorkCatalogTest {

    // -----------------------------------------------------------------------
    // Record key for the in-memory stub
    // -----------------------------------------------------------------------
    private static final class RunKey {
        final String basedir, vogroup, label, run;

        RunKey(String basedir, String vogroup, String label, String run) {
            this.basedir = basedir;
            this.vogroup = vogroup;
            this.label = label;
            this.run = run;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof RunKey)) return false;
            RunKey k = (RunKey) o;
            return Objects.equals(basedir, k.basedir)
                    && Objects.equals(vogroup, k.vogroup)
                    && Objects.equals(label, k.label)
                    && Objects.equals(run, k.run);
        }

        @Override
        public int hashCode() {
            return Objects.hash(basedir, vogroup, label, run);
        }
    }

    // -----------------------------------------------------------------------
    // Minimal in-memory stub for behavioral contract tests
    // -----------------------------------------------------------------------
    private static class StubWorkCatalog implements WorkCatalog {

        private boolean mClosed = true;
        private final Map<RunKey, int[]> mStore = new HashMap<>(); // value: [state]

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
        public int insert(
                String basedir,
                String vogroup,
                String label,
                String run,
                String creator,
                Date cTime,
                Date mTime,
                int state)
                throws WorkCatalogException {
            mStore.put(new RunKey(basedir, vogroup, label, run), new int[] {state});
            return 1;
        }

        @Override
        public int delete(String basedir, String vogroup, String label, String run)
                throws WorkCatalogException {
            return mStore.remove(new RunKey(basedir, vogroup, label, run)) != null ? 1 : 0;
        }

        int size() {
            return mStore.size();
        }

        boolean contains(String basedir, String vogroup, String label, String run) {
            return mStore.containsKey(new RunKey(basedir, vogroup, label, run));
        }
    }

    // -----------------------------------------------------------------------
    // Test fixture
    // -----------------------------------------------------------------------
    private StubWorkCatalog catalog;
    private final Date NOW = new Date();

    @BeforeEach
    public void setUp() {
        catalog = new StubWorkCatalog();
        catalog.connect(new Properties());
    }

    // -----------------------------------------------------------------------
    // Constant values
    // -----------------------------------------------------------------------

    @Test
    public void testVersionConstant() {
        assertThat(WorkCatalog.VERSION, equalTo("1.0"));
    }

    @Test
    public void testVersionConstantNotEmpty() {
        assertThat(WorkCatalog.VERSION, is(not(emptyString())));
    }

    @Test
    public void testPropertyPrefixConstant() {
        assertThat(WorkCatalog.c_prefix, equalTo("pegasus.catalog.work"));
    }

    @Test
    public void testDbPrefixConstant() {
        assertThat(WorkCatalog.DB_PREFIX, equalTo("pegasus.catalog.work.db"));
    }

    // -----------------------------------------------------------------------
    // Constant modifiers
    // -----------------------------------------------------------------------

    @Test
    public void testAllConstantsArePublicStaticFinal() throws NoSuchFieldException {
        for (String name : new String[] {"VERSION", "c_prefix", "DB_PREFIX"}) {
            Field f = WorkCatalog.class.getDeclaredField(name);
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
    public void testInsertMethodExists() throws NoSuchMethodException {
        Method m =
                WorkCatalog.class.getDeclaredMethod(
                        "insert",
                        String.class,
                        String.class,
                        String.class,
                        String.class,
                        String.class,
                        Date.class,
                        Date.class,
                        int.class);
        assertThat(m, is(notNullValue()));
    }

    @Test
    public void testInsertMethodReturnType() throws NoSuchMethodException {
        Method m =
                WorkCatalog.class.getDeclaredMethod(
                        "insert",
                        String.class,
                        String.class,
                        String.class,
                        String.class,
                        String.class,
                        Date.class,
                        Date.class,
                        int.class);
        assertThat(m.getReturnType(), is(int.class));
    }

    @Test
    public void testInsertMethodParameterCount() throws NoSuchMethodException {
        Method m =
                WorkCatalog.class.getDeclaredMethod(
                        "insert",
                        String.class,
                        String.class,
                        String.class,
                        String.class,
                        String.class,
                        Date.class,
                        Date.class,
                        int.class);
        assertThat(m.getParameterCount(), is(8));
    }

    @Test
    public void testInsertMethodParameterTypes() throws NoSuchMethodException {
        Method m =
                WorkCatalog.class.getDeclaredMethod(
                        "insert",
                        String.class,
                        String.class,
                        String.class,
                        String.class,
                        String.class,
                        Date.class,
                        Date.class,
                        int.class);
        Class<?>[] types = m.getParameterTypes();
        assertArrayEquals(
                new Class<?>[] {
                    String.class, String.class, String.class, String.class,
                    String.class, Date.class, Date.class, int.class
                },
                types);
    }

    @Test
    public void testDeleteMethodExists() throws NoSuchMethodException {
        Method m =
                WorkCatalog.class.getDeclaredMethod(
                        "delete", String.class, String.class, String.class, String.class);
        assertThat(m, is(notNullValue()));
    }

    @Test
    public void testDeleteMethodReturnType() throws NoSuchMethodException {
        Method m =
                WorkCatalog.class.getDeclaredMethod(
                        "delete", String.class, String.class, String.class, String.class);
        assertThat(m.getReturnType(), is(int.class));
    }

    @Test
    public void testDeleteMethodParameterCount() throws NoSuchMethodException {
        Method m =
                WorkCatalog.class.getDeclaredMethod(
                        "delete", String.class, String.class, String.class, String.class);
        assertThat(m.getParameterCount(), is(4));
    }

    @Test
    public void testDeleteMethodParameterTypes() throws NoSuchMethodException {
        Method m =
                WorkCatalog.class.getDeclaredMethod(
                        "delete", String.class, String.class, String.class, String.class);
        for (Class<?> t : m.getParameterTypes()) {
            assertThat(t, is(String.class));
        }
    }

    // -----------------------------------------------------------------------
    // Exception declarations
    // -----------------------------------------------------------------------

    @Test
    public void testInsertDeclaresWorkCatalogException() throws NoSuchMethodException {
        Method m =
                WorkCatalog.class.getDeclaredMethod(
                        "insert",
                        String.class,
                        String.class,
                        String.class,
                        String.class,
                        String.class,
                        Date.class,
                        Date.class,
                        int.class);
        List<Class<?>> exceptions = Arrays.asList(m.getExceptionTypes());
        assertThat(exceptions, hasItem(WorkCatalogException.class));
    }

    @Test
    public void testDeleteDeclaresWorkCatalogException() throws NoSuchMethodException {
        Method m =
                WorkCatalog.class.getDeclaredMethod(
                        "delete", String.class, String.class, String.class, String.class);
        List<Class<?>> exceptions = Arrays.asList(m.getExceptionTypes());
        assertThat(exceptions, hasItem(WorkCatalogException.class));
    }

    // -----------------------------------------------------------------------
    // Catalog lifecycle
    // -----------------------------------------------------------------------

    @Test
    public void testIsClosedBeforeConnect() {
        assertThat(new StubWorkCatalog().isClosed(), is(true));
    }

    @Test
    public void testConnectOpensTheCatalog() {
        StubWorkCatalog fresh = new StubWorkCatalog();
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
        catalog.connect(new Properties());
        assertThat(catalog.isClosed(), is(false));
    }

    // -----------------------------------------------------------------------
    // insert
    // -----------------------------------------------------------------------

    @Test
    public void testInsert_returnsOne() throws WorkCatalogException {
        assertThat(catalog.insert("/base", "vog", "label", "001", "alice", NOW, NOW, 0), is(1));
    }

    @Test
    public void testInsert_recordIsTracked() throws WorkCatalogException {
        catalog.insert("/base", "vog", "label", "001", "alice", NOW, NOW, 0);
        assertThat(catalog.size(), is(1));
        assertThat(catalog.contains("/base", "vog", "label", "001"), is(true));
    }

    @Test
    public void testInsert_multipleDistinctRuns_allTracked() throws WorkCatalogException {
        catalog.insert("/base", "vog", "label", "001", "alice", NOW, NOW, 0);
        catalog.insert("/base", "vog", "label", "002", "alice", NOW, NOW, 1);
        catalog.insert("/base", "vog", "label", "003", "alice", NOW, NOW, 2);
        assertThat(catalog.size(), is(3));
    }

    @Test
    public void testInsert_sameKey_overwritesPreviousRecord() throws WorkCatalogException {
        catalog.insert("/base", "vog", "label", "001", "alice", NOW, NOW, 0);
        catalog.insert("/base", "vog", "label", "001", "bob", NOW, NOW, 1);
        assertThat(catalog.size(), is(1));
    }

    @Test
    public void testInsert_differentBasedir_treatedAsSeparateRecord() throws WorkCatalogException {
        catalog.insert("/base1", "vog", "label", "001", "alice", NOW, NOW, 0);
        catalog.insert("/base2", "vog", "label", "001", "alice", NOW, NOW, 0);
        assertThat(catalog.size(), is(2));
    }

    @Test
    public void testInsert_differentVogroup_treatedAsSeparateRecord() throws WorkCatalogException {
        catalog.insert("/base", "vog1", "label", "001", "alice", NOW, NOW, 0);
        catalog.insert("/base", "vog2", "label", "001", "alice", NOW, NOW, 0);
        assertThat(catalog.size(), is(2));
    }

    @Test
    public void testInsert_differentLabel_treatedAsSeparateRecord() throws WorkCatalogException {
        catalog.insert("/base", "vog", "label1", "001", "alice", NOW, NOW, 0);
        catalog.insert("/base", "vog", "label2", "001", "alice", NOW, NOW, 0);
        assertThat(catalog.size(), is(2));
    }

    @Test
    public void testInsert_nullDates_accepted() throws WorkCatalogException {
        assertThat(catalog.insert("/base", "vog", "label", "001", "alice", null, null, 0), is(1));
    }

    // -----------------------------------------------------------------------
    // delete
    // -----------------------------------------------------------------------

    @Test
    public void testDelete_afterInsert_returnsOne() throws WorkCatalogException {
        catalog.insert("/base", "vog", "label", "001", "alice", NOW, NOW, 0);
        assertThat(catalog.delete("/base", "vog", "label", "001"), is(1));
    }

    @Test
    public void testDelete_afterInsert_recordIsGone() throws WorkCatalogException {
        catalog.insert("/base", "vog", "label", "001", "alice", NOW, NOW, 0);
        catalog.delete("/base", "vog", "label", "001");
        assertThat(catalog.contains("/base", "vog", "label", "001"), is(false));
        assertThat(catalog.size(), is(0));
    }

    @Test
    public void testDelete_unknownRecord_returnsZero() throws WorkCatalogException {
        assertThat(catalog.delete("/base", "vog", "label", "999"), is(0));
    }

    @Test
    public void testDelete_doesNotAffectOtherRecords() throws WorkCatalogException {
        catalog.insert("/base", "vog", "label", "001", "alice", NOW, NOW, 0);
        catalog.insert("/base", "vog", "label", "002", "alice", NOW, NOW, 0);
        catalog.delete("/base", "vog", "label", "001");
        assertThat(catalog.size(), is(1));
        assertThat(catalog.contains("/base", "vog", "label", "002"), is(true));
    }

    @Test
    public void testDelete_wrongBasedir_returnsZero() throws WorkCatalogException {
        catalog.insert("/base", "vog", "label", "001", "alice", NOW, NOW, 0);
        assertThat(catalog.delete("/other", "vog", "label", "001"), is(0));
        assertThat(catalog.size(), is(1));
    }

    @Test
    public void testDelete_wrongVogroup_returnsZero() throws WorkCatalogException {
        catalog.insert("/base", "vog", "label", "001", "alice", NOW, NOW, 0);
        assertThat(catalog.delete("/base", "other", "label", "001"), is(0));
        assertThat(catalog.size(), is(1));
    }

    @Test
    public void testDelete_wrongLabel_returnsZero() throws WorkCatalogException {
        catalog.insert("/base", "vog", "label", "001", "alice", NOW, NOW, 0);
        assertThat(catalog.delete("/base", "vog", "other", "001"), is(0));
        assertThat(catalog.size(), is(1));
    }

    @Test
    public void testDelete_wrongRun_returnsZero() throws WorkCatalogException {
        catalog.insert("/base", "vog", "label", "001", "alice", NOW, NOW, 0);
        assertThat(catalog.delete("/base", "vog", "label", "002"), is(0));
        assertThat(catalog.size(), is(1));
    }

    @Test
    public void testDelete_calledTwiceForSameKey_secondCallReturnsZero()
            throws WorkCatalogException {
        catalog.insert("/base", "vog", "label", "001", "alice", NOW, NOW, 0);
        catalog.delete("/base", "vog", "label", "001");
        assertThat(catalog.delete("/base", "vog", "label", "001"), is(0));
    }

    // -----------------------------------------------------------------------
    // insert + delete round-trip
    // -----------------------------------------------------------------------

    @Test
    public void testInsertThenDelete_emptyStore() throws WorkCatalogException {
        catalog.insert("/base", "vog", "label", "001", "alice", NOW, NOW, 0);
        catalog.delete("/base", "vog", "label", "001");
        assertThat(catalog.size(), is(0));
    }

    @Test
    public void testReinsertAfterDelete_recordIsPresent() throws WorkCatalogException {
        catalog.insert("/base", "vog", "label", "001", "alice", NOW, NOW, 0);
        catalog.delete("/base", "vog", "label", "001");
        assertThat(catalog.insert("/base", "vog", "label", "001", "alice", NOW, NOW, 2), is(1));
        assertThat(catalog.contains("/base", "vog", "label", "001"), is(true));
    }

    @Test
    public void testWorkCatalogExtendsCatalog() {
        assertThat(Catalog.class.isAssignableFrom(WorkCatalog.class), is(true));
    }

    @Test
    public void testAllDeclaredMethodsArePublicAndAbstract() {
        for (Method method : WorkCatalog.class.getDeclaredMethods()) {
            int mods = method.getModifiers();
            assertThat(Modifier.isPublic(mods), is(true));
            assertThat(Modifier.isAbstract(mods), is(true));
        }
    }

    @Test
    public void testWorkCatalogDeclaresExpectedNumberOfMethods() {
        assertThat(WorkCatalog.class.getDeclaredMethods().length, is(2));
    }
}
