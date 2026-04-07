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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.catalog.classes.SysInfo;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;
import edu.isi.pegasus.planner.classes.PegasusBag;
import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Structural and behavioral tests for the TransformationCatalog interface.
 *
 * @author Rajiv Mayani
 */
public class TransformationCatalogTest {

    // -----------------------------------------------------------------------
    // Minimal in-memory stub for behavioral contract tests
    // -----------------------------------------------------------------------
    private static class StubTransformationCatalog implements TransformationCatalog {

        private boolean mClosed = true;
        private boolean mTransient = false;
        private final List<TransformationCatalogEntry> mEntries = new ArrayList<>();

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

        private boolean matches(
                TransformationCatalogEntry e,
                String ns,
                String name,
                String version,
                String site,
                TCType type) {
            if (name != null && !name.equals(e.getLogicalName())) return false;
            if (ns != null && !ns.equals(e.getLogicalNamespace())) return false;
            if (version != null && !version.equals(e.getLogicalVersion())) return false;
            if (site != null && !site.equals(e.getResourceId())) return false;
            if (type != null && !type.equals(e.getType())) return false;
            return true;
        }

        @Override
        public List<TransformationCatalogEntry> lookup(
                String ns, String name, String version, String site, TCType type) {
            List<TransformationCatalogEntry> result = new ArrayList<>();
            for (TransformationCatalogEntry e : mEntries) {
                if (matches(e, ns, name, version, site, type)) result.add(e);
            }
            return result.isEmpty() ? null : result;
        }

        @Override
        @SuppressWarnings("rawtypes")
        public List<TransformationCatalogEntry> lookup(
                String ns, String name, String version, List sites, TCType type) {
            if (sites == null) return lookup(ns, name, version, (String) null, type);
            List<TransformationCatalogEntry> result = new ArrayList<>();
            for (Object site : sites) {
                List<TransformationCatalogEntry> sub =
                        lookup(ns, name, version, (String) site, type);
                if (sub != null) result.addAll(sub);
            }
            return result.isEmpty() ? null : result;
        }

        @Override
        public List<String> lookupSites(String ns, String name, String version, TCType type) {
            List<TransformationCatalogEntry> entries =
                    lookup(ns, name, version, (String) null, type);
            if (entries == null) return null;
            Set<String> sites = new LinkedHashSet<>();
            for (TransformationCatalogEntry e : entries) sites.add(e.getResourceId());
            return new ArrayList<>(sites);
        }

        @Override
        public List<TransformationCatalogEntry> lookupNoProfiles(
                String ns, String name, String version, String site, TCType type) {
            List<TransformationCatalogEntry> entries = lookup(ns, name, version, site, type);
            if (entries == null) return null;
            List<TransformationCatalogEntry> result = new ArrayList<>();
            for (TransformationCatalogEntry e : entries) {
                TransformationCatalogEntry copy =
                        new TransformationCatalogEntry(
                                e.getLogicalNamespace(), e.getLogicalName(), e.getLogicalVersion());
                copy.setResourceId(e.getResourceId());
                copy.setType(e.getType());
                copy.setPhysicalTransformation(e.getPhysicalTransformation());
                result.add(copy);
            }
            return result;
        }

        @Override
        public List<String[]> getTCLogicalNames(String site, TCType type) {
            List<TransformationCatalogEntry> entries = lookup(null, null, null, site, type);
            if (entries == null) return null;
            List<String[]> result = new ArrayList<>();
            for (TransformationCatalogEntry e : entries) {
                result.add(
                        new String[] {
                            e.getResourceId(),
                            e.getLogicalNamespace()
                                    + "::"
                                    + e.getLogicalName()
                                    + ":"
                                    + e.getLogicalVersion(),
                            e.getType() == null ? null : e.getType().toString()
                        });
            }
            return result;
        }

        @Override
        @SuppressWarnings("rawtypes")
        public List lookupLFNProfiles(String ns, String name, String version) {
            List<TransformationCatalogEntry> entries =
                    lookup(ns, name, version, (String) null, null);
            if (entries == null) return null;
            List<Object> profiles = new ArrayList<>();
            for (TransformationCatalogEntry e : entries) {
                List<?> p = e.getProfiles();
                if (p != null) profiles.addAll(p);
            }
            return profiles.isEmpty() ? null : profiles;
        }

        @Override
        @SuppressWarnings("rawtypes")
        public List lookupPFNProfiles(String pfn, String site, TCType type) {
            for (TransformationCatalogEntry e : mEntries) {
                if (pfn.equals(e.getPhysicalTransformation())
                        && (site == null || site.equals(e.getResourceId()))
                        && (type == null || type.equals(e.getType()))) {
                    return e.getProfiles();
                }
            }
            return null;
        }

        @Override
        public List<TransformationCatalogEntry> getContents() {
            return new ArrayList<>(mEntries);
        }

        @Override
        public int insert(List<TransformationCatalogEntry> entries) {
            mEntries.addAll(entries);
            return entries.size();
        }

        @Override
        public int insert(TransformationCatalogEntry entry) {
            mEntries.add(entry);
            return 1;
        }

        @Override
        public int insert(TransformationCatalogEntry entry, boolean write) {
            mEntries.add(entry);
            return 1;
        }

        @Override
        @SuppressWarnings("rawtypes")
        public int insert(
                String ns,
                String name,
                String version,
                String pfn,
                TCType type,
                String site,
                List lfnprofiles,
                List pfnprofiles,
                SysInfo sysinfo) {
            TransformationCatalogEntry e = new TransformationCatalogEntry(ns, name, version);
            e.setPhysicalTransformation(pfn);
            e.setType(type);
            e.setResourceId(site);
            if (sysinfo != null) e.setSysInfo(sysinfo);
            mEntries.add(e);
            return 1;
        }

        @Override
        @SuppressWarnings("rawtypes")
        public int addLFNProfile(String ns, String name, String version, List profiles) {
            if (profiles == null || profiles.isEmpty()) return 0;
            List<TransformationCatalogEntry> entries =
                    lookup(ns, name, version, (String) null, null);
            if (entries == null) return 0;
            return profiles.size();
        }

        @Override
        @SuppressWarnings("rawtypes")
        public int addPFNProfile(String pfn, TCType type, String site, List profiles) {
            if (profiles == null || profiles.isEmpty()) return 0;
            int count = 0;
            for (TransformationCatalogEntry e : mEntries) {
                if (pfn.equals(e.getPhysicalTransformation())
                        && (type == null || type.equals(e.getType()))
                        && (site == null || site.equals(e.getResourceId()))) {
                    count += profiles.size();
                }
            }
            return count;
        }

        @Override
        public int removeByLFN(String ns, String name, String version, String site, TCType type) {
            int before = mEntries.size();
            mEntries.removeIf(e -> matches(e, ns, name, version, site, type));
            return before - mEntries.size();
        }

        @Override
        public int removeByPFN(
                String pfn, String ns, String name, String version, String site, TCType type) {
            int before = mEntries.size();
            mEntries.removeIf(
                    e ->
                            pfn.equals(e.getPhysicalTransformation())
                                    && matches(e, ns, name, version, site, type));
            return before - mEntries.size();
        }

        @Override
        public int removeByType(TCType type, String site) {
            int before = mEntries.size();
            mEntries.removeIf(
                    e ->
                            (type == null || type.equals(e.getType()))
                                    && (site == null || site.equals(e.getResourceId())));
            return before - mEntries.size();
        }

        @Override
        public int removeBySiteID(String site) {
            int before = mEntries.size();
            mEntries.removeIf(e -> site.equals(e.getResourceId()));
            return before - mEntries.size();
        }

        @Override
        public int removeBySysInfo(SysInfo sysinfo) {
            int before = mEntries.size();
            mEntries.removeIf(e -> sysinfo != null && sysinfo.equals(e.getSysInfo()));
            return before - mEntries.size();
        }

        @Override
        public int clear() {
            int count = mEntries.size();
            mEntries.clear();
            return count;
        }

        @Override
        @SuppressWarnings("rawtypes")
        public int deletePFNProfiles(String pfn, TCType type, String site, List profiles) {
            return 0; // simplified stub
        }

        @Override
        @SuppressWarnings("rawtypes")
        public int deleteLFNProfiles(String ns, String name, String version, List profiles) {
            return 0; // simplified stub
        }

        @Override
        public String getDescription() {
            return "StubTransformationCatalog";
        }

        @Override
        public File getFileSource() {
            return null;
        }

        @Override
        public boolean isTransient() {
            return mTransient;
        }

        void setTransient(boolean t) {
            mTransient = t;
        }
    }

    // -----------------------------------------------------------------------
    // Test fixture
    // -----------------------------------------------------------------------
    private StubTransformationCatalog catalog;

    @BeforeEach
    public void setUp() {
        catalog = new StubTransformationCatalog();
        catalog.connect(new Properties());
    }

    // helper
    private TransformationCatalogEntry entry(
            String ns, String name, String version, String pfn, TCType type, String site) {
        TransformationCatalogEntry e = new TransformationCatalogEntry(ns, name, version);
        e.setPhysicalTransformation(pfn);
        e.setType(type);
        e.setResourceId(site);
        return e;
    }

    // -----------------------------------------------------------------------
    // Constant values
    // -----------------------------------------------------------------------

    @Test
    public void testVersionConstantValue() {
        assertThat(TransformationCatalog.VERSION, equalTo("1.5"));
    }

    @Test
    public void testVersionConstantNotEmpty() {
        assertThat(TransformationCatalog.VERSION, is(not(emptyString())));
    }

    @Test
    public void testPropertyPrefixConstant() {
        assertThat(TransformationCatalog.c_prefix, equalTo("pegasus.catalog.transformation"));
    }

    @Test
    public void testFileKeyConstant() {
        assertThat(TransformationCatalog.FILE_KEY, equalTo("file"));
    }

    @Test
    public void testVariableExpansionKeyConstant() {
        assertThat(TransformationCatalog.VARIABLE_EXPANSION_KEY, equalTo("expand"));
    }

    @Test
    public void testTransientKeyConstant() {
        assertThat(TransformationCatalog.TRANSIENT_KEY, equalTo("transient"));
    }

    // -----------------------------------------------------------------------
    // Constant modifiers
    // -----------------------------------------------------------------------

    @Test
    public void testAllConstantsArePublicStaticFinal() throws NoSuchFieldException {
        String[] names = {
            "VERSION", "c_prefix", "FILE_KEY", "VARIABLE_EXPANSION_KEY", "TRANSIENT_KEY"
        };
        for (String name : names) {
            Field f = TransformationCatalog.class.getDeclaredField(name);
            int mods = f.getModifiers();
            assertThat(Modifier.isPublic(mods), is(true));
            assertThat(Modifier.isStatic(mods), is(true));
            assertThat(Modifier.isFinal(mods), is(true));
        }
    }

    // -----------------------------------------------------------------------
    // Method signatures — query methods
    // -----------------------------------------------------------------------

    @Test
    public void testInitializeMethodSignature() throws NoSuchMethodException {
        Method m = TransformationCatalog.class.getDeclaredMethod("initialize", PegasusBag.class);
        assertThat(m.getReturnType(), is(void.class));
        assertThat(m.getParameterCount(), is(1));
    }

    @Test
    public void testLookupSingleSiteMethodSignature() throws NoSuchMethodException {
        Method m =
                TransformationCatalog.class.getDeclaredMethod(
                        "lookup",
                        String.class,
                        String.class,
                        String.class,
                        String.class,
                        TCType.class);
        assertThat(m.getReturnType(), is(List.class));
        assertThat(m.getParameterCount(), is(5));
    }

    @Test
    public void testLookupMultiSiteMethodSignature() throws NoSuchMethodException {
        Method m =
                TransformationCatalog.class.getDeclaredMethod(
                        "lookup",
                        String.class,
                        String.class,
                        String.class,
                        List.class,
                        TCType.class);
        assertThat(m.getReturnType(), is(List.class));
        assertThat(m.getParameterCount(), is(5));
    }

    @Test
    public void testLookupSitesMethodSignature() throws NoSuchMethodException {
        Method m =
                TransformationCatalog.class.getDeclaredMethod(
                        "lookupSites", String.class, String.class, String.class, TCType.class);
        assertThat(m.getReturnType(), is(List.class));
        assertThat(m.getParameterCount(), is(4));
    }

    @Test
    public void testLookupNoProfilesMethodSignature() throws NoSuchMethodException {
        Method m =
                TransformationCatalog.class.getDeclaredMethod(
                        "lookupNoProfiles",
                        String.class,
                        String.class,
                        String.class,
                        String.class,
                        TCType.class);
        assertThat(m.getReturnType(), is(List.class));
        assertThat(m.getParameterCount(), is(5));
    }

    @Test
    public void testGetTCLogicalNamesMethodSignature() throws NoSuchMethodException {
        Method m =
                TransformationCatalog.class.getDeclaredMethod(
                        "getTCLogicalNames", String.class, TCType.class);
        assertThat(m.getReturnType(), is(List.class));
        assertThat(m.getParameterCount(), is(2));
    }

    @Test
    public void testLookupLFNProfilesMethodSignature() throws NoSuchMethodException {
        Method m =
                TransformationCatalog.class.getDeclaredMethod(
                        "lookupLFNProfiles", String.class, String.class, String.class);
        assertThat(m.getReturnType(), is(List.class));
        assertThat(m.getParameterCount(), is(3));
    }

    @Test
    public void testLookupPFNProfilesMethodSignature() throws NoSuchMethodException {
        Method m =
                TransformationCatalog.class.getDeclaredMethod(
                        "lookupPFNProfiles", String.class, String.class, TCType.class);
        assertThat(m.getReturnType(), is(List.class));
        assertThat(m.getParameterCount(), is(3));
    }

    @Test
    public void testGetContentsMethodSignature() throws NoSuchMethodException {
        Method m = TransformationCatalog.class.getDeclaredMethod("getContents");
        assertThat(m.getReturnType(), is(List.class));
        assertThat(m.getParameterCount(), is(0));
    }

    // -----------------------------------------------------------------------
    // Method signatures — insert / profile methods
    // -----------------------------------------------------------------------

    @Test
    public void testInsertListMethodSignature() throws NoSuchMethodException {
        Method m = TransformationCatalog.class.getDeclaredMethod("insert", List.class);
        assertThat(m.getReturnType(), is(int.class));
        assertThat(m.getParameterCount(), is(1));
    }

    @Test
    public void testInsertEntryMethodSignature() throws NoSuchMethodException {
        Method m =
                TransformationCatalog.class.getDeclaredMethod(
                        "insert", TransformationCatalogEntry.class);
        assertThat(m.getReturnType(), is(int.class));
        assertThat(m.getParameterCount(), is(1));
    }

    @Test
    public void testInsertEntryWithWriteFlagMethodSignature() throws NoSuchMethodException {
        Method m =
                TransformationCatalog.class.getDeclaredMethod(
                        "insert", TransformationCatalogEntry.class, boolean.class);
        assertThat(m.getReturnType(), is(int.class));
        assertThat(m.getParameterCount(), is(2));
    }

    @Test
    public void testInsertAllParamsMethodSignature() throws NoSuchMethodException {
        Method m =
                TransformationCatalog.class.getDeclaredMethod(
                        "insert",
                        String.class,
                        String.class,
                        String.class,
                        String.class,
                        TCType.class,
                        String.class,
                        List.class,
                        List.class,
                        SysInfo.class);
        assertThat(m.getReturnType(), is(int.class));
        assertThat(m.getParameterCount(), is(9));
    }

    @Test
    public void testAddLFNProfileMethodSignature() throws NoSuchMethodException {
        Method m =
                TransformationCatalog.class.getDeclaredMethod(
                        "addLFNProfile", String.class, String.class, String.class, List.class);
        assertThat(m.getReturnType(), is(int.class));
        assertThat(m.getParameterCount(), is(4));
    }

    @Test
    public void testAddPFNProfileMethodSignature() throws NoSuchMethodException {
        Method m =
                TransformationCatalog.class.getDeclaredMethod(
                        "addPFNProfile", String.class, TCType.class, String.class, List.class);
        assertThat(m.getReturnType(), is(int.class));
        assertThat(m.getParameterCount(), is(4));
    }

    // -----------------------------------------------------------------------
    // Method signatures — remove / delete methods
    // -----------------------------------------------------------------------

    @Test
    public void testRemoveByLFNMethodSignature() throws NoSuchMethodException {
        Method m =
                TransformationCatalog.class.getDeclaredMethod(
                        "removeByLFN",
                        String.class,
                        String.class,
                        String.class,
                        String.class,
                        TCType.class);
        assertThat(m.getReturnType(), is(int.class));
        assertThat(m.getParameterCount(), is(5));
    }

    @Test
    public void testRemoveByPFNMethodSignature() throws NoSuchMethodException {
        Method m =
                TransformationCatalog.class.getDeclaredMethod(
                        "removeByPFN",
                        String.class,
                        String.class,
                        String.class,
                        String.class,
                        String.class,
                        TCType.class);
        assertThat(m.getReturnType(), is(int.class));
        assertThat(m.getParameterCount(), is(6));
    }

    @Test
    public void testRemoveByTypeMethodSignature() throws NoSuchMethodException {
        Method m =
                TransformationCatalog.class.getDeclaredMethod(
                        "removeByType", TCType.class, String.class);
        assertThat(m.getReturnType(), is(int.class));
        assertThat(m.getParameterCount(), is(2));
    }

    @Test
    public void testRemoveBySiteIDMethodSignature() throws NoSuchMethodException {
        Method m = TransformationCatalog.class.getDeclaredMethod("removeBySiteID", String.class);
        assertThat(m.getReturnType(), is(int.class));
        assertThat(m.getParameterCount(), is(1));
    }

    @Test
    public void testRemoveBySysInfoMethodSignature() throws NoSuchMethodException {
        Method m = TransformationCatalog.class.getDeclaredMethod("removeBySysInfo", SysInfo.class);
        assertThat(m.getReturnType(), is(int.class));
        assertThat(m.getParameterCount(), is(1));
    }

    @Test
    public void testClearMethodSignature() throws NoSuchMethodException {
        Method m = TransformationCatalog.class.getDeclaredMethod("clear");
        assertThat(m.getReturnType(), is(int.class));
        assertThat(m.getParameterCount(), is(0));
    }

    @Test
    public void testDeletePFNProfilesMethodSignature() throws NoSuchMethodException {
        Method m =
                TransformationCatalog.class.getDeclaredMethod(
                        "deletePFNProfiles", String.class, TCType.class, String.class, List.class);
        assertThat(m.getReturnType(), is(int.class));
        assertThat(m.getParameterCount(), is(4));
    }

    @Test
    public void testDeleteLFNProfilesMethodSignature() throws NoSuchMethodException {
        Method m =
                TransformationCatalog.class.getDeclaredMethod(
                        "deleteLFNProfiles", String.class, String.class, String.class, List.class);
        assertThat(m.getReturnType(), is(int.class));
        assertThat(m.getParameterCount(), is(4));
    }

    // -----------------------------------------------------------------------
    // Method signatures — info methods
    // -----------------------------------------------------------------------

    @Test
    public void testGetDescriptionMethodSignature() throws NoSuchMethodException {
        Method m = TransformationCatalog.class.getDeclaredMethod("getDescription");
        assertThat(m.getReturnType(), is(String.class));
        assertThat(m.getParameterCount(), is(0));
    }

    @Test
    public void testGetFileSourceMethodSignature() throws NoSuchMethodException {
        Method m = TransformationCatalog.class.getDeclaredMethod("getFileSource");
        assertThat(m.getReturnType(), is(File.class));
        assertThat(m.getParameterCount(), is(0));
    }

    @Test
    public void testIsTransientMethodSignature() throws NoSuchMethodException {
        Method m = TransformationCatalog.class.getDeclaredMethod("isTransient");
        assertThat(m.getReturnType(), is(boolean.class));
        assertThat(m.getParameterCount(), is(0));
    }

    // -----------------------------------------------------------------------
    // Exception declarations
    // -----------------------------------------------------------------------

    @Test
    public void testAllQueryAndMutationMethodsDeclareException() throws NoSuchMethodException {
        Method[] methods = {
            TransformationCatalog.class.getDeclaredMethod(
                    "lookup", String.class, String.class, String.class, String.class, TCType.class),
            TransformationCatalog.class.getDeclaredMethod(
                    "lookup", String.class, String.class, String.class, List.class, TCType.class),
            TransformationCatalog.class.getDeclaredMethod(
                    "lookupSites", String.class, String.class, String.class, TCType.class),
            TransformationCatalog.class.getDeclaredMethod(
                    "lookupNoProfiles",
                    String.class,
                    String.class,
                    String.class,
                    String.class,
                    TCType.class),
            TransformationCatalog.class.getDeclaredMethod(
                    "getTCLogicalNames", String.class, TCType.class),
            TransformationCatalog.class.getDeclaredMethod(
                    "lookupLFNProfiles", String.class, String.class, String.class),
            TransformationCatalog.class.getDeclaredMethod(
                    "lookupPFNProfiles", String.class, String.class, TCType.class),
            TransformationCatalog.class.getDeclaredMethod("getContents"),
            TransformationCatalog.class.getDeclaredMethod("insert", List.class),
            TransformationCatalog.class.getDeclaredMethod(
                    "insert", TransformationCatalogEntry.class),
            TransformationCatalog.class.getDeclaredMethod(
                    "insert", TransformationCatalogEntry.class, boolean.class),
            TransformationCatalog.class.getDeclaredMethod(
                    "insert",
                    String.class,
                    String.class,
                    String.class,
                    String.class,
                    TCType.class,
                    String.class,
                    List.class,
                    List.class,
                    SysInfo.class),
            TransformationCatalog.class.getDeclaredMethod(
                    "addLFNProfile", String.class, String.class, String.class, List.class),
            TransformationCatalog.class.getDeclaredMethod(
                    "addPFNProfile", String.class, TCType.class, String.class, List.class),
            TransformationCatalog.class.getDeclaredMethod(
                    "removeByLFN",
                    String.class,
                    String.class,
                    String.class,
                    String.class,
                    TCType.class),
            TransformationCatalog.class.getDeclaredMethod(
                    "removeByPFN",
                    String.class,
                    String.class,
                    String.class,
                    String.class,
                    String.class,
                    TCType.class),
            TransformationCatalog.class.getDeclaredMethod(
                    "removeByType", TCType.class, String.class),
            TransformationCatalog.class.getDeclaredMethod("removeBySiteID", String.class),
            TransformationCatalog.class.getDeclaredMethod("removeBySysInfo", SysInfo.class),
            TransformationCatalog.class.getDeclaredMethod("clear"),
            TransformationCatalog.class.getDeclaredMethod(
                    "deletePFNProfiles", String.class, TCType.class, String.class, List.class),
            TransformationCatalog.class.getDeclaredMethod(
                    "deleteLFNProfiles", String.class, String.class, String.class, List.class),
        };
        for (Method m : methods) {
            List<Class<?>> exTypes = Arrays.asList(m.getExceptionTypes());
            assertThat(exTypes, hasItem(Exception.class));
        }
    }

    // -----------------------------------------------------------------------
    // Catalog lifecycle
    // -----------------------------------------------------------------------

    @Test
    public void testIsClosedBeforeConnect() {
        StubTransformationCatalog fresh = new StubTransformationCatalog();
        assertTrue(fresh.isClosed());
    }

    @Test
    public void testConnectOpensTheCatalog() {
        StubTransformationCatalog fresh = new StubTransformationCatalog();
        assertTrue(fresh.connect(new Properties()));
        assertFalse(fresh.isClosed());
    }

    @Test
    public void testCloseTransitionsToClosed() {
        catalog.close();
        assertTrue(catalog.isClosed());
    }

    // -----------------------------------------------------------------------
    // insert / getContents
    // -----------------------------------------------------------------------

    @Test
    public void testInsertEntry_returnsOne() throws Exception {
        assertThat(
                catalog.insert(
                        entry(
                                "pegasus",
                                "myjob",
                                "1.0",
                                "/usr/bin/myjob",
                                TCType.INSTALLED,
                                "site1")),
                is(1));
    }

    @Test
    public void testInsertEntry_appearsInGetContents() throws Exception {
        TransformationCatalogEntry e =
                entry("pegasus", "myjob", "1.0", "/usr/bin/myjob", TCType.INSTALLED, "site1");
        catalog.insert(e);
        assertThat(catalog.getContents(), hasSize(1));
        assertSame(e, catalog.getContents().get(0));
    }

    @Test
    public void testInsertList_returnsCount() throws Exception {
        List<TransformationCatalogEntry> entries =
                Arrays.asList(
                        entry("ns", "job1", "1.0", "/bin/job1", TCType.INSTALLED, "site1"),
                        entry("ns", "job2", "1.0", "/bin/job2", TCType.INSTALLED, "site1"));
        assertThat(catalog.insert(entries), is(2));
        assertThat(catalog.getContents(), hasSize(2));
    }

    @Test
    public void testInsertWithWriteFlag_returnsOne() throws Exception {
        TransformationCatalogEntry e =
                entry("ns", "job", "1.0", "/bin/job", TCType.INSTALLED, "site1");
        assertThat(catalog.insert(e, true), is(1));
        assertThat(catalog.getContents(), hasSize(1));
    }

    @Test
    public void testInsertWithWriteFalse_stillInsertsToMemory() throws Exception {
        TransformationCatalogEntry e =
                entry("ns", "job", "1.0", "/bin/job", TCType.INSTALLED, "site1");
        catalog.insert(e, false);
        assertThat(catalog.getContents(), hasSize(1));
    }

    @Test
    public void testInsertAllParams_returnsOne() throws Exception {
        int n =
                catalog.insert(
                        "ns",
                        "job",
                        "1.0",
                        "/bin/job",
                        TCType.INSTALLED,
                        "site1",
                        null,
                        null,
                        null);
        assertThat(n, is(1));
        assertThat(catalog.getContents(), hasSize(1));
    }

    @Test
    public void testGetContents_emptyInitially() throws Exception {
        assertTrue(catalog.getContents().isEmpty());
    }

    // -----------------------------------------------------------------------
    // lookup
    // -----------------------------------------------------------------------

    @Test
    public void testLookup_byExactKey_returnsEntry() throws Exception {
        catalog.insert(
                entry("pegasus", "myjob", "1.0", "/usr/bin/myjob", TCType.INSTALLED, "site1"));
        List<TransformationCatalogEntry> result =
                catalog.lookup("pegasus", "myjob", "1.0", "site1", TCType.INSTALLED);
        assertNotNull(result);
        assertThat(result, hasSize(1));
        assertThat(result.get(0).getLogicalName(), equalTo("myjob"));
    }

    @Test
    public void testLookup_nullSiteAndType_returnsAll() throws Exception {
        catalog.insert(entry("ns", "job", "1.0", "/bin/a", TCType.INSTALLED, "site1"));
        catalog.insert(entry("ns", "job", "1.0", "/bin/b", TCType.STAGEABLE, "site2"));
        List<TransformationCatalogEntry> result =
                catalog.lookup("ns", "job", "1.0", (String) null, null);
        assertNotNull(result);
        assertThat(result, hasSize(2));
    }

    @Test
    public void testLookup_noMatch_returnsNull() throws Exception {
        assertNull(catalog.lookup("ns", "ghost", "1.0", "site1", TCType.INSTALLED));
    }

    @Test
    public void testLookupMultiSite_returnsEntriesForAllSites() throws Exception {
        catalog.insert(entry("ns", "job", "1.0", "/bin/a", TCType.INSTALLED, "site1"));
        catalog.insert(entry("ns", "job", "1.0", "/bin/b", TCType.INSTALLED, "site2"));
        List<TransformationCatalogEntry> result =
                catalog.lookup(
                        "ns", "job", "1.0", Arrays.asList("site1", "site2"), TCType.INSTALLED);
        assertNotNull(result);
        assertThat(result, hasSize(2));
    }

    @Test
    public void testLookupMultiSite_nullSites_returnsAll() throws Exception {
        catalog.insert(entry("ns", "job", "1.0", "/bin/a", TCType.INSTALLED, "site1"));
        catalog.insert(entry("ns", "job", "1.0", "/bin/b", TCType.INSTALLED, "site2"));
        List<TransformationCatalogEntry> result =
                catalog.lookup("ns", "job", "1.0", (List<?>) null, TCType.INSTALLED);
        assertNotNull(result);
        assertThat(result, hasSize(2));
    }

    // -----------------------------------------------------------------------
    // lookupSites
    // -----------------------------------------------------------------------

    @Test
    public void testLookupSites_returnsSiteIds() throws Exception {
        catalog.insert(entry("ns", "job", "1.0", "/bin/a", TCType.INSTALLED, "site1"));
        catalog.insert(entry("ns", "job", "1.0", "/bin/b", TCType.INSTALLED, "site2"));
        List<String> sites = catalog.lookupSites("ns", "job", "1.0", null);
        assertNotNull(sites);
        assertThat(sites, hasItem("site1"));
        assertThat(sites, hasItem("site2"));
    }

    @Test
    public void testLookupSites_noMatch_returnsNull() throws Exception {
        assertNull(catalog.lookupSites("ns", "ghost", "1.0", TCType.INSTALLED));
    }

    // -----------------------------------------------------------------------
    // lookupNoProfiles
    // -----------------------------------------------------------------------

    @Test
    public void testLookupNoProfiles_returnsEntryWithPfnAndSite() throws Exception {
        catalog.insert(entry("ns", "job", "1.0", "/bin/job", TCType.INSTALLED, "site1"));
        List<TransformationCatalogEntry> result =
                catalog.lookupNoProfiles("ns", "job", "1.0", "site1", TCType.INSTALLED);
        assertNotNull(result);
        assertThat(result, hasSize(1));
        assertThat(result.get(0).getPhysicalTransformation(), equalTo("/bin/job"));
        assertThat(result.get(0).getResourceId(), equalTo("site1"));
    }

    @Test
    public void testLookupNoProfiles_noMatch_returnsNull() throws Exception {
        assertNull(catalog.lookupNoProfiles("ns", "ghost", "1.0", "site1", TCType.INSTALLED));
    }

    // -----------------------------------------------------------------------
    // getTCLogicalNames
    // -----------------------------------------------------------------------

    @Test
    public void testGetTCLogicalNames_returnsArraysForSite() throws Exception {
        catalog.insert(entry("ns", "job", "1.0", "/bin/job", TCType.INSTALLED, "site1"));
        List<String[]> result = catalog.getTCLogicalNames("site1", null);
        assertNotNull(result);
        assertThat(result, hasSize(1));
        assertThat(result.get(0)[0], equalTo("site1"));
        assertThat(result.get(0)[1], containsString("job"));
    }

    @Test
    public void testGetTCLogicalNames_noMatch_returnsNull() throws Exception {
        assertNull(catalog.getTCLogicalNames("ghostsite", null));
    }

    // -----------------------------------------------------------------------
    // removeByLFN
    // -----------------------------------------------------------------------

    @Test
    public void testRemoveByLFN_exactMatch_removesEntry() throws Exception {
        catalog.insert(entry("ns", "job", "1.0", "/bin/job", TCType.INSTALLED, "site1"));
        int removed = catalog.removeByLFN("ns", "job", "1.0", "site1", TCType.INSTALLED);
        assertEquals(1, removed);
        assertTrue(catalog.getContents().isEmpty());
    }

    @Test
    public void testRemoveByLFN_nullSiteAndType_removesAll() throws Exception {
        catalog.insert(entry("ns", "job", "1.0", "/bin/a", TCType.INSTALLED, "site1"));
        catalog.insert(entry("ns", "job", "1.0", "/bin/b", TCType.STAGEABLE, "site2"));
        int removed = catalog.removeByLFN("ns", "job", "1.0", null, null);
        assertEquals(2, removed);
        assertTrue(catalog.getContents().isEmpty());
    }

    @Test
    public void testRemoveByLFN_noMatch_returnsZero() throws Exception {
        catalog.insert(entry("ns", "job", "1.0", "/bin/job", TCType.INSTALLED, "site1"));
        assertEquals(0, catalog.removeByLFN("ns", "ghost", "1.0", null, null));
        assertEquals(1, catalog.getContents().size());
    }

    // -----------------------------------------------------------------------
    // removeByPFN
    // -----------------------------------------------------------------------

    @Test
    public void testRemoveByPFN_removesMatchingEntry() throws Exception {
        catalog.insert(entry("ns", "job", "1.0", "/bin/job", TCType.INSTALLED, "site1"));
        int removed =
                catalog.removeByPFN("/bin/job", "ns", "job", "1.0", "site1", TCType.INSTALLED);
        assertEquals(1, removed);
        assertTrue(catalog.getContents().isEmpty());
    }

    @Test
    public void testRemoveByPFN_pfnMismatch_returnsZero() throws Exception {
        catalog.insert(entry("ns", "job", "1.0", "/bin/job", TCType.INSTALLED, "site1"));
        assertEquals(
                0,
                catalog.removeByPFN("/bin/other", "ns", "job", "1.0", "site1", TCType.INSTALLED));
        assertEquals(1, catalog.getContents().size());
    }

    // -----------------------------------------------------------------------
    // removeByType
    // -----------------------------------------------------------------------

    @Test
    public void testRemoveByType_removesAllOfType() throws Exception {
        catalog.insert(entry("ns", "job1", "1.0", "/bin/a", TCType.INSTALLED, "site1"));
        catalog.insert(entry("ns", "job2", "1.0", "/bin/b", TCType.STAGEABLE, "site1"));
        int removed = catalog.removeByType(TCType.INSTALLED, null);
        assertEquals(1, removed);
        assertEquals(1, catalog.getContents().size());
        assertEquals(TCType.STAGEABLE, catalog.getContents().get(0).getType());
    }

    @Test
    public void testRemoveByType_withSiteFilter_onlyRemovesFromSite() throws Exception {
        catalog.insert(entry("ns", "job", "1.0", "/bin/a", TCType.INSTALLED, "site1"));
        catalog.insert(entry("ns", "job", "1.0", "/bin/b", TCType.INSTALLED, "site2"));
        int removed = catalog.removeByType(TCType.INSTALLED, "site1");
        assertEquals(1, removed);
        assertEquals("site2", catalog.getContents().get(0).getResourceId());
    }

    // -----------------------------------------------------------------------
    // removeBySiteID
    // -----------------------------------------------------------------------

    @Test
    public void testRemoveBySiteID_removesAllForSite() throws Exception {
        catalog.insert(entry("ns", "job1", "1.0", "/bin/a", TCType.INSTALLED, "site1"));
        catalog.insert(entry("ns", "job2", "1.0", "/bin/b", TCType.INSTALLED, "site1"));
        catalog.insert(entry("ns", "job3", "1.0", "/bin/c", TCType.INSTALLED, "site2"));
        int removed = catalog.removeBySiteID("site1");
        assertEquals(2, removed);
        assertEquals(1, catalog.getContents().size());
        assertEquals("site2", catalog.getContents().get(0).getResourceId());
    }

    @Test
    public void testRemoveBySiteID_unknownSite_returnsZero() throws Exception {
        catalog.insert(entry("ns", "job", "1.0", "/bin/job", TCType.INSTALLED, "site1"));
        assertEquals(0, catalog.removeBySiteID("ghost"));
        assertEquals(1, catalog.getContents().size());
    }

    // -----------------------------------------------------------------------
    // clear
    // -----------------------------------------------------------------------

    @Test
    public void testClear_removesAllAndReturnsCount() throws Exception {
        catalog.insert(entry("ns", "job1", "1.0", "/bin/a", TCType.INSTALLED, "site1"));
        catalog.insert(entry("ns", "job2", "1.0", "/bin/b", TCType.INSTALLED, "site1"));
        int removed = catalog.clear();
        assertEquals(2, removed);
        assertTrue(catalog.getContents().isEmpty());
    }

    @Test
    public void testClear_emptyReturnsZero() throws Exception {
        assertEquals(0, catalog.clear());
    }

    // -----------------------------------------------------------------------
    // profile methods — smoke tests
    // -----------------------------------------------------------------------

    @Test
    public void testAddLFNProfile_emptyList_returnsZero() throws Exception {
        catalog.insert(entry("ns", "job", "1.0", "/bin/job", TCType.INSTALLED, "site1"));
        assertEquals(0, catalog.addLFNProfile("ns", "job", "1.0", Collections.emptyList()));
    }

    @Test
    public void testAddPFNProfile_emptyList_returnsZero() throws Exception {
        catalog.insert(entry("ns", "job", "1.0", "/bin/job", TCType.INSTALLED, "site1"));
        assertEquals(
                0,
                catalog.addPFNProfile(
                        "/bin/job", TCType.INSTALLED, "site1", Collections.emptyList()));
    }

    @Test
    public void testDeleteLFNProfiles_doesNotThrow() throws Exception {
        catalog.insert(entry("ns", "job", "1.0", "/bin/job", TCType.INSTALLED, "site1"));
        assertDoesNotThrow(() -> catalog.deleteLFNProfiles("ns", "job", "1.0", null));
    }

    @Test
    public void testDeletePFNProfiles_doesNotThrow() throws Exception {
        catalog.insert(entry("ns", "job", "1.0", "/bin/job", TCType.INSTALLED, "site1"));
        assertDoesNotThrow(
                () -> catalog.deletePFNProfiles("/bin/job", TCType.INSTALLED, "site1", null));
    }

    // -----------------------------------------------------------------------
    // getDescription / isTransient / getFileSource
    // -----------------------------------------------------------------------

    @Test
    public void testGetDescription_returnsNonNull() {
        assertNotNull(catalog.getDescription());
        assertFalse(catalog.getDescription().isEmpty());
    }

    @Test
    public void testIsTransient_defaultFalse() {
        assertFalse(catalog.isTransient());
    }

    @Test
    public void testIsTransient_canBeSetToTrue() {
        catalog.setTransient(true);
        assertTrue(catalog.isTransient());
    }

    @Test
    public void testGetFileSource_returnsNull() {
        assertNull(catalog.getFileSource());
    }

    @Test
    public void testTransformationCatalogExtendsCatalog() {
        assertTrue(Catalog.class.isAssignableFrom(TransformationCatalog.class));
    }

    @Test
    public void testAllDeclaredMethodsArePublicAndAbstract() {
        for (Method method : TransformationCatalog.class.getDeclaredMethods()) {
            int mods = method.getModifiers();
            assertTrue(Modifier.isPublic(mods), method.getName() + " should be public");
            assertTrue(Modifier.isAbstract(mods), method.getName() + " should be abstract");
        }
    }

    @Test
    public void testTransformationCatalogDeclaresExpectedNumberOfMethods() {
        assertThat(TransformationCatalog.class.getDeclaredMethods().length, is(26));
    }
}
