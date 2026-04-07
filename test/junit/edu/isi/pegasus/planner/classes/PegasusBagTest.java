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
package edu.isi.pegasus.planner.classes;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;

import edu.isi.pegasus.common.logging.LogFormatter;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.TransformationCatalog;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.catalog.transformation.Mapper;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.mapper.MapperException;
import edu.isi.pegasus.planner.mapper.StagingMapper;
import edu.isi.pegasus.planner.mapper.SubmitMapper;
import java.io.File;
import java.io.PrintStream;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class PegasusBagTest {

    private static final class NoOpLogManager extends LogManager {
        private int mLevel;

        @Override
        public void initialize(LogFormatter formatter, Properties properties) {}

        @Override
        public void configure(boolean prefixTimestamp) {}

        @Override
        protected void setLevel(int level, boolean info) {
            mLevel = level;
        }

        @Override
        public int getLevel() {
            return mLevel;
        }

        @Override
        public void setWriters(String out) {}

        @Override
        public void setWriter(STREAM_TYPE type, PrintStream ps) {}

        @Override
        public PrintStream getWriter(STREAM_TYPE type) {
            return null;
        }

        @Override
        public void log(String message, Exception e, int level) {}

        @Override
        public void log(String message, int level) {}

        @Override
        protected void logAlreadyFormattedMessage(String message, int level) {}

        @Override
        public void logEventCompletion(int level) {}
    }

    private static final class TestingMapper extends Mapper {
        TestingMapper(PegasusBag bag) {
            super(bag);
        }

        @Override
        public Map getSiteMap(
                String namespace, String name, String version, java.util.List siteids) {
            return null;
        }

        @Override
        public String getMode() {
            return "test";
        }
    }

    private SubmitMapper createSubmitMapper() {
        return new SubmitMapper() {
            @Override
            public String description() {
                return "submit";
            }

            @Override
            public void initialize(PegasusBag bag, Properties properties, File base) {}

            @Override
            public File getRelativeDir(Job job) {
                return null;
            }

            @Override
            public File getDir(Job job) {
                return null;
            }
        };
    }

    private StagingMapper createStagingMapper() {
        return new StagingMapper() {
            @Override
            public String description() {
                return "staging";
            }

            @Override
            public void initialize(PegasusBag bag, Properties properties) {}

            @Override
            public File mapToRelativeDirectory(
                    Job job,
                    edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry site,
                    String lfn) {
                return null;
            }

            @Override
            public File getRelativeDirectory(String site, String lfn) {
                return null;
            }

            @Override
            public String map(
                    Job job,
                    File addOn,
                    edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry site,
                    edu.isi.pegasus.planner.catalog.site.classes.FileServer.OPERATION operation,
                    String lfn)
                    throws MapperException {
                return null;
            }
        };
    }

    private TransformationCatalog createTransformationCatalog() {
        return (TransformationCatalog)
                Proxy.newProxyInstance(
                        TransformationCatalog.class.getClassLoader(),
                        new Class<?>[] {TransformationCatalog.class},
                        (proxy, method, args) -> defaultValue(method.getReturnType()));
    }

    private Object defaultValue(Class<?> type) {
        if (!type.isPrimitive()) {
            return null;
        }
        if (Boolean.TYPE.equals(type)) {
            return Boolean.FALSE;
        }
        if (Integer.TYPE.equals(type)) {
            return 0;
        }
        if (Long.TYPE.equals(type)) {
            return 0L;
        }
        if (Float.TYPE.equals(type)) {
            return 0.0f;
        }
        if (Double.TYPE.equals(type)) {
            return 0.0d;
        }
        if (Short.TYPE.equals(type)) {
            return (short) 0;
        }
        if (Byte.TYPE.equals(type)) {
            return (byte) 0;
        }
        if (Character.TYPE.equals(type)) {
            return (char) 0;
        }
        return null;
    }

    @Test
    public void testDefaultConstructorNotNull() {
        PegasusBag bag = new PegasusBag();
        assertThat(bag, is(notNullValue()));
    }

    @Test
    public void testDefaultUsesPMCIsFalse() {
        PegasusBag bag = new PegasusBag();
        assertThat(bag.plannerUsesPMC(), is(false));
    }

    @Test
    public void testDefaultPlannerDirectoryIsUserDir() {
        PegasusBag bag = new PegasusBag();
        File dir = bag.getPlannerDirectory();
        assertThat(dir, is(notNullValue()));
        assertThat(dir, is(new File(System.getProperty("user.dir"))));
    }

    @Test
    public void testAddAndGetPegasusProperties() {
        PegasusBag bag = new PegasusBag();
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, props);
        assertThat(bag.getPegasusProperties(), is(sameInstance(props)));
    }

    @Test
    public void testGetPegasusPropertiesNullByDefault() {
        PegasusBag bag = new PegasusBag();
        assertThat(bag.getPegasusProperties(), is(org.hamcrest.Matchers.nullValue()));
    }

    @Test
    public void testAddPegasusPropertiesStoresOriginalCopy() {
        PegasusBag bag = new PegasusBag();
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, props);
        // original props should be stored on first set
        assertThat(bag.getOriginalPegasusProperties(), is(notNullValue()));
    }

    @Test
    public void testAddPlannerOptions() {
        PegasusBag bag = new PegasusBag();
        PlannerOptions options = new PlannerOptions();
        bag.add(PegasusBag.PLANNER_OPTIONS, options);
        assertThat(bag.getPlannerOptions(), is(sameInstance(options)));
    }

    @Test
    public void testGetPlannerOptionsNullByDefault() {
        PegasusBag bag = new PegasusBag();
        assertThat(bag.getPlannerOptions(), is(org.hamcrest.Matchers.nullValue()));
    }

    @Test
    public void testAddReplicaCatalogFileSource() {
        PegasusBag bag = new PegasusBag();
        File rcFile = new File("/tmp/replica.yml");
        bag.add(PegasusBag.REPLICA_CATALOG_FILE_SOURCE, rcFile);
        assertThat(bag.getReplicaCatalogFileSource(), is(rcFile));
    }

    @Test
    public void testGetReplicaCatalogFileSourceNullByDefault() {
        PegasusBag bag = new PegasusBag();
        assertThat(bag.getReplicaCatalogFileSource(), is(nullValue()));
    }

    @Test
    public void testAddWorkerPackageMap() {
        PegasusBag bag = new PegasusBag();
        Map<String, String> wpMap = new HashMap<String, String>();
        wpMap.put("siteA", "/path/to/worker.tar.gz");
        bag.add(PegasusBag.WORKER_PACKAGE_MAP, wpMap);
        Map<String, String> retrieved = bag.getWorkerPackageMap();
        assertThat(retrieved, is(notNullValue()));
        assertThat(retrieved.get("siteA"), is("/path/to/worker.tar.gz"));
    }

    @Test
    public void testGetWorkerPackageMapNullByDefault() {
        PegasusBag bag = new PegasusBag();
        assertThat(bag.getWorkerPackageMap(), is(nullValue()));
    }

    @Test
    public void testAddUsesPMCTrue() {
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.USES_PMC, Boolean.TRUE);
        assertThat(bag.plannerUsesPMC(), is(true));
    }

    @Test
    public void testAddUsesPMCFalse() {
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.USES_PMC, Boolean.TRUE);
        bag.add(PegasusBag.USES_PMC, Boolean.FALSE);
        assertThat(bag.plannerUsesPMC(), is(false));
    }

    @Test
    public void testAddPlannerDirectory() {
        PegasusBag bag = new PegasusBag();
        File dir = new File("/tmp/planner-dir");
        bag.add(PegasusBag.PLANNER_DIRECTORY, dir);
        assertThat(bag.getPlannerDirectory(), is(dir));
    }

    @Test
    public void testAddInvalidTypeForKeyThrowsRuntimeException() {
        PegasusBag bag = new PegasusBag();
        // PEGASUS_PROPERTIES key expects PegasusProperties, not a String
        // When value is non-null but wrong type, RuntimeException is thrown
        assertThrows(
                RuntimeException.class,
                () -> bag.add(PegasusBag.PEGASUS_PROPERTIES, "not-a-properties-object"));
    }

    @Test
    public void testAddNullValueForKeyReturnsFalse() {
        PegasusBag bag = new PegasusBag();
        boolean result = bag.add(PegasusBag.PEGASUS_PROPERTIES, null);
        assertThat(result, is(false));
    }

    @Test
    public void testAddValidTypeReturnsTrue() {
        PegasusBag bag = new PegasusBag();
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        boolean result = bag.add(PegasusBag.PEGASUS_PROPERTIES, props);
        assertThat(result, is(true));
    }

    @Test
    public void testAddInvalidKeyThrowsRuntimeException() {
        PegasusBag bag = new PegasusBag();
        assertThrows(RuntimeException.class, () -> bag.add(999, "some-value"));
    }

    @Test
    public void testGetWithInvalidKeyThrowsRuntimeException() {
        PegasusBag bag = new PegasusBag();
        assertThrows(RuntimeException.class, () -> bag.get(999));
    }

    @Test
    public void testContainsKeyForValidKeys() {
        PegasusBag bag = new PegasusBag();
        assertThat(bag.containsKey(PegasusBag.PEGASUS_PROPERTIES), is(true));
        assertThat(bag.containsKey(PegasusBag.PLANNER_OPTIONS), is(true));
        assertThat(bag.containsKey(PegasusBag.REPLICA_CATALOG_FILE_SOURCE), is(true));
        assertThat(bag.containsKey(PegasusBag.SITE_CATALOG), is(true));
        assertThat(bag.containsKey(PegasusBag.TRANSFORMATION_CATALOG), is(true));
        assertThat(bag.containsKey(PegasusBag.TRANSFORMATION_MAPPER), is(true));
        assertThat(bag.containsKey(PegasusBag.PEGASUS_LOGMANAGER), is(true));
        assertThat(bag.containsKey(PegasusBag.SITE_STORE), is(true));
        assertThat(bag.containsKey(PegasusBag.PLANNER_CACHE), is(true));
        assertThat(bag.containsKey(PegasusBag.WORKER_PACKAGE_MAP), is(true));
        assertThat(bag.containsKey(PegasusBag.USES_PMC), is(true));
        assertThat(bag.containsKey(PegasusBag.PLANNER_METRICS), is(true));
    }

    @Test
    public void testContainsKeyReturnsFalseForInvalidKey() {
        PegasusBag bag = new PegasusBag();
        assertThat(bag.containsKey(999), is(false));
        assertThat(bag.containsKey(-1), is(false));
    }

    @Test
    public void testContainsKeyReturnsFalseForNonIntegerKey() {
        PegasusBag bag = new PegasusBag();
        assertThat(bag.containsKey("not-an-integer"), is(false));
    }

    @Test
    public void testGetReturnsNullForUnsetProperty() {
        PegasusBag bag = new PegasusBag();
        assertThat(bag.get(PegasusBag.PLANNER_OPTIONS), is(nullValue()));
    }

    @Test
    public void testAddSitesCatalogKeyThrowsRuntimeException() {
        PegasusBag bag = new PegasusBag();
        // SITE_CATALOG always sets valid=false; since value is non-null, RuntimeException is thrown
        assertThrows(RuntimeException.class, () -> bag.add(PegasusBag.SITE_CATALOG, new Object()));
    }

    @Test
    public void testPegasusInfoArrayHasCorrectSize() {
        assertThat(PegasusBag.PEGASUS_INFO.length, is(15));
    }

    @Test
    public void testConvenienceMethodGetPlannerOptions() {
        PegasusBag bag = new PegasusBag();
        PlannerOptions options = new PlannerOptions();
        bag.add(PegasusBag.PLANNER_OPTIONS, options);
        PlannerOptions retrieved = bag.getPlannerOptions();
        assertThat(retrieved, is(sameInstance(options)));
    }

    @Test
    public void testOriginalPropertiesNotOverwrittenOnSecondSet() {
        PegasusBag bag = new PegasusBag();
        PegasusProperties props1 = PegasusProperties.nonSingletonInstance();
        PegasusProperties props2 = PegasusProperties.nonSingletonInstance();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, props1);
        PegasusProperties original = bag.getOriginalPegasusProperties();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, props2);
        // original should remain the same object captured on first set
        assertThat(bag.getOriginalPegasusProperties(), is(notNullValue()));
        assertThat(bag.getOriginalPegasusProperties(), is(sameInstance(original)));
    }

    @Test
    public void testAddAndGetLogger() {
        PegasusBag bag = new PegasusBag();
        NoOpLogManager logger = new NoOpLogManager();
        logger.setLevel(Level.DEBUG);

        assertThat(bag.add(PegasusBag.PEGASUS_LOGMANAGER, logger), is(true));
        assertThat(bag.getLogger(), is(sameInstance(logger)));
        assertThat(bag.get(PegasusBag.PEGASUS_LOGMANAGER), is(sameInstance(logger)));
    }

    @Test
    public void testAddAndGetSiteStore() {
        PegasusBag bag = new PegasusBag();
        SiteStore store = new SiteStore();

        assertThat(bag.add(PegasusBag.SITE_STORE, store), is(true));
        assertThat(bag.getHandleToSiteStore(), is(sameInstance(store)));
        assertThat(bag.get(PegasusBag.SITE_STORE), is(sameInstance(store)));
    }

    @Test
    public void testAddAndGetTransformationCatalog() {
        PegasusBag bag = new PegasusBag();
        TransformationCatalog catalog = createTransformationCatalog();

        assertThat(bag.add(PegasusBag.TRANSFORMATION_CATALOG, catalog), is(true));
        assertThat(bag.getHandleToTransformationCatalog(), is(sameInstance(catalog)));
        assertThat(bag.get(PegasusBag.TRANSFORMATION_CATALOG), is(sameInstance(catalog)));
    }

    @Test
    public void testAddAndGetTransformationMapper() {
        PegasusBag bag = new PegasusBag();
        Mapper mapper = new TestingMapper(bag);

        assertThat(bag.add(PegasusBag.TRANSFORMATION_MAPPER, mapper), is(true));
        assertThat(bag.getHandleToTransformationMapper(), is(sameInstance(mapper)));
        assertThat(bag.get(PegasusBag.TRANSFORMATION_MAPPER), is(sameInstance(mapper)));
    }

    @Test
    public void testAddAndGetPlannerCache() {
        PegasusBag bag = new PegasusBag();
        PlannerCache cache = new PlannerCache();

        assertThat(bag.add(PegasusBag.PLANNER_CACHE, cache), is(true));
        assertThat(bag.getHandleToPlannerCache(), is(sameInstance(cache)));
        assertThat(bag.get(PegasusBag.PLANNER_CACHE), is(sameInstance(cache)));
    }

    @Test
    public void testAddAndGetPlannerMetrics() {
        PegasusBag bag = new PegasusBag();
        PlannerMetrics metrics = new PlannerMetrics();

        assertThat(bag.add(PegasusBag.PLANNER_METRICS, metrics), is(true));
        assertThat(bag.get(PegasusBag.PLANNER_METRICS), is(sameInstance(metrics)));
    }

    @Test
    public void testAddAndGetSubmitMapper() {
        PegasusBag bag = new PegasusBag();
        SubmitMapper mapper = createSubmitMapper();

        assertThat(bag.add(PegasusBag.PEGASUS_SUBMIT_MAPPER, mapper), is(true));
        assertThat(bag.getSubmitMapper(), is(sameInstance(mapper)));
        assertThat(bag.get(PegasusBag.PEGASUS_SUBMIT_MAPPER), is(sameInstance(mapper)));
    }

    @Test
    public void testAddAndGetStagingMapper() {
        PegasusBag bag = new PegasusBag();
        StagingMapper mapper = createStagingMapper();

        assertThat(bag.add(PegasusBag.PEGASUS_STAGING_MAPPER, mapper), is(true));
        assertThat(bag.getStagingMapper(), is(sameInstance(mapper)));
        assertThat(bag.get(PegasusBag.PEGASUS_STAGING_MAPPER), is(sameInstance(mapper)));
    }

    @Test
    public void testGetUsesPMCReturnsBooleanWrapper() {
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.USES_PMC, Boolean.TRUE);

        assertThat(bag.get(PegasusBag.USES_PMC), is(Boolean.TRUE));
    }

    @Test
    public void testContainsKeyReturnsFalseForSupportedHighKeys() {
        PegasusBag bag = new PegasusBag();

        assertThat(bag.containsKey(PegasusBag.PEGASUS_SUBMIT_MAPPER), is(false));
        assertThat(bag.containsKey(PegasusBag.PEGASUS_STAGING_MAPPER), is(false));
        assertThat(bag.containsKey(PegasusBag.PLANNER_DIRECTORY), is(false));
    }

    @Test
    public void testCloneReturnsDistinctBagWithSameStoredReferences() {
        PegasusBag bag = new PegasusBag();
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        PlannerOptions options = new PlannerOptions();
        PlannerCache cache = new PlannerCache();
        File plannerDir = new File("/tmp/clone-dir");
        bag.add(PegasusBag.PEGASUS_PROPERTIES, props);
        bag.add(PegasusBag.PLANNER_OPTIONS, options);
        bag.add(PegasusBag.PLANNER_CACHE, cache);
        bag.add(PegasusBag.PLANNER_DIRECTORY, plannerDir);

        PegasusBag cloned = (PegasusBag) bag.clone();

        assertThat(cloned, is(not(sameInstance(bag))));
        assertThat(cloned.getPegasusProperties(), is(sameInstance(props)));
        assertThat(
                cloned.getOriginalPegasusProperties(),
                is(sameInstance(bag.getOriginalPegasusProperties())));
        assertThat(cloned.getPlannerOptions(), is(sameInstance(options)));
        assertThat(cloned.getHandleToPlannerCache(), is(sameInstance(cache)));
        assertThat(cloned.getPlannerDirectory(), is(sameInstance(plannerDir)));
    }
}
