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
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import edu.isi.pegasus.common.logging.LogFormatter;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.ReplicaCatalog;
import edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.FileServerType.OPERATION;
import edu.isi.pegasus.planner.common.PegasusProperties;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Properties;
import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.test.util.ReflectionTestUtils;

/** @author Rajiv Mayani */
public class PlannerCacheTest {

    private PlannerCache mCache;

    @TempDir Path mTempDir;

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

    @BeforeEach
    public void setUp() {
        mCache = new PlannerCache();
    }

    @AfterEach
    public void tearDown() {
        // close is safe to call even before initialize
        mCache.close();
    }

    // --- Constants ---

    @Test
    public void plannerCacheReplicaCatalogKeyIsFile() {
        assertThat(PlannerCache.PLANNER_CACHE_REPLICA_CATALOG_KEY, is("file"));
    }

    @Test
    public void plannerCacheReplicaCatalogImplementerIsSimpleFile() {
        assertThat(PlannerCache.PLANNER_CACHE_REPLICA_CATALOG_IMPLEMENTER, is("SimpleFile"));
    }

    // --- Construction ---

    @Test
    public void defaultConstructorDoesNotThrow() {
        // Should construct without any exception
        assertThat(mCache, is(notNullValue()));
    }

    // --- toString() ---

    @Test
    public void toStringThrowsUnsupportedOperationException() {
        assertThrows(UnsupportedOperationException.class, () -> mCache.toString());
    }

    // --- close() before initialize() ---

    @Test
    public void closeBeforeInitializeDoesNotThrow() {
        // close() has null checks, so it should be safe to call before initialize
        assertDoesNotThrow(() -> mCache.close());
    }

    @Test
    public void closeCanBeCalledMultipleTimes() {
        // Calling close twice should not throw
        assertDoesNotThrow(
                () -> {
                    mCache.close();
                    mCache.close();
                });
    }

    // --- insert() / lookup() before initialize() throw NullPointerException ---

    @Test
    public void insertBeforeInitializeThrowsNullPointerException() {
        assertThrows(
                NullPointerException.class,
                () -> mCache.insert("f.txt", "file:///tmp/f.txt", "local", OPERATION.get));
    }

    @Test
    public void lookupByHandleBeforeInitializeThrowsNullPointerException() {
        assertThrows(
                NullPointerException.class, () -> mCache.lookup("f.txt", "local", OPERATION.get));
    }

    @Test
    public void lookupByLFNBeforeInitializeThrowsNullPointerException() {
        assertThrows(NullPointerException.class, () -> mCache.lookup("f.txt", OPERATION.get));
    }

    @Test
    public void lookupAllEntriesBeforeInitializeThrowsNullPointerException() {
        assertThrows(
                NullPointerException.class,
                () -> mCache.lookupAllEntries("f.txt", "local", OPERATION.get));
    }

    // --- insert / lookup with unsupported OPERATION type ---

    @Test
    public void insertWithOperationAllThrowsRuntimeException() {
        // OPERATION.all is not supported by PlannerCache — it hits the else branch
        // and throws RuntimeException regardless of initialization state
        assertThrows(
                RuntimeException.class,
                () -> mCache.insert("f.txt", "file:///tmp/f.txt", "local", OPERATION.all));
    }

    @Test
    public void insertRCEWithOperationAllThrowsRuntimeException() {
        assertThrows(
                RuntimeException.class,
                () ->
                        mCache.insert(
                                "f.txt",
                                new edu.isi.pegasus.planner.catalog.replica.ReplicaCatalogEntry(
                                        "file:///tmp/f.txt", "local"),
                                OPERATION.all));
    }

    @Test
    public void lookupByHandleWithOperationAllThrowsRuntimeException() {
        assertThrows(RuntimeException.class, () -> mCache.lookup("f.txt", "local", OPERATION.all));
    }

    @Test
    public void lookupByLFNWithOperationAllThrowsRuntimeException() {
        assertThrows(RuntimeException.class, () -> mCache.lookup("f.txt", OPERATION.all));
    }

    @Test
    public void lookupAllEntriesWithOperationAllThrowsRuntimeException() {
        assertThrows(
                RuntimeException.class,
                () -> mCache.lookupAllEntries("f.txt", "local", OPERATION.all));
    }

    @Test
    public void initializeAllowsInsertAndLookupForGetOperation() {
        initializeCache(null);

        assertThat(mCache.insert("f.txt", "file:///tmp/f.txt", "local", OPERATION.get), is(1));
        assertThat(mCache.lookup("f.txt", "local", OPERATION.get), is("file:///tmp/f.txt"));

        ReplicaCatalogEntry entry = mCache.lookup("f.txt", OPERATION.get);
        assertThat(entry, is(notNullValue()));
        assertThat(entry.getPFN(), is("file:///tmp/f.txt"));
    }

    @Test
    public void initializeAllowsInsertAndLookupForPutOperation() {
        initializeCache(null);
        ReplicaCatalogEntry entry = new ReplicaCatalogEntry("file:///tmp/output.txt", "stash");

        assertThat(mCache.insert("output.txt", entry, OPERATION.put), is(1));
        assertThat(
                mCache.lookup("output.txt", "stash", OPERATION.put), is("file:///tmp/output.txt"));

        ReplicaCatalogEntry lookedUp = mCache.lookup("output.txt", OPERATION.put);
        assertThat(lookedUp, is(notNullValue()));
        assertThat(lookedUp.getPFN(), is("file:///tmp/output.txt"));
    }

    @Test
    public void getAndPutCachesRemainIndependent() {
        initializeCache(null);

        mCache.insert("shared.txt", "file:///tmp/get.txt", "get-site", OPERATION.get);
        mCache.insert("shared.txt", "file:///tmp/put.txt", "put-site", OPERATION.put);

        assertThat(
                mCache.lookup("shared.txt", "get-site", OPERATION.get), is("file:///tmp/get.txt"));
        assertThat(
                mCache.lookup("shared.txt", "put-site", OPERATION.put), is("file:///tmp/put.txt"));
        assertThat(mCache.lookup("shared.txt", "put-site", OPERATION.get), is(nullValue()));
        assertThat(mCache.lookup("shared.txt", "get-site", OPERATION.put), is(nullValue()));
    }

    @Test
    public void lookupReturnsNullForMissingEntryAfterInitialize() {
        initializeCache(null);

        assertThat(mCache.lookup("missing.txt", OPERATION.get), is(nullValue()));
        assertThat(mCache.lookup("missing.txt", "local", OPERATION.get), is(nullValue()));
    }

    @Test
    public void lookupAllEntriesReturnsEmptyCollectionWhenNoEntryMatches() {
        initializeCache(null);

        Collection<ReplicaCatalogEntry> entries =
                mCache.lookupAllEntries("missing.txt", "local", OPERATION.put);

        assertThat(entries, is(notNullValue()));
        assertThat(entries.isEmpty(), is(true));
    }

    @Test
    public void initializeUsesDagLabelAndIndexWhenBasenamePrefixIsUnset() throws Exception {
        initializeCache(null);

        ReplicaCatalog getCache = getReplicaCatalogField("mGetRCCache");
        ReplicaCatalog putCache = getReplicaCatalogField("mPutRCCache");

        assertThat(getCacheFileName(getCache), endsWith("workflow-7.getcache"));
        assertThat(getCacheFileName(putCache), endsWith("workflow-7.putcache"));
    }

    @Test
    public void initializeUsesBasenamePrefixWhenConfigured() throws Exception {
        initializeCache("custom-prefix");

        ReplicaCatalog getCache = getReplicaCatalogField("mGetRCCache");
        ReplicaCatalog putCache = getReplicaCatalogField("mPutRCCache");

        assertThat(getCacheFileName(getCache), endsWith("custom-prefix.getcache"));
        assertThat(getCacheFileName(putCache), endsWith("custom-prefix.putcache"));
    }

    private void initializeCache(String basenamePrefix) {
        PegasusBag bag = new PegasusBag();
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        PlannerOptions options = new PlannerOptions();
        options.setSubmitDirectory(mTempDir.toString());
        if (basenamePrefix != null) {
            options.setBasenamePrefix(basenamePrefix);
        }

        NoOpLogManager logger = new NoOpLogManager();
        logger.setLevel(Level.DEBUG);
        bag.add(PegasusBag.PEGASUS_PROPERTIES, props);
        bag.add(PegasusBag.PLANNER_OPTIONS, options);
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, logger);

        ADag dag = new ADag();
        dag.setLabel("workflow");
        dag.setIndex("7");

        mCache.initialize(bag, dag);
    }

    private ReplicaCatalog getReplicaCatalogField(String fieldName) throws Exception {
        return (ReplicaCatalog) ReflectionTestUtils.getField(mCache, fieldName);
    }

    private String getCacheFileName(ReplicaCatalog catalog) throws Exception {
        return (String) ReflectionTestUtils.getField(catalog, "m_filename");
    }
}
