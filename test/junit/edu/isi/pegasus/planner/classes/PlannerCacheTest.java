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
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.catalog.site.classes.FileServerType.OPERATION;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class PlannerCacheTest {

    private PlannerCache mCache;

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

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
}
