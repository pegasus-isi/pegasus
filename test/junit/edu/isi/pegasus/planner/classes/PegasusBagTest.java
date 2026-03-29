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

import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.common.PegasusProperties;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class PegasusBagTest {
    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testDefaultConstructorNotNull() {
        PegasusBag bag = new PegasusBag();
        assertNotNull(bag);
    }

    @Test
    public void testDefaultUsesPMCIsFalse() {
        PegasusBag bag = new PegasusBag();
        assertFalse(bag.plannerUsesPMC());
    }

    @Test
    public void testDefaultPlannerDirectoryIsUserDir() {
        PegasusBag bag = new PegasusBag();
        File dir = bag.getPlannerDirectory();
        assertNotNull(dir);
        assertEquals(new File(System.getProperty("user.dir")), dir);
    }

    @Test
    public void testAddAndGetPegasusProperties() {
        PegasusBag bag = new PegasusBag();
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, props);
        assertSame(props, bag.getPegasusProperties());
    }

    @Test
    public void testGetPegasusPropertiesNullByDefault() {
        PegasusBag bag = new PegasusBag();
        assertNull(bag.getPegasusProperties());
    }

    @Test
    public void testAddPegasusPropertiesStoresOriginalCopy() {
        PegasusBag bag = new PegasusBag();
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, props);
        // original props should be stored on first set
        assertNotNull(bag.getOriginalPegasusProperties());
    }

    @Test
    public void testAddPlannerOptions() {
        PegasusBag bag = new PegasusBag();
        PlannerOptions options = new PlannerOptions();
        bag.add(PegasusBag.PLANNER_OPTIONS, options);
        assertSame(options, bag.getPlannerOptions());
    }

    @Test
    public void testGetPlannerOptionsNullByDefault() {
        PegasusBag bag = new PegasusBag();
        assertNull(bag.getPlannerOptions());
    }

    @Test
    public void testAddReplicaCatalogFileSource() {
        PegasusBag bag = new PegasusBag();
        File rcFile = new File("/tmp/replica.yml");
        bag.add(PegasusBag.REPLICA_CATALOG_FILE_SOURCE, rcFile);
        assertEquals(rcFile, bag.getReplicaCatalogFileSource());
    }

    @Test
    public void testGetReplicaCatalogFileSourceNullByDefault() {
        PegasusBag bag = new PegasusBag();
        assertNull(bag.getReplicaCatalogFileSource());
    }

    @Test
    public void testAddWorkerPackageMap() {
        PegasusBag bag = new PegasusBag();
        Map<String, String> wpMap = new HashMap<String, String>();
        wpMap.put("siteA", "/path/to/worker.tar.gz");
        bag.add(PegasusBag.WORKER_PACKAGE_MAP, wpMap);
        Map<String, String> retrieved = bag.getWorkerPackageMap();
        assertNotNull(retrieved);
        assertEquals("/path/to/worker.tar.gz", retrieved.get("siteA"));
    }

    @Test
    public void testGetWorkerPackageMapNullByDefault() {
        PegasusBag bag = new PegasusBag();
        assertNull(bag.getWorkerPackageMap());
    }

    @Test
    public void testAddUsesPMCTrue() {
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.USES_PMC, Boolean.TRUE);
        assertTrue(bag.plannerUsesPMC());
    }

    @Test
    public void testAddUsesPMCFalse() {
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.USES_PMC, Boolean.TRUE);
        bag.add(PegasusBag.USES_PMC, Boolean.FALSE);
        assertFalse(bag.plannerUsesPMC());
    }

    @Test
    public void testAddPlannerDirectory() {
        PegasusBag bag = new PegasusBag();
        File dir = new File("/tmp/planner-dir");
        bag.add(PegasusBag.PLANNER_DIRECTORY, dir);
        assertEquals(dir, bag.getPlannerDirectory());
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
        assertFalse(result);
    }

    @Test
    public void testAddValidTypeReturnsTrue() {
        PegasusBag bag = new PegasusBag();
        PegasusProperties props = PegasusProperties.nonSingletonInstance();
        boolean result = bag.add(PegasusBag.PEGASUS_PROPERTIES, props);
        assertTrue(result);
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
        assertTrue(bag.containsKey(PegasusBag.PEGASUS_PROPERTIES));
        assertTrue(bag.containsKey(PegasusBag.PLANNER_OPTIONS));
        assertTrue(bag.containsKey(PegasusBag.REPLICA_CATALOG_FILE_SOURCE));
        assertTrue(bag.containsKey(PegasusBag.SITE_CATALOG));
        assertTrue(bag.containsKey(PegasusBag.TRANSFORMATION_CATALOG));
        assertTrue(bag.containsKey(PegasusBag.TRANSFORMATION_MAPPER));
        assertTrue(bag.containsKey(PegasusBag.PEGASUS_LOGMANAGER));
        assertTrue(bag.containsKey(PegasusBag.SITE_STORE));
        assertTrue(bag.containsKey(PegasusBag.PLANNER_CACHE));
        assertTrue(bag.containsKey(PegasusBag.WORKER_PACKAGE_MAP));
        assertTrue(bag.containsKey(PegasusBag.USES_PMC));
        assertTrue(bag.containsKey(PegasusBag.PLANNER_METRICS));
    }

    @Test
    public void testContainsKeyReturnsFalseForInvalidKey() {
        PegasusBag bag = new PegasusBag();
        assertFalse(bag.containsKey(999));
        assertFalse(bag.containsKey(-1));
    }

    @Test
    public void testContainsKeyReturnsFalseForNonIntegerKey() {
        PegasusBag bag = new PegasusBag();
        assertFalse(bag.containsKey("not-an-integer"));
    }

    @Test
    public void testGetReturnsNullForUnsetProperty() {
        PegasusBag bag = new PegasusBag();
        assertNull(bag.get(PegasusBag.PLANNER_OPTIONS));
    }

    @Test
    public void testAddSitesCatalogKeyThrowsRuntimeException() {
        PegasusBag bag = new PegasusBag();
        // SITE_CATALOG always sets valid=false; since value is non-null, RuntimeException is thrown
        assertThrows(RuntimeException.class, () -> bag.add(PegasusBag.SITE_CATALOG, new Object()));
    }

    @Test
    public void testPegasusInfoArrayHasCorrectSize() {
        assertEquals(15, PegasusBag.PEGASUS_INFO.length);
    }

    @Test
    public void testConvenienceMethodGetPlannerOptions() {
        PegasusBag bag = new PegasusBag();
        PlannerOptions options = new PlannerOptions();
        bag.add(PegasusBag.PLANNER_OPTIONS, options);
        PlannerOptions retrieved = bag.getPlannerOptions();
        assertSame(options, retrieved);
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
        assertNotNull(bag.getOriginalPegasusProperties());
        assertSame(original, bag.getOriginalPegasusProperties());
    }
}
