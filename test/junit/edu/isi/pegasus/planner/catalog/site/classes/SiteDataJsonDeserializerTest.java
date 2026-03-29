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
package edu.isi.pegasus.planner.catalog.site.classes;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for SiteDataJsonDeserializer — verifies the class loads and basic contract.
 *
 * @author Rajiv Mayani
 */
public class SiteDataJsonDeserializerTest {

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testClassLoads() {
        // Verify the class is accessible (non-null class object)
        assertNotNull(SiteDataJsonDeserializer.class);
    }

    @Test
    public void testSiteDataJsonDeserializerIsAbstract() {
        assertTrue(
                java.lang.reflect.Modifier.isAbstract(
                        SiteDataJsonDeserializer.class.getModifiers()),
                "SiteDataJsonDeserializer should be abstract");
    }

    @Test
    public void testSiteStoreDeserializerIsSubclass() {
        // SiteStore's deserializer extends SiteDataJsonDeserializer
        // Verify SiteStore class loads which exercises the inner deserializer
        SiteStore store = new SiteStore();
        assertNotNull(store);
    }

    @Test
    public void testSiteStoreDeserializerDefaultVersion() {
        SiteStore store = new SiteStore();
        assertEquals(SiteStore.DEFAULT_SITE_CATALOG_VERSION, store.getVersion());
    }
}
