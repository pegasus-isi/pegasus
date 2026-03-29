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

/** Tests for the ReplicaCatalog class in site classes. */
public class ReplicaCatalogTest {
    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testReplicaCatalogExtendsAbstractSiteData() {
        assertTrue(AbstractSiteData.class.isAssignableFrom(ReplicaCatalog.class));
    }

    @Test
    public void testDefaultConstructor() {
        ReplicaCatalog rc = new ReplicaCatalog();
        assertNotNull(rc);
    }

    @Test
    public void testConstructorWithUrlAndType() {
        ReplicaCatalog rc = new ReplicaCatalog("http://example.com/rc", "File");
        assertNotNull(rc);
    }

    @Test
    public void testGetUrlReturnsSetValue() {
        ReplicaCatalog rc = new ReplicaCatalog("http://example.com/rc", "File");
        assertEquals("http://example.com/rc", rc.getURL());
    }

    @Test
    public void testGetTypeReturnsSetValue() {
        ReplicaCatalog rc = new ReplicaCatalog("http://example.com/rc", "File");
        assertEquals("File", rc.getType());
    }

    @Test
    public void testClassIsNotAbstract() {
        assertFalse(java.lang.reflect.Modifier.isAbstract(ReplicaCatalog.class.getModifiers()));
    }

    @Test
    public void testClassIsNotInterface() {
        assertFalse(ReplicaCatalog.class.isInterface());
    }
}
