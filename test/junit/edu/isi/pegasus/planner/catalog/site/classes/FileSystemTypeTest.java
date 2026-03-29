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
 * Tests for FileSystemType via InternalMountPoint (concrete subclass).
 *
 * @author Rajiv Mayani
 */
public class FileSystemTypeTest {

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testDefaultConstructorEmptyStrings() {
        InternalMountPoint imp = new InternalMountPoint();
        assertEquals("", imp.getMountPoint());
        assertEquals("", imp.getTotalSize());
        assertEquals("", imp.getFreeSize());
    }

    @Test
    public void testOverloadedConstructorWithAllParameters() {
        InternalMountPoint imp = new InternalMountPoint("/data", "100GB", "50GB");
        assertEquals("/data", imp.getMountPoint());
        assertEquals("100GB", imp.getTotalSize());
        assertEquals("50GB", imp.getFreeSize());
    }

    @Test
    public void testSetAndGetMountPoint() {
        InternalMountPoint imp = new InternalMountPoint();
        imp.setMountPoint("/scratch");
        assertEquals("/scratch", imp.getMountPoint());
    }

    @Test
    public void testSetAndGetTotalSize() {
        InternalMountPoint imp = new InternalMountPoint();
        imp.setTotalSize("200GB");
        assertEquals("200GB", imp.getTotalSize());
    }

    @Test
    public void testSetAndGetFreeSize() {
        InternalMountPoint imp = new InternalMountPoint();
        imp.setFreeSize("75GB");
        assertEquals("75GB", imp.getFreeSize());
    }

    @Test
    public void testCloneProducesEqualButDistinctInstance() {
        InternalMountPoint imp = new InternalMountPoint("/work", "512GB", "256GB");
        InternalMountPoint cloned = (InternalMountPoint) imp.clone();
        assertNotSame(imp, cloned);
        assertEquals(imp.getMountPoint(), cloned.getMountPoint());
        assertEquals(imp.getTotalSize(), cloned.getTotalSize());
        assertEquals(imp.getFreeSize(), cloned.getFreeSize());
    }
}
