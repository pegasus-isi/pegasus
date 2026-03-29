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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.io.StringWriter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class InternalMountPointTest {

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testDefaultConstructorIsEmpty() {
        InternalMountPoint imp = new InternalMountPoint();
        assertTrue(imp.isEmpty());
    }

    @Test
    public void testSingleArgConstructorSetsOnlyMountPoint() {
        InternalMountPoint imp = new InternalMountPoint("/tmp");
        assertEquals("/tmp", imp.getMountPoint());
        assertNull(imp.getTotalSize());
        assertNull(imp.getFreeSize());
    }

    @Test
    public void testFullConstructorSetsAllFields() {
        InternalMountPoint imp = new InternalMountPoint("/data", "100GB", "50GB");
        assertEquals("/data", imp.getMountPoint());
        assertEquals("100GB", imp.getTotalSize());
        assertEquals("50GB", imp.getFreeSize());
    }

    @Test
    public void testIsEmptyFalseWhenMountPointSet() {
        InternalMountPoint imp = new InternalMountPoint("/scratch");
        assertFalse(imp.isEmpty());
    }

    @Test
    public void testToXMLContainsMountPoint() throws IOException {
        InternalMountPoint imp = new InternalMountPoint("/lustre");
        StringWriter sw = new StringWriter();
        imp.toXML(sw, "");
        String xml = sw.toString();
        assertThat(xml, containsString("mount-point=\"/lustre\""));
        assertThat(xml, containsString("<internal-mount-point"));
    }

    @Test
    public void testToXMLEmptyProducesNoOutput() throws IOException {
        InternalMountPoint imp = new InternalMountPoint();
        StringWriter sw = new StringWriter();
        imp.toXML(sw, "");
        assertEquals("", sw.toString());
    }

    @Test
    public void testToXMLWithSizesIncludesSizeAttributes() throws IOException {
        InternalMountPoint imp = new InternalMountPoint("/data", "200GB", "100GB");
        StringWriter sw = new StringWriter();
        imp.toXML(sw, "");
        String xml = sw.toString();
        assertThat(xml, containsString("total-size=\"200GB\""));
        assertThat(xml, containsString("free-size=\"100GB\""));
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
