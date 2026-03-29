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
package edu.isi.pegasus.planner.refiner.cleanup;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** Structural tests for RM cleanup implementation. */
public class RMTest {

    @Test
    public void testImplementsCleanupImplementation() {
        assertTrue(CleanupImplementation.class.isAssignableFrom(RM.class));
    }

    @Test
    public void testDefaultRmLogicalName() {
        assertEquals("rm", RM.DEFAULT_RM_LOGICAL_NAME);
    }

    @Test
    public void testDefaultRmLocation() {
        assertEquals("/bin/rm", RM.DEFAULT_RM_LOCATION);
    }

    @Test
    public void testDefaultPriorityKey() {
        assertEquals("1000", RM.DEFAULT_PRIORITY_KEY);
    }

    @Test
    public void testDefaultConstructor() {
        RM rm = new RM();
        assertNotNull(rm);
    }

    @Test
    public void testIsNotAbstract() {
        assertFalse(java.lang.reflect.Modifier.isAbstract(RM.class.getModifiers()));
    }
}
