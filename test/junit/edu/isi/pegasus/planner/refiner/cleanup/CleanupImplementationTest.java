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

/** Structural tests for CleanupImplementation interface. */
public class CleanupImplementationTest {

    @Test
    public void testIsInterface() {
        assertTrue(CleanupImplementation.class.isInterface());
    }

    @Test
    public void testVersionConstant() {
        assertEquals("1.1", CleanupImplementation.VERSION);
    }

    @Test
    public void testDefaultCleanupCategoryKey() {
        assertEquals("cleanup", CleanupImplementation.DEFAULT_CLEANUP_CATEGORY_KEY);
    }

    @Test
    public void testRMImplementsCleanupImplementation() {
        assertTrue(CleanupImplementation.class.isAssignableFrom(RM.class));
    }

    @Test
    public void testCleanupImplementsCleanupImplementation() {
        assertTrue(CleanupImplementation.class.isAssignableFrom(Cleanup.class));
    }

    @Test
    public void testHasCreateCleanupJobMethod() throws Exception {
        assertNotNull(
                CleanupImplementation.class.getMethod(
                        "createCleanupJob",
                        String.class,
                        java.util.List.class,
                        edu.isi.pegasus.planner.classes.Job.class));
    }
}
