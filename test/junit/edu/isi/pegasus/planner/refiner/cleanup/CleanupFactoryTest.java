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

import java.lang.reflect.Modifier;
import org.junit.jupiter.api.Test;

/** Structural tests for CleanupFactory. */
public class CleanupFactoryTest {

    @Test
    public void testDefaultPackageName() {
        assertEquals(
                "edu.isi.pegasus.planner.refiner.cleanup", CleanupFactory.DEFAULT_PACKAGE_NAME);
    }

    @Test
    public void testIsNotAbstract() {
        assertFalse(Modifier.isAbstract(CleanupFactory.class.getModifiers()));
    }

    @Test
    public void testHasLoadCleanupStrategyMethod() throws Exception {
        assertNotNull(
                CleanupFactory.class.getMethod(
                        "loadCleanupStraegyInstance",
                        edu.isi.pegasus.planner.classes.PegasusBag.class));
    }

    @Test
    public void testLoadMethodIsStatic() throws Exception {
        java.lang.reflect.Method m =
                CleanupFactory.class.getMethod(
                        "loadCleanupStraegyInstance",
                        edu.isi.pegasus.planner.classes.PegasusBag.class);
        assertTrue(Modifier.isStatic(m.getModifiers()));
    }
}
