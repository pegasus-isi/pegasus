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
package edu.isi.pegasus.planner.refiner;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** Structural tests for MainEngine. */
public class MainEngineTest {

    @Test
    public void testExtendsEngine() {
        assertTrue(Engine.class.isAssignableFrom(MainEngine.class));
    }

    @Test
    public void testCleanupDirConstant() {
        assertEquals("cleanup", MainEngine.CLEANUP_DIR);
    }

    @Test
    public void testCatalogsDirBasenameConstant() {
        assertEquals("catalogs", MainEngine.CATALOGS_DIR_BASENAME);
    }

    @Test
    public void testIsNotAbstract() {
        assertFalse(java.lang.reflect.Modifier.isAbstract(MainEngine.class.getModifiers()));
    }
}
