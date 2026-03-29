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
package edu.isi.pegasus.planner.refiner.createdir;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** Structural tests for createdir Implementation interface. */
public class ImplementationTest {

    @Test
    public void testIsInterface() {
        assertTrue(Implementation.class.isInterface());
    }

    @Test
    public void testVersionConstant() {
        assertEquals("1.1", Implementation.VERSION);
    }

    @Test
    public void testDefaultImplementationImplementsInterface() {
        assertTrue(Implementation.class.isAssignableFrom(DefaultImplementation.class));
    }

    @Test
    public void testHasMakeCreateDirJobMethod() throws Exception {
        assertNotNull(
                Implementation.class.getMethod(
                        "makeCreateDirJob", String.class, String.class, String.class));
    }

    @Test
    public void testHasInitializeMethod() throws Exception {
        assertNotNull(
                Implementation.class.getMethod(
                        "initialize", edu.isi.pegasus.planner.classes.PegasusBag.class));
    }
}
