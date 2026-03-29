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

/** Structural tests for Refiner interface. */
public class RefinerTest {

    @Test
    public void testIsInterface() {
        assertTrue(Refiner.class.isInterface());
    }

    @Test
    public void testVersionConstant() {
        assertEquals("1.0", Refiner.VERSION);
    }

    @Test
    public void testHasGetWorkflowMethod() throws Exception {
        assertNotNull(Refiner.class.getMethod("getWorkflow"));
    }

    @Test
    public void testGetWorkflowReturnsADag() throws Exception {
        assertEquals(
                edu.isi.pegasus.planner.classes.ADag.class,
                Refiner.class.getMethod("getWorkflow").getReturnType());
    }
}
