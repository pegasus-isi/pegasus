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

/** Structural tests for InPlace cleanup strategy. */
public class InPlaceTest {

    @Test
    public void testExtendsAbstractCleanupStrategy() {
        assertTrue(AbstractCleanupStrategy.class.isAssignableFrom(InPlace.class));
    }

    @Test
    public void testImplementsCleanupStrategy() {
        assertTrue(CleanupStrategy.class.isAssignableFrom(InPlace.class));
    }

    @Test
    public void testNumJobsPerLevelConstant() {
        assertEquals(5.0f, InPlace.NUM_JOBS_PER_LEVEL_PER_CLEANUP_JOB, 0.0001f);
    }

    @Test
    public void testDefaultConstructor() {
        InPlace ip = new InPlace();
        assertNotNull(ip);
    }

    @Test
    public void testIsNotAbstract() {
        assertFalse(java.lang.reflect.Modifier.isAbstract(InPlace.class.getModifiers()));
    }
}
