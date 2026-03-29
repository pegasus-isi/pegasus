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

/** Structural tests for HourGlass createdir strategy. */
public class HourGlassTest {

    @Test
    public void testExtendsAbstractStrategy() {
        assertTrue(AbstractStrategy.class.isAssignableFrom(HourGlass.class));
    }

    @Test
    public void testImplementsStrategy() {
        assertTrue(Strategy.class.isAssignableFrom(HourGlass.class));
    }

    @Test
    public void testDummyConcatJobConstant() {
        assertEquals("pegasus_concat", HourGlass.DUMMY_CONCAT_JOB);
    }

    @Test
    public void testDummyConcatJobPrefixConstant() {
        assertEquals("pegasus_concat_", HourGlass.DUMMY_CONCAT_JOB_PREFIX);
    }

    @Test
    public void testTransformationNamespace() {
        assertEquals("pegasus", HourGlass.TRANSFORMATION_NAMESPACE);
    }

    @Test
    public void testDefaultConstructor() {
        HourGlass hg = new HourGlass();
        assertNotNull(hg);
    }
}
