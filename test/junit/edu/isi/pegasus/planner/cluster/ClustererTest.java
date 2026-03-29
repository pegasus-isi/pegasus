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
package edu.isi.pegasus.planner.cluster;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/**
 * Tests for the Clusterer interface: verifies constants and that known implementations conform to
 * the interface.
 */
public class ClustererTest {

    @Test
    public void testVersionConstantNotNull() {
        assertNotNull(Clusterer.VERSION, "Clusterer.VERSION should not be null");
    }

    @Test
    public void testVersionConstantNotEmpty() {
        assertFalse(Clusterer.VERSION.isEmpty(), "Clusterer.VERSION should not be empty");
    }

    @Test
    public void testHorizontalImplementsClusterer() {
        Horizontal h = new Horizontal();
        assertInstanceOf(Clusterer.class, h, "Horizontal should implement Clusterer");
    }

    @Test
    public void testVerticalImplementsClusterer() {
        Vertical v = new Vertical();
        assertInstanceOf(Clusterer.class, v, "Vertical should implement Clusterer");
    }

    @Test
    public void testHorizontalCanBeInstantiated() {
        assertDoesNotThrow(Horizontal::new, "Horizontal should be instantiatable");
    }

    @Test
    public void testVerticalCanBeInstantiated() {
        assertDoesNotThrow(Vertical::new, "Vertical should be instantiatable");
    }

    @Test
    public void testHorizontalDescriptionNotNull() {
        Horizontal h = new Horizontal();
        assertNotNull(h.description(), "Horizontal description() should not return null");
    }

    @Test
    public void testVerticalDescriptionNotNull() {
        Vertical v = new Vertical();
        assertNotNull(v.description(), "Vertical description() should not return null");
    }
}
