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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for the Horizontal clusterer class. */
public class HorizontalTest {

    private Horizontal mHorizontal;

    @BeforeEach
    public void setUp() {
        mHorizontal = new Horizontal();
    }

    @Test
    public void testInstantiation() {
        assertNotNull(mHorizontal, "Horizontal should be instantiatable");
    }

    @Test
    public void testImplementsClusterer() {
        assertInstanceOf(Clusterer.class, mHorizontal, "Horizontal should implement Clusterer");
    }

    @Test
    public void testDescriptionNotNull() {
        assertNotNull(mHorizontal.description(), "description() should not return null");
    }

    @Test
    public void testDescriptionNotEmpty() {
        assertFalse(mHorizontal.description().isEmpty(), "description() should not be empty");
    }

    @Test
    public void testDefaultConstructorDoesNotThrow() {
        assertDoesNotThrow(Horizontal::new, "Horizontal should construct without throwing");
    }

    @Test
    public void testIsNotSameInstanceAsVertical() {
        Vertical v = new Vertical();
        assertNotEquals(
                mHorizontal.getClass(),
                v.getClass(),
                "Horizontal and Vertical should be different classes");
    }

    @Test
    public void testDescriptionDiffersFromVertical() {
        Vertical v = new Vertical();
        // Descriptions should differ between implementations
        assertNotEquals(
                mHorizontal.description(),
                v.description(),
                "Horizontal and Vertical descriptions should differ");
    }
}
