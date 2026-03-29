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

/** Tests for the Vertical clusterer class. */
public class VerticalTest {

    private Vertical mVertical;

    @BeforeEach
    public void setUp() {
        mVertical = new Vertical();
    }

    @Test
    public void testInstantiation() {
        assertNotNull(mVertical, "Vertical should be instantiatable");
    }

    @Test
    public void testImplementsClusterer() {
        assertInstanceOf(Clusterer.class, mVertical, "Vertical should implement Clusterer");
    }

    @Test
    public void testDescriptionNotNull() {
        assertNotNull(mVertical.description(), "description() should not return null");
    }

    @Test
    public void testDescriptionNotEmpty() {
        assertFalse(mVertical.description().isEmpty(), "description() should not be empty");
    }

    @Test
    public void testDefaultConstructorDoesNotThrow() {
        assertDoesNotThrow(Vertical::new, "Vertical should construct without throwing");
    }

    @Test
    public void testDescriptionIsDifferentFromHorizontal() {
        Horizontal h = new Horizontal();
        assertNotEquals(
                mVertical.description(),
                h.description(),
                "Vertical and Horizontal descriptions should differ");
    }

    @Test
    public void testIsInstanceOfVertical() {
        assertInstanceOf(Vertical.class, mVertical, "Object should be an instance of Vertical");
    }
}
