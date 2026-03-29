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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for the Abstract cluster class structure. */
public class AbstractTest {

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testAbstractImplementsClusterer() {
        // edu.isi.pegasus.planner.cluster.Abstract implements Clusterer
        assertTrue(
                Clusterer.class.isAssignableFrom(edu.isi.pegasus.planner.cluster.Abstract.class));
    }

    @Test
    public void testAbstractIsAbstract() {
        int modifiers = edu.isi.pegasus.planner.cluster.Abstract.class.getModifiers();
        assertTrue(java.lang.reflect.Modifier.isAbstract(modifiers));
    }

    @Test
    public void testHorizontalImplementsClustererDirectly() {
        // Horizontal implements Clusterer directly (not via Abstract)
        assertFalse(
                edu.isi.pegasus.planner.cluster.Abstract.class.isAssignableFrom(Horizontal.class));
        assertTrue(Clusterer.class.isAssignableFrom(Horizontal.class));
    }

    @Test
    public void testVerticalExtendsAbstract() {
        assertTrue(edu.isi.pegasus.planner.cluster.Abstract.class.isAssignableFrom(Vertical.class));
    }

    @Test
    public void testHorizontalInstantiation() {
        Horizontal h = new Horizontal();
        assertNotNull(h);
    }

    @Test
    public void testVerticalInstantiation() {
        Vertical v = new Vertical();
        assertNotNull(v);
    }
}
