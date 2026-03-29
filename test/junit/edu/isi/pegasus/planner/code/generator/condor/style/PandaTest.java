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
package edu.isi.pegasus.planner.code.generator.condor.style;

import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.code.generator.condor.CondorStyle;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for the Panda style class. */
public class PandaTest {
    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testPandaExtendsGLite() {
        assertTrue(GLite.class.isAssignableFrom(Panda.class));
    }

    @Test
    public void testPandaImplementsCondorStyle() {
        assertTrue(CondorStyle.class.isAssignableFrom(Panda.class));
    }

    @Test
    public void testPandaIsConcreteClass() {
        assertFalse(java.lang.reflect.Modifier.isAbstract(Panda.class.getModifiers()));
    }

    @Test
    public void testPandaIsNotInterface() {
        assertFalse(Panda.class.isInterface());
    }

    @Test
    public void testStyleNameConstant() {
        assertEquals("PANDA", Panda.STYLE_NAME);
    }

    @Test
    public void testGridResourceKeyNotNull() {
        assertNotNull(Panda.GRID_RESOURCE_KEY);
    }

    @Test
    public void testInstantiation() {
        Panda style = new Panda();
        assertNotNull(style);
    }
}
