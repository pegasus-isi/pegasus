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
package edu.isi.pegasus.planner.code.gridstart;

import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.code.GridStart;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for the Distribute GridStart implementation. */
public class DistributeTest {
    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testDistributeImplementsGridStart() {
        assertTrue(GridStart.class.isAssignableFrom(Distribute.class));
    }

    @Test
    public void testDistributeIsConcreteClass() {
        assertFalse(java.lang.reflect.Modifier.isAbstract(Distribute.class.getModifiers()));
    }

    @Test
    public void testDistributeIsNotInterface() {
        assertFalse(Distribute.class.isInterface());
    }

    @Test
    public void testClassNameConstant() {
        assertEquals("Distribute", Distribute.CLASSNAME);
    }

    @Test
    public void testShortNameConstant() {
        assertEquals("distribute", Distribute.SHORT_NAME);
    }

    @Test
    public void testClassExists() {
        assertNotNull(Distribute.class);
    }
}
