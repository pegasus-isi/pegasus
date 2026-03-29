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
package edu.isi.pegasus.planner.code.gridstart.container;

import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.code.gridstart.container.impl.Docker;
import edu.isi.pegasus.planner.code.gridstart.container.impl.None;
import edu.isi.pegasus.planner.code.gridstart.container.impl.Singularity;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for the ContainerShellWrapper interface. */
public class ContainerShellWrapperTest {
    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testContainerShellWrapperIsInterface() {
        assertTrue(ContainerShellWrapper.class.isInterface());
    }

    @Test
    public void testVersionConstant() {
        assertEquals("1.1", ContainerShellWrapper.VERSION);
    }

    @Test
    public void testVersionConstantNotNull() {
        assertNotNull(ContainerShellWrapper.VERSION);
    }

    @Test
    public void testNoneImplementsContainerShellWrapper() {
        assertTrue(ContainerShellWrapper.class.isAssignableFrom(None.class));
    }

    @Test
    public void testDockerImplementsContainerShellWrapper() {
        assertTrue(ContainerShellWrapper.class.isAssignableFrom(Docker.class));
    }

    @Test
    public void testSingularityImplementsContainerShellWrapper() {
        assertTrue(ContainerShellWrapper.class.isAssignableFrom(Singularity.class));
    }
}
