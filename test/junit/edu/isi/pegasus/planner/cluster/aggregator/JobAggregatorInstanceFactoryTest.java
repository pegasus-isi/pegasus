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
package edu.isi.pegasus.planner.cluster.aggregator;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for the JobAggregatorInstanceFactory class structure. */
public class JobAggregatorInstanceFactoryTest {

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testDefaultConstructor() {
        JobAggregatorInstanceFactory factory = new JobAggregatorInstanceFactory();
        assertNotNull(factory);
    }

    @Test
    public void testFactoryIsPublicClass() {
        int modifiers = JobAggregatorInstanceFactory.class.getModifiers();
        assertTrue(java.lang.reflect.Modifier.isPublic(modifiers));
    }

    @Test
    public void testInitializeMethodExists() throws NoSuchMethodException {
        assertNotNull(
                JobAggregatorInstanceFactory.class.getMethod(
                        "initialize",
                        edu.isi.pegasus.planner.classes.ADag.class,
                        edu.isi.pegasus.planner.classes.PegasusBag.class));
    }

    @Test
    public void testLoadInstanceMethodExists() throws NoSuchMethodException {
        assertNotNull(
                JobAggregatorInstanceFactory.class.getMethod(
                        "loadInstance", edu.isi.pegasus.planner.classes.Job.class));
    }

    @Test
    public void testLoadInstanceThrowsWhenNotInitialized() {
        JobAggregatorInstanceFactory factory = new JobAggregatorInstanceFactory();
        edu.isi.pegasus.planner.classes.Job job = new edu.isi.pegasus.planner.classes.Job();
        assertThrows(JobAggregatorFactoryException.class, () -> factory.loadInstance(job));
    }
}
