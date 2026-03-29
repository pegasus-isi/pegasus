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
package edu.isi.pegasus.planner.code.generator.condor;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for the SUBDAXGenerator class constants and structure. */
public class SUBDAXGeneratorTest {
    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testDefaultSubdaxCategoryKey() {
        assertEquals("subwf", SUBDAXGenerator.DEFAULT_SUBDAX_CATEGORY_KEY);
    }

    @Test
    public void testGenerateSubdagKeyword() {
        assertFalse(SUBDAXGenerator.GENERATE_SUBDAG_KEYWORD);
    }

    @Test
    public void testCplannerLogicalName() {
        assertEquals("pegasus-plan", SUBDAXGenerator.CPLANNER_LOGICAL_NAME);
    }

    @Test
    public void testCondorDagmanNamespace() {
        assertEquals("condor", SUBDAXGenerator.CONDOR_DAGMAN_NAMESPACE);
    }

    @Test
    public void testCondorDagmanLogicalName() {
        assertEquals("dagman", SUBDAXGenerator.CONDOR_DAGMAN_LOGICAL_NAME);
    }

    @Test
    public void testNamespace() {
        assertEquals("pegasus", SUBDAXGenerator.NAMESPACE);
    }

    @Test
    public void testRetryLogicalName() {
        assertEquals("pegasus-plan", SUBDAXGenerator.RETRY_LOGICAL_NAME);
    }

    @Test
    public void testClassIsNotAbstract() {
        assertFalse(java.lang.reflect.Modifier.isAbstract(SUBDAXGenerator.class.getModifiers()));
    }

    @Test
    public void testClassIsNotInterface() {
        assertFalse(SUBDAXGenerator.class.isInterface());
    }
}
