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
package edu.isi.pegasus.planner.mapper.submit;

import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.mapper.SubmitMapper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for the Named submit mapper class structure. */
public class NamedTest {

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testNamedImplementsSubmitMapper() {
        assertTrue(SubmitMapper.class.isAssignableFrom(Named.class));
    }

    @Test
    public void testDefaultInstantiation() {
        Named named = new Named();
        assertNotNull(named);
    }

    @Test
    public void testNamedIsPublicClass() {
        int modifiers = Named.class.getModifiers();
        assertTrue(java.lang.reflect.Modifier.isPublic(modifiers));
    }

    @Test
    public void testGetRelativeDirMethodExists() throws NoSuchMethodException {
        assertNotNull(
                Named.class.getMethod("getRelativeDir", edu.isi.pegasus.planner.classes.Job.class));
    }

    @Test
    public void testGetDirMethodExists() throws NoSuchMethodException {
        assertNotNull(Named.class.getMethod("getDir", edu.isi.pegasus.planner.classes.Job.class));
    }
}
