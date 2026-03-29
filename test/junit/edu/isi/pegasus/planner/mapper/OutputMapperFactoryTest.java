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
package edu.isi.pegasus.planner.mapper;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for the OutputMapperFactory class constants and structure. */
public class OutputMapperFactoryTest {

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testDefaultPackageNameConstant() {
        assertEquals(
                "edu.isi.pegasus.planner.mapper.output", OutputMapperFactory.DEFAULT_PACKAGE_NAME);
    }

    @Test
    public void testPropertyKeyConstant() {
        assertEquals("pegasus.dir.storage.mapper", OutputMapperFactory.PROPERTY_KEY);
    }

    @Test
    public void testDefaultOutputMapperImplementationConstant() {
        assertEquals("Flat", OutputMapperFactory.DEFAULT_OUTPUT_MAPPER_IMPLEMENTATION);
    }

    @Test
    public void testHashedOutputMapperImplementationConstant() {
        assertEquals("Hashed", OutputMapperFactory.HASHED_OUTPUT_MAPPER_IMPLEMENTATION);
    }

    @Test
    public void testFactoryClassIsPublic() {
        int modifiers = OutputMapperFactory.class.getModifiers();
        assertTrue(java.lang.reflect.Modifier.isPublic(modifiers));
    }
}
