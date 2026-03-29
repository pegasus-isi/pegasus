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

/** Tests for the Hashed submit mapper class structure. */
public class HashedTest {

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testHashedImplementsSubmitMapper() {
        assertTrue(SubmitMapper.class.isAssignableFrom(Hashed.class));
    }

    @Test
    public void testMultiplicatorPropertyKeyConstant() {
        assertEquals("hashed.multiplier", Hashed.MULIPLICATOR_PROPERTY_KEY);
    }

    @Test
    public void testDefaultMultiplicatorFactorConstant() {
        assertEquals(5, Hashed.DEFAULT_MULTIPLICATOR_FACTOR);
    }

    @Test
    public void testLevelsPropertyKeyConstant() {
        assertEquals("hashed.levels", Hashed.LEVELS_PROPERTY_KEY);
    }

    @Test
    public void testDefaultLevelsConstant() {
        assertEquals(2, Hashed.DEFAULT_LEVELS);
    }

    @Test
    public void testDefaultInstantiation() {
        Hashed hashed = new Hashed();
        assertNotNull(hashed);
    }

    @Test
    public void testHashedIsPublicClass() {
        int modifiers = Hashed.class.getModifiers();
        assertTrue(java.lang.reflect.Modifier.isPublic(modifiers));
    }
}
