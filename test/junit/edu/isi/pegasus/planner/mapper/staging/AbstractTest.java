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
package edu.isi.pegasus.planner.mapper.staging;

import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.mapper.StagingMapper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for the Abstract staging mapper class structure. */
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
    public void testAbstractImplementsStagingMapper() {
        assertTrue(StagingMapper.class.isAssignableFrom(Abstract.class));
    }

    @Test
    public void testAbstractIsAbstractClass() {
        int modifiers = Abstract.class.getModifiers();
        assertTrue(java.lang.reflect.Modifier.isAbstract(modifiers));
    }

    @Test
    public void testFlatExtendsAbstract() {
        assertTrue(Abstract.class.isAssignableFrom(Flat.class));
    }

    @Test
    public void testHashedExtendsAbstract() {
        assertTrue(Abstract.class.isAssignableFrom(Hashed.class));
    }

    @Test
    public void testFlatInstantiation() {
        Flat flat = new Flat();
        assertNotNull(flat);
    }

    @Test
    public void testHashedInstantiation() {
        Hashed hashed = new Hashed();
        assertNotNull(hashed);
    }
}
