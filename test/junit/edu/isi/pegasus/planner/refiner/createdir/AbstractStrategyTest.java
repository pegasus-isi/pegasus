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
package edu.isi.pegasus.planner.refiner.createdir;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Modifier;
import org.junit.jupiter.api.Test;

/** Structural tests for AbstractStrategy. */
public class AbstractStrategyTest {

    @Test
    public void testIsAbstract() {
        assertTrue(Modifier.isAbstract(AbstractStrategy.class.getModifiers()));
    }

    @Test
    public void testImplementsStrategy() {
        assertTrue(Strategy.class.isAssignableFrom(AbstractStrategy.class));
    }

    @Test
    public void testCreateDirSuffixConstant() {
        assertEquals("_cdir", AbstractStrategy.CREATE_DIR_SUFFIX);
    }

    @Test
    public void testCreateDirPrefixConstant() {
        assertEquals("create_dir_", AbstractStrategy.CREATE_DIR_PREFIX);
    }

    @Test
    public void testHourGlassExtendsAbstractStrategy() {
        assertTrue(AbstractStrategy.class.isAssignableFrom(HourGlass.class));
    }

    @Test
    public void testMinimalExtendsAbstractStrategy() {
        assertTrue(AbstractStrategy.class.isAssignableFrom(Minimal.class));
    }
}
