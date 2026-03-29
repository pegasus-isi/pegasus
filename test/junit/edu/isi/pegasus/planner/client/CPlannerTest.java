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
package edu.isi.pegasus.planner.client;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import org.junit.jupiter.api.Test;

/**
 * Structural tests for the CPlanner client class via reflection.
 *
 * @author Rajiv Mayani
 */
public class CPlannerTest {

    @Test
    public void testCPlannerIsConcreteClass() {
        assertFalse(
                Modifier.isAbstract(CPlanner.class.getModifiers()),
                "CPlanner should be a concrete class");
    }

    @Test
    public void testCPlannerIsNotInterface() {
        assertFalse(CPlanner.class.isInterface(), "CPlanner should not be an interface");
    }

    @Test
    public void testCPlannerExtendsExecutable() {
        Class<?> superClass = CPlanner.class.getSuperclass();
        assertNotNull(superClass, "CPlanner should have a superclass");
        assertEquals(Executable.class, superClass, "CPlanner should extend Executable");
    }

    @Test
    public void testCPlannerHasMainMethod() throws NoSuchMethodException {
        Method main = CPlanner.class.getMethod("main", String[].class);
        assertNotNull(main, "CPlanner should have a main(String[]) method");
        assertTrue(Modifier.isStatic(main.getModifiers()), "main should be static");
        assertTrue(Modifier.isPublic(main.getModifiers()), "main should be public");
    }

    @Test
    public void testCPlannerHasGenerateValidOptionsMethod() throws NoSuchMethodException {
        Method genOpts = CPlanner.class.getMethod("generateValidOptions");
        assertNotNull(genOpts, "CPlanner should have a generateValidOptions() method");
    }

    @Test
    public void testCPlannerCanBeInstantiatedWithNoArgConstructor() {
        // CPlanner(LogManager) constructor - pass null for no-dependency instantiation
        CPlanner planner = new CPlanner();
        assertNotNull(planner, "CPlanner should be instantiable with no-arg constructor");
    }

    @Test
    public void testCPlannerGeneratesOptions() {
        CPlanner planner = new CPlanner();
        gnu.getopt.LongOpt[] opts = planner.generateValidOptions();
        assertNotNull(opts, "generateValidOptions should not return null");
        assertTrue(opts.length > 0, "generateValidOptions should return at least one option");
    }
}
