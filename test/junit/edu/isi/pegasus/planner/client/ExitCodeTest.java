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
import org.griphyn.vdl.toolkit.Toolkit;
import org.junit.jupiter.api.Test;

/** Structural tests for the ExitCode client class via reflection. */
public class ExitCodeTest {

    @Test
    public void testExitCodeIsConcreteClass() {
        assertFalse(
                Modifier.isAbstract(ExitCode.class.getModifiers()),
                "ExitCode should be a concrete class");
    }

    @Test
    public void testExitCodeExtendsToolkit() {
        assertTrue(
                Toolkit.class.isAssignableFrom(ExitCode.class), "ExitCode should extend Toolkit");
    }

    @Test
    public void testUsage1ConstantNotNull() {
        assertNotNull(ExitCode.m_usage1, "ExitCode.m_usage1 should not be null");
    }

    @Test
    public void testUsage1ConstantNotEmpty() {
        assertFalse(ExitCode.m_usage1.isEmpty(), "ExitCode.m_usage1 should not be empty");
    }

    @Test
    public void testExitCodeCanBeInstantiated() {
        ExitCode ec = new ExitCode("test-exitcode");
        assertNotNull(ec, "ExitCode should be instantiable with an application name");
    }

    @Test
    public void testHasShowUsageMethod() throws NoSuchMethodException {
        Method showUsage = ExitCode.class.getMethod("showUsage");
        assertNotNull(showUsage, "ExitCode should have a showUsage() method");
    }

    @Test
    public void testHasGenerateValidOptionsMethod() throws NoSuchMethodException {
        // generateValidOptions() is protected, not public — use getDeclaredMethod
        Method genOpts = ExitCode.class.getDeclaredMethod("generateValidOptions");
        assertNotNull(genOpts, "ExitCode should have a generateValidOptions() method");
    }

    @Test
    public void testGenerateValidOptionsReturns11Elements() {
        ExitCode ec = new ExitCode("test");
        gnu.getopt.LongOpt[] opts = ec.generateValidOptions();
        assertEquals(11, opts.length, "generateValidOptions should return 11 options");
    }
}
