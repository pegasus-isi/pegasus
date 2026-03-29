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
package edu.isi.pegasus.planner.code.generator;

import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.code.CodeGenerator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for the Abstract code generator class. */
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
    public void testAbstractIsAbstractClass() {
        assertTrue(java.lang.reflect.Modifier.isAbstract(Abstract.class.getModifiers()));
    }

    @Test
    public void testAbstractImplementsCodeGenerator() {
        assertTrue(CodeGenerator.class.isAssignableFrom(Abstract.class));
    }

    @Test
    public void testPostscriptLogSuffixConstant() {
        assertNotNull(Abstract.POSTSCRIPT_LOG_SUFFIX);
        assertEquals(".exitcode.log", Abstract.POSTSCRIPT_LOG_SUFFIX);
    }

    @Test
    public void testPBSExtendsAbstract() {
        assertTrue(Abstract.class.isAssignableFrom(PBS.class));
    }

    @Test
    public void testPMCExtendsAbstract() {
        assertTrue(Abstract.class.isAssignableFrom(PMC.class));
    }

    @Test
    public void testShellExtendsAbstract() {
        assertTrue(Abstract.class.isAssignableFrom(Shell.class));
    }
}
