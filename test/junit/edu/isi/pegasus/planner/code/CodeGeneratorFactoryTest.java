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
package edu.isi.pegasus.planner.code;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** Tests for CodeGeneratorFactory constants and structure */
public class CodeGeneratorFactoryTest {

    @Test
    public void testDefaultPackageName() {
        assertEquals(
                "edu.isi.pegasus.planner.code.generator",
                CodeGeneratorFactory.DEFAULT_PACKAGE_NAME);
    }

    @Test
    public void testCondorCodeGeneratorClassName() {
        assertEquals(
                "edu.isi.pegasus.planner.code.generator.condor.CondorGenerator",
                CodeGeneratorFactory.CONDOR_CODE_GENERATOR_CLASS);
    }

    @Test
    public void testStampedeEventGeneratorClassName() {
        assertEquals(
                "edu.isi.pegasus.planner.code.generator.Stampede",
                CodeGeneratorFactory.STAMPEDE_EVENT_GENERATOR_CLASS);
    }

    @Test
    public void testCondorGeneratorClassIsLoadable() throws ClassNotFoundException {
        // Verify the condor generator class actually exists on the classpath
        Class<?> clazz = Class.forName(CodeGeneratorFactory.CONDOR_CODE_GENERATOR_CLASS);
        assertNotNull(clazz);
    }

    @Test
    public void testStampedeGeneratorClassIsLoadable() throws ClassNotFoundException {
        Class<?> clazz = Class.forName(CodeGeneratorFactory.STAMPEDE_EVENT_GENERATOR_CLASS);
        assertNotNull(clazz);
    }

    @Test
    public void testLoadInstanceThrowsWhenNullBag() {
        assertThrows(NullPointerException.class, () -> CodeGeneratorFactory.loadInstance(null));
    }
}
