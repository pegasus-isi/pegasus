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

/** Tests for CodeGenerator interface structure */
public class CodeGeneratorTest {

    @Test
    public void testVersionConstantExists() {
        assertEquals("1.5", CodeGenerator.VERSION);
    }

    @Test
    public void testInterfaceIsPublic() {
        assertTrue(java.lang.reflect.Modifier.isPublic(CodeGenerator.class.getModifiers()));
    }

    @Test
    public void testInterfaceHasInitializeMethod() throws NoSuchMethodException {
        // Verify initialize(PegasusBag) method is declared
        assertNotNull(
                CodeGenerator.class.getMethod(
                        "initialize", edu.isi.pegasus.planner.classes.PegasusBag.class));
    }

    @Test
    public void testInterfaceHasGenerateCodeMethod() throws NoSuchMethodException {
        assertNotNull(
                CodeGenerator.class.getMethod(
                        "generateCode", edu.isi.pegasus.planner.classes.ADag.class));
    }

    @Test
    public void testCondorGeneratorImplementsCodeGenerator() {
        assertTrue(
                CodeGenerator.class.isAssignableFrom(
                        edu.isi.pegasus.planner.code.generator.condor.CondorGenerator.class));
    }
}
