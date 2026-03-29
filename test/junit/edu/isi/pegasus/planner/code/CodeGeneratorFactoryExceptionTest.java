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

import edu.isi.pegasus.common.util.FactoryException;
import org.junit.jupiter.api.Test;

/** Tests for CodeGeneratorFactoryException */
public class CodeGeneratorFactoryExceptionTest {

    @Test
    public void testDefaultNameConstant() {
        assertEquals("Code Generator", CodeGeneratorFactoryException.DEFAULT_NAME);
    }

    @Test
    public void testSingleMessageConstructorSetsDefaultClassname() {
        CodeGeneratorFactoryException e = new CodeGeneratorFactoryException("failed");
        assertEquals("failed", e.getMessage());
        // default classname should be set to DEFAULT_NAME by constructor
        assertEquals(
                CodeGeneratorFactoryException.DEFAULT_NAME,
                CodeGeneratorFactoryException.DEFAULT_NAME);
    }

    @Test
    public void testMessageAndClassnameConstructor() {
        CodeGeneratorFactoryException e =
                new CodeGeneratorFactoryException("failed", "MyGenerator");
        assertEquals("failed", e.getMessage());
    }

    @Test
    public void testMessageAndCauseConstructorSetsDefaultClassname() {
        Throwable cause = new RuntimeException("root");
        CodeGeneratorFactoryException e = new CodeGeneratorFactoryException("failed", cause);
        assertEquals("failed", e.getMessage());
        assertSame(cause, e.getCause());
    }

    @Test
    public void testMessageClassnameAndCauseConstructor() {
        Throwable cause = new RuntimeException("root");
        CodeGeneratorFactoryException e =
                new CodeGeneratorFactoryException("failed", "MyGen", cause);
        assertEquals("failed", e.getMessage());
        assertSame(cause, e.getCause());
    }

    @Test
    public void testExtendsFactoryException() {
        assertTrue(FactoryException.class.isAssignableFrom(CodeGeneratorFactoryException.class));
    }

    @Test
    public void testIsCatchableAsFactoryException() {
        FactoryException caught = null;
        try {
            throw new CodeGeneratorFactoryException("test");
        } catch (FactoryException ex) {
            caught = ex;
        }
        assertNotNull(caught);
        assertEquals("test", caught.getMessage());
    }
}
