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

/** Tests for CodeGeneratorException */
public class CodeGeneratorExceptionTest {

    @Test
    public void testDefaultConstructor() {
        CodeGeneratorException e = new CodeGeneratorException();
        assertNotNull(e);
        assertNull(e.getMessage());
        assertNull(e.getCause());
    }

    @Test
    public void testMessageConstructor() {
        String msg = "code generation failed";
        CodeGeneratorException e = new CodeGeneratorException(msg);
        assertEquals(msg, e.getMessage());
        assertNull(e.getCause());
    }

    @Test
    public void testMessageAndCauseConstructor() {
        String msg = "code generation failed with cause";
        Throwable cause = new RuntimeException("root cause");
        CodeGeneratorException e = new CodeGeneratorException(msg, cause);
        assertEquals(msg, e.getMessage());
        assertSame(cause, e.getCause());
    }

    @Test
    public void testCauseOnlyConstructor() {
        Throwable cause = new IllegalArgumentException("bad arg");
        CodeGeneratorException e = new CodeGeneratorException(cause);
        assertSame(cause, e.getCause());
    }

    @Test
    public void testIsCheckedException() {
        assertTrue(Exception.class.isAssignableFrom(CodeGeneratorException.class));
    }

    @Test
    public void testExceptionIsThrowableAndCatchable() {
        String msg = "test throw";
        Exception caught = null;
        try {
            throw new CodeGeneratorException(msg);
        } catch (CodeGeneratorException ex) {
            caught = ex;
        }
        assertNotNull(caught);
        assertEquals(msg, caught.getMessage());
    }
}
