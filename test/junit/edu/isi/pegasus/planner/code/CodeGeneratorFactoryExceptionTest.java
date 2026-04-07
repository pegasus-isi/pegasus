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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.util.FactoryException;
import org.junit.jupiter.api.Test;

/** Tests for CodeGeneratorFactoryException */
public class CodeGeneratorFactoryExceptionTest {

    @Test
    public void testDefaultNameConstant() {
        assertThat(CodeGeneratorFactoryException.DEFAULT_NAME, is("Code Generator"));
    }

    @Test
    public void testSingleMessageConstructorSetsDefaultClassname() {
        CodeGeneratorFactoryException e = new CodeGeneratorFactoryException("failed");
        assertThat(e.getMessage(), is("failed"));
        assertThat(e.getClassname(), is(CodeGeneratorFactoryException.DEFAULT_NAME));
    }

    @Test
    public void testMessageAndClassnameConstructor() {
        CodeGeneratorFactoryException e =
                new CodeGeneratorFactoryException("failed", "MyGenerator");
        assertThat(e.getMessage(), is("failed"));
        assertThat(e.getClassname(), is("MyGenerator"));
    }

    @Test
    public void testMessageAndCauseConstructorSetsDefaultClassname() {
        Throwable cause = new RuntimeException("root");
        CodeGeneratorFactoryException e = new CodeGeneratorFactoryException("failed", cause);
        assertThat(e.getMessage(), is("failed"));
        assertThat(e.getCause(), sameInstance(cause));
    }

    @Test
    public void testMessageClassnameAndCauseConstructor() {
        Throwable cause = new RuntimeException("root");
        CodeGeneratorFactoryException e =
                new CodeGeneratorFactoryException("failed", "MyGen", cause);
        assertThat(e.getMessage(), is("failed"));
        assertThat(e.getCause(), sameInstance(cause));
    }

    @Test
    public void testExtendsFactoryException() {
        assertThat(
                FactoryException.class.isAssignableFrom(CodeGeneratorFactoryException.class),
                is(true));
    }

    @Test
    public void testIsCatchableAsFactoryException() {
        FactoryException caught = null;
        try {
            throw new CodeGeneratorFactoryException("test");
        } catch (FactoryException ex) {
            caught = ex;
        }
        assertThat(caught, notNullValue());
        assertThat(caught.getMessage(), is("test"));
    }

    @Test
    public void testSingleMessageConstructorStoresDefaultClassname() {
        CodeGeneratorFactoryException e = new CodeGeneratorFactoryException("failed");

        assertThat(e.getClassname(), is(CodeGeneratorFactoryException.DEFAULT_NAME));
    }

    @Test
    public void testMessageAndClassnameConstructorStoresExplicitClassname() {
        CodeGeneratorFactoryException e =
                new CodeGeneratorFactoryException("failed", "MyGenerator");

        assertThat(e.getClassname(), is("MyGenerator"));
    }

    @Test
    public void testMessageAndCauseConstructorAllowsNullCause() {
        CodeGeneratorFactoryException e =
                new CodeGeneratorFactoryException("failed", (Throwable) null);

        assertThat(e.getMessage(), is("failed"));
        assertThat(e.getCause(), nullValue());
        assertThat(e.getClassname(), is(CodeGeneratorFactoryException.DEFAULT_NAME));
    }

    @Test
    public void testMessageClassnameAndCauseConstructorAllowsNullCause() {
        CodeGeneratorFactoryException e =
                new CodeGeneratorFactoryException("failed", "MyGen", null);

        assertThat(e.getMessage(), is("failed"));
        assertThat(e.getClassname(), is("MyGen"));
        assertThat(e.getCause(), nullValue());
    }

    @Test
    public void testMessageAndClassnameConstructorAllowsNullClassname() {
        CodeGeneratorFactoryException e =
                new CodeGeneratorFactoryException("failed", (String) null);

        assertThat(e.getMessage(), is("failed"));
        assertThat(e.getClassname(), nullValue());
    }
}
