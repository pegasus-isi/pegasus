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
package edu.isi.pegasus.planner.code.generator.condor;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import edu.isi.pegasus.common.util.FactoryException;
import org.junit.jupiter.api.Test;

/** Tests for the CondorStyleFactoryException class. */
public class CondorStyleFactoryExceptionTest {

    @Test
    public void testExceptionExtendsFactoryException() {
        assertThat(
                FactoryException.class.isAssignableFrom(CondorStyleFactoryException.class),
                is(true));
    }

    @Test
    public void testDefaultNameConstant() {
        assertThat(CondorStyleFactoryException.DEFAULT_NAME, is("Code Generator"));
    }

    @Test
    public void testConstructorWithMessageSetsDefaultName() {
        CondorStyleFactoryException ex = new CondorStyleFactoryException("test message");
        assertThat(ex, notNullValue());
        assertThat(ex.getMessage(), is("test message"));
    }

    @Test
    public void testConstructorWithMessageAndClassname() {
        CondorStyleFactoryException ex = new CondorStyleFactoryException("test message", "MyClass");
        assertThat(ex, notNullValue());
        assertThat(ex.getMessage(), is("test message"));
    }

    @Test
    public void testConstructorWithMessageAndCause() {
        Throwable cause = new RuntimeException("root cause");
        CondorStyleFactoryException ex = new CondorStyleFactoryException("test message", cause);
        assertThat(ex, notNullValue());
        assertThat(ex.getCause(), is(cause));
    }

    @Test
    public void testConstructorWithMessageClassnameAndCause() {
        Throwable cause = new RuntimeException("root cause");
        CondorStyleFactoryException ex =
                new CondorStyleFactoryException("test message", "MyClass", cause);
        assertThat(ex, notNullValue());
        assertThat(ex.getCause(), is(cause));
    }

    @Test
    public void testConstructorWithMessageSetsFactoryDefaultClassname() {
        CondorStyleFactoryException ex = new CondorStyleFactoryException("test message");

        assertThat(ex.getClassname(), is(CondorStyleFactoryException.DEFAULT_NAME));
    }

    @Test
    public void testConstructorWithMessageAndClassnamePreservesExplicitClassname() {
        CondorStyleFactoryException ex = new CondorStyleFactoryException("test message", "MyClass");

        assertThat(ex.getClassname(), is("MyClass"));
        assertThat(ex.getCause(), is((Throwable) null));
    }

    @Test
    public void testConstructorWithMessageAndNullCauseSetsDefaultClassname() {
        CondorStyleFactoryException ex =
                new CondorStyleFactoryException("test message", (Throwable) null);

        assertThat(ex.getMessage(), is("test message"));
        assertThat(ex.getCause(), is((Throwable) null));
        assertThat(ex.getClassname(), is(CondorStyleFactoryException.DEFAULT_NAME));
    }

    @Test
    public void testConstructorWithMessageClassnameAndNullCausePreservesClassname() {
        CondorStyleFactoryException ex =
                new CondorStyleFactoryException("test message", "MyClass", null);

        assertThat(ex.getMessage(), is("test message"));
        assertThat(ex.getCause(), is((Throwable) null));
        assertThat(ex.getClassname(), is("MyClass"));
    }
}
