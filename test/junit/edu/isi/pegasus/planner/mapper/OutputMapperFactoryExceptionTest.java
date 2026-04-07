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
package edu.isi.pegasus.planner.mapper;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

import org.junit.jupiter.api.Test;

/** Tests for the OutputMapperFactoryException class. */
public class OutputMapperFactoryExceptionTest {

    @Test
    public void testExtendsFactoryException() {
        assertThat(
                edu.isi.pegasus.common.util.FactoryException.class.isAssignableFrom(
                        OutputMapperFactoryException.class),
                is(true));
    }

    @Test
    public void testDefaultNameConstant() {
        assertThat(OutputMapperFactoryException.DEFAULT_NAME, is("Output Mapper"));
    }

    @Test
    public void testConstructWithMessageSetsDefaultClassname() {
        OutputMapperFactoryException ex = new OutputMapperFactoryException("test message");
        assertThat(ex.getMessage(), is("test message"));
        assertThat(ex.getClassname(), is(OutputMapperFactoryException.DEFAULT_NAME));
    }

    @Test
    public void testConstructWithMessageAndClassname() {
        OutputMapperFactoryException ex =
                new OutputMapperFactoryException("test message", "MyClass");
        assertThat(ex.getMessage(), is("test message"));
        assertThat(ex.getClassname(), is("MyClass"));
    }

    @Test
    public void testConstructWithMessageAndCause() {
        RuntimeException cause = new RuntimeException("cause");
        OutputMapperFactoryException ex = new OutputMapperFactoryException("test message", cause);
        assertThat(ex.getMessage(), is("test message"));
        assertThat(ex.getCause(), is(sameInstance(cause)));
        assertThat(ex.getClassname(), is(OutputMapperFactoryException.DEFAULT_NAME));
    }

    @Test
    public void testConstructWithMessageClassnameAndCause() {
        RuntimeException cause = new RuntimeException("cause");
        OutputMapperFactoryException ex =
                new OutputMapperFactoryException("test message", "MyClass", cause);
        assertThat(ex.getMessage(), is("test message"));
        assertThat(ex.getClassname(), is("MyClass"));
        assertThat(ex.getCause(), is(sameInstance(cause)));
    }

    @Test
    public void testConstructWithMessageAndNullCauseKeepsDefaultClassname() {
        OutputMapperFactoryException ex =
                new OutputMapperFactoryException("test message", (Throwable) null);

        assertThat(ex.getMessage(), is("test message"));
        assertThat(ex.getCause(), is(nullValue()));
        assertThat(ex.getClassname(), is(OutputMapperFactoryException.DEFAULT_NAME));
    }

    @Test
    public void testConstructWithMessageAndNullClassnamePreservesNull() {
        OutputMapperFactoryException ex =
                new OutputMapperFactoryException("test message", (String) null);

        assertThat(ex.getMessage(), is("test message"));
        assertThat(ex.getClassname(), is(nullValue()));
    }

    @Test
    public void testConstructWithMessageNullClassnameAndNullCausePreservesNull() {
        OutputMapperFactoryException ex =
                new OutputMapperFactoryException("test message", null, null);

        assertThat(ex.getMessage(), is("test message"));
        assertThat(ex.getClassname(), is(nullValue()));
        assertThat(ex.getCause(), is(nullValue()));
    }
}
