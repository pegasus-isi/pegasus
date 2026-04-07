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

/** Tests for the SubmitMapperFactoryException class. */
public class SubmitMapperFactoryExceptionTest {

    @Test
    public void testExtendsFactoryException() {
        assertThat(
                edu.isi.pegasus.common.util.FactoryException.class.isAssignableFrom(
                        SubmitMapperFactoryException.class),
                is(true));
    }

    @Test
    public void testDefaultNameConstant() {
        assertThat(SubmitMapperFactoryException.DEFAULT_NAME, is("Directory Creator"));
    }

    @Test
    public void testConstructWithMessageSetsDefaultClassname() {
        SubmitMapperFactoryException ex = new SubmitMapperFactoryException("test message");
        assertThat(ex.getMessage(), is("test message"));
        assertThat(ex.getClassname(), is(SubmitMapperFactoryException.DEFAULT_NAME));
    }

    @Test
    public void testConstructWithMessageAndClassname() {
        SubmitMapperFactoryException ex =
                new SubmitMapperFactoryException("test message", "MyClass");
        assertThat(ex.getMessage(), is("test message"));
        assertThat(ex.getClassname(), is("MyClass"));
    }

    @Test
    public void testConstructWithMessageAndCause() {
        RuntimeException cause = new RuntimeException("cause");
        SubmitMapperFactoryException ex = new SubmitMapperFactoryException("test message", cause);
        assertThat(ex.getMessage(), is("test message"));
        assertThat(ex.getCause(), is(sameInstance(cause)));
        assertThat(ex.getClassname(), is(SubmitMapperFactoryException.DEFAULT_NAME));
    }

    @Test
    public void testConstructWithMessageClassnameAndCause() {
        RuntimeException cause = new RuntimeException("cause");
        SubmitMapperFactoryException ex =
                new SubmitMapperFactoryException("test message", "MyClass", cause);
        assertThat(ex.getMessage(), is("test message"));
        assertThat(ex.getClassname(), is("MyClass"));
        assertThat(ex.getCause(), is(sameInstance(cause)));
    }

    @Test
    public void testConstructWithMessageAndNullCauseKeepsDefaultClassname() {
        SubmitMapperFactoryException ex =
                new SubmitMapperFactoryException("test message", (Throwable) null);

        assertThat(ex.getMessage(), is("test message"));
        assertThat(ex.getCause(), is(nullValue()));
        assertThat(ex.getClassname(), is(SubmitMapperFactoryException.DEFAULT_NAME));
    }

    @Test
    public void testConstructWithMessageAndNullClassnamePreservesNull() {
        SubmitMapperFactoryException ex =
                new SubmitMapperFactoryException("test message", (String) null);

        assertThat(ex.getMessage(), is("test message"));
        assertThat(ex.getClassname(), is(nullValue()));
    }

    @Test
    public void testConstructWithMessageNullClassnameAndNullCausePreservesNull() {
        SubmitMapperFactoryException ex =
                new SubmitMapperFactoryException("test message", null, null);

        assertThat(ex.getMessage(), is("test message"));
        assertThat(ex.getClassname(), is(nullValue()));
        assertThat(ex.getCause(), is(nullValue()));
    }
}
