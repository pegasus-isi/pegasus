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

/** Tests for the MapperException class. */
public class MapperExceptionTest {

    @Test
    public void testExtendsRuntimeException() {
        assertThat(RuntimeException.class.isAssignableFrom(MapperException.class), is(true));
    }

    @Test
    public void testDefaultConstructor() {
        MapperException ex = new MapperException();
        assertThat(ex, is(org.hamcrest.Matchers.notNullValue()));
        assertThat(ex.getMessage(), is(nullValue()));
    }

    @Test
    public void testConstructWithMessage() {
        MapperException ex = new MapperException("test error");
        assertThat(ex.getMessage(), is("test error"));
    }

    @Test
    public void testConstructWithMessageAndCause() {
        RuntimeException cause = new RuntimeException("cause");
        MapperException ex = new MapperException("test error", cause);
        assertThat(ex.getMessage(), is("test error"));
        assertThat(ex.getCause(), is(sameInstance(cause)));
    }

    @Test
    public void testConstructWithCause() {
        RuntimeException cause = new RuntimeException("cause");
        MapperException ex = new MapperException(cause);
        assertThat(ex.getCause(), is(sameInstance(cause)));
    }

    @Test
    public void testCanBeCaught() {
        boolean caught = false;
        try {
            throw new MapperException("mapper failed");
        } catch (MapperException e) {
            caught = true;
            assertThat(e.getMessage(), is("mapper failed"));
        }
        assertThat(caught, is(true));
    }

    @Test
    public void testCauseOnlyConstructorUsesCauseToStringAsMessage() {
        IllegalStateException cause = new IllegalStateException("bad state");

        MapperException ex = new MapperException(cause);

        assertThat(ex.getMessage(), is(cause.toString()));
        assertThat(ex.getCause(), is(sameInstance(cause)));
    }

    @Test
    public void testCauseOnlyConstructorAllowsNullCause() {
        MapperException ex = new MapperException((Throwable) null);

        assertThat(ex.getMessage(), is(nullValue()));
        assertThat(ex.getCause(), is(nullValue()));
    }

    @Test
    public void testMessageAndCauseConstructorAllowsNullCause() {
        MapperException ex = new MapperException("mapper failed", null);

        assertThat(ex.getMessage(), is("mapper failed"));
        assertThat(ex.getCause(), is(nullValue()));
    }
}
