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
package edu.isi.pegasus.planner.selector.replica;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import org.junit.jupiter.api.Test;

/** Tests for the ReplicaSelectorFactoryException. */
public class ReplicaSelectorFactoryExceptionTest {

    @Test
    public void testDefaultName() {
        assertThat(
                "Replica Selector",
                ReplicaSelectorFactoryException.DEFAULT_NAME,
                equalTo("Replica Selector"));
    }

    @Test
    public void testConstructorWithMessage() {
        ReplicaSelectorFactoryException ex = new ReplicaSelectorFactoryException("test error");
        assertThat(ex.getMessage(), equalTo("test error"));
    }

    @Test
    public void testConstructorWithMessageAndClassname() {
        ReplicaSelectorFactoryException ex =
                new ReplicaSelectorFactoryException("test error", "TestClass");
        assertThat(ex.getMessage(), notNullValue());
        assertThat(ex.getClassname(), equalTo("TestClass"));
    }

    @Test
    public void testConstructorWithMessageAndCause() {
        Throwable cause = new RuntimeException("root cause");
        ReplicaSelectorFactoryException ex =
                new ReplicaSelectorFactoryException("test error", cause);
        assertThat(ex.getCause(), sameInstance(cause));
        assertThat(ex.getClassname(), equalTo(ReplicaSelectorFactoryException.DEFAULT_NAME));
    }

    @Test
    public void testConstructorWithMessageClassnameAndCause() {
        Throwable cause = new RuntimeException("root cause");
        ReplicaSelectorFactoryException ex =
                new ReplicaSelectorFactoryException("test error", "TestClass", cause);
        assertThat(ex.getCause(), sameInstance(cause));
        assertThat(ex.getClassname(), equalTo("TestClass"));
    }

    @Test
    public void testDefaultClassname() {
        ReplicaSelectorFactoryException ex = new ReplicaSelectorFactoryException("test");
        assertThat(ex.getClassname(), equalTo(ReplicaSelectorFactoryException.DEFAULT_NAME));
    }

    @Test
    public void testIsFactoryException() {
        ReplicaSelectorFactoryException ex = new ReplicaSelectorFactoryException("test");
        assertThat(ex, instanceOf(edu.isi.pegasus.common.util.FactoryException.class));
    }

    @Test
    public void testConstructorWithMessageAndNullCauseUsesDefaultClassname() {
        ReplicaSelectorFactoryException ex =
                new ReplicaSelectorFactoryException("test error", (Throwable) null);
        assertThat(ex.getCause(), nullValue());
        assertThat(ex.getClassname(), equalTo(ReplicaSelectorFactoryException.DEFAULT_NAME));
    }

    @Test
    public void testConstructorWithMessageAndNullClassnamePreservesNull() {
        ReplicaSelectorFactoryException ex =
                new ReplicaSelectorFactoryException("test error", (String) null);
        assertThat(ex.getClassname(), nullValue());
    }

    @Test
    public void testConstructorWithNullClassnameAndNullCausePreservesNullClassname() {
        ReplicaSelectorFactoryException ex =
                new ReplicaSelectorFactoryException("test error", null, null);
        assertThat(ex.getCause(), nullValue());
        assertThat(ex.getClassname(), nullValue());
    }
}
