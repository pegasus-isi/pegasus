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
package edu.isi.pegasus.planner.cluster.aggregator;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.util.FactoryException;
import org.junit.jupiter.api.Test;

/** Tests for the JobAggregatorFactoryException class. */
public class JobAggregatorFactoryExceptionTest {

    @Test
    public void testExtendsFactoryException() {
        assertThat(
                FactoryException.class.isAssignableFrom(JobAggregatorFactoryException.class),
                is(true));
    }

    @Test
    public void testDefaultNameConstant() {
        assertThat(JobAggregatorFactoryException.DEFAULT_NAME, is("Job Aggregator"));
    }

    @Test
    public void testConstructWithMessageSetsDefaultClassname() {
        JobAggregatorFactoryException ex = new JobAggregatorFactoryException("test message");
        assertThat(ex.getMessage(), is("test message"));
        assertThat(ex.getClassname(), is(JobAggregatorFactoryException.DEFAULT_NAME));
    }

    @Test
    public void testConstructWithMessageAndClassname() {
        JobAggregatorFactoryException ex =
                new JobAggregatorFactoryException("test message", "MyClass");
        assertThat(ex.getMessage(), is("test message"));
        assertThat(ex.getClassname(), is("MyClass"));
    }

    @Test
    public void testConstructWithMessageAndCause() {
        RuntimeException cause = new RuntimeException("cause");
        JobAggregatorFactoryException ex = new JobAggregatorFactoryException("test message", cause);
        assertThat(ex.getMessage(), is("test message"));
        assertThat(ex.getCause(), is(cause));
        assertThat(ex.getClassname(), is(JobAggregatorFactoryException.DEFAULT_NAME));
    }

    @Test
    public void testConstructWithMessageClassnameAndCause() {
        RuntimeException cause = new RuntimeException("cause");
        JobAggregatorFactoryException ex =
                new JobAggregatorFactoryException("test message", "MyClass", cause);
        assertThat(ex.getMessage(), is("test message"));
        assertThat(ex.getClassname(), is("MyClass"));
        assertThat(ex.getCause(), is(cause));
    }

    @Test
    public void testIsRuntimeException() {
        assertThat(
                RuntimeException.class.isAssignableFrom(JobAggregatorFactoryException.class),
                is(true));
    }

    @Test
    public void testMessageAndCauseConstructorAllowsNullCause() {
        JobAggregatorFactoryException ex =
                new JobAggregatorFactoryException("test message", (Throwable) null);

        assertThat(ex.getMessage(), is("test message"));
        assertThat(ex.getCause(), nullValue());
        assertThat(ex.getClassname(), is(JobAggregatorFactoryException.DEFAULT_NAME));
    }

    @Test
    public void testMessageClassnameAndCauseConstructorAllowsNullCause() {
        JobAggregatorFactoryException ex =
                new JobAggregatorFactoryException("test message", "MyClass", null);

        assertThat(ex.getMessage(), is("test message"));
        assertThat(ex.getClassname(), is("MyClass"));
        assertThat(ex.getCause(), nullValue());
    }

    @Test
    public void testMessageAndClassnameConstructorAllowsNullClassname() {
        JobAggregatorFactoryException ex =
                new JobAggregatorFactoryException("test message", (String) null);

        assertThat(ex.getMessage(), is("test message"));
        assertThat(ex.getClassname(), nullValue());
    }
}
