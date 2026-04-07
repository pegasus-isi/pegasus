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
package edu.isi.pegasus.planner.estimate;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** Tests for the EstimatorFactoryException. */
public class EstimatorFactoryExceptionTest {

    @Test
    public void testDefaultName() {
        assertThat(EstimatorFactoryException.DEFAULT_NAME, is("Estimator"));
    }

    @Test
    public void testConstructorWithMessage() {
        EstimatorFactoryException ex = new EstimatorFactoryException("test error");
        assertThat(ex.getMessage(), is("test error"));
    }

    @Test
    public void testConstructorWithMessageAndClassname() {
        EstimatorFactoryException ex = new EstimatorFactoryException("test error", "TestEstimator");
        assertThat(ex.getMessage(), notNullValue());
        assertThat(ex.getClassname(), is("TestEstimator"));
    }

    @Test
    public void testConstructorWithMessageAndCause() {
        Throwable cause = new RuntimeException("root cause");
        EstimatorFactoryException ex = new EstimatorFactoryException("test error", cause);
        assertThat(ex.getCause(), is(cause));
        assertThat(ex.getClassname(), is(EstimatorFactoryException.DEFAULT_NAME));
    }

    @Test
    public void testConstructorWithAllParams() {
        Throwable cause = new RuntimeException("root cause");
        EstimatorFactoryException ex =
                new EstimatorFactoryException("test error", "TestClass", cause);
        assertThat(ex.getClassname(), is("TestClass"));
        assertThat(ex.getCause(), is(cause));
    }

    @Test
    public void testIsFactoryException() {
        EstimatorFactoryException ex = new EstimatorFactoryException("test");
        assertThat(ex, instanceOf(edu.isi.pegasus.common.util.FactoryException.class));
    }

    @Test
    public void testDefaultClassnameSet() {
        EstimatorFactoryException ex = new EstimatorFactoryException("test");
        assertThat(ex.getClassname(), is(EstimatorFactoryException.DEFAULT_NAME));
    }

    @Test
    public void testConstructorWithMessagePreservesDefaultClassname() {
        EstimatorFactoryException ex = new EstimatorFactoryException("failure");

        assertThat(ex.getClassname(), is(EstimatorFactoryException.DEFAULT_NAME));
    }

    @Test
    public void testConstructorWithMessageAndNullCauseUsesDefaultClassname() {
        EstimatorFactoryException ex = new EstimatorFactoryException("failure", (Throwable) null);

        assertThat(ex.getCause(), nullValue());
        assertThat(ex.getClassname(), is(EstimatorFactoryException.DEFAULT_NAME));
    }

    @Test
    public void testConstructorWithExplicitClassnameAndNullCausePreservesClassname() {
        EstimatorFactoryException ex =
                new EstimatorFactoryException("failure", "CustomEstimator", null);

        assertThat(ex.getCause(), nullValue());
        assertThat(ex.getClassname(), is("CustomEstimator"));
    }

    @Test
    public void testConstructorWithMessageAndNullClassnamePreservesNull() {
        EstimatorFactoryException ex = new EstimatorFactoryException("failure", (String) null);

        assertThat(ex.getClassname(), nullValue());
    }
}
