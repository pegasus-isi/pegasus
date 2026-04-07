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
package edu.isi.pegasus.planner.catalog.transformation;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.util.FactoryException;
import org.junit.jupiter.api.Test;

/** Tests for TransformationFactoryException. */
public class TransformationFactoryExceptionTest {

    @Test
    public void testDefaultNameConstantValue() {
        assertThat(TransformationFactoryException.DEFAULT_NAME, equalTo("Transformation Catalog"));
    }

    @Test
    public void testSingleArgConstructorSetsMessage() {
        TransformationFactoryException ex =
                new TransformationFactoryException("catalog load failed");
        assertThat(ex.getMessage(), equalTo("catalog load failed"));
    }

    @Test
    public void testSingleArgConstructorSetsDefaultClassname() {
        TransformationFactoryException ex =
                new TransformationFactoryException("catalog load failed");
        assertThat(ex.getClassname(), equalTo(TransformationFactoryException.DEFAULT_NAME));
    }

    @Test
    public void testTwoArgConstructorSetsMessageAndClassname() {
        TransformationFactoryException ex =
                new TransformationFactoryException("load failed", "MyTCClass");
        assertThat(ex.getMessage(), equalTo("load failed"));
        assertThat(ex.getClassname(), equalTo("MyTCClass"));
    }

    @Test
    public void testConstructorWithCauseSetsDefaultClassname() {
        RuntimeException cause = new RuntimeException("root cause");
        TransformationFactoryException ex = new TransformationFactoryException("outer", cause);
        assertThat(ex.getClassname(), equalTo(TransformationFactoryException.DEFAULT_NAME));
        assertThat(ex.getCause(), is(sameInstance(cause)));
    }

    @Test
    public void testConstructorWithClassnameAndCause() {
        RuntimeException cause = new RuntimeException("root");
        TransformationFactoryException ex =
                new TransformationFactoryException("message", "SomeClass", cause);
        assertThat(ex.getMessage(), equalTo("message"));
        assertThat(ex.getClassname(), equalTo("SomeClass"));
        assertThat(ex.getCause(), is(sameInstance(cause)));
    }

    @Test
    public void testIsRuntimeException() {
        TransformationFactoryException ex = new TransformationFactoryException("test");
        assertThat(ex instanceof RuntimeException, is(true));
    }

    @Test
    public void testIsFactoryException() {
        TransformationFactoryException ex = new TransformationFactoryException("test");
        assertThat(ex instanceof FactoryException, is(true));
    }

    @Test
    public void testConstructorWithNullCausePreservesNullCause() {
        TransformationFactoryException ex =
                new TransformationFactoryException("message", (Throwable) null);

        assertThat(ex.getMessage(), equalTo("message"));
        assertThat(ex.getClassname(), equalTo(TransformationFactoryException.DEFAULT_NAME));
        assertThat(ex.getCause(), is(nullValue()));
    }

    @Test
    public void testConstructorWithClassnameAndNullCausePreservesClassname() {
        TransformationFactoryException ex =
                new TransformationFactoryException("message", "SomeClass", null);

        assertThat(ex.getClassname(), equalTo("SomeClass"));
        assertThat(ex.getCause(), is(nullValue()));
    }

    @Test
    public void testConvertExceptionReturnsNonNullString() {
        TransformationFactoryException ex =
                new TransformationFactoryException("catalog load failed");
        String result = ex.convertException();
        assertThat(result, is(notNullValue()));
        assertThat(result.isEmpty(), is(false));
    }

    @Test
    public void testConvertExceptionContainsMessage() {
        TransformationFactoryException ex =
                new TransformationFactoryException("unique-error-message");
        String result = ex.convertException();
        assertThat(result, containsString("unique-error-message"));
    }

    @Test
    public void testConvertExceptionWithIndexStartsCountingFromProvidedIndex() {
        TransformationFactoryException ex =
                new TransformationFactoryException("catalog load failed");

        String result = ex.convertException(5);

        assertThat(result, containsString("[6]"));
    }
}
