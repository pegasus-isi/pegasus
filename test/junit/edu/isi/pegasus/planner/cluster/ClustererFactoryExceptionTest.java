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
package edu.isi.pegasus.planner.cluster;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.util.FactoryException;
import org.junit.jupiter.api.Test;

/** Tests for the ClustererFactoryException class. */
public class ClustererFactoryExceptionTest {

    @Test
    public void testDefaultName() {
        assertThat(ClustererFactoryException.DEFAULT_NAME, is("Clusterer"));
    }

    @Test
    public void testConstructorWithMessage() {
        ClustererFactoryException ex = new ClustererFactoryException("test error");
        assertThat(ex.getMessage(), notNullValue());
        assertThat(ex.getClassname(), is(ClustererFactoryException.DEFAULT_NAME));
    }

    @Test
    public void testConstructorWithMessageAndClassname() {
        ClustererFactoryException ex = new ClustererFactoryException("test error", "MyClusterer");
        assertThat(ex.getClassname(), is("MyClusterer"));
    }

    @Test
    public void testConstructorWithMessageAndCause() {
        Throwable cause = new RuntimeException("root cause");
        ClustererFactoryException ex = new ClustererFactoryException("test error", cause);
        assertThat(ex.getCause(), is(cause));
        assertThat(ex.getClassname(), is(ClustererFactoryException.DEFAULT_NAME));
    }

    @Test
    public void testConstructorWithAllParams() {
        Throwable cause = new RuntimeException("root cause");
        ClustererFactoryException ex =
                new ClustererFactoryException("test error", "MyClusterer", cause);
        assertThat(ex.getClassname(), is("MyClusterer"));
        assertThat(ex.getCause(), is(cause));
    }

    @Test
    public void testExtendsFactoryException() {
        ClustererFactoryException ex = new ClustererFactoryException("test");
        assertThat(ex, instanceOf(FactoryException.class));
    }

    @Test
    public void testDefaultClassnameSet() {
        ClustererFactoryException ex = new ClustererFactoryException("test");
        assertThat(ex.getClassname(), is(ClustererFactoryException.DEFAULT_NAME));
    }

    @Test
    public void testMessageOnlyConstructorPreservesMessage() {
        ClustererFactoryException ex = new ClustererFactoryException("specific error");
        assertThat(ex.getMessage(), is("specific error"));
    }

    @Test
    public void testMessageAndCauseConstructorAllowsNullCause() {
        ClustererFactoryException ex =
                new ClustererFactoryException("test error", (Throwable) null);
        assertThat(ex.getMessage(), is("test error"));
        assertThat(ex.getCause(), nullValue());
        assertThat(ex.getClassname(), is(ClustererFactoryException.DEFAULT_NAME));
    }

    @Test
    public void testMessageClassnameAndCauseConstructorAllowsNullCause() {
        ClustererFactoryException ex =
                new ClustererFactoryException("test error", "NamedClusterer", null);
        assertThat(ex.getMessage(), is("test error"));
        assertThat(ex.getClassname(), is("NamedClusterer"));
        assertThat(ex.getCause(), nullValue());
    }

    @Test
    public void testMessageAndClassnameConstructorAllowsNullClassname() {
        ClustererFactoryException ex = new ClustererFactoryException("test error", (String) null);
        assertThat(ex.getMessage(), is("test error"));
        assertThat(ex.getClassname(), nullValue());
    }
}
