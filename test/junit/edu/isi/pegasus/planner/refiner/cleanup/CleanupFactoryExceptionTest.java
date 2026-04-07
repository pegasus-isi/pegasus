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
package edu.isi.pegasus.planner.refiner.cleanup;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.util.FactoryException;
import org.junit.jupiter.api.Test;

/** Tests for CleanupFactoryException. */
public class CleanupFactoryExceptionTest {

    @Test
    public void testExtendsFactoryException() {
        assertThat(
                FactoryException.class.isAssignableFrom(CleanupFactoryException.class), is(true));
    }

    @Test
    public void testDefaultNameConstant() {
        assertThat(CleanupFactoryException.DEFAULT_NAME, is("File Cleanup"));
    }

    @Test
    public void testConstructorWithMessage() {
        CleanupFactoryException ex = new CleanupFactoryException("test error");
        assertThat(ex.getMessage(), is("test error"));
    }

    @Test
    public void testConstructorWithMessageSetsDefaultClassname() {
        CleanupFactoryException ex = new CleanupFactoryException("test error");
        assertThat(ex.getClassname(), is("File Cleanup"));
    }

    @Test
    public void testConstructorWithMessageAndClassname() {
        CleanupFactoryException ex = new CleanupFactoryException("error", "InPlace");
        assertThat(ex.getClassname(), is("InPlace"));
    }

    @Test
    public void testConstructorWithMessageAndCause() {
        Throwable cause = new RuntimeException("root cause");
        CleanupFactoryException ex = new CleanupFactoryException("error", cause);
        assertThat(ex.getCause(), sameInstance(cause));
    }

    @Test
    public void testConstructorWithMessageAndNullCauseUsesDefaultClassname() {
        CleanupFactoryException ex = new CleanupFactoryException("error", (Throwable) null);
        assertThat(ex.getClassname(), is("File Cleanup"));
        assertThat(ex.getCause(), nullValue());
    }

    @Test
    public void testConstructorWithMessageAndNullClassnamePreservesNull() {
        CleanupFactoryException ex = new CleanupFactoryException("error", (String) null);
        assertThat(ex.getClassname(), nullValue());
    }

    @Test
    public void testConstructorWithMessageNullClassnameAndNullCausePreservesNull() {
        CleanupFactoryException ex = new CleanupFactoryException("error", null, null);
        assertThat(ex.getClassname(), nullValue());
        assertThat(ex.getCause(), nullValue());
    }
}
