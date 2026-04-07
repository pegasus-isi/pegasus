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
package edu.isi.pegasus.planner.catalog.site;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.util.FactoryException;
import org.junit.jupiter.api.Test;

/** Tests for the SiteFactoryException class. */
public class SiteFactoryExceptionTest {

    @Test
    public void testExceptionExtendsFactoryException() {
        assertThat(FactoryException.class.isAssignableFrom(SiteFactoryException.class), is(true));
    }

    @Test
    public void testDefaultNameConstant() {
        assertThat(SiteFactoryException.DEFAULT_NAME, is("Site Catalog"));
    }

    @Test
    public void testConstructorWithMessage() {
        SiteFactoryException ex = new SiteFactoryException("test message");
        assertThat(ex, is(notNullValue()));
        assertThat(ex.getMessage(), is("test message"));
        assertThat(ex.getClassname(), is(SiteFactoryException.DEFAULT_NAME));
        assertThat(ex.getCause(), is(nullValue()));
    }

    @Test
    public void testConstructorWithMessageAndClassname() {
        SiteFactoryException ex = new SiteFactoryException("test message", "MyClass");
        assertThat(ex, is(notNullValue()));
        assertThat(ex.getMessage(), is("test message"));
        assertThat(ex.getClassname(), is("MyClass"));
        assertThat(ex.getCause(), is(nullValue()));
    }

    @Test
    public void testConstructorWithMessageAndCauseUsesDefaultClassname() {
        Throwable cause = new IllegalStateException("root cause");

        SiteFactoryException ex = new SiteFactoryException("test message", cause);

        assertThat(ex, is(notNullValue()));
        assertThat(ex.getMessage(), is("test message"));
        assertThat(ex.getCause(), is(cause));
        assertThat(ex.getClassname(), is(SiteFactoryException.DEFAULT_NAME));
    }

    @Test
    public void testConstructorWithMessageClassnameAndCause() {
        Throwable cause = new IllegalArgumentException("root cause");

        SiteFactoryException ex = new SiteFactoryException("test message", "MyClass", cause);

        assertThat(ex, is(notNullValue()));
        assertThat(ex.getMessage(), is("test message"));
        assertThat(ex.getCause(), is(cause));
        assertThat(ex.getClassname(), is("MyClass"));
    }

    @Test
    public void testConstructorWithNullCauseStillUsesDefaultClassname() {
        SiteFactoryException ex = new SiteFactoryException("test message", (Throwable) null);

        assertThat(ex, is(notNullValue()));
        assertThat(ex.getMessage(), is("test message"));
        assertThat(ex.getCause(), is(nullValue()));
        assertThat(ex.getClassname(), is(SiteFactoryException.DEFAULT_NAME));
    }

    @Test
    public void testExceptionIsRuntimeException() {
        assertThat(RuntimeException.class.isAssignableFrom(SiteFactoryException.class), is(true));
    }

    @Test
    public void testExceptionIsThrowable() {
        assertThat(Throwable.class.isAssignableFrom(SiteFactoryException.class), is(true));
    }

    @Test
    public void testExceptionCanBeThrown() {
        assertThrows(
                SiteFactoryException.class,
                () -> {
                    throw new SiteFactoryException("test");
                });
    }
}
