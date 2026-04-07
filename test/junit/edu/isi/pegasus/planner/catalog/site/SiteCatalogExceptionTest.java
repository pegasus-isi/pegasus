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

import edu.isi.pegasus.planner.catalog.CatalogException;
import org.junit.jupiter.api.Test;

/** Tests for the SiteCatalogException class. */
public class SiteCatalogExceptionTest {

    @Test
    public void testExceptionExtendsCatalogException() {
        assertThat(CatalogException.class.isAssignableFrom(SiteCatalogException.class), is(true));
    }

    @Test
    public void testDefaultConstructor() {
        SiteCatalogException ex = new SiteCatalogException();
        assertThat(ex, is(notNullValue()));
        assertThat(ex.getMessage(), is(nullValue()));
        assertThat(ex.getNextException(), is(nullValue()));
    }

    @Test
    public void testConstructorWithMessage() {
        SiteCatalogException ex = new SiteCatalogException("test message");
        assertThat(ex, is(notNullValue()));
        assertThat(ex.getMessage(), is("test message"));
    }

    @Test
    public void testConstructorWithMessageAndCause() {
        Throwable cause = new RuntimeException("root cause");
        SiteCatalogException ex = new SiteCatalogException("test message", cause);
        assertThat(ex, is(notNullValue()));
        assertThat(ex.getMessage(), is("test message"));
        assertThat(ex.getCause(), is(cause));
    }

    @Test
    public void testConstructorWithCauseOnly() {
        Throwable cause = new RuntimeException("root cause");
        SiteCatalogException ex = new SiteCatalogException(cause);
        assertThat(ex, is(notNullValue()));
        assertThat(ex.getCause(), is(cause));
        assertThat(ex.getMessage(), is(cause.toString()));
    }

    @Test
    public void testExceptionCanBeThrown() {
        assertThrows(
                SiteCatalogException.class,
                () -> {
                    throw new SiteCatalogException("test");
                });
    }

    @Test
    public void testExceptionIsRuntimeException() {
        SiteCatalogException ex = new SiteCatalogException("test");

        assertThat(ex, is(org.hamcrest.Matchers.instanceOf(RuntimeException.class)));
    }

    @Test
    public void testConstructorWithNullCauseOnly() {
        SiteCatalogException ex = new SiteCatalogException((Throwable) null);

        assertThat(ex.getCause(), is(nullValue()));
        assertThat(ex.getMessage(), is(nullValue()));
    }

    @Test
    public void testSetNextExceptionAppendsToTail() {
        SiteCatalogException root = new SiteCatalogException("root");
        SiteCatalogException child1 = new SiteCatalogException("child1");
        SiteCatalogException child2 = new SiteCatalogException("child2");

        root.setNextException(child1);
        root.setNextException(child2);

        assertThat(root.getNextException(), is(child1));
        assertThat(child1.getNextException(), is(child2));
        assertThat(child2.getNextException(), is(nullValue()));
    }
}
