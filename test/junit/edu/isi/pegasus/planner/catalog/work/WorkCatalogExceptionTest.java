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
package edu.isi.pegasus.planner.catalog.work;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.catalog.CatalogException;
import org.junit.jupiter.api.Test;

/** Tests for the WorkCatalogException class. */
public class WorkCatalogExceptionTest {

    @Test
    public void testExceptionExtendsCatalogException() {
        assertThat(CatalogException.class.isAssignableFrom(WorkCatalogException.class), is(true));
    }

    @Test
    public void testDefaultConstructor() {
        WorkCatalogException ex = new WorkCatalogException();
        assertThat(ex, is(notNullValue()));
    }

    @Test
    public void testConstructorWithMessage() {
        WorkCatalogException ex = new WorkCatalogException("test message");
        assertThat(ex, is(notNullValue()));
        assertThat(ex.getMessage(), equalTo("test message"));
    }

    @Test
    public void testConstructorWithMessageAndCause() {
        Throwable cause = new RuntimeException("root cause");
        WorkCatalogException ex = new WorkCatalogException("test message", cause);
        assertThat(ex, is(notNullValue()));
        assertThat(ex.getMessage(), equalTo("test message"));
        assertThat(ex.getCause(), is(cause));
    }

    @Test
    public void testConstructorWithCauseOnly() {
        Throwable cause = new RuntimeException("root cause");
        WorkCatalogException ex = new WorkCatalogException(cause);
        assertThat(ex, is(notNullValue()));
        assertThat(ex.getCause(), is(cause));
    }

    @Test
    public void testDefaultConstructorHasNullMessageAndNextException() {
        WorkCatalogException ex = new WorkCatalogException();

        assertThat(ex.getMessage(), is(nullValue()));
        assertThat(ex.getNextException(), is(nullValue()));
    }

    @Test
    public void testConstructorWithCauseOnlyUsesCauseMessage() {
        RuntimeException cause = new RuntimeException("root cause");
        WorkCatalogException ex = new WorkCatalogException(cause);

        assertThat(ex.getMessage(), containsString("root cause"));
        assertThat(ex.getCause(), is(sameInstance(cause)));
    }

    @Test
    public void testConstructorWithNullCausePreservesNullCause() {
        WorkCatalogException ex = new WorkCatalogException((Throwable) null);

        assertThat(ex.getCause(), is(nullValue()));
        assertThat(ex.getMessage(), is(nullValue()));
    }

    @Test
    public void testSetNextExceptionAppendsToTailOfChain() {
        WorkCatalogException root = new WorkCatalogException("root");
        WorkCatalogException second = new WorkCatalogException("second");
        WorkCatalogException third = new WorkCatalogException("third");

        root.setNextException(second);
        root.setNextException(third);

        assertThat(root.getNextException(), is(sameInstance(second)));
        assertThat(root.getNextException().getNextException(), is(sameInstance(third)));
        assertThat(third.getNextException(), is(nullValue()));
    }

    @Test
    public void testExceptionCanBeThrown() {
        assertThrows(
                WorkCatalogException.class,
                () -> {
                    throw new WorkCatalogException("test");
                });
    }
}
